// Copyright (C) 2025 Michael Herstine <sp1ff@pobox.com>
//
// This file is part of indielinks.
//
// indielinks is free software: you can redistribute it and/or modify it under the terms of the GNU
// General Public License as published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// indielinks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with mpdpopm.  If not,
// see <http://www.gnu.org/licenses/>.

//! # Integration tests related to indielinks and the ActivityPub protocol
//!
//! This will probably wind-up being re-factored, but I'm starting here.

use std::{collections::HashSet, sync::Arc, time::Duration};

use libtest_mimic::Failed;
use reqwest::{Client, Url};
use tracing::info;
use uuid::Uuid;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use indielinks::{
    actor::CollectionPage,
    ap_entities::{Accept, ActorField, Create, Jld, Like, Note, ObjectField},
    entities::{FollowId, Username},
    origin::Origin,
    peppers::{Pepper, Version as PepperVersion},
    users::FollowReq,
};

use crate::{Helper, PeerUser, make_signed_request, peer_actor};

pub async fn posting_creates_note(
    url: Url,
    pepper_version: PepperVersion,
    pepper_key: Pepper,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    // Clean-up this test user, just in case it's laying around from a prior, failed test.
    let username = Username::new("posting_creates_note_user").unwrap(/* known good */);
    helper.remove_user(&username).await?;

    // Alright: let's create a mock AP server with which indielinks can federate...
    let mock_server = MockServer::start().await;
    // along with a mock user on that peer:
    let mock_user = PeerUser::new()?;

    // For this test, we expect indielinks to post a `Create` activity to the shared inboxes of it's
    // peers that contain followers of the user as whom we'll post:
    Mock::given(method("POST"))
        .and(path("/inbox"))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&mock_server)
        .await;
    // Indielinks will also hit each follower's server to discover their shared inbox (I know-- very
    // inefficient), so let's handle that, as well.
    let mock_origin: Origin = mock_server.uri().try_into()?;
    peer_actor(&mock_user, &mock_origin)
        .await?
        .mount(&mock_server)
        .await;

    // OK-- now let's give ourselves a test indielinks user that follows `peer_user`:
    let api_key = helper
        .create_user(
            &pepper_version,
            &pepper_key,
            &username,
            // 16 bytes from `/dev/urandom` to ensure the password has enough entropy (else it will
            // be rejected 😛)
            &"0534e7529239fed032a49953ee6ba4d9".to_owned().into(),
            &HashSet::from([mock_user.id(&mock_origin)?.into()]),
            &HashSet::new(),
        )
        .await?;

    info!(
        "Created test user posting_creates_note_user with api_key: {}",
        api_key
    );

    // Alright! Let's create a post!
    let client = Client::builder()
        .user_agent("indielinks integration tests/0.0.1; +sp1ff@pobox.com")
        .build()?;

    client.get(format!("{}api/v1/posts/add?url=https://wsj.com&description=The%20Wall%20Street%20Journal&tags=news,daily,economy&shared=true&replace=true", url))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {}:{}",  username, api_key))
        .send()
        .await?;

    // So this is tricky-- I want to test that my mock server received the expected number of
    // requests, but indielinks will send them asynchronously. Let's wait a bit to be sure they
    // show-up. We expect two: a hit on the actor (to get the public key), and an `Accept` to the
    // test users's inbox (for the `Accept`).
    tokio::time::sleep(Duration::from_millis(250)).await;
    let mut num_requests = mock_server
        .received_requests()
        .await
        .map(|x| x.len())
        .unwrap_or(0);
    let mut n = 0;
    while num_requests < 2 && n < 8 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        num_requests = mock_server
            .received_requests()
            .await
            .map(|x| x.len())
            .unwrap_or(0);
        n += 1;
    }

    assert_eq!(num_requests, 2);
    let create = serde_json::from_slice::<Create>(
        mock_server
            .received_requests()
            .await
            .unwrap()
            .get(1)
            .unwrap()
            .body
            .as_slice(),
    );
    assert!(create.is_ok());

    // Now let's send a `Like` for this note
    let create = create.unwrap();
    info!("Received a {:#?}", create);
    let mut id = mock_user.id(&mock_origin)?;
    id.path_segments_mut()
            .unwrap(/* known good */)
            .extend(vec!["likes".to_string(), Uuid::new_v4().to_string()]);
    let like = Like::new(
        create.object_id()?,
        id,
        ActorField::Iri(mock_user.id(&mock_origin)?),
    );
    info!("Sending a {:#?}", like);

    let request = make_signed_request(
        axum::http::Method::POST,
        url.join(&format!("/users/{}/inbox", username))?,
        Jld::new(&like, None)?.to_string().into(),
        &mock_origin,
        mock_user.priv_key(),
        mock_user.name(),
    )
    .await?;

    let rsp = client.execute(request).await?;
    assert_eq!(rsp.status(), reqwest::StatusCode::CREATED);

    Ok(())
}

pub async fn send_follow(
    url: Url,
    pepper_version: PepperVersion,
    pepper_key: Pepper,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    // Clean-up this test user, just in case it's laying around from a prior, failed test.
    let username = Username::new("send_follow_user").unwrap(/* known good */);
    helper.remove_user(&username).await?;

    let id = Url::parse(&format!("{}/users/{}", url, username)).unwrap(/* known good */);

    // Alright: let's create a mock AP server with which indielinks can federate...
    let mock_server = MockServer::start().await;
    // along with a mock user on that peer:
    let mock_user = PeerUser::new()?;

    let mock_origin: Origin = mock_server.uri().try_into()?;

    // For this test, we expect indielinks to post a `Follow` activity to the mock user's personal inbox:
    Mock::given(method("POST"))
        .and(path(format!("/users/{}/inbox", mock_user.name())))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    // We also expect indielinks to retrieve the Actor (so as to resolve the inbox)
    peer_actor(&mock_user, &mock_origin)
        .await?
        .mount(&mock_server)
        .await;

    let api_key = helper
        .create_user(
            &pepper_version,
            &pepper_key,
            &username,
            // 16 bytes from `/dev/urandom` to ensure the password has enough entropy (else it will
            // be rejected 😛)
            &"0534e7529239fed032a49953ee6ba4d9".to_owned().into(),
            &HashSet::from([mock_user.id(&mock_origin)?.into()]),
            &HashSet::new(),
        )
        .await?;

    // Let's ask to send a follow:
    let client = Client::builder()
        .user_agent("indielinks integration tests/0.0.1; +sp1ff@pobox.com")
        .build()?;

    client
        .post(format!("{}api/v1/users/follow", url))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}:{}", username, api_key),
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::to_string(&FollowReq {
            id: mock_user.id(&mock_origin)?,
        }).unwrap(/* known good */),
        )
        .send()
        .await?;

    // So this is tricky-- I want to test that my mock server received the expected number of
    // requests, but indielinks will send them asynchronously. Let's wait a bit to be sure they
    // show-up. We expect just one: the Follow request.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let mut num_requests = mock_server
        .received_requests()
        .await
        .map(|x| x.len())
        .unwrap_or(0);
    let mut n = 0;
    while num_requests < 2 && n < 8 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        num_requests = mock_server
            .received_requests()
            .await
            .map(|x| x.len())
            .unwrap_or(0);
        n += 1;
    }

    assert_eq!(num_requests, 2);

    // Next, we send the Accept on behalf of our mock server
    let request = make_signed_request(
        axum::http::Method::POST,
        url.join(&format!("/users/{}/inbox", username))?,
        Jld::new(
            &Accept::new(
                ObjectField::Iri(mock_user
                                              .id(&mock_origin)
                                              .unwrap(/* known good */)),
                ActorField::Iri(id.clone()),
            ),
            None,
        )?
        .to_string()
        .into(),
        &mock_origin,
        mock_user.priv_key(),
        mock_user.name(),
    )
    .await?;

    client.execute(request).await?;

    let following = client
        .get(format!("{}users/{}/following", url, username))
        .send()
        .await?
        .json::<CollectionPage>()
        .await?;

    assert!(following.first.is_some());
    let first = following.first.unwrap();

    let page = client
        .get(first)
        .send()
        .await?
        .json::<CollectionPage>()
        .await?;

    assert!(page.ordered_items.is_some());

    let items = page.ordered_items.unwrap();

    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0],
        Url::parse(&format!("{}/users/{}", mock_origin, mock_user.name()))?.into()
    );
    Ok(())
}

pub async fn as_follower(
    indielinks: Url,
    pepper_version: PepperVersion,
    pepper_key: Pepper,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    let username = Username::new("as_follower_user").unwrap(/* known good */);
    helper.remove_user(&username).await?;

    // Alright: let's create a mock AP server with which indielinks can federate...
    let mock_server = MockServer::start().await;
    // along with a mock user on that peer:
    let mock_user = PeerUser::new()?;

    let mock_origin: Origin = mock_server.uri().try_into()?;

    // For this test, we expect indielinks to post a `Create` activity to the shared inboxes of it's
    // peers that contain followers of the user as whom we'll post:
    Mock::given(method("POST"))
        .and(path("/inbox"))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&mock_server)
        .await;

    peer_actor(&mock_user, &mock_origin)
        .await?
        .mount(&mock_server)
        .await;

    let api_key = helper
        .create_user(
            &pepper_version,
            &pepper_key,
            &username,
            // 16 bytes from `/dev/urandom` to ensure the password has enough entropy (else it will
            // be rejected 😛)
            &"5161b92b86085a8ecf703cbe8eb1cb2a'".to_owned().into(),
            &HashSet::from([mock_user.id(&mock_origin)?.into()]),
            &HashSet::from([(mock_user.id(&mock_origin)?.into(), FollowId::default())]),
        )
        .await?;

    // Alright! Let's create a post!
    let client = Client::builder()
        .user_agent("indielinks integration tests/0.0.1; +sp1ff@pobox.com")
        .build()?;

    client.get(format!("{}api/v1/posts/add?url=https://wsj.com&description=The%20Wall%20Street%20Journal&tags=news,daily,economy&shared=true&replace=true", indielinks))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {}:{}",  username, api_key))
        .send()
        .await?;

    tokio::time::sleep(Duration::from_millis(250)).await;
    let mut num_requests = mock_server
        .received_requests()
        .await
        .map(|x| x.len())
        .unwrap_or(0);
    let mut n = 0;
    while num_requests < 2 && n < 8 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        num_requests = mock_server
            .received_requests()
            .await
            .map(|x| x.len())
            .unwrap_or(0);
        n += 1;
    }

    assert_eq!(num_requests, 2);

    let create = serde_json::from_slice::<Create>(
        mock_server
            .received_requests()
            .await
            .unwrap()
            .get(1)
            .unwrap()
            .body
            .as_slice(),
    );
    assert!(create.is_ok());
    let create = create.unwrap();

    // Let's have our mock user reply:
    let id = Url::parse(&format!(
        "{}/users/{}/posts/{}",
        mock_origin,
        mock_user.name(),
        Uuid::new_v4()
    ))?;
    let reply = Note::new_from_parts(
        id.clone(),
        Some(create.object_id()?),
        id,
        Url::parse(&format!("{}/users/{}", mock_origin, mock_user.name()))?,
        vec![Url::parse("https://www.w3.org/ns/activitystreams#Public")?].into_iter(),
        vec![Url::parse(&format!(
            "{}/users/{}/followers",
            mock_origin,
            mock_user.name()
        ))?]
        .into_iter(),
        "<p>Greate site!</p>".to_owned(),
    )?;

    let reply: Create = reply.try_into()?;
    info!("Sending a {:#?}", reply);

    let request = make_signed_request(
        axum::http::Method::POST,
        indielinks.join("/inbox")?,
        Jld::new(&reply, None)?.to_string().into(),
        &mock_origin,
        mock_user.priv_key(),
        mock_user.name(),
    )
    .await?;

    let rsp = client.execute(request).await?;
    info!("reply :=> {rsp:?}");
    assert_eq!(rsp.status(), reqwest::StatusCode::ACCEPTED);

    // Still to test:
    // - retrieve my original Note-- the like & the reply should be listed!
    // More stuff to test
    // - boost
    // - mention @sp1ff
    // - make a completely new post

    Ok(())
}
