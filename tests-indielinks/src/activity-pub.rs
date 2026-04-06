// Copyright (C) 2025-2026 Michael Herstine <sp1ff@pobox.com>
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
// You should have received a copy of the GNU General Public License along with indielnks.  If not,
// see <http://www.gnu.org/licenses/>.

//! # Integration tests related to indielinks and the ActivityPub protocol
//!
//! This code grew like a weed, and is in the process of being refactored.

use std::{collections::HashSet, sync::Arc, time::Duration};

use lazy_static::lazy_static;
use libtest_mimic::Failed;
use reqwest::{header::ACCEPT, header::CONTENT_TYPE, Client, Url};
use secrecy::SecretString;
use tap::Pipe;
use tracing::{debug, instrument};
use uuid::Uuid;
use wiremock::{
    matchers::{method, path, query_param},
    Mock, MockServer, ResponseTemplate,
};

use indielinks_shared::{
    api::{FollowReq, ThreadContextRequest, ThreadContextResponse},
    entities::Username,
    origin::Origin,
};

use indielinks::{
    actor::CollectionPage,
    ap_entities::{Accept, ActorField, Create, Jld, Like, Note, ObjectField, Replies},
    entities::FollowId,
    peppers::{Pepper, Version as PepperVersion},
};

use crate::{helper::Helper, make_signed_request, peer_actor, PeerUser};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           utilities                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

lazy_static! {
    // 16 bytes from `/dev/urandom` to ensure the password has enough entropy (else it will
    // be rejected 😛)
    static ref TEST_PASSWORD: SecretString = "0534e7529239fed032a49953ee6ba4d9".to_owned().into();
}

/// Setup basic infrastructure for a test case in this module:
///
/// - make sure `username` doesn't already exist in the indielinks instance under test
/// - spin-up a [wiremock] HTTP server to act as a federated ActivityPub server
/// - create a mock ActivityPub actor on that peer instance
/// - create an async [reqwest] HTTP client for general use
///
/// Note that we don't create a test indielinks user; that's to allow the caller some flexibility in
/// declaring followers & follows, should they wish to do so.
pub async fn setup_test(
    username: &Username,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(MockServer, PeerUser, Client), Failed> {
    helper.remove_user(username).await?;

    // Alright: let's create a mock AP server with which indielinks can federate...
    let mock_server = MockServer::start().await;
    // along with a mock user on that peer:
    let mock_user = PeerUser::new()?;

    let mock_origin: Origin = mock_server.uri().try_into()?;
    peer_actor(&mock_user, &mock_origin)
        .await?
        .mount(&mock_server)
        .await;

    let client = Client::builder()
        .user_agent("indielinks integration tests/0.0.1; +sp1ff@pobox.com")
        .build()?;

    Ok((mock_server, mock_user, client))
}

pub async fn posting_creates_note(
    url: Url,
    pepper_version: PepperVersion,
    pepper_key: Pepper,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    let username = Username::new("posting_creates_note_user").unwrap(/* known good */);

    let (mock_server, mock_user, client) = setup_test(&username, helper.clone()).await?;

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

    // OK-- now let's give ourselves a test indielinks user that follows `peer_user`:
    let api_key = helper
        .create_user(
            &pepper_version,
            &pepper_key,
            &username,
            &TEST_PASSWORD,
            &HashSet::from([mock_user.id(&mock_origin)?.into()]),
            &HashSet::new(),
        )
        .await?;

    debug!(
        "Created test user posting_creates_note_user with api_key: {}",
        api_key
    );

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
    assert!(create.is_ok(), "{create:#?}");

    // Now let's send a `Like` for this note
    let create = create.unwrap();
    debug!("Received a {:#?}", create);
    let mut id = mock_user.id(&mock_origin)?;
    id.path_segments_mut()
            .unwrap(/* known good */)
            .extend(vec!["likes".to_string(), Uuid::new_v4().to_string()]);
    let like = Like::new(
        create.object_id()?,
        id,
        ActorField::Iri(mock_user.id(&mock_origin)?),
    );
    debug!("Sending a {:#?}", like);

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
    let username = Username::new("send_follow_user").unwrap(/* known good */);

    let (mock_server, mock_user, client) = setup_test(&username, helper.clone()).await?;

    let id = Url::parse(&format!("{}/users/{}", url, username)).unwrap(/* known good */);

    let mock_origin: Origin = mock_server.uri().try_into()?;

    // For this test, we expect indielinks to post a `Follow` activity to the mock user's personal inbox:
    Mock::given(method("POST"))
        .and(path(format!("/users/{}/inbox", mock_user.name())))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let api_key = helper
        .create_user(
            &pepper_version,
            &pepper_key,
            &username,
            &TEST_PASSWORD,
            &HashSet::from([mock_user.id(&mock_origin)?.into()]),
            &HashSet::new(),
        )
        .await?;

    // Let's ask to send a follow:
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
    while num_requests < 3 && n < 8 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        num_requests = mock_server
            .received_requests()
            .await
            .map(|x| x.len())
            .unwrap_or(0);
        n += 1;
    }

    assert_eq!(num_requests, 3);

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

    let (mock_server, mock_user, client) = setup_test(&username, helper.clone()).await?;

    let mock_origin: Origin = mock_server.uri().try_into()?;

    // For this test, we expect indielinks to post a `Create` activity to the shared inboxes of it's
    // peers that contain followers of the user as whom we'll post:
    Mock::given(method("POST"))
        .and(path("/inbox"))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
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
        Some(id.clone()),
        Url::parse(&format!("{}/users/{}", mock_origin, mock_user.name()))?,
        vec![Url::parse("https://www.w3.org/ns/activitystreams#Public")?].into_iter(),
        vec![Url::parse(&format!(
            "{}/users/{}/followers",
            mock_origin,
            mock_user.name()
        ))?]
        .into_iter(),
        "<p>Greate site!</p>".into(),
        Replies::new(
            Url::parse(&format!("{}/replies", id))?,
            Url::parse(&format!("{}/replies?page=true", id))?,
            None,
        ),
    )?;

    let reply: Create = reply.try_into()?;
    debug!("Sending a {:#?}", reply);

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
    debug!("reply :=> {rsp:?}");
    assert_eq!(rsp.status(), reqwest::StatusCode::ACCEPTED);

    // Still to test:
    // - retrieve my original Note-- the like & the reply should be listed!
    // More stuff to test
    // - boost
    // - mention @sp1ff
    // - make a completely new post

    Ok(())
}

/// Integration test the `/context` endpoint.
///
/// This is a simple test, encoding what I learned while federating locally with a Mastodon instance.
#[instrument(level = "debug", skip(helper, pepper_key))]
pub async fn context_with_mastodon(
    indielinks: Url,
    pepper_version: PepperVersion,
    pepper_key: Pepper,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    // Stan-up the usual test scaffolding:
    let test_username = Username::new("context_with_mastodon").unwrap(/* known good */);
    let (peer_server, _peer_user, client) = setup_test(&test_username, helper.clone()).await?;
    let api_key = helper
        .create_user(
            &pepper_version,
            &pepper_key,
            &test_username,
            &TEST_PASSWORD,
            &HashSet::new(), // no followers
            &HashSet::new(), // no following
        )
        .await?;

    // OK-- we have a (mock) peer ActivityPub server up & running...
    let peer_origin: Origin = peer_server.uri().try_into()?;
    debug!("ActivityPub peer server at {peer_origin}");

    // let's provision it with a note...
    let note_id = Url::parse(&format!("{peer_origin}/@admin/116370106879969740"))?;
    let note_jld = format!(include_str!("../templates/mastodon-note.in"), peer_origin);
    Mock::given(method("GET"))
        .and(path("/@admin/116370106879969740"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(note_jld)
                .insert_header("Content-Type", "application/activity+json"),
        )
        .expect(1)
        .mount(&peer_server)
        .await;

    // along with that note's replies. Irritatingly, we have to begin with the second page, because
    // the URL matcher is more specific than that for the first.
    let replies_page_1_jld = format!(
        include_str!("../templates/mastodon-reply-page-1.in"),
        peer_origin
    );
    Mock::given(method("GET"))
        .and(path(
            "/ap/users/116370086208557206/statuses/116370106879969740/replies",
        ))
        .and(query_param("only_other_accounts", "true"))
        .and(query_param("page", "true"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(replies_page_1_jld)
                .insert_header("Content-Type", "application/activity+json"),
        )
        .expect(1)
        .mount(&peer_server)
        .await;

    // Now for the first page-- this is getting tedious enough that I wonder if there's a macro or
    // utility function to be found, here?
    let replies_page_0_jld = format!(
        include_str!("../templates/mastodon-reply-page-0.in"),
        peer_origin
    );
    Mock::given(method("GET"))
        .and(path(
            "/ap/users/116370086208557206/statuses/116370106879969740/replies",
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(replies_page_0_jld)
                .insert_header("Content-Type", "application/activity+json"),
        )
        .expect(1)
        .mount(&peer_server)
        .await;

    // Alright-- with all that setup, we should be able to issue a "context" request:
    let response = client
        .get(format!("{}api/v1/users/context", indielinks))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}:{}", test_username, api_key),
        )
        .body(serde_json::to_string(&ThreadContextRequest {
            ap_id: note_id,
        })?)
        .header(ACCEPT, "application/activity+json")
        .header(CONTENT_TYPE, "application/activity+json")
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?
        .pipe(|text| serde_json::from_str::<ThreadContextResponse>(&text))?;

    assert_eq!(
        response.post.id,
        Url::parse(&format!(
            "{peer_origin}/ap/users/116370086208557206/statuses/116370106879969740"
        ))?
    );
    assert!(response.parent.is_none());
    assert!(response.children.len() == 1);

    Ok(())
}
