// Copyright (C) 2026 Michael Herstine <sp1ff@pobox.com>
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
// You should have received a copy of the GNU General Public License along with indielinks.  If not,
// see <http://www.gnu.org/licenses/>.

//! # Integration tests for the indielinks home timeline
//!
//! Each test function spins up a wiremock peer AP server with a mock outbox, creates a local
//! indielinks user who follows the peer, and exercises the `/api/v1/users/timeline` endpoint.

use std::{collections::HashSet, sync::Arc};

use lazy_static::lazy_static;
use libtest_mimic::Failed;
use reqwest::{Client, Url};
use secrecy::SecretString;
use tap::{Pipe, TryConv};
use uuid::Uuid;
use wiremock::{
    matchers::{method, path, query_param},
    Mock, MockServer, ResponseTemplate,
};

use indielinks_shared::{
    api::{TimelineBeforeRsp, TimelineInitialRsp, TimelineReq},
    entities::Username,
    origin::Origin,
};

use indielinks::{
    ap_entities::{
        make_user_followers, make_user_id, make_user_outbox, AnnounceOrCreate, Create, Jld, Note,
        Outbox, OutboxPage, Replies,
    },
    entities::FollowId,
    peppers::{Pepper, Version as PepperVersion},
    sanitized_html::SanitizedHtml,
};

use crate::{helper::Helper, peer_actor, PeerUser};

lazy_static! {
    static ref TEST_PASSWORD: SecretString = "1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d".to_owned().into();
}

/// Build a `Create`-wrapped `Note` for use as an outbox item.
fn make_note(
    peer_user: &PeerUser,
    origin: &Origin,
    idx: usize,
) -> Result<AnnounceOrCreate, Failed> {
    let id = Url::parse(&format!(
        "{}/users/{}/posts/{}",
        origin,
        peer_user.name(),
        Uuid::new_v4()
    ))?;

    Note::new_from_parts(
        id.clone(),
        None,
        Some(id.clone()),
        make_user_id(peer_user.name(), origin)?,
        vec![Url::parse("https://www.w3.org/ns/activitystreams#Public")?].into_iter(),
        vec![make_user_followers(peer_user.name(), origin)?].into_iter(),
        SanitizedHtml::from(format!("<p>Test post #{idx}</p>").as_str()),
        Replies::new(
            Url::parse(&format!("{id}/replies"))?,
            Url::parse(&format!("{id}/replies?page=true"))?,
            None,
        ),
    )?
    .try_conv::<Create>()?
    .pipe(AnnounceOrCreate::Create)
    .pipe(Ok)
}

/// Build two wiremock [Mock]s covering the peer outbox:
///
/// - The first mock (more specific): `GET /users/{name}/outbox?page=true` → [OutboxPage]
/// - The second mock (less specific): `GET /users/{name}/outbox` → [Outbox] root
///
/// Mount the first mock before the second so wiremock's specificity-based matching selects the
/// page mock for requests carrying the query parameter.
fn peer_outbox(
    user: &PeerUser,
    origin: &Origin,
    items: Vec<AnnounceOrCreate>,
) -> Result<(Mock, Mock), Failed> {
    let username = user.name();
    let outbox_url = make_user_outbox(username, origin)?;
    let page_url = Url::parse(&format!("{outbox_url}?page=true"))?;

    let num_items = items.len();

    let page = OutboxPage {
        id: page_url.clone(),
        next: None,
        prev: None,
        part_of: outbox_url.clone(),
        ordered_items: items,
    };
    let page_jld = Jld::new(&page, None)?.to_string();

    let outbox = Outbox {
        id: outbox_url.clone(),
        total_items: Some(num_items),
        first: page_url.clone(),
        last: Some(page_url),
    };
    let outbox_jld = Jld::new(&outbox, None)?.to_string();

    let page_mock = Mock::given(method("GET"))
        .and(path(format!("/users/{username}/outbox")))
        .and(query_param("page", "true"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(page_jld)
                .insert_header("Content-Type", "application/activity+json"),
        );

    let root_mock = Mock::given(method("GET"))
        .and(path(format!("/users/{username}/outbox")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(outbox_jld)
                .insert_header("Content-Type", "application/activity+json"),
        );

    Ok((page_mock, root_mock))
}

/// Set up a home-timeline test:
///
/// - Removes any pre-existing `username` from the DB.
/// - Starts a mock AP peer server loaded with `num_items` posts in its outbox.
/// - Creates a local indielinks user who follows the peer.
/// - Returns `(MockServer, PeerUser, Client, api_key)`.
pub async fn setup_test(
    username: &Username,
    num_items: usize,
    pepper_version: &PepperVersion,
    pepper_key: &Pepper,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(MockServer, PeerUser, Client, String), Failed> {
    helper.remove_user(username).await?;

    let mock_server = MockServer::start().await;
    let mock_user = PeerUser::new()?;
    let mock_origin: Origin = mock_server.uri().try_into()?;

    peer_actor(&mock_user, &mock_origin)
        .await?
        .mount(&mock_server)
        .await;

    let items = (0..num_items)
        .map(|i| make_note(&mock_user, &mock_origin, i))
        .collect::<Result<Vec<_>, _>>()?;

    let (page_mock, root_mock) = peer_outbox(&mock_user, &mock_origin, items)?;
    page_mock.mount(&mock_server).await;
    root_mock.mount(&mock_server).await;

    let api_key = helper
        .create_user(
            pepper_version,
            pepper_key,
            username,
            &TEST_PASSWORD,
            &HashSet::new(),
            &HashSet::from([(mock_user.id(&mock_origin)?.into(), FollowId::default())]),
        )
        .await?;

    let client = Client::builder()
        .user_agent("indielinks integration tests/0.0.1; +sp1ff@pobox.com")
        .build()?;

    Ok((mock_server, mock_user, client, api_key))
}

/// Request the initial page of a home timeline and assert it contains the expected number of posts.
pub async fn timeline_initial(
    url: Url,
    pepper_version: PepperVersion,
    pepper_key: Pepper,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    let username = Username::new("home_timeline_initial").unwrap(/* known good */);
    let (_mock_server, _mock_user, client, api_key) =
        setup_test(&username, 3, &pepper_version, &pepper_key, helper).await?;

    let rsp = client
        .post(format!("{url}api/v1/users/timeline"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {username}:{api_key}"),
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_string(&TimelineReq::Initial {
            max_posts: None,
        })?)
        .send()
        .await?
        .error_for_status()?
        .json::<TimelineInitialRsp>()
        .await?;

    assert!(rsp.is_some(), "expected non-empty timeline");
    assert_eq!(rsp.unwrap().posts.len().get(), 3);

    Ok(())
}

/// Request a page of posts before the tail of an initial page; assert the result is empty since the
/// outbox is a single page.
pub async fn timeline_before(
    url: Url,
    pepper_version: PepperVersion,
    pepper_key: Pepper,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    let username = Username::new("home_timeline_before").unwrap(/* known good */);
    let (_mock_server, _mock_user, client, api_key) =
        setup_test(&username, 3, &pepper_version, &pepper_key, helper).await?;

    let initial_rsp = client
        .post(format!("{url}api/v1/users/timeline"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {username}:{api_key}"),
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_string(&TimelineReq::Initial {
            max_posts: None,
        })?)
        .send()
        .await?
        .error_for_status()?
        .json::<TimelineInitialRsp>()
        .await?;

    assert!(initial_rsp.is_some(), "expected non-empty initial timeline");
    let before_token = initial_rsp.unwrap().before;

    let rsp = client
        .post(format!("{url}api/v1/users/timeline"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {username}:{api_key}"),
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_string(&TimelineReq::Before {
            before: before_token,
            max_posts: None,
        })?)
        .send()
        .await?
        .error_for_status()?
        .json::<TimelineBeforeRsp>()
        .await?;

    assert!(
        rsp.is_none(),
        "expected None for 'before' on a single-page timeline"
    );

    Ok(())
}

/// Request the initial page of a home timeline for a user who follows a peer with no posts; assert
/// the result is empty.
pub async fn timeline_empty(
    url: Url,
    pepper_version: PepperVersion,
    pepper_key: Pepper,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    let username = Username::new("home_timeline_empty").unwrap(/* known good */);
    let (_mock_server, _mock_user, client, api_key) =
        setup_test(&username, 0, &pepper_version, &pepper_key, helper).await?;

    let rsp = client
        .post(format!("{url}api/v1/users/timeline"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {username}:{api_key}"),
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_string(&TimelineReq::Initial {
            max_posts: None,
        })?)
        .send()
        .await?
        .error_for_status()?
        .json::<TimelineInitialRsp>()
        .await?;

    assert!(rsp.is_none(), "expected empty timeline");

    Ok(())
}
