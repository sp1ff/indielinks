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

//! Integration tests for ActivityPub follows
//!
//! I'm collecting integration tests for sending & receiving ActivityPub [Follow]s here.
//!
//! [Follow]: https://www.w3.org/TR/activitystreams-vocabulary/#dfn-follow

use std::time::Duration;

use indielinks_shared::entities::Username;

use indielinks::{
    actor::CollectionPage,
    ap_entities::{self, Jld},
    origin::Origin,
};
use libtest_mimic::Failed;
use reqwest::{Client, Url};
use uuid::Uuid;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

use crate::{make_signed_request, peer_actor, PeerUser, TEST_USER_AGENT};

/// First integration test for ActivityPub [Follow]s.
///
/// [Follow]: https://www.w3.org/TR/activitystreams-vocabulary/#dfn-follow
///
/// Very simple: send a follow, validate that we receive an [Accept], then hit the followers
/// endpoint & make sure we show-up.
///
/// [Accept]: https://www.w3.org/TR/activitystreams-vocabulary/#dfn-accept
pub async fn accept_follow_smoke(
    url: Url,
    username: Username,
    origin: Origin,
) -> Result<(), Failed> {
    // This test takes the form of a conversation between a mock ActivityPub server implemented in
    // this test, and indielinks.
    // along with a mock user on that peer:
    let mock_user = PeerUser::new()?;

    // We'll also need a client:
    let client = Client::builder().user_agent(TEST_USER_AGENT).build()?;

    let mock_server = MockServer::start().await;

    let mock_origin: Origin = mock_server.uri().try_into()?;
    peer_actor(&mock_user, &mock_origin)
        .await?
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path(format!("/users/{}/inbox", mock_user.name())))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&mock_server)
        .await;

    // That's it-- our server is up & running. Now let's build ourselves a `Follow` request:
    let follow = ap_entities::Follow::new(
        url.join("/users/sp1ff/inbox")?,
        Url::parse(&format!("{}/{}", mock_origin, Uuid::new_v4().hyphenated()))?,
        Url::parse(&format!("{}/users/{}", mock_origin, mock_user.name()))?,
    );
    // Nb. that `request` is now a *reqwest* `Request`, not an axum `Request`. Sign it:
    let request = make_signed_request(
        axum::http::Method::POST,
        url.join(&format!("/users/{}/inbox", &username))?,
        Jld::new(&follow, None)?.to_string().into(),
        &mock_origin,
        mock_user.priv_key(),
        mock_user.name(),
    )
    .await?;

    let rsp = client.execute(request).await?;
    assert!(rsp.status() == reqwest::StatusCode::CREATED);

    // Let's at least check that the follower now shows-up!
    let request = make_signed_request(
        axum::http::Method::GET,
        url.join(&format!("/users/{}/followers", &username))?,
        reqwest::Body::default(),
        &mock_origin,
        mock_user.priv_key(),
        mock_user.name(),
    )
    .await?;

    let rsp = client.execute(request).await?;
    assert_eq!(rsp.status(), http::StatusCode::OK);

    let page = rsp.json::<CollectionPage>().await?;
    assert_eq!(page.total_items, 1);

    let first = page.first.unwrap(/* known good */);
    let request = make_signed_request(
        axum::http::Method::GET,
        first,
        reqwest::Body::default(),
        &mock_origin,
        mock_user.priv_key(),
        mock_user.name(),
    )
    .await?;

    let rsp = client.execute(request).await?;
    assert_eq!(rsp.status(), http::StatusCode::OK);
    let page = rsp.json::<CollectionPage>().await?;
    assert_eq!(
        page.first.unwrap(),
        Url::parse(&format!("{}/users/sp1ff/followers?page=0", origin))?
    );
    assert!(page.next.is_none());
    assert_eq!(
        page.part_of.unwrap(),
        Url::parse(&format!("{}/users/sp1ff/followers", origin))?
    );
    let items = page.ordered_items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].to_string(),
        format!("{}/users/{}", mock_origin, mock_user.name())
    );

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

    assert_eq!(2, num_requests);

    Ok(())
}
