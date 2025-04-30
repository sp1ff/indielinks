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
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

use indielinks::{
    ap_entities::Create,
    entities::Username,
    origin::Origin,
    peppers::{Pepper, Version as PepperVersion},
};

use crate::{peer_actor, Helper, PeerUser};

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
        )
        .await?;

    info!(
        "Created test user posting_creates_note_user with api_key: {}",
        api_key
    );

    // Alright! Let's create a post!
    Client::builder()
        .user_agent("indielinks integration tests/0.0.1; +sp1ff@pobox.com")
        .build()?
        .get(format!("{}api/v1/posts/add?url=https://wsj.com&description=The%20Wall%20Street%20Journal&tags=news,daily,economy&shared=true&replace=true", url))
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
    while num_requests < 1 && n < 8 {
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

    Ok(())
}
