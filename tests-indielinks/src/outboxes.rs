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

use std::{collections::HashSet, sync::Arc};

use chrono::{SecondsFormat, Utc};
use lazy_static::lazy_static;
use libtest_mimic::Failed;
use reqwest::{Method, StatusCode, Url};
use secrecy::SecretString;

use indielinks_shared::{entities::Username, origin::Origin};

use indielinks::{
    ap_entities::{AnnounceOrCreate, CreateObject, Outbox, OutboxPage},
    peppers::{Pepper, Version as PepperVersion},
};

use crate::{activity_pub, helper::Helper, make_signed_request};

lazy_static! {
    static ref TEST_PASSWORD: SecretString = "0534e7529239fed032a49953ee6ba4d9".to_owned().into();
}

pub async fn outbox_smoke_test(
    indielinks: Url,
    pepper_version: PepperVersion,
    pepper_key: Pepper,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    let username = Username::new("outbox-smoke-user").unwrap(/* known good */);
    let (mock_server, peer_user, client) =
        activity_pub::setup_test(&username, Arc::clone(&helper)).await?;

    let api_key = helper
        .create_user(
            &pepper_version,
            &pepper_key,
            &username,
            &TEST_PASSWORD,
            &HashSet::new(),
            &HashSet::new(),
        )
        .await?;

    let mock_origin: Origin = mock_server.uri().try_into()?;

    // Capture a reference time T; posts get explicit timestamps before T, replies/shares use
    // Utc::now() internally so they land at ~T or slightly after.
    let t = Utc::now();
    let dt = |offset_secs: i64| {
        (t - chrono::Duration::seconds(offset_secs)).to_rfc3339_opts(SecondsFormat::Secs, true)
    };

    // Public post 1 (T - 30s) — expected in outbox
    assert_eq!(
        client
            .get(format!(
                "{}api/v1/posts/add?url=https://example.com/post1&description=Post1&dt={}&shared=true",
                indielinks,
                dt(30)
            ))
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {username}:{api_key}"),
            )
            .send()
            .await?
            .status(),
        StatusCode::CREATED
    );

    // Public post 2 (T - 20s) — expected in outbox
    assert_eq!(
        client
            .get(format!(
                "{}api/v1/posts/add?url=https://example.com/post2&description=Post2&dt={}&shared=true",
                indielinks,
                dt(20)
            ))
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {username}:{api_key}"),
            )
            .send()
            .await?
            .status(),
        StatusCode::CREATED
    );

    // Private post (T - 15s) — must NOT appear in outbox
    assert_eq!(
        client
            .get(format!(
                "{}api/v1/posts/add?url=https://example.com/private&description=Private&dt={}&shared=false",
                indielinks,
                dt(15)
            ))
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {username}:{api_key}"),
            )
            .send()
            .await?
            .status(),
        StatusCode::CREATED
    );

    // Outgoing like — must NOT appear in outbox
    helper
        .add_outgoing_like(&username, &"https://peer/note1".parse()?)
        .await?;

    // Outgoing reply — expected in outbox (timestamp ≈ now)
    helper
        .add_outgoing_reply(&username, &"https://peer/note2".parse()?, "great post")
        .await?;

    // Sleep so the share gets a timestamp strictly later than the reply → share is newest
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Outgoing share — expected in outbox (timestamp ≈ now + 50ms, so it's newest)
    helper
        .add_outgoing_share(&username, &"https://peer/note3".parse()?)
        .await?;

    // Step 1: get the OrderedCollection summary (no pagination token)
    let rsp = client
        .execute(
            make_signed_request(
                Method::GET,
                indielinks.join(&format!("/users/{username}/outbox"))?,
                reqwest::Body::default(),
                &mock_origin,
                peer_user.priv_key(),
                peer_user.name(),
            )
            .await?,
        )
        .await?;
    assert_eq!(rsp.status(), StatusCode::OK);
    let outbox: Outbox = rsp.json().await?;

    // Step 2: follow `first` and paginate until empty
    let mut all_items: Vec<AnnounceOrCreate> = Vec::new();
    let mut next_url: Option<Url> = Some(outbox.first.clone());
    let mut num_pages = 0usize;
    while let Some(page_url) = next_url {
        let rsp = client
            .execute(
                make_signed_request(
                    Method::GET,
                    page_url,
                    reqwest::Body::default(),
                    &mock_origin,
                    peer_user.priv_key(),
                    peer_user.name(),
                )
                .await?,
            )
            .await?;
        assert_eq!(rsp.status(), StatusCode::OK);
        let page: OutboxPage = rsp.json().await?;
        num_pages += 1;
        // Failsafe; just in case pagination is broken, don't let this test run forever
        assert!(num_pages < 2);
        if page.ordered_items.is_empty() {
            break;
        }
        all_items.extend(page.ordered_items);
        next_url = page.next.clone();
    }

    // Assertions

    // We expect exactly 4 items: post1, post2, reply, share (private post and like excluded)
    assert_eq!(
        all_items.len(),
        4,
        "expected 4 outbox items, got {}: {all_items:?}",
        all_items.len()
    );

    // Extract Notes for inspection; all items should be Create(Note(...))
    let notes: Vec<_> = all_items
        .iter()
        .map(|item| match item {
            AnnounceOrCreate::Create(c) => match c.object() {
                CreateObject::Note(note) => note,
            },
            AnnounceOrCreate::Announce(_) => panic!("unexpected Announce in outbox"),
        })
        .collect();

    // Private post must not appear (its content would contain the private URL)
    assert!(
        !notes
            .iter()
            .any(|n| n.content().as_ref().contains("private")),
        "private post appeared in outbox"
    );

    // Like must not appear (we inserted only 1 reply and 1 share as LRS items, so if count is 4
    // and neither private nor like appears, that's guaranteed -- but let's be explicit)

    // Public posts, reply, and share must appear (checked by content)
    assert!(
        notes
            .iter()
            .any(|n| n.content().as_ref().contains("example.com/post1")),
        "post1 missing from outbox"
    );
    assert!(
        notes
            .iter()
            .any(|n| n.content().as_ref().contains("example.com/post2")),
        "post2 missing from outbox"
    );
    assert!(
        notes
            .iter()
            .any(|n| n.content().as_ref().contains("great post")),
        "reply missing from outbox"
    );

    // Reverse-chronological order: share (newest) → reply → post2 → post1 (oldest)
    let timestamps: Vec<_> = notes.iter().map(|n| *n.published()).collect();
    assert!(
        timestamps.windows(2).all(|w| w[0] >= w[1]),
        "outbox not in reverse-chronological order: {timestamps:?}"
    );

    helper.remove_user(&username).await?;

    Ok(())
}
