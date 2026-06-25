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

use http::{header::AUTHORIZATION, HeaderValue, StatusCode};

use lazy_static::lazy_static;
use libtest_mimic::Failed;

use indielinks_shared::{api::ClusterStatsResponse, entities::Username};

use indielinks::{
    delicious::GenericRsp,
    peppers::{Pepper, Version as PepperVersion},
};
use secrecy::SecretString;
use url::Url;

use crate::helper::Helper;

lazy_static! {
    static ref TEST_PASSWORD: SecretString = "1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d".to_owned().into();
}

pub async fn test_counts(
    indielinks: Url,
    pepper_version: PepperVersion,
    pepper_key: Pepper,
    utils: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    // Setup a test user
    let username = Username::new("counts-user").unwrap(/* known good */);
    let api_key = utils
        .create_user(
            &pepper_version,
            &pepper_key,
            &username,
            &TEST_PASSWORD,
            &HashSet::new(),
            &HashSet::new(),
        )
        .await?;
    utils.clear_posts(&username).await?;

    // From here in, we'll use the Authorization header, so let's set-up a proper client:
    let client = reqwest::Client::builder()
        .default_headers(
            [(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {username}:{api_key}"))?,
            )]
            .into_iter()
            .collect(),
        )
        .build()?;

    // Add a single post:
    let response = client.get(indielinks.join("/api/v1/posts/add?url=https://instapundit.com&description=Instapundit&tags=blog,daily,glenn-reynolds")
                         ?)
        .send().await?;
    assert!(StatusCode::CREATED == response.status());
    let body = response.json::<GenericRsp>().await?;
    assert!(body.result_code == "done");

    // Finally, actually exercise the `cluster-stats` endpoint
    let response = client
        .get(indielinks.join("/api/v1/users/cluster-stats")?)
        .send()
        .await?;
    assert!(StatusCode::OK == response.status());
    let ClusterStatsResponse {
        origin: _,
        num_users,
        num_posts,
        raft_initialized,
        raft_term: _,
        raft_leader,
    } = response.json::<ClusterStatsResponse>().await?;

    assert_eq!(num_users, 1);
    assert_eq!(num_posts, 1);
    assert!(raft_initialized.is_some());
    assert!(raft_leader.is_some());

    utils.clear_posts(&username).await?;
    utils.remove_user(&username).await?;

    Ok(())
}
