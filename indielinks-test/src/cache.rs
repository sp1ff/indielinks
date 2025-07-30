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

//! Integration tests for indielinks-cache.

use std::{collections::BTreeMap, sync::Arc};

use libtest_mimic::Failed;
use reqwest::{Url, blocking::Client};
use tracing::{debug, error};

use indielinks_shared::StorUrl;

use indielinks_cache::{
    raft::StorageError,
    types::{ClusterNode, NodeId},
};

use indielinks::{
    cache::{Backend, CacheInsertRequest, CacheLookupRequest, LogStore},
    entities::FollowerId,
};

struct Dropper {
    // backend: Arc<RwLock<dyn Backend + Send + Sync>>,
    backend: Arc<dyn Backend + Send + Sync>,
}

impl Drop for Dropper {
    fn drop(&mut self) {
        let backend = self.backend.clone();
        let result = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move {
                backend./*read().await.*/drop_all_rows().await
            })
        });
        if result.is_err() {
            error!("Failed to cleanup Raft storage: {result:#?}");
            panic!();
        }
    }
}

struct Builder {
    // backend: Arc<RwLock<dyn Backend + Send + Sync>>,
    backend: Arc<dyn Backend + Send + Sync>,
}

impl Builder {
    // pub fn new(backend: Arc<RwLock<dyn Backend + Send + Sync>>) -> Builder {
    pub fn new(backend: Arc<dyn Backend + Send + Sync>) -> Builder {
        Builder { backend }
    }
}

impl indielinks_cache::raft::test::StoreBuilder<LogStore, Dropper> for Builder {
    async fn build(&self) -> Result<(Dropper, LogStore), StorageError<NodeId>> {
        Ok((
            Dropper {
                backend: self.backend.clone(),
            },
            LogStore::new(self.backend.clone()),
        ))
    }
}

/// Execute the [openraft] test suite against the [indielinks](crate) log store implementation.
///
/// [openraft]: https://docs.rs/openraft/latest/openraft/index.html
pub fn openraft_test_suite(backend: Arc<dyn Backend + Send + Sync>) -> Result<(), Failed> {
    let result = indielinks_cache::raft::test::test_storage(Builder::new(backend));
    if let Err(ref err) = result {
        error!("{err:#?}");
    }
    assert!(result.is_ok());
    Ok(())
}

// Cache smoke test; stubbed for now, but will be the integration test for managing the Raft
// cluster; initializing, driving, adding learners & so on.
pub fn raft_ops(
    ops: Url,
    nodes: impl IntoIterator<Item = (NodeId, ClusterNode)>,
) -> Result<(), Failed> {
    debug!("Executing test raft_ops (ops is {ops})");

    let client = Client::builder()
        .user_agent("indielinks-test/raft-ops 0.0.1 (+sp1ff@pobox.com)")
        .build()?;

    let mut all_nodes = nodes.into_iter().collect::<Vec<(NodeId, ClusterNode)>>();
    all_nodes.sort_by(|lhs, rhs| lhs.0.cmp(&rhs.0));
    let first_three = all_nodes
        .iter()
        .take(3)
        .cloned()
        .collect::<BTreeMap<NodeId, ClusterNode>>();

    // Let's start by initializing a three-node cluster:
    assert_eq!(
        client
            .post(ops.join("ops/cache/init-cluster")?)
            .json(&first_three)
            .send()
            .expect("Failed to send first init-cluster request")
            .error_for_status()
            .expect("Error response to first init-cluster request")
            .content_length(),
        Some(0)
    );

    // Should be able to call it again with no error
    assert_eq!(
        client
            .post(ops.join("ops/cache/init-cluster")?)
            .json(&first_three)
            .send()?
            .error_for_status()?
            .content_length(),
        Some(0)
    );

    // Alright-- let's hit the cache:
    let follower_id = FollowerId::default();

    let result = client
        .get(ops.join("ops/cache/cache-query")?)
        .json(&CacheLookupRequest {
            cache: 1000,
            key: serde_json::to_value(follower_id)?,
        })
        .send()?
        .error_for_status()?
        .json::<Option<StorUrl>>()?;
    assert_eq!(result, None);

    let url: StorUrl = Url::parse("http://foo.com")?.into();
    client
        .post(ops.join("ops/cache/cache-insert")?)
        .json(&CacheInsertRequest {
            cache: 1000,
            key: serde_json::to_value(follower_id)?,
            value: serde_json::to_value(&url)?,
        })
        .send()?
        .error_for_status()?; // No particular return value ATM.

    let result = client
        .get(ops.join("ops/cache/cache-query")?)
        .json(&CacheLookupRequest {
            cache: 1000,
            key: serde_json::to_value(follower_id)?,
        })
        .send()?
        .error_for_status()?
        .json::<Option<StorUrl>>()?;
    assert_eq!(result, Some(url));

    debug!("Completed test raft_ops-- returning success.");

    Ok(())
}
