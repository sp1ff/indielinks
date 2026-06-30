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
// You should have received a copy of the GNU General Public License along with indielinks.  If not,
// see <http://www.gnu.org/licenses/>.

//! Integration tests for [indielinks] as a distributed cache.
//!
//! This module implements a (very small, at the moment) set of tests for [indielinks] as a
//! distributed cache. Each public function herein is meant to be invoked by one or more
//! [IntegrationTest] implementations.
//!
//! [IntegrationTest]: ../tests_support/trait.IntegrationTest.html

use std::sync::Arc;

use libtest_mimic::Failed;
use reqwest::{blocking::Client, Url};
use tracing::{debug, error};

use indielinks_cache::{
    raft::StorageError,
    types::{ClusterNode, NodeId},
};

use indielinks::{
    cache::{Backend, LogStore, SLOT_RECENT_POSTS, SLOT_TOP_K_TAGS},
    grpc::InitClusterRequest,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                           Scaffolding for the `openraft` test suite                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

struct Dropper {
    // backend: Arc<RwLock<dyn Backend + Send + Sync>>,
    backend: Arc<dyn Backend + Send + Sync>,
}

impl Drop for Dropper {
    #[allow(clippy::result_large_err)]
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

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       integration tests                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

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

/// Cache smoke test; stubbed for now, but will be the integration test for managing the Raft
/// cluster; initializing, driving, adding learners & so on.
pub fn raft_ops(
    ops_endpoint: Url,
    nodes: impl IntoIterator<Item = (NodeId, ClusterNode)>,
) -> Result<(), Failed> {
    debug!("Executing test raft_ops (ops is {ops_endpoint})");

    let client = Client::builder()
        .user_agent("indielinks-test/raft-ops 0.0.1 (+sp1ff@pobox.com)")
        .build()?;

    let mut all_nodes = nodes.into_iter().collect::<Vec<(NodeId, ClusterNode)>>();
    all_nodes.sort_by_key(|lhs| lhs.0);

    assert!(
        all_nodes.len() >= 3,
        "raft_ops requires a cluster of at least three nodes"
    );

    let first_three = all_nodes
        .iter()
        .take(3)
        .cloned()
        .collect::<Vec<(NodeId, ClusterNode)>>();

    // Make the lowest-id node (node 0, after the sort above) responsible for the cluster's "recent
    // posts" list, so the `003recent_posts` test that runs after us has a known owner.
    let request = InitClusterRequest {
        slots: vec![
            (*SLOT_RECENT_POSTS, first_three[0].0),
            (*SLOT_TOP_K_TAGS, first_three[1].0),
        ],
        nodes: first_three,
    };

    // Let's start by initializing a three-node cluster:
    assert_eq!(
        client
            .post(ops_endpoint.join("ops/cache/init-cluster")?)
            .json(&request)
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
            .post(ops_endpoint.join("ops/cache/init-cluster")?)
            .json(&request)
            .send()?
            .error_for_status()?
            .content_length(),
        Some(0)
    );

    // Would be nice to be able to test the cache facility, here.

    debug!("Completed test raft_ops-- returning success.");

    Ok(())
}
