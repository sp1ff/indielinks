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

//! Integration test for the cluster-wide "recent posts" list.
//!
//! [RecentPostsList] is a facade: a public post added on *any* node is forwarded over gRPC to the
//! single node that owns the [SLOT_RECENT_POSTS] Raft slot, where it accumulates; reads from any
//! node likewise proxy to the owner. This test (which runs after [raft_ops], so the cluster is
//! already initialized with node 0 owning the slot) drives adds & reads across several nodes to
//! exercise that proxying, then hands the slot to node 1 & confirms the new owner starts empty (the
//! list does not migrate).
//!
//! [RecentPostsList]: ../indielinks/recent_posts_lists/struct.RecentPostsList.html
//! [SLOT_RECENT_POSTS]: ../indielinks/cache/static.SLOT_RECENT_POSTS.html
//! [raft_ops]: super::cache::raft_ops

use std::time::Duration;

use chrono::Utc;
use libtest_mimic::Failed;
use reqwest::Url;
use tracing::debug;

use indielinks_shared::entities::{Post, StorUrl};

use indielinks_cache::types::{ClusterNode, NodeId};

use indielinks::{
    cache::{GrpcClient, SLOT_RECENT_POSTS},
    recent_posts_lists::Client,
};

/// Build a public [Post] with a distinct URL & the given title.
fn make_post(slug: &str, title: &str) -> Post {
    let url: StorUrl = format!("https://example.com/{slug}")
        .try_into()
        .expect("valid URL");
    Post::new(
        &url,
        &Default::default(),
        &Default::default(),
        &Utc::now(),
        title,
        None,
        &Default::default(),
        true,
        false,
    )
}

/// Exercise the cluster-wide recent-posts list across multiple nodes.
///
/// Expects a cluster of at least three nodes with node 0 owning [SLOT_RECENT_POSTS] (see
/// [raft_ops](super::cache::raft_ops)).
pub fn recent_posts(
    ops_endpoint: Url,
    nodes: impl IntoIterator<Item = (NodeId, ClusterNode)>,
) -> Result<(), Failed> {
    debug!("Executing test recent_posts (ops is {ops_endpoint})");

    let mut all_nodes = nodes.into_iter().collect::<Vec<(NodeId, ClusterNode)>>();
    all_nodes.sort_by_key(|lhs| lhs.0);
    assert!(
        all_nodes.len() >= 3,
        "recent_posts requires a cluster of at least three nodes"
    );

    let rt = tokio::runtime::Runtime::new().expect("Failed to build a tokio runtime");
    rt.block_on(async move {
        // Node 0 owns SLOT_RECENT_POSTS (set by raft_ops). Distribute adds around the cluster: an
        // add sent to a non-owner is proxied over gRPC to the owner, so they all accumulate on
        // node 0.
        let mut c0 = GrpcClient::new(all_nodes[0].0, all_nodes[0].1.addr);
        let mut c1 = GrpcClient::new(all_nodes[1].0, all_nodes[1].1.addr);
        let mut c2 = GrpcClient::new(all_nodes[2].0, all_nodes[2].1.addr);

        // Sequential awaits ⇒ deterministic, monotonically increasing owner-side timestamps.
        c0.add_post(&make_post("1", "Post 1"))
            .await
            .expect("add Post 1 via node 0");
        c1.add_post(&make_post("2", "Post 2"))
            .await
            .expect("add Post 2 via node 1");
        c2.add_post(&make_post("3", "Post 3"))
            .await
            .expect("add Post 3 via node 2");
        c1.add_post(&make_post("4", "Post 4"))
            .await
            .expect("add Post 4 via node 1");
        c0.add_post(&make_post("5", "Post 5"))
            .await
            .expect("add Post 5 via node 0");

        // Read from two non-owner nodes; both proxy to the owner & must return the same list,
        // newest-first.
        let expected = ["Post 5", "Post 4", "Post 3", "Post 2", "Post 1"];
        for (label, client) in [("node 1", &mut c1), ("node 2", &mut c2)] {
            let (page, _token) = client
                .get_posts(None, None)
                .await
                .expect("get_posts succeeds")
                .expect("expected a populated page");
            assert_eq!(
                page.len().get(),
                expected.len(),
                "wrong post count from {label}"
            );
            let titles = page
                .into_iter()
                .map(|p| p.title().to_owned())
                .collect::<Vec<String>>();
            assert_eq!(
                titles,
                expected.map(str::to_owned),
                "wrong post order from {label}"
            );
        }

        // Hand ownership of the slot to node 1.
        reqwest::Client::new()
            .post(
                ops_endpoint
                    .join("ops/cache/slots")
                    .expect("valid slots URL"),
            )
            .json(&vec![(*SLOT_RECENT_POSTS, Some(all_nodes[1].0))])
            .send()
            .await
            .expect("set-slots request sent")
            .error_for_status()
            .expect("set-slots returned success");

        // The new owner (node 1) starts empty-- the list does not migrate. Poll it (bounded) to
        // absorb Raft slot-change propagation lag.
        let empty = {
            let mut found_empty = false;
            for _ in 0..20 {
                match c1.get_posts(None, None).await.expect("get_posts succeeds") {
                    None => {
                        found_empty = true;
                        break;
                    }
                    Some(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
            found_empty
        };
        assert!(
            empty,
            "node 1's recent-posts list was not empty within the timeout after it took ownership"
        );

        Ok::<(), Failed>(())
    })
}
