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

//! # "top-k" tags
//!
//! ## Introduction
//!
//! This is a very simple integration test for the "top-k" tags feature.

use approx::assert_abs_diff_eq;
use libtest_mimic::Failed;
use nonzero::nonzero;
use snafu::prelude::*;
use tracing::debug;
use url::Url;

use indielinks_cache::types::{ClusterNode, NodeId};

use indielinks_shared::{entities::Tagname, known_good};

use indielinks::{
    cache::{GrpcClient, SLOT_TOP_K_TAGS},
    popular_items::Client as PiClient,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        the "smoke" test                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// "Smoke test" the "top-k" tags functionality
///
/// Expects a cluster of at least three nodes with node 1 owning [SLOT_TOP_K_TAGS] (see
/// [raft_ops](super::cache::raft_ops)).
///
/// [SLOT_TOP_K_TAGS]: ../indielinks/cache/static.SLOT_TOP_K_TAGS.html
pub async fn smoke_test_top_k_tags(
    _indielinks: Url,
    ops_endpoint: Url,
    nodes: impl IntoIterator<Item = (NodeId, ClusterNode)>,
) -> Result<(), Failed> {
    debug!("Executing test smoke_test_top_k_tags (ops is {ops_endpoint})");

    let mut all_nodes = nodes.into_iter().collect::<Vec<(NodeId, ClusterNode)>>();
    all_nodes.sort_by_key(|lhs| lhs.0);
    assert!(
        all_nodes.len() >= 3,
        "recent_posts requires a cluster of at least three nodes"
    );

    // Node 1 owns the "top-k" tags collection (set by raft_ops). Distribute adds around the
    // cluster: an add sent to a non-owner is proxied over gRPC to the owner, so they all accumulate
    // on node 1.
    let mut c0 = GrpcClient::new(all_nodes[0].0, all_nodes[0].1.addr);
    let mut c1 = GrpcClient::new(all_nodes[1].0, all_nodes[1].1.addr);
    let mut c2 = GrpcClient::new(all_nodes[2].0, all_nodes[2].1.addr);

    async fn add_tags<T: IntoIterator<Item = &'static str>>(
        client: &mut GrpcClient,
        tags: T,
        message: &str,
    ) {
        let tags: Vec<Tagname> = tags
            .into_iter()
            .map(|s| known_good!(s.try_into()))
            .collect();
        client
            .add_sightings(*SLOT_TOP_K_TAGS, tags.into_iter())
            .await
            .expect(message)
    }

    // This is kinda lame (invoking `add_sightings()`) directly, but, then, this *is* an integration
    // test focused on the clustered aspects of indielinks. A test that hit `/posts/add` a few times
    // & then `/usrs/top-k-tags` probably belongs in `smoke-tests`, anyway.
    add_tags(&mut c0, ["a", "b"], "add tags a & b via node 0").await;
    add_tags(&mut c1, ["a", "c"], "add tags a & c via node 1").await;
    add_tags(&mut c2, ["a", "d"], "add tags a & d via node 2").await;
    add_tags(&mut c0, ["b", "e"], "add tags b & e via node 0").await;
    add_tags(&mut c1, ["c", "f"], "add tags c & f via node 1").await;
    add_tags(&mut c2, ["a", "g"], "add tags a & g via node 2").await;

    // Should be
    // [
    //     ("a", 4.0),
    //     ("c", 2.0),
    //     ("b", 2.0),
    //     ("g", 1.0),
    //     ("f", 1.0),
    //     ("e", 1.0),
    //     ("d", 1.0),
    // ]

    for (_label, client) in [("node 0", &mut c0), ("node 1", &mut c1)] {
        let v: Vec<(Tagname, _)> = client
            .get_top_k(*SLOT_TOP_K_TAGS, nonzero!(10usize))
            .await
            .expect("get_top_k succeeds");
        assert_eq!(v[0].0.as_ref(), "a");
        assert_abs_diff_eq!(v[0].1.into_inner(), 4.0, epsilon = f64::EPSILON);
        assert_eq!(v[1].0.as_ref(), "c");
        assert_abs_diff_eq!(v[1].1.into_inner(), 2.0, epsilon = f64::EPSILON);
        assert_eq!(v[2].0.as_ref(), "b");
        assert_abs_diff_eq!(v[2].1.into_inner(), 2.0, epsilon = f64::EPSILON);
        assert_eq!(v[3].0.as_ref(), "g");
        assert_abs_diff_eq!(v[3].1.into_inner(), 1.0, epsilon = f64::EPSILON);
        assert_eq!(v[4].0.as_ref(), "f");
        assert_abs_diff_eq!(v[4].1.into_inner(), 1.0, epsilon = f64::EPSILON);
        assert_eq!(v[5].0.as_ref(), "e");
        assert_abs_diff_eq!(v[5].1.into_inner(), 1.0, epsilon = f64::EPSILON);
        assert_eq!(v[6].0.as_ref(), "d");
        assert_abs_diff_eq!(v[6].1.into_inner(), 1.0, epsilon = f64::EPSILON);
    }

    // Would be nice to change ownership of the list and verify what happens, then.

    Ok(())
}
