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

use std::{collections::HashSet, fs, path::PathBuf, sync::Arc, time::Duration};

use http::StatusCode;
use indielinks_shared::api::{RaftState, TopKTagsRequest};
use libtest_mimic::Failed;
use nonempty_collections::NEVec;
use openraft::{log_id::LogId, CommittedLeaderId};
use reqwest::{blocking::Client, Url};
use tap::{Conv, Pipe};
use tracing::{debug, error, info};

use indielinks_cache::{
    raft::Metrics as RaftMetrics,
    types::{ClusterNode, NodeId},
};

use indielinks_cache::raft::Backend;

use indielinks::{
    cache::{SLOT_RECENT_POSTS, SLOT_TOP_K_TAGS},
    grpc::InitClusterRequest,
};
use waitpid_any::WaitHandle;

use crate::run::run;

////////////////////////////////////////////////////////////////////////////////////////////////////

fn kill_instance(local_state_base: &str, node_id: usize) -> Result<(), Failed> {
    let node_pid = format!("{local_state_base}{node_id}/indielinksd.pid")
        .conv::<PathBuf>()
        .pipe(fs::read)?
        .pipe(String::from_utf8)?
        .parse::<i32>()?;
    let mut wait_handle = WaitHandle::open(node_pid).expect("WaitHandle should succeed");
    unsafe {
        libc::kill(node_pid, libc::SIGKILL);
    }
    // Slight race condition here: what if node 1 terminates and the PID is reused?
    assert!(wait_handle.wait_timeout(Duration::from_secs(2))?.is_some());
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       integration tests                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Execute the [openraft] test suite against the [indielinks](crate) log store implementation.
///
/// [openraft]: https://docs.rs/openraft/latest/openraft/index.html
pub fn openraft_test_suite(backend: Arc<dyn Backend + Send + Sync>) -> Result<(), Failed> {
    let result = indielinks_cache::raft::test_backend_implementations::test_backend(backend);
    if let Err(ref err) = result {
        error!("{err:#?}");
    }
    assert!(result.is_ok());
    Ok(())
}

/// Cache smoke test; integration test for managing the Raft cluster; initializing, driving, adding
/// learners & so on.
pub fn raft_ops(
    nodes: impl IntoIterator<Item = (NodeId, (ClusterNode, Url))> + Clone,
    local_state_base: &str,
    config_base: &str,
    indielinks: &Url,
) -> Result<(), Failed> {
    let client = Client::builder()
        .user_agent("indielinks-test/raft-ops 0.0.1 (+sp1ff@pobox.com)")
        .build()?;

    let mut ops_endpoints: Vec<(NodeId, (ClusterNode, Url))> = nodes
        .clone()
        .into_iter()
        .collect::<Vec<(NodeId, (ClusterNode, Url))>>();
    ops_endpoints.sort_by_key(|lhs| lhs.0);
    let ops_endpoints: NEVec<Url> = ops_endpoints
        .into_iter()
        .map(|(_, (_, ops))| ops)
        .collect::<Vec<Url>>()
        .try_into()?;

    let mut all_nodes = nodes
        .into_iter()
        .collect::<Vec<(NodeId, (ClusterNode, Url))>>();
    all_nodes.sort_by_key(|lhs| lhs.0);

    assert!(
        all_nodes.len() >= 3,
        "raft_ops requires a cluster of at least three nodes"
    );

    let first_three = all_nodes
        .iter()
        .take(3)
        .map(|(node_id, (cluster_node, _))| (*node_id, cluster_node.clone()))
        .collect::<Vec<(NodeId, ClusterNode)>>();

    // Make the lowest-id node (node 0, after the sort above) responsible for the cluster's "recent
    // posts" list.
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
            .post(ops_endpoints.first().join("ops/cache/init-cluster")?)
            .json(&request)
            .send()?
            .error_for_status()?
            .content_length(),
        Some(0)
    );

    // Should be able to call it again with no error
    assert_eq!(
        client
            .post(ops_endpoints.first().join("ops/cache/init-cluster")?)
            .json(&request)
            .send()?
            .error_for_status()?
            .content_length(),
        Some(0)
    );

    // Shoot instance 1 in the head; this should leave the cluster with quorum, so it can still make
    // progress, but any requests routed to instance 1 will fail.
    kill_instance(local_state_base, 1)?;

    info!("Node one should be down, at this point.");

    // Request the "top-k" tags; should fail
    let status = client
        .get(indielinks.join("/api/v1/users/top-k-tags")?)
        .json(&TopKTagsRequest { num_items: None })
        .send()?
        .status();
    assert!(
        status == StatusCode::INTERNAL_SERVER_ERROR || status == StatusCode::SERVICE_UNAVAILABLE
    );

    // Observe the hash ring, should be the same
    let raft_state = client
        .get(ops_endpoints.first().join("/ops/cache/state")?)
        .send()?
        .error_for_status()?
        .json::<RaftState>()?;

    debug!("Got {raft_state:#?}");

    let cache_nodes = raft_state
        .hash_ring
        .iter()
        .map(|(_, (node, _))| *node)
        .collect::<HashSet<u64>>();
    assert_eq!(cache_nodes, HashSet::from_iter(vec![0, 1, 2]));

    // Move responsibility for slot 0/recent posts to node 2
    let status = client
        .post(ops_endpoints.first().join("/ops/cache/slots")?)
        .json(&vec![(*SLOT_RECENT_POSTS, 2), (*SLOT_TOP_K_TAGS, 1)])
        .send()?
        .status();
    assert_eq!(status, StatusCode::OK);

    // Re-start instance 1; it should "catch-up", but I'm not sure how to observe that.
    run(
        "../infra/indielinks-cluster-node-up",
        ["-L", local_state_base, "-C", config_base, "1"]
            .into_iter()
            .map(str::to_owned),
    )?;

    info!("Re-started instance 1.");

    // In testing, once up, a debug build took ~500ms to catch-up.
    std::thread::sleep(Duration::from_millis(1500));

    let node_0_metrics = client
        .get(ops_endpoints[0].join("/ops/cache/metrics")?)
        .send()?
        .error_for_status()?
        .json::<RaftMetrics>()?;
    let node_0_last_applied = node_0_metrics.raft.last_applied;

    let node_1_last_applied = client
        .get(ops_endpoints[1].join("/ops/cache/metrics")?)
        .send()?
        .error_for_status()?
        .json::<RaftMetrics>()?
        .raft
        .last_applied;

    assert_eq!(node_0_last_applied, node_1_last_applied);

    // Now shoot instance 0 in head; cluster still has quorum, but we've lost everything that node
    // had in memory.
    kill_instance(local_state_base, 0)?;

    // Add instance 3 as a learner. To do this, I need to figure-out who's the new leader.
    std::thread::sleep(Duration::from_millis(3000));

    let leader = client
        .get(ops_endpoints[1].join("/ops/cache/metrics")?)
        .send()?
        .error_for_status()?
        .json::<RaftMetrics>()?
        .raft
        .current_leader
        .unwrap();

    debug!("The new leader is {leader}");

    assert!(leader != 0, "The leader should no longer be node zero!");

    let status = client
        .post(ops_endpoints[leader as usize].join("/ops/cache/add-learner")?)
        .json(&(3, all_nodes[3].1 .0.addr))
        .send()?
        .status();
    assert_eq!(StatusCode::OK, status);

    // Change the membership: remove instace 0 and add instance 3
    let status = client
        .post(ops_endpoints[leader as usize].join("/ops/cache/membership")?)
        .json(&vec![1, 2, 3])
        .send()?
        .status();
    assert_eq!(StatusCode::OK, status);

    // The cluster should now be 1(slot 1/top-k tags), 2 (slot 0/recent posts), 3.
    // Should be healthy.
    let current_metrics = client
        .get(ops_endpoints[leader as usize].join("/ops/cache/metrics")?)
        .send()?
        .error_for_status()?
        .json::<RaftMetrics>()?;

    debug!("Cluster metrics: {current_metrics:#?}");

    assert!(node_0_metrics.raft.current_term < current_metrics.raft.current_term);

    Ok(())
}

/// Snapshots tests-- force a snapshot or two to be taken, then re-start the cluster
pub fn raft_snapshot(
    nodes: impl IntoIterator<Item = (NodeId, (ClusterNode, Url))> + Clone,
    local_state_base: &str,
    config_base: &str,
) -> Result<(), Failed> {
    let client = Client::builder()
        .user_agent("indielinks-test/raft-ops 0.0.1 (+sp1ff@pobox.com)")
        .build()?;

    let mut ops_endpoints: Vec<(NodeId, (ClusterNode, Url))> = nodes
        .clone()
        .into_iter()
        .collect::<Vec<(NodeId, (ClusterNode, Url))>>();
    ops_endpoints.sort_by_key(|lhs| lhs.0);
    let ops_endpoints: NEVec<Url> = ops_endpoints
        .into_iter()
        .map(|(_, (_, ops))| ops)
        .collect::<Vec<Url>>()
        .try_into()?;

    let mut all_nodes = nodes
        .into_iter()
        .collect::<Vec<(NodeId, (ClusterNode, Url))>>();
    all_nodes.sort_by_key(|lhs| lhs.0);

    assert!(
        all_nodes.len() >= 3,
        "raft_snapshot requires a cluster of at least three nodes"
    );

    let first_three = all_nodes
        .iter()
        .take(3)
        .map(|(node_id, (cluster_node, _))| (*node_id, cluster_node.clone()))
        .collect::<Vec<(NodeId, ClusterNode)>>();

    let request = InitClusterRequest {
        slots: vec![
            (*SLOT_RECENT_POSTS, first_three[0].0),
            (*SLOT_TOP_K_TAGS, first_three[1].0),
        ],
        nodes: first_three.clone(),
    };

    // Let's start by initializing a three-node cluster:
    assert_eq!(
        client
            .post(ops_endpoints.first().join("ops/cache/init-cluster")?)
            .json(&request)
            .send()?
            .error_for_status()?
            .content_length(),
        Some(0)
    );

    // Now, let's force some Raft logging, enough to trigger a snapshot
    assert_eq!(
        client
            .post(ops_endpoints.first().join("ops/cache/slots")?)
            .json(&vec![
                (*SLOT_RECENT_POSTS, first_three[1].0),
                (*SLOT_TOP_K_TAGS, first_three[2].0)
            ])
            .send()?
            .error_for_status()?
            .content_length(),
        Some(0)
    );

    assert_eq!(
        client
            .post(ops_endpoints.first().join("ops/cache/slots")?)
            .json(&vec![
                (*SLOT_RECENT_POSTS, first_three[0].0),
                (*SLOT_TOP_K_TAGS, first_three[1].0)
            ])
            .send()?
            .error_for_status()?
            .content_length(),
        Some(0)
    );

    assert_eq!(
        client
            .post(ops_endpoints.first().join("ops/cache/slots")?)
            .json(&vec![
                (*SLOT_RECENT_POSTS, first_three[1].0),
                (*SLOT_TOP_K_TAGS, first_three[2].0)
            ])
            .send()?
            .error_for_status()?
            .content_length(),
        Some(0)
    );

    kill_instance(local_state_base, 2)?;
    kill_instance(local_state_base, 1)?;
    kill_instance(local_state_base, 0)?;

    std::thread::sleep(Duration::from_millis(1500));

    run(
        "../infra/indielinks-cluster-node-up",
        ["-L", local_state_base, "-C", config_base, "0"]
            .into_iter()
            .map(str::to_owned),
    )?;
    run(
        "../infra/indielinks-cluster-node-up",
        ["-L", local_state_base, "-C", config_base, "1"]
            .into_iter()
            .map(str::to_owned),
    )?;
    run(
        "../infra/indielinks-cluster-node-up",
        ["-L", local_state_base, "-C", config_base, "2"]
            .into_iter()
            .map(str::to_owned),
    )?;

    std::thread::sleep(Duration::from_millis(1500));

    let leader = client
        .get(ops_endpoints[1].join("/ops/cache/metrics")?)
        .send()?
        .error_for_status()?
        .json::<RaftMetrics>()?
        .raft
        .current_leader
        .unwrap();

    debug!("The new leader is {leader}");

    let current_metrics = client
        .get(ops_endpoints[leader as usize].join("/ops/cache/metrics")?)
        .send()?
        .error_for_status()?
        .json::<RaftMetrics>()?;

    debug!("Cluster metrics: {current_metrics:#?}");

    assert_eq!(
        // openraft::metrics::RaftMetrics<NodeId, ClusterNode>
        current_metrics.raft.snapshot, // Option<LogId<NID>>
        Some(LogId::<NodeId> {
            leader_id: CommittedLeaderId::<NodeId> {
                term: 1,
                node_id: 0,
            },
            index: 3, // <===
        })
    );

    Ok(())
}
