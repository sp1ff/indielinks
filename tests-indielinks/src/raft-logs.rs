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

//! # Integration tests for the `purge-raft-logs` indio sub-command

use std::{result::Result as StdResult, sync::Arc};

use indielinks::grpc::InitClusterRequest;
use indielinks_cache::types::{ClusterNode, NodeId};
use indielinks_shared::api::ClusterStatsResponse;
use libtest_mimic::Failed;
use reqwest::Client;
use url::Url;

use crate::helper::{Helper, IndielinksDownParams, IndielinksUpParams, PurgeRaftLogsParams};

/// Assert whether `raft_initialized` is `Some` or `None`.
async fn assert_raft_initialized(ops: &Url, expect_initialized: bool) -> StdResult<(), Failed> {
    let stats = fetch_cluster_stats(ops).await?;
    if expect_initialized {
        assert!(
            stats.raft_initialized.is_some(),
            "expected Raft to be initialized"
        );
    } else {
        assert!(
            stats.raft_initialized.is_none(),
            "expected Raft to be uninitialized"
        );
    }
    Ok(())
}

/// Fetch and deserialize `/ops/cluster-stats`.
async fn fetch_cluster_stats(ops: &Url) -> StdResult<ClusterStatsResponse, Failed> {
    let client = Client::builder()
        .user_agent("indielinks-test/purge-raft-logs 0.0.1 (+sp1ff@pobox.com)")
        .build()?;
    Ok(client
        .get(ops.join("/ops/cluster-stats")?)
        .send()
        .await?
        .error_for_status()?
        .json::<ClusterStatsResponse>()
        .await?)
}

pub async fn smoke(
    up_params: IndielinksUpParams,
    down_params: IndielinksDownParams,
    purge_raft_logs_params: PurgeRaftLogsParams,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    let ops = helper.ops();

    assert_raft_initialized(&ops, true).await?;

    helper.indielinks_down(&down_params)?;

    helper.purge_raft_logs(&purge_raft_logs_params)?;

    helper.indielinks_up(&up_params, true)?;

    assert_raft_initialized(&ops, false).await?;

    let client = Client::builder()
        .user_agent("indielinks-test/purge-raft-logs 0.0.1 (+sp1ff@pobox.com)")
        .build()?;

    let nodes = (0u64..)
        .zip(helper.nodes())
        .map(|(id, (_, _, addr))| (id, ClusterNode { addr }))
        .collect::<Vec<(NodeId, ClusterNode)>>();

    let _ = client
        .post(ops.join("/ops/cache/init-cluster")?)
        .json(&InitClusterRequest {
            nodes,
            slots: Default::default(),
        })
        .send()
        .await?
        .error_for_status()?;

    assert_raft_initialized(&ops, true).await?;

    Ok(())
}
