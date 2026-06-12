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

use http::header::CONTENT_TYPE;
use indielinks_cache::types::{NodeId, SlotIndex};
use indielinks_shared::known_good;
use libtest_mimic::Failed;
use reqwest::blocking::ClientBuilder;
use tracing::debug;

use crate::{GetSlotRequest, GetSlotResponse, SetSlotsRequest, smoke::get_metrics};

pub fn test(base_port: u16) -> Result<(), Failed> {
    // We expect a five-node cluster to be up & ready to take traffic on localhost, port `base_port`
    // through `base_port` + 4.
    let client = ClientBuilder::new()
        .user_agent("tests-indielinks-cache/0.0.1")
        .build()?;

    // The /admin/init endpoint blocks until a leader is elected, so we expect one now.
    let metrics = get_metrics(&client, base_port)?;
    assert!(
        metrics.raft.current_leader.is_some_and(|id| id <= 4),
        "expected a leader in [0, 4] after initialization, got {:?}",
        metrics.raft.current_leader,
    );
    let leader = metrics.raft.current_leader.unwrap();
    debug!("The current Raft leader: {:?}", leader);

    client
        .post(format!(
            "http://127.0.0.1:{}/slots/set",
            base_port as u64 + leader
        ))
        .header(CONTENT_TYPE, "application/json")
        .json(&SetSlotsRequest {
            slots: vec![
                (known_good!(SlotIndex::new(1)), Some(2)),
                (known_good!(SlotIndex::new(2)), Some(0)),
            ],
        })
        .send()?
        .error_for_status()?;

    let followers = vec![0u64, 1u64, 2u64, 3u64, 4u64]
        .into_iter()
        .filter(|x| *x == leader)
        .collect::<Vec<NodeId>>();

    for node_id in followers {
        debug!("Querying node {node_id}...");
        let response = client
            .get(format!(
                "http://127.0.0.1:{}/slots/get",
                base_port as u64 + node_id
            ))
            .header(CONTENT_TYPE, "application/json")
            .json(&GetSlotRequest {
                slot: known_good!(SlotIndex::new(1)),
            })
            .send()?
            .error_for_status()?;
        assert_eq!(response.json::<GetSlotResponse>()?.slot, Some(2));

        let response = client
            .get(format!(
                "http://127.0.0.1:{}/slots/get",
                base_port as u64 + node_id
            ))
            .header(CONTENT_TYPE, "application/json")
            .json(&GetSlotRequest {
                slot: known_good!(SlotIndex::new(2)),
            })
            .send()?
            .error_for_status()?;
        assert_eq!(response.json::<GetSlotResponse>()?.slot, Some(0));
        debug!("Querying node {node_id}...done.");
    }

    Ok(())
}
