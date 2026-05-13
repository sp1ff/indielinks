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

use std::{thread::sleep, time::Duration};

use http::header::CONTENT_TYPE;
use libtest_mimic::Failed;
use reqwest::blocking::ClientBuilder;

use crate::{insert_key, lookup_key, smoke::get_metrics};

/// Remove a cluster member and verify that:
///   1. The membership change invalidates (at least some) cached entries.
///   2. New inserts and lookups work correctly on the reduced cluster.
///
/// Expects the five-node cluster from the InMemory/OnDisk fixture (nodes 0-4 on ports
/// `base_port` through `base_port+4`).
pub fn remove_member(base_port: u16) -> Result<(), Failed> {
    let client = ClientBuilder::new()
        .user_agent("indielinks-cache-test/0.0.1")
        .build()?;

    // Insert a handful of keys before the membership change so we can check invalidation.
    for i in 0..5usize {
        let port = base_port + (i % 3) as u16;
        insert_key(&client, port, &format!("rm-before-{i}"), i)?;
    }

    // Find the current leader so we can direct the membership-change request to it.
    let leader_port = get_metrics(&client, base_port)?
        .raft
        .current_leader
        .map(|id| base_port + id as u16)
        .expect("no leader established after smoke test");

    // Remove node 4 from the cluster.
    let rsp = client
        .post(format!("http://127.0.0.1:{leader_port}/admin/membership"))
        .header(CONTENT_TYPE, "application/json")
        .json(&vec![0u64, 1, 2, 3])
        .send()?;
    if !rsp.status().is_success() {
        // Leader may have changed; try every remaining node.
        let mut succeeded = false;
        for i in 0..4u16 {
            let rsp = client
                .post(format!(
                    "http://127.0.0.1:{}/admin/membership",
                    base_port + i
                ))
                .header(CONTENT_TYPE, "application/json")
                .json(&vec![0u64, 1, 2, 3])
                .send()?;
            if rsp.status().is_success() {
                succeeded = true;
                break;
            }
        }
        assert!(
            succeeded,
            "no node accepted the membership change to [0,1,2,3]"
        );
    }

    // Give the cluster time to propagate the new membership and rebuild the hash ring.
    sleep(Duration::from_secs(2));

    // After the membership change the hash ring is recomputed.  Keys that now map to a different
    // node will appear as cache misses.  Assert that at least one of the pre-change keys is gone.
    let mut any_evicted = false;
    for i in 0..5usize {
        let port = base_port + (i % 3) as u16;
        if lookup_key(&client, port, &format!("rm-before-{i}"))?.is_none() {
            any_evicted = true;
            break;
        }
    }
    assert!(
        any_evicted,
        "expected at least one pre-membership-change key to be invalidated after removing node 4",
    );

    // New inserts to the four-node cluster must round-trip correctly.
    for i in 0..5usize {
        let port = base_port + (i % 3) as u16;
        insert_key(&client, port, &format!("rm-after-{i}"), i + 10)?;
    }
    for i in 0..5usize {
        let port = base_port + ((i + 1) % 3) as u16;
        assert_eq!(
            lookup_key(&client, port, &format!("rm-after-{i}"))?,
            Some(i + 10),
            "wrong value for \"rm-after-{i}\" after membership change",
        );
    }

    Ok(())
}
