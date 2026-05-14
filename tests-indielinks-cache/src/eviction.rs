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

use libtest_mimic::Failed;
use reqwest::blocking::ClientBuilder;

use crate::{insert_key, lookup_key};

/// Insert 257 entries (one over the LRU capacity of 256) and verify that at least one of the
/// earliest entries was evicted.
///
/// Uses the SingleNode fixture where all entries live on the same node and we can reason about
/// eviction order directly.
pub fn test(port: u16) -> Result<(), Failed> {
    let client = ClientBuilder::new()
        .user_agent("indielinks-cache-test/0.0.1")
        .build()?;
    for i in 0usize..=256 {
        insert_key(&client, port, &format!("evict-{i}"), i)?;
    }
    // The LRU capacity is 256 entries per node. After inserting 257 keys we expect at least one
    // early key to have been evicted. We check the first two to give a margin against any keys
    // inserted by prior tests in this fixture.
    let v0 = lookup_key(&client, port, "evict-0")?;
    let v1 = lookup_key(&client, port, "evict-1")?;
    assert!(
        v0.is_none() || v1.is_none(),
        "expected at least one of \"evict-0\" or \"evict-1\" to be evicted, but got {v0:?} and {v1:?}",
    );
    // The most recently inserted key must still be present.
    assert_eq!(
        lookup_key(&client, port, "evict-256")?,
        Some(256),
        "expected \"evict-256\" to be present",
    );
    Ok(())
}
