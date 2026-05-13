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

/// Keys used by this test; chosen to not collide with "foo" (smoke test) or other tests.
const KEYS: &[&str] = &[
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet",
];

/// Insert ten distinct keys across all three nodes (round-robin), then read each back from a
/// different node, verifying the correct value is returned.
pub fn test(base_port: u16) -> Result<(), Failed> {
    let client = ClientBuilder::new()
        .user_agent("indielinks-cache-test/0.0.1")
        .build()?;
    for (i, key) in KEYS.iter().enumerate() {
        let port = base_port + (i % 3) as u16;
        insert_key(&client, port, key, i + 1)?;
    }
    for (i, key) in KEYS.iter().enumerate() {
        let port = base_port + ((i + 1) % 3) as u16;
        let val = lookup_key(&client, port, key)?;
        assert_eq!(
            val,
            Some(i + 1),
            "wrong value for key \"{key}\" on port {port}"
        );
    }
    Ok(())
}

/// Single-node variant: all inserts and reads on the same port.
pub fn single_node(port: u16) -> Result<(), Failed> {
    let client = ClientBuilder::new()
        .user_agent("indielinks-cache-test/0.0.1")
        .build()?;
    for (i, key) in KEYS.iter().enumerate() {
        insert_key(&client, port, key, i + 1)?;
    }
    for (i, key) in KEYS.iter().enumerate() {
        let val = lookup_key(&client, port, key)?;
        assert_eq!(val, Some(i + 1), "wrong value for key \"{key}\"");
    }
    Ok(())
}
