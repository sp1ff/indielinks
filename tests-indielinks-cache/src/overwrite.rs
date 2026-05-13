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

/// Insert "bar" => 100 via node 0, read back from node 1, then overwrite with 200 via node 2 and
/// read back from node 0.
pub fn test(base_port: u16) -> Result<(), Failed> {
    let client = ClientBuilder::new()
        .user_agent("indielinks-cache-test/0.0.1")
        .build()?;
    insert_key(&client, base_port, "bar", 100)?;
    assert_eq!(lookup_key(&client, base_port + 1, "bar")?, Some(100));
    insert_key(&client, base_port + 2, "bar", 200)?;
    assert_eq!(lookup_key(&client, base_port, "bar")?, Some(200));
    Ok(())
}

/// Single-node variant.
pub fn single_node(port: u16) -> Result<(), Failed> {
    let client = ClientBuilder::new()
        .user_agent("indielinks-cache-test/0.0.1")
        .build()?;
    insert_key(&client, port, "bar", 100)?;
    assert_eq!(lookup_key(&client, port, "bar")?, Some(100));
    insert_key(&client, port, "bar", 200)?;
    assert_eq!(lookup_key(&client, port, "bar")?, Some(200));
    Ok(())
}
