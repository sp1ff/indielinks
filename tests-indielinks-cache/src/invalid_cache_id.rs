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

use http::{StatusCode, header::CONTENT_TYPE};
use libtest_mimic::Failed;
use reqwest::blocking::ClientBuilder;

use crate::{CacheInsertRequest, CacheLookupRequest};

/// Verify that requests with an unknown cache ID are rejected with 400 Bad Request.
pub fn test(base_port: u16) -> Result<(), Failed> {
    let client = ClientBuilder::new()
        .user_agent("indielinks-cache-test/0.0.1")
        .build()?;
    let insert_status = client
        .post(format!("http://127.0.0.1:{base_port}/cache/insert"))
        .header(CONTENT_TYPE, "application/json")
        .json(&CacheInsertRequest {
            cache: 2,
            key: serde_json::to_value("any-key")?,
            value: serde_json::to_value(0usize)?,
        })
        .send()?
        .status();
    assert_eq!(
        insert_status,
        StatusCode::BAD_REQUEST,
        "expected 400 for unknown cache ID on insert"
    );

    let lookup_status = client
        .get(format!("http://127.0.0.1:{base_port}/cache/lookup"))
        .header(CONTENT_TYPE, "application/json")
        .json(&CacheLookupRequest {
            cache: 2,
            key: serde_json::to_value("any-key")?,
        })
        .send()?
        .status();
    assert_eq!(
        lookup_status,
        StatusCode::BAD_REQUEST,
        "expected 400 for unknown cache ID on lookup"
    );

    Ok(())
}
