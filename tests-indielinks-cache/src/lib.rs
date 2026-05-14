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

pub mod admin;
pub mod eviction;
pub mod healthcheck;
pub mod invalid_cache_id;
pub mod multi_key;
pub mod overwrite;
pub mod smoke;

use indielinks_cache::types::CacheId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct CacheInsertRequest {
    pub cache: CacheId,
    pub key: serde_json::Value,
    pub value: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CacheLookupRequest {
    pub cache: CacheId,
    pub key: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CacheLookupResponse {
    pub value: Option<serde_json::Value>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                     Shared test helpers                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Insert a `String -> usize` key/value pair into cache 1 on the given node.
pub fn insert_key(
    client: &reqwest::blocking::Client,
    port: u16,
    key: &str,
    value: usize,
) -> Result<(), libtest_mimic::Failed> {
    client
        .post(format!("http://127.0.0.1:{port}/cache/insert"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .json(&CacheInsertRequest {
            cache: 1,
            key: serde_json::to_value(key)?,
            value: serde_json::to_value(value)?,
        })
        .send()?
        .error_for_status()?
        .json::<()>()?;
    Ok(())
}

/// Look up a `String` key from cache 1 on the given node, returning the `usize` value if present.
pub fn lookup_key(
    client: &reqwest::blocking::Client,
    port: u16,
    key: &str,
) -> Result<Option<usize>, libtest_mimic::Failed> {
    let rsp = client
        .get(format!("http://127.0.0.1:{port}/cache/lookup"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .json(&CacheLookupRequest {
            cache: 1,
            key: serde_json::to_value(key)?,
        })
        .send()?
        .error_for_status()?
        .json::<CacheLookupResponse>()?;
    Ok(rsp.value.map(serde_json::from_value::<usize>).transpose()?)
}
