// Copyright (C) 2025 Michael Herstine <sp1ff@pobox.com>
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
