// Copyright (C) 2024-2025 Michael Herstine <sp1ff@pobox.com>
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

//! # indielinks
//!
//! Right now, the library crate has the same name as the binary, meaning that `rustdoc` will
//! ignore the binary create.
pub mod _docs;
pub mod acct;
pub mod authn;
pub mod delicious;
pub mod dynamodb;
pub mod entities;
pub mod http;
pub mod metrics;
pub mod peppers;
pub mod scylla;
#[path = "signing-keys.rs"]
pub mod signing_keys;
pub mod storage;
pub mod token;
pub mod users;
pub mod util;
pub mod webfinger;
