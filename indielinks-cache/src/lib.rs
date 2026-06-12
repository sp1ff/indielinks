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

//! # indielinks-cache
//!
//! A distributed LRU key-value cache powered by Raft. This crate provides an adaptation of the
//! (rather complex) API exposed by the [openraft] crate, specialized to distributed key-value
//! stores. The application/consumer is responsible for providing implementations of several
//! interfaces defined herein (handling network comms, towards which this crate is agnostic, for
//! instance).
//!
//! More detailed notes can be found in the [documentation module][_docs].

pub mod _docs;
pub mod cache;
pub mod network;
pub mod raft;
pub mod types;
