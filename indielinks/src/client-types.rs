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

//! # indielinks HTTP Client Types
//!
//! ## Discussion
//!
//! In indielinks-client, I make the client type generic, with a type constraint like:
//!
//! ```ignore
//!     impl Service<
//!         http::Request<Bytes>,
//!         Response = http::Response<Bytes>,
//!         Error = Box<dyn std::error::Error + Send + Sync>,
//!     > + Clone,
//! ```
//!
//! In this crate, however, I find myself naming the type more frequently. I first laboriously
//! copied the entire type of the resulting [tower] stack here. That was inconvenient, but better
//! than making every type and function that deals with our client type generic. Since the types
//! were extremely large, I moved them off into their own module to avoid cluttering the code.
//!
//! After burning an afternoon trying to add a single layer into the stack, I gave up and just
//! erased the type altogether. This, unfortunately, means imposing a number of additional
//! contraints on the generic parameters to `make_client()`.

use bytes::Bytes;
use indielinks_shared::service::BoxedError;
use tower::util::BoxCloneSyncService;

/// Our type-erased HTTP client
pub type GenericClientType =
    BoxCloneSyncService<http::Request<Bytes>, http::Response<Bytes>, BoxedError>;

/// Our concrete HTTP client type
pub type ClientType = BoxCloneSyncService<http::Request<Bytes>, http::Response<Bytes>, BoxedError>;
