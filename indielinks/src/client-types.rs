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

use std::{convert::Infallible, error::Error as StdError, fmt};

use bytes::Bytes;
use tower::util::BoxCloneSyncService;
use tower_gcra::keyed::KeyExtractor;

#[derive(Debug)]
pub struct BoxedError(pub Box<dyn std::error::Error + Send + Sync + 'static>);

impl fmt::Display for BoxedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl StdError for BoxedError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.source()
    }
}

/// The `Error` type returned by our HTTP client [Service](tower::Service)
pub type GenericClientErrorType<KE> =
    either::Either<<KE as KeyExtractor<http::Request<Bytes>>>::Error, BoxedError>;

/// Our type-erased HTTP client
pub type GenericClientType<KE> =
    BoxCloneSyncService<http::Request<Bytes>, http::Response<Bytes>, GenericClientErrorType<KE>>;

/// Our concrete HTTP client type
pub type ClientType = BoxCloneSyncService<
    http::Request<Bytes>,
    http::Response<Bytes>,
    either::Either<Infallible, BoxedError>,
>;
