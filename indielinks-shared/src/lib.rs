// Copyright (C) 2024-2026 Michael Herstine <sp1ff@pobox.com>
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

use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};

pub mod acct;
pub mod api;
pub mod credentials;
#[cfg(feature = "backend")]
pub mod dynamodb;
pub mod entities;
#[path = "exactly-two.rs"]
pub mod exactly_two;
pub mod fin;
#[path = "instance-state.rs"]
pub mod instance_state;
#[path = "nonempty-string.rs"]
pub mod nonempty_string;
pub mod origin;
#[cfg(feature = "backend")]
pub mod scylla;
pub mod service;

#[macro_export]
macro_rules! known_good {
    ($e:expr) => {
        $e.expect(concat!(stringify!($e), " is expected to be valid!"))
    };
}

pub fn mk_cors<H, M, O>(
    allow_credentials: bool,
    allow_headers: H,
    allow_methods: M,
    allow_origin: O,
) -> CorsLayer
where
    H: Into<AllowHeaders>,
    M: Into<AllowMethods>,
    O: Into<AllowOrigin>,
{
    CorsLayer::new()
        .allow_credentials(allow_credentials)
        .allow_private_network(true)
        .allow_headers(allow_headers)
        .allow_methods(allow_methods)
        .allow_origin(allow_origin)
}
