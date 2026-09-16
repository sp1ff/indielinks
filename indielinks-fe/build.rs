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

//! Build-time validation of the frontend's compile-time configuration.
//!
//! `INDIELINKS_FE_DARK_THEME` is consumed by the crate through `option_env!`; validating it here
//! turns an operator mistake into a build failure with a clear diagnostic rather than a bundle
//! that misbehaves after deployment.

use std::env;

fn main() {
    println!("cargo:rerun-if-env-changed=INDIELINKS_FE_DARK_THEME");
    if let Ok(value) = env::var("INDIELINKS_FE_DARK_THEME") {
        assert!(
            ["true", "false"].contains(&value.as_str()),
            "invalid INDIELINKS_FE_DARK_THEME value `{value}`; expected `true` or `false`"
        );
    }
}
