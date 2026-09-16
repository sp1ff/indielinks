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

use std::{
    env::{self, VarError},
    ffi::OsStr,
};

use url::Url;

fn validate_env_variable<K, F>(name: K, check: F)
where
    K: AsRef<OsStr> + Clone,
    F: FnOnce(&str) -> (bool, &'static str),
{
    match env::var(name.clone()) {
        Ok(value) => match check(value.as_ref()) {
            (true, _) => (),
            (false, msg) => {
                panic!("{}: {value}: {msg}", name.as_ref().to_string_lossy());
            }
        },
        Err(VarError::NotPresent) => (),
        Err(err) => {
            panic!(
                "failed to fetch {}: {err:#?}",
                name.as_ref().to_string_lossy()
            );
        }
    }
}

fn main() {
    println!("cargo:rerun-if-env-changed=INDIELINKS_FE_DARK_THEME");
    println!("cargo:rerun-if-env-changed=INDIELINKS_PAGE_SIZE");
    println!("cargo:rerun-if-env-changed=INDIELINKS_FE_API");
    validate_env_variable("INDIELINKS_FE_DARK_THEME", |value| {
        (
            ["true", "false"].contains(&value),
            "expected `true` or `false`",
        )
    });
    validate_env_variable("INDIELINKS_PAGE_SIZE", |value| {
        (
            value.parse::<usize>().is_ok(),
            "expected an unsigned integer",
        )
    });
    validate_env_variable("INDIELINKS_FE_API", |value| {
        (value.parse::<Url>().is_ok(), "expected an URL")
    });
}
