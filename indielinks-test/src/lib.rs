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
// You should have received a copy of the GNU General Public License along with mpdpopm.  If not,
// see <http://www.gnu.org/licenses/>.

//! # The delicious API Integration Tests
//!
//! # Introduction
//!
//! The Rust unit & integration testing framework is really oriented toward testing *libraries*, not
//! programs. There's no notion of test fixtures, nor even of simple setup & teardown operations
//! that apply to multiple tests. [Nextest] provides a great number of features above & beyond what
//! `cargo test` offers, but 1) is implemented as a Cargo plugin (i.e. to use it one needs to say
//! `cargo nextest` rather than `cargo test`) and 2) still provides nothing in terms of test
//! fixtures.
//!
//! [Nextest]: https://nexte.st
//!
//! T.J. Telan [found] himself in the same boat when building [Fluvio] and lays out an approach that
//! takes more work, but offers limitless opportunities for customization while still fitting into
//! the `cargo test` framework is to change-out the default *test harness*. In Cargo.toml, one is
//! free to explicitly define tests, and opt-out of `libharness`, the default testing harness
//! provided by Rust:
//!
//! ```toml
//! [[test]]
//!     name = "delicious"
//!     harness = false
//! ```
//!
//! [found]: https://tjtelan.com/blog/rust-custom-test-harness/
//! [Fluvio]: https://github.com/infinyon/fluvio
//!
//! With this configuration, `cargo test` expects to find a file named `delicious.rs` in the `tests`
//! subdirectory containing a `main()`. `cargo test` will compile it and execute it, passing any
//! command-line parameters passed to `cargo test` after the `--`. It is expected to exit with
//! status zero if all tests passed, and one else. That's it-- that's the contract.
//!
//! [Advanced Testing in Rust] notes that a compliant implementation could simply ignore the
//! command-line parameters, at the cost of surprising one's users, and suggests the use of
//! [libtest-mimic] to avoid that.
//!
//! [Advanced Testing in Rust]: https://rust-exercises.com/advanced-testing/00_intro/00_welcome.html
//! [libtest-mimic]: https://docs.rs/libtest-mimic/latest/libtest_mimic/index.html
//!
//! For all that guidance, I'm still feeling my way. My general idea is to build a set of
//! integration test programs, each exercising some aspect of indielinks. For now, I'm going to
//! treat each integration test program as a fixture unto itself. I don't really see a lot of
//! difference between this (one integration test per fixture) and allowing multiple fixtures per
//! integration test; I just prefer to present a finer-grained test suite to the Rust test
//! framework.

use reqwest::Url;

pub mod delicious;

/// Hit the `indielinks` healthcheck endpoint; panic on anything other than success.
pub fn test_healthcheck(url: &Url) {
    assert!(
        "GOOD"
            == reqwest::blocking::get(url.join("/healthcheck").expect("Invalid URL"))
                .expect("Failed request")
                .text()
                .expect("Failed to retrieve response text")
    );
}
