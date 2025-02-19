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

//! # The indielinks Integration Tests
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
//! the `cargo test` framework: change-out the default *test harness*. In Cargo.toml, one is free to
//! explicitly define tests, and opt-out of `libharness`, the default testing harness provided by
//! Rust:
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
//! command-line parameters, albeit at the cost of surprising one's users, and suggests the use of
//! [libtest-mimic] to avoid that.
//!
//! [Advanced Testing in Rust]: https://rust-exercises.com/advanced-testing/00_intro/00_welcome.html
//! [libtest-mimic]: https://docs.rs/libtest-mimic/latest/libtest_mimic/index.html
//!
//! For all that guidance, I'm still feeling my way, and already this implementation has grown
/// far beyond what I'd envisioned when I started down this path. My general idea is to build a set of
/// integration test programs, each exercising some aspect of indielinks. For now, I'm going to
/// treat each integration test program as a fixture unto itself. I don't really see a lot of
/// difference between this (one integration test per fixture) and allowing multiple fixtures per
/// integration test; I just prefer to present a finer-grained test suite to the Rust test
/// framework.
///
/// # Project Structure
///
/// The crate that owns this file, `indielinks-libtest` is a crate that produces a library package
/// (this one) and multiple integration test packages (one binary for each). Code that's applicable
/// to all integration tests belongs here ([test_healthcheck], e.g., can be usefully registered as a
/// test by any integration test).
///
/// Code relating to the test framework itself (e.g. the `Test` struct) belongs in `tests/common`.
/// Integration test programs themselves go in `tests`.
use async_trait::async_trait;
use indielinks::entities::Username;
use libtest_mimic::Failed;
use reqwest::Url;

pub mod delicious;
pub mod users;
pub mod webfinger;

/// Hit the `indielinks` healthcheck endpoint; panic on anything other than success.
///
/// This is a legit test that also demonstrates a few things that can be done with this framework.
///
/// Firstly, it's async: when registering it, the `Test` instance can use
/// [futures::executor::block_on], like this:
///
/// ```ignore
/// inventory::submit!(Test {
///     name: "000test_healthcheck",
///     test_fn: |cfg, _drop_posts| { futures::executor::block_on(test_healthcheck(&cfg.url)) },
/// });
/// ```
///
/// Secondly, it returns a `Result<(), Failed>` ([Failed] comes from [libtest-mimic]). This let's me
/// use the `?` sigil conveniently for fallible code that I nevertheless expect to succeed and am
/// not interested in testing.
///
/// [libtest-mimic]: https://docs.rs/libtest-mimic/latest/libtest_mimic/index.html
pub async fn test_healthcheck(url: Url) -> Result<(), Failed> {
    assert!(
        "GOOD"
            == reqwest::get(url.join("/healthcheck")?)
                .await?
                .text()
                .await?
    );
    Ok(())
}

/// Implementations of this trait will be passed to each test function to enable them to do things
/// like talk directly to the back-end, ensure a known starting point for the test, & so on. Each
/// integration test will need to provide an implemetnation.
#[async_trait]
pub trait Helper {
    async fn clear_posts(&self, username: &Username) -> Result<(), Failed>;
}
