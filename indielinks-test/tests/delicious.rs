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
//! treat each integration test program as a fixture unto itself. [libtest-mimic] isn't terribly
//! flexible in its handling of command-line arguments, so I'll just have each program accept one
//! extra option: the path to a configuration file. [libtest-mimic] already claims a trailing
//! argument (for the test filter), so this parameter has to be an option/option argument pair.
//!
//! [libtest-mimic]: https://docs.rs/libtest-mimic/latest/libtest_mimic/index.html

use common::Test;
use indielinks_test::parse_configuration_file_option;

use libtest_mimic::{Arguments, Failed, Trial /*, Trial*/};
use snafu::{prelude::*, Backtrace};

use std::{env, process::Command};

mod common;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Failed to take ScyllaDB down: {source}"))]
    Process {
        command: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to run {command}"))]
    Command {
        command: String,
        code: Option<i32>,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

fn run(cmd: &str) -> Result<()> {
    let output = Command::new(cmd).output().context(ProcessSnafu {
        command: cmd.to_string(),
    })?;
    if output.status.success() {
        Ok(())
    } else {
        CommandSnafu {
            command: cmd.to_string(),
            code: output.status.code(),
            stdout: output.stdout,
            stderr: output.stderr,
        }
        .fail()
    }
}

fn setup() -> Result<()> {
    teardown()?;
    run("../infra/scylla-up")?;
    run("../../infra/indielinks-up")
}

fn teardown() -> Result<()> {
    // Let's try & make this idempotent
    run("../infra/scylla-down")
}

fn test_healthcheck() -> StdResult<(), Failed> {
    assert!(
        "GOOD"
            == reqwest::blocking::get("http://localhost:20673/healthcheck")
                .unwrap()
                .text()
                .unwrap()
    );
    Ok(())
}

inventory::submit!(Test {
    name: "test_healthcheck",
    test_fn: test_healthcheck,
});

fn main() -> Result<()> {
    // Parse our custom --config option out of the command-line arguments (libtest-mimic doesn't
    // handle unknown arguments):
    let (_config, rest) = parse_configuration_file_option(env::args_os());
    let args = Arguments::from_iter(rest.into_iter());

    // TODO(sp1ff): Resolve `_config` to a path & parse the configuration file there

    setup()?;

    // Nb. this program is always run from the root directory of the owning crate
    let conclusion = libtest_mimic::run(
        &args,
        inventory::iter::<common::Test>
            .into_iter()
            .map(|trial| Trial::test(trial.name, trial.test_fn))
            .collect(),
    );

    let _ = teardown();

    conclusion.exit();
}
