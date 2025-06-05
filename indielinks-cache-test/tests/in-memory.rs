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
// You should have received a copy of the GNU General Public License along with indielinks.  If not,
// see <http://www.gnu.org/licenses/>.

//! # in-memory integration tests
//!
//! Integration tests run against a Raft cluster made of of the `src` `in-memory` binaries

use common::{Configuration, InMemoryTest};
use itertools::Itertools;
use libtest_mimic::{Arguments, Failed, Trial};
use snafu::Snafu;

mod common;

#[derive(Snafu)]
enum Error {}

impl std::fmt::Debug for Error {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("No errors, yet!");
    }
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

fn smoke_test() -> StdResult<(), Failed> {
    // Steps:
    // 1. stand-up three instances of `in-memory`, with logging
    // 2. initialize the cluster
    // 3. run a scripted sequence of cache operations
    // 4. tear-down all instances
    Ok(())
}

inventory::submit!(InMemoryTest {
    name: "00_smoke",
    test_fn: |_| { smoke_test() },
});

fn main() -> Result<()> {
    let mut args = Arguments::from_args();

    if !matches!(args.test_threads, Some(1)) {
        eprintln!("Temporarily overriding --test-threads to 1.");
        args.test_threads = Some(1);
    }

    let conclusion = libtest_mimic::run(
        &args,
        inventory::iter::<common::InMemoryTest>
            .into_iter()
            .sorted_by_key(|t| t.name)
            .map(|test| {
                Trial::test(test.name, {
                    let cfg = Configuration;
                    || (test.test_fn)(cfg)
                })
            })
            .collect(),
    );

    conclusion.exit();
}
