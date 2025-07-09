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

//! # [indielinks-cache](crate) integration tests via the `on-disk` log store implementation

use std::io;

use common::{Configuration, Test, run};
use itertools::Itertools;
use libtest_mimic::{Arguments, Trial};
use snafu::{ResultExt, Snafu};
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};

mod common;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Failed to run {cmd}: {source}"))]
    Command { cmd: String, source: common::Error },
    #[snafu(display("Error obtaining test configuration: {source}"))]
    Configuration { source: common::Error },
    #[snafu(display("Failed to parse RUST_LOG: {source}"))]
    Filter {
        source: tracing_subscriber::filter::FromEnvError,
    },
    #[snafu(display("Failed to set the global tracing subscriber: {source}"))]
    SetGlobalDefault {
        source: tracing::subscriber::SetGlobalDefaultError,
    },
}

type Result<T> = std::result::Result<T, Error>;

inventory::submit!(Test {
    name: "00_smoke",
    test_fn: |config| { common::smoke::test(config.base_port) },
});

fn setup(base_port: u16) -> Result<()> {
    teardown()?;
    run(
        "../infra/cache-test-cluster-up",
        &["on-disk", &format!("{base_port}")],
        None,
    )
    .context(CommandSnafu {
        cmd: "cache-test-cluster-up".to_owned(),
    })
}

fn teardown() -> Result<()> {
    // Let's try & make this idempotent
    run("../infra/cache-test-cluster-down", &["on-disk"], None).context(CommandSnafu {
        cmd: "cache-test-cluster-down".to_owned(),
    })
}

fn main() -> Result<()> {
    // We have no way to augment the set of command-line arguments this program will accept, so
    // we'll examine an environment variable to determine where to get our configuration:
    let config = Configuration::new().context(ConfigurationSnafu)?;

    let mut args = Arguments::from_args();

    if config.logging {
        let filter = EnvFilter::builder()
            .with_default_directive(config.log_level.into())
            .from_env()
            .context(FilterSnafu)?;
        tracing::subscriber::set_global_default(
            Registry::default()
                .with(fmt::Layer::default().compact().with_writer(io::stdout))
                .with(filter),
        )
        .context(SetGlobalDefaultSnafu)?;
    }

    if !config.no_setup {
        setup(config.base_port)?;
    }

    if !matches!(args.test_threads, Some(1)) {
        eprintln!("Temporarily overriding --test-threads to 1.");
        args.test_threads = Some(1);
    }

    let conclusion = libtest_mimic::run(
        &args,
        inventory::iter::<common::Test>
            .into_iter()
            .sorted_by_key(|t| t.name)
            .map(|test| {
                Trial::test(test.name, {
                    let config = config.clone();
                    move || (test.test_fn)(config)
                })
            })
            .collect(),
    );

    if !config.no_teardown {
        let _ = teardown();
    }

    conclusion.exit();
}
