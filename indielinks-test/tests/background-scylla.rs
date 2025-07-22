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

use std::{fmt::Display, io, sync::Arc};

/// # Background task processing integration tests for ScyllaDB
use common::{BackgroundTest, Configuration, run};

use indielinks_test::background::first_background;
use itertools::Itertools;
use libtest_mimic::{Arguments, Trial};
use snafu::prelude::*;
use tokio::runtime::Runtime;
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};

mod common;

#[derive(Snafu)]
enum Error {
    #[snafu(display("Failed to run {cmd}: {source}"))]
    Command { cmd: String, source: common::Error },
    #[snafu(display("Error obtaining test configuration: {source}"))]
    Configuration { source: common::Error },
    #[snafu(display("Failed to parse RUST_LOG: {source}"))]
    Filter {
        source: tracing_subscriber::filter::FromEnvError,
    },
    #[snafu(display("Failed to create ScyllaDB session: {source}"))]
    Session { source: indielinks::scylla::Error },
    #[snafu(display("Failed to set the global tracing subscriber: {source}"))]
    SetGlobalDefault {
        source: tracing::subscriber::SetGlobalDefaultError,
    },
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Command { cmd, source } => {
                write!(f, "Failed to run command {}: {}", cmd, source)
            }
            _ => Display::fmt(&self, f),
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

fn setup() -> Result<()> {
    teardown()?;
    run("../infra/scylla-up", &[]).context(CommandSnafu {
        cmd: "scylla-up".to_string(),
    })?;
    Ok(())
}

fn teardown() -> Result<()> {
    // Let's try & make this idempotent
    run("../infra/scylla-down", &[]).context(CommandSnafu {
        cmd: "scylla-down".to_string(),
    })
}

struct State {
    session: Arc<indielinks::scylla::Session>,
}

impl State {
    pub async fn new(cfg: &Configuration) -> Result<State> {
        Ok(State {
            session: Arc::new(
                indielinks::scylla::Session::new(
                    cfg.scylla.hosts.clone(),
                    &cfg.scylla
                        .credentials
                        .clone()
                        .map(|(x, y)| (x.into(), y.into())),
                    0,
                )
                .await
                .context(SessionSnafu)?,
            ),
        })
    }
}

inventory::submit!(BackgroundTest {
    name: "010first_background_test",
    test_fn: |cfg: Configuration, backend, storage| {
        Box::pin(first_background(
            cfg.indielinks.try_into().unwrap(),
            backend,
            storage,
        ))
    },
});

fn main() -> Result<()> {
    // Regrettably, the Scylla API has forced us to use async Rust. This is inconvenient as the
    // libtest-mimic crate expects *synchronous* test functions. Using `#[tokio::main]` leaves us
    // with no way to get a reference, or a "handle" to the Tokio runtime in which we're running, so
    // I eschew that here and create it myself:
    let rt = Arc::new(Runtime::new().expect("Failed to build a tokio multi-threaded runtime"));

    // We have no way to augment the set of command-line arguments this program will accept, so
    // we'll examine an environment variable to determine where to get our configuration:
    let config = Configuration::new().context(ConfigurationSnafu)?;

    // Configure logging:
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
        setup()?;
    }

    let state = Arc::new(rt.block_on(async { State::new(&config).await })?);

    let mut args = Arguments::from_args();

    // This, together with prefixing my function names with numbers, is a hopefully temporary
    // workaround to the fact that my tests can't be run out-of-order or simultaneously.
    if !matches!(args.test_threads, Some(1)) {
        eprintln!("Temporarily overriding --test-threads to 1.");
        args.test_threads = Some(1);
    }

    // Nb. this program is always run from the root directory of the owning crate.
    let conclusion = libtest_mimic::run(
        &args,
        inventory::iter::<common::BackgroundTest>
            .into_iter()
            .sorted_by_key(|t| t.name)
            .map(|test| {
                Trial::test(test.name, {
                    let rt = rt.clone();
                    let cfg = config.clone();
                    let session = state.session.clone();
                    let storage = state.session.clone();
                    move || rt.block_on(async { (test.test_fn)(cfg, session, storage).await })
                })
            })
            .collect(),
    );

    if !config.no_teardown {
        let _ = teardown();
    }

    conclusion.exit();
}
