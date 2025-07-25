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

// This integration test is a little different: I want to handle multiple fixtures in a single
// executable. I now have three different test suites (app smoke tests, background task processing
// and this one) with two different fixtures: ScyllaDB versus DynamoDB. That gives six different
// test binaries-- not terrible. However, I want introduce another facet to the abstraction of
// fixture: indielinks deployed in a cluster as opposed to a single node. That would make for
// *twelve* test binaries, along with a lot of code duplication among them.
//
// With `cache-tests`, I want to "move up" a level of complexity: instead of driving the binary off
// a list of tests, I want to drive it off a list of *fixtures*, each of which will run a given
// suite of tests.

use std::{collections::HashSet, io, process::ExitCode, sync::Arc};

use futures::future::{BoxFuture, FutureExt};
use itertools::Itertools;
use libtest_mimic::{Arguments, Conclusion, Failed, Trial};
use snafu::{ResultExt, Snafu};
use tokio::runtime::Runtime;
use tracing::debug;
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};

use indielinks::cache::Backend as CacheBackend;

use indielinks_test::cache::{openraft_test_suite, raft_ops};

use common::{Configuration, Fixture, run};

mod common;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Failed to create a DynamoDB session: {source}"))]
    Client { source: indielinks::dynamodb::Error },
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

type StdResult<T, E> = std::result::Result<T, E>;

// I can't quite generalize the notions of "test" or "fixture" across all three sorts of test
// (indielinks, background & cache). For now, I'm putting 'em here. They should probably be in their
// own module, but before that refactor, think-through the Error strategy.

struct CacheTest {
    pub name: &'static str,
    pub test_fn:
        fn(Configuration, Arc<dyn CacheBackend + Send + Sync>) -> std::result::Result<(), Failed>,
    // None => run this test in all fixtures; Some<vec![a, b]> => only run in fixtures a & b
    pub fixtures: Option<&'static [Fixture]>,
}

inventory::collect!(CacheTest);

struct TestFixture {
    pub fixture: Fixture,
    pub setup: fn(&Configuration) -> Result<()>,
    pub teardown: fn(&Configuration) -> Result<()>,
    pub mk_backend:
        fn(Configuration) -> BoxFuture<'static, Result<Arc<dyn CacheBackend + Send + Sync>>>,
}

inventory::collect!(TestFixture);

fn run_fixture(
    fix: &TestFixture,
    args: &Arguments,
    config: Configuration,
    rt: Arc<Runtime>,
) -> Result<libtest_mimic::Conclusion> {
    if !config.no_setup {
        (fix.setup)(&config)?;
    }

    debug!("Fixture setup complete; creating the backend");

    let backend = rt.block_on((fix.mk_backend)(config.clone()))?;

    debug!("Backend created; executing tests.");

    let conclusion = libtest_mimic::run(
        args,
        inventory::iter::<CacheTest>
            .into_iter()
            .sorted_by_key(|t| t.name)
            .filter_map(|test| {
                if test
                    .fixtures
                    .map(|f| f.contains(&fix.fixture))
                    .unwrap_or(true)
                {
                    Some(Trial::test(test.name, {
                        let cfg = config.clone();
                        let backend = backend.clone();
                        move || (test.test_fn)(cfg, backend)
                    }))
                } else {
                    None
                }
            })
            .collect(),
    );

    debug!("Tearing-down this fixture");

    if !config.no_teardown {
        (fix.teardown)(&config)?;
    }

    debug!("Fixture complete; returning {conclusion:?}");

    Ok(conclusion)
}

fn setup_indielinks_cluster_alternator() -> Result<()> {
    teardown_indielinks_cluster()?;
    run(
        "../infra/indielinks-cluster-up",
        &["indielinks-alternator", "5"],
    )
    .context(CommandSnafu {
        cmd: "indielinks-cluster-up".to_owned(),
    })
}

fn setup_scylla() -> Result<()> {
    teardown_scylla()?;
    run("../infra/scylla-up", &[]).context(CommandSnafu {
        cmd: "scylla-up".to_owned(),
    })
}

fn teardown_indielinks_cluster() -> Result<()> {
    run("../infra/indielinks-cluster-down", &[]).context(CommandSnafu {
        cmd: "indielinks-cluster-down".to_owned(),
    })
}

fn teardown_scylla() -> Result<()> {
    run("../infra/scylla-down", &[]).context(CommandSnafu {
        cmd: "scylla-down".to_string(),
    })
}

inventory::submit!(TestFixture {
    fixture: Fixture::DynamoDBCluster,
    setup: |_| {
        debug!("DynamoDBCluster setup...");
        setup_scylla()?;
        setup_indielinks_cluster_alternator()?;
        debug!("DynamoDBCluster setup...done.");
        Ok(())
    },
    teardown: |_| {
        debug!("DynamoDBCluster teardown...");
        teardown_indielinks_cluster()?;
        teardown_scylla()?;
        debug!("DynamoDBCluster teardown...done");
        Ok(())
    },
    mk_backend: |cfg| -> BoxFuture<'static, Result<Arc<dyn CacheBackend + Send + Sync>>> {
        async move {
            indielinks::dynamodb::Client::new(
                &cfg.dynamo.location,
                &cfg.dynamo
                    .credentials
                    .clone()
                    .map(|(x, y)| (x.into(), y.into())),
                0,
            )
            .await
            .context(ClientSnafu)
            .map(|x| Arc::new(x) as Arc<dyn CacheBackend + Send + Sync>) // Unsize coercion
        }
        .boxed()
    }
});

inventory::submit!(CacheTest {
    name: "001openraft_test_suite",
    test_fn: |_, backend| openraft_test_suite(backend),
    // Makes no sense to run this more than once
    fixtures: Some(&[Fixture::DynamoDBCluster])
});

inventory::submit!(CacheTest {
    name: "002raft_ops",
    test_fn: |cfg, _| raft_ops(cfg.ops, cfg.raft_nodes),
    fixtures: Some(&[Fixture::SycllaCluster, Fixture::DynamoDBCluster])
});

// This will exit with status zero on test success, 101 on test failure & 1 on error-- I don't think
// this is stricly compliant with the cargo convention, but I like the distinction between test
// failure & program error (nb that if a test returns `Failed`, that will show-up as a test failure).
fn main() -> Result<ExitCode> {
    // Both the
    let rt = Arc::new(Runtime::new().expect("Failed to build a tokio multi-threaded runtime"));

    // We have no way to augment the set of command-line arguments this program will accept, so
    // we'll examine an environment variable to determine where to get our configuration:
    let config = Configuration::new().context(ConfigurationSnafu)?;

    // Armed with our configuration, we can configure logging: if logging is requested, we'll log at
    // the configured default level to `stdout`. The filter can be overridden via the (conventional)
    // `RUST_LOG` environment variable.
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

    debug!("Logging configured.");

    let mut args = Arguments::from_args();

    // This, together with prefixing my function names with numbers, is a hopefully temporary
    // workaround to the fact that my tests can't be run out-of-order or simultaneously.
    if !matches!(args.test_threads, Some(1)) {
        eprintln!("Temporarily overriding --test-threads to 1.");
        args.test_threads = Some(1);
    }

    let active = HashSet::<&Fixture>::from_iter(config.fixtures.as_ref().iter());

    if inventory::iter::<TestFixture>
        .into_iter()
        .filter_map(|fix| {
            debug!("Executing fixture {}", fix.fixture);
            active.get(&fix.fixture).map(|_| {
                let config = config.clone();
                run_fixture(fix, &args, config, rt.clone())
            })
        })
        .collect::<StdResult<Vec<Conclusion>, _>>()?
        .into_iter()
        .any(|c| c.has_failed())
    {
        Ok(ExitCode::from(101))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}
