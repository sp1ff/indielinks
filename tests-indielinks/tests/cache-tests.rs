// Copyright (C) 2025-2026 Michael Herstine <sp1ff@pobox.com>
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

//! # [indielinks] as cache integration tests
//!
//! This (admittedly sparse) integration test exercises [indielinks] configured to run in a cluster
//! as a distributed cache baed on [openraft].

use std::{process::ExitCode, result::Result as StdResult, str::FromStr, sync::Arc};

use async_trait::async_trait;
use libtest_mimic::Failed;
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use tracing::debug;

use indielinks::cache::Backend as CacheBackend;

use tests_indielinks::cache::{openraft_test_suite, raft_ops};

use tests_support::{sync_integration_test, Fixture, IntegrationTest, SyncIntegrationTest};

use common::{run, Configuration};

mod common;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create a DynamoDB session: {source}"))]
    Client { source: indielinks::dynamodb::Error },
    #[snafu(display("Failed to run {cmd}: {source}"))]
    Command { cmd: String, source: common::Error },
    #[snafu(display("Error obtaining test configuration: {source}"))]
    Configuration { source: common::Error },
    #[snafu(display("Couldn't parse {text} as a FixtureId"))]
    FixtureId { text: String },
    #[snafu(display("{source}"))]
    IntegrationTest {
        #[snafu(source(from(tests_support::Error<CacheFixture>, Box::new)))]
        source: Box<tests_support::Error<CacheFixture>>,
    },
}

type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       fixture utilities                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

// A collection of little functions for standing-up & tearing-down associated services.

fn setup_indielinks_cluster_alternator() -> Result<()> {
    teardown_indielinks_cluster()?;
    run(
        "../infra/indielinks-cluster-up",
        &["indielinksd-alternator", "5"],
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

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          test fixture                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

// The "PreConfigured" variants are handy because the test need not be aware (at all) of whether
// indielinks is running as a single- or multi-node cluster: it can just write requests to a single
// address.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum FixtureId {
    ScyllaSingleNode,
    SycllaCluster,
    SycllaClusterPreConfigured,
    DynamoDBSingleNode,
    DynamoDBCluster,
    DynamoDBClusterPreConfigured,
}

impl FromStr for FixtureId {
    type Err = Error;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        match s {
            "scylla-single-node" => Ok(FixtureId::ScyllaSingleNode),
            "scylla-cluster" => Ok(FixtureId::SycllaCluster),
            "scylla-cluster-pre-configured" => Ok(FixtureId::SycllaClusterPreConfigured),
            "dynamodb-single-node" => Ok(FixtureId::DynamoDBSingleNode),
            "dynamodb-cluster" => Ok(FixtureId::DynamoDBCluster),
            "dynamodb-cluster-pre-configured" => Ok(FixtureId::DynamoDBClusterPreConfigured),
            _ => Err(FixtureIdSnafu { text: s.to_owned() }.build()),
        }
    }
}

#[derive(Debug)]
pub struct CacheFixture {
    id: FixtureId,
    name: &'static str,
}

#[async_trait]
impl Fixture for CacheFixture {
    type Error = Error;
    // Both the DDB & Scylla clients implement `CacheBackend`, so for now we just use a type-erased
    // reference to one or the other, as appropriate for the particular fixture instance.
    type Backend = Arc<dyn CacheBackend + Send + Sync>;
    type Configuration = Configuration;
    type Id = FixtureId;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn get_name(&self) -> String {
        self.name.to_owned()
    }

    async fn new_backend(
        &self,
        cfg: &Self::Configuration,
    ) -> StdResult<Self::Backend, Self::Error> {
        indielinks::dynamodb::Client::new(&cfg.dynamo.location, &cfg.dynamo.credentials, 0)
            .await
            .context(ClientSnafu)
            .map(|x| Arc::new(x) as Arc<dyn CacheBackend + Send + Sync>) // Unsize coercion
    }

    async fn setup(&self, _: &Self::Configuration) -> StdResult<(), Self::Error> {
        match self.id {
            FixtureId::DynamoDBCluster => {
                debug!("DynamoDBCluster setup...");
                setup_scylla()?;
                setup_indielinks_cluster_alternator()?;
                debug!("DynamoDBCluster setup...done.");
            }
            _ => unimplemented!(),
        }
        Ok(())
    }

    async fn teardown(&self, _: &Self::Configuration) -> StdResult<(), Self::Error> {
        match self.id() {
            FixtureId::DynamoDBCluster => {
                debug!("DynamoDBCluster teardown...");
                teardown_indielinks_cluster()?;
                teardown_scylla()?;
                debug!("DynamoDBCluster teardown...done");
            }
            _ => unimplemented!(),
        }
        Ok(())
    }
}

inventory::collect!(CacheFixture);

// This is kinda lame-- I've only built-out one fixture, so far.
inventory::submit!(CacheFixture {
    id: FixtureId::DynamoDBCluster,
    name: "Alternator (cluster)"
});

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             tests                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

struct CacheTest {
    pub name: &'static str,
    pub test_fn:
        fn(Configuration, Arc<dyn CacheBackend + Send + Sync>) -> std::result::Result<(), Failed>,
    // None => run this test in all fixtures; Some<vec![a, b]> => only run in fixtures a & b
    pub fixtures: Option<&'static [FixtureId]>,
}

impl IntegrationTest for CacheTest {
    type F = CacheFixture;

    fn germane(&self, fix: <Self::F as Fixture>::Id) -> bool {
        self.fixtures.map(|f| f.contains(&fix)).unwrap_or(true)
    }
    fn name(&self) -> String {
        self.name.to_owned()
    }
}

#[async_trait]
impl SyncIntegrationTest for CacheTest {
    fn run(
        &self,
        config: <Self::F as Fixture>::Configuration,
        backend: <Self::F as Fixture>::Backend,
    ) -> StdResult<(), Failed> {
        (self.test_fn)(config, backend)
    }
}

inventory::collect!(CacheTest);

inventory::submit!(CacheTest {
    name: "001openraft_test_suite",
    test_fn: |_, backend| openraft_test_suite(backend),
    // Makes no sense to run this more than once
    fixtures: Some(&[FixtureId::DynamoDBCluster])
});

inventory::submit!(CacheTest {
    name: "002raft_ops",
    test_fn: |cfg, _| raft_ops(cfg.ops, cfg.raft_nodes),
    fixtures: Some(&[FixtureId::SycllaCluster, FixtureId::DynamoDBCluster])
});

fn main() -> Result<ExitCode> {
    // We have no way to augment the set of command-line arguments this program will accept, so
    // we'll examine an environment variable to determine where to get our configuration:
    let config = Configuration::new().context(ConfigurationSnafu)?;

    sync_integration_test::<_, _, CacheFixture, CacheTest>(
        tests_support::TestConfiguration {
            runner: Some(tests_support::TestRunnerConfiguration {
                log_level: config.log_level,
                logging: config.logging,
                no_setup: config.no_setup,
                no_teardown: config.no_teardown,
                enforce_single_threaded: true,
            }),
            domain: config,
        },
        inventory::iter::<CacheFixture>.into_iter(),
        inventory::iter::<CacheTest>,
    )
    .context(IntegrationTestSnafu)
}
