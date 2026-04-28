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

//! # Integration tests for the [indielinks] background task processing facility
//!
//! [indielinks]: ../indielinks/index.html
//!
//! ## Introduction
//!
//! This is an integration test for the [indielinks] background task processing facility. See the
//! [tests-support] documentation for a lengthy discussion of the [indielinks] integration testing
//! strategy.
//!
//! [tests-support]: ../tests-support/index.html

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Error type                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

use std::{
    path::{Path, PathBuf},
    process::ExitCode,
    result::Result as StdResult,
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use futures::future::BoxFuture;
use indielinks_shared::origin::Origin;
use inventory;
use libtest_mimic::Failed;
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use tracing::{instrument, Level};

use indielinks::{background_tasks::Backend as TasksBackend, storage::Backend as StorageBackend};

use tests_support::{async_integration_test, TestConfiguration};

use tests_indielinks::{
    background::first_background,
    helper::{DynamoConfig, ScyllaConfig},
};

use common::run;

mod common;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create a DynamoDB session: {source}"))]
    Client { source: indielinks::dynamodb::Error },
    #[snafu(display("Failed to run {cmd}: {source}"))]
    Command { cmd: String, source: common::Error },
    #[snafu(display("While obtaining test configuration, {source}"))]
    Configuration {
        #[snafu(source(from(tests_support::Error<Fixture>, Box::new)))]
        source: Box<tests_support::Error<Fixture>>,
    },
    #[snafu(display("Couldn't parse {text} as a FixtureId"))]
    FixtureId { text: String },
    #[snafu(display("{source}"))]
    IntegrationTest {
        #[snafu(source(from(tests_support::Error<Fixture>, Box::new)))]
        source: Box<tests_support::Error<Fixture>>,
    },
    #[snafu(display("ScyllaDB error {source}"))]
    Scylla { source: indielinks::scylla::Error },
}

type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////

#[instrument(level = Level::DEBUG)]
fn setup_single_node(scylla_env_file: Option<&Path>) -> Result<()> {
    teardown_single_node(scylla_env_file)?;
    run("../infra/scylla-up", scylla_env_file.into_iter()).context(CommandSnafu {
        cmd: "scylla-up".to_string(),
    })
}

#[instrument(level = Level::DEBUG)]
fn teardown_single_node(scylla_env_file: Option<&Path>) -> Result<()> {
    run("../infra/scylla-down", scylla_env_file.into_iter()).context(CommandSnafu {
        cmd: "scylla-down".to_string(),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        the fixture type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// The fixture identifier
///
/// [indielinks] fixture parameters:
///
/// - ScyllaDB versus DynamoDB (well, ScyllaDB/Alternator, but still)
/// - [indielinks] running as a single-node or as a cluster
/// - the database pre-charged with a set of test data as part of fixture stand-up, or empty
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum FixtureId {
    ScyllaSingleNode,
    ScyllaCluster,
    DynamoDBSingleNode,
    DynamoDBCluster,
}

impl FromStr for FixtureId {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "scylla-single-node" => Ok(FixtureId::ScyllaSingleNode),
            "scylla-cluster" => Ok(FixtureId::ScyllaCluster),
            "dynamodb-single-node" => Ok(FixtureId::DynamoDBSingleNode),
            "dynamodb-cluster" => Ok(FixtureId::DynamoDBCluster),
            _ => Err(FixtureIdSnafu { text: s.to_owned() }.build()),
        }
    }
}

/// Domain-specific configuration
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub origin: Origin,
    /// The path to a .env file for the scylla Docker compose cluster; None means default
    #[serde(rename = "scylla-env-file")]
    pub scylla_env_file: Option<PathBuf>,
    pub scylla: ScyllaConfig,
    pub dynamo: DynamoConfig,
}

// I'm not sure I'm going to be able to keep this `Default`, but I'm going to start as if I can;
// that way, the user can just say `cargo test` with no config files, no environment variables.
impl Default for Configuration {
    fn default() -> Self {
        Self {
            origin: "http://indiemark.local:20679".parse::<Origin>().unwrap(/* known good */),
            scylla_env_file: None,
            scylla: Default::default(),
            dynamo: Default::default(),
        }
    }
}

// The "backend" for this set of integration tests is so simple that I don't see the need to factor
// this out.
#[async_trait]
pub trait BackgroundTasks {
    async fn make_backends(
        &self,
    ) -> Result<(
        Arc<dyn TasksBackend + Send + Sync>,
        Arc<dyn StorageBackend + Send + Sync>,
    )>;
}

struct ScyllaBackgroundTasks {
    session: Arc<indielinks::scylla::Session>,
}

impl ScyllaBackgroundTasks {
    pub async fn new(configuration: &ScyllaConfig) -> Result<Self> {
        Ok(Self {
            session: Arc::new(
                indielinks::scylla::Session::new(
                    configuration.hosts.clone(),
                    configuration.credentials.as_ref(),
                    0,
                    configuration.translations.as_ref().map(|x| x.clone()),
                )
                .await
                .context(ScyllaSnafu)?,
            ),
        })
    }
}

#[async_trait]
impl BackgroundTasks for ScyllaBackgroundTasks {
    async fn make_backends(
        &self,
    ) -> Result<(
        Arc<dyn TasksBackend + Send + Sync>,
        Arc<dyn StorageBackend + Send + Sync>,
    )> {
        Ok((self.session.clone(), self.session.clone()))
    }
}

struct AlternatorBackgroundTasks {
    session: Arc<indielinks::dynamodb::Client>,
}

impl AlternatorBackgroundTasks {
    pub async fn new(configuration: &DynamoConfig) -> Result<Self> {
        Ok(Self {
            session: Arc::new(
                indielinks::dynamodb::Client::new(
                    &configuration.location,
                    &configuration.credentials,
                    0,
                )
                .await
                .context(ClientSnafu)?,
            ),
        })
    }
}

#[async_trait]
impl BackgroundTasks for AlternatorBackgroundTasks {
    async fn make_backends(
        &self,
    ) -> Result<(
        Arc<dyn TasksBackend + Send + Sync>,
        Arc<dyn StorageBackend + Send + Sync>,
    )> {
        Ok((self.session.clone(), self.session.clone()))
    }
}

#[derive(Debug)]
pub struct Fixture {
    id: FixtureId,
    name: &'static str,
}

#[async_trait]
impl tests_support::Fixture for Fixture {
    type Error = Error;
    type Backend = Arc<dyn BackgroundTasks + Send + Sync>;
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
        config: &Self::Configuration,
    ) -> StdResult<Self::Backend, Self::Error> {
        let backend = match self.id {
            FixtureId::ScyllaSingleNode | FixtureId::ScyllaCluster => {
                let backend = Arc::new(ScyllaBackgroundTasks::new(&config.scylla).await?);
                backend as Arc<dyn BackgroundTasks + Send + Sync>
            }
            FixtureId::DynamoDBSingleNode | FixtureId::DynamoDBCluster => {
                // This is really kind of lame; it would be nicer to do this as part of `setup()`,
                // but because `Fixture` instances are immutable, we have to do it here.
                let backend = Arc::new(AlternatorBackgroundTasks::new(&config.dynamo).await?);
                backend as Arc<dyn BackgroundTasks + Send + Sync>
            }
        };
        Ok(backend)
    }

    async fn setup(&self, configuration: &Self::Configuration) -> StdResult<(), Self::Error> {
        match self.id {
            FixtureId::ScyllaSingleNode | FixtureId::DynamoDBSingleNode => {
                setup_single_node(configuration.scylla_env_file.as_deref())
            }
            _ => unimplemented!(),
        }
    }

    async fn teardown(&self, configuration: &Self::Configuration) -> StdResult<(), Self::Error> {
        match self.id {
            FixtureId::ScyllaSingleNode | FixtureId::DynamoDBSingleNode => {
                teardown_single_node(configuration.scylla_env_file.as_deref())
            }
            _ => unimplemented!(),
        }
    }
}

inventory::collect!(Fixture);

inventory::submit!(Fixture {
    id: FixtureId::ScyllaSingleNode,
    name: "Scylla (single-node)"
});

inventory::submit!(Fixture {
    id: FixtureId::DynamoDBSingleNode,
    name: "Alternator (single-node)"
});

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         the test type                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

struct Test {
    pub name: &'static str,
    // We need a layer of indirection between the API we present to [tests-support] (i.e. just
    // getting the configuration & the test helper) and that exposed by our actual testing logic in
    // the `/src` directory.
    pub test_fn: fn(
        Configuration,
        Arc<dyn BackgroundTasks + Send + Sync>,
    ) -> BoxFuture<'static, StdResult<(), Failed>>,
    // None => run this test in all fixtures; Some<vec![a, b]> => only run in fixtures a & b
    pub fixtures: Option<&'static [FixtureId]>,
}

impl tests_support::IntegrationTest for Test {
    type F = Fixture;

    fn germane(&self, fix: FixtureId) -> bool {
        self.fixtures.map(|f| f.contains(&fix)).unwrap_or(true)
    }
    fn name(&self) -> String {
        self.name.to_owned()
    }
}

#[async_trait]
impl tests_support::AsyncIntegrationTest for Test {
    async fn run(
        &self,
        config: Configuration,
        backend: Arc<dyn BackgroundTasks + Send + Sync>,
    ) -> StdResult<(), Failed> {
        (self.test_fn)(config, backend).await
    }
}

inventory::collect!(Test);

inventory::submit!(Test {
    name: "010first_background_test",
    test_fn: |configuration: Configuration, backend: Arc<dyn BackgroundTasks + Send + Sync>| {
        Box::pin(async move {
            let (backend, storage) = backend
                .make_backends()
                .await
                .expect("Test failed to create backends");
            first_background(configuration.origin, backend, storage).await
        })
    },
    fixtures: Some(&[FixtureId::ScyllaSingleNode]),
});

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          The Big Tuna                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() -> Result<ExitCode> {
    // I'm not sure I'm going to be able to keep this `Default`, but I'm going to start as if I can;
    // that way, the user can just say `cargo test` with no config files, no environment variables.
    async_integration_test(
        TestConfiguration::<Fixture>::new_or_default().context(ConfigurationSnafu)?,
        inventory::iter::<Fixture>.into_iter(),
        inventory::iter::<Test>,
    )
    .context(IntegrationTestSnafu)
}
