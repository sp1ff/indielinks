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

use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    process::ExitCode,
    result::Result as StdResult,
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use indielinks_cache::types::{ClusterNode, NodeId};
use libtest_mimic::Failed;
use serde::Deserialize;
use snafu::{IntoError, ResultExt, Snafu};
use tap::Pipe;
use tracing::debug;

use indielinks::cache::Backend as CacheBackend;

use tests_indielinks::{
    cache::{openraft_test_suite, raft_ops},
    helper::{DynamoConfig, ScyllaConfig},
};

use tests_support::{
    sync_integration_test, Fixture, IntegrationTest, SyncIntegrationTest, TestConfiguration,
};

use common::run;
use url::Url;

mod common;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create a DynamoDB session: {source}"))]
    Client { source: indielinks::dynamodb::Error },
    #[snafu(display("Failed to run {cmd}: {source}"))]
    Command { cmd: String, source: common::Error },
    #[snafu(display("Error obtaining test configuration: {source}"))]
    Configuration { source: common::Error },
    #[snafu(display("Failed to parse {pth}: {source}"))]
    De {
        pth: String,
        source: toml::de::Error,
    },
    #[snafu(display("Failed to read INDIELINKS_TEST_CONFIG: {source}"))]
    Env { source: std::env::VarError },
    #[snafu(display("Couldn't parse {text} as a FixtureId"))]
    FixtureId { text: String },
    #[snafu(display("{source}"))]
    IntegrationTest {
        #[snafu(source(from(tests_support::Error<CacheFixture>, Box::new)))]
        source: Box<tests_support::Error<CacheFixture>>,
    },
    #[snafu(display("Failed to read {pth}: {source}"))]
    Read { pth: String, source: std::io::Error },
}

type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       fixture utilities                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

// A collection of little functions for standing-up & tearing-down associated services.

fn setup_indielinks_cluster_alternator(
    config_base: &str,
    local_state_dir_base: &str,
    haproxy_id: &str,
    haproxy_port: u16,
) -> Result<()> {
    teardown_indielinks_cluster(local_state_dir_base, haproxy_id)?;
    run(
        "../infra/indielinks-cluster-up",
        [
            "-L",
            local_state_dir_base,
            "-C",
            config_base,
            "-I",
            haproxy_id,
            "-H",
        ]
        .into_iter()
        .map(str::to_owned)
        .chain([format!("{haproxy_port}"), "5".to_owned()].into_iter()),
    )
    .context(CommandSnafu {
        cmd: "indielinks-cluster-up".to_owned(),
    })
}

fn setup_scylla(scylla_env_file: Option<&Path>) -> Result<()> {
    teardown_scylla(scylla_env_file.clone())?;
    run("../infra/scylla-up", scylla_env_file.into_iter()).context(CommandSnafu {
        cmd: "scylla-up".to_owned(),
    })
}

fn teardown_indielinks_cluster(local_state_dir_base: &str, haproxy_id: &str) -> Result<()> {
    run(
        "../infra/indielinks-cluster-down",
        ["-L", local_state_dir_base, "-I", haproxy_id]
            .into_iter()
            .map(str::to_owned),
    )
    .context(CommandSnafu {
        cmd: "indielinks-cluster-down".to_owned(),
    })
}

fn teardown_scylla(scylla_env_file: Option<&Path>) -> Result<()> {
    run("../infra/scylla-down", scylla_env_file.into_iter()).context(CommandSnafu {
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

/// Cache tests configuration
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    /// The network location at which an operational interface can be reached
    pub ops: Url,
    /// .env file for ScyllaDB docker compose cluster
    #[serde(rename = "scylla-env-file")]
    pub scylla_env_file: Option<PathBuf>,
    /// Prefix for local state directories for all cluster members
    #[serde(rename = "local-state-base")]
    pub local_state_base: String,
    /// Prefix for Alternator indielinks configuration files for all cluster members
    #[serde(rename = "alternator-config-base")]
    pub alternator_config_base: String,
    /// Prefix for Scylla indielinks configuration files for all cluster members
    #[serde(rename = "scylla-config-base")]
    pub scylla_config_base: String,
    /// Arbitrary identifier to distinguish the haproxy instance from others that may be running
    #[serde(rename = "haproxy-id")]
    pub haproxy_id: String,
    /// Port on which haproxy should listen
    #[serde(rename = "haproxy-port")]
    pub haproxy_port: u16,
    /// gRPC endpoints for Raft configuration nodes, when run in cluster mode
    #[serde(rename = "raft-nodes", deserialize_with = "de_raft_nodes::deserialize")]
    pub raft_nodes: HashMap<NodeId, ClusterNode>,
    pub scylla: ScyllaConfig,
    pub dynamo: DynamoConfig,
}

mod de_raft_nodes {
    use std::{collections::HashMap, num::ParseIntError};

    use indielinks_cache::types::{ClusterNode, NodeId};
    use serde::{Deserialize, Deserializer};
    use tap::Pipe;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<NodeId, ClusterNode>, D::Error>
    where
        D: Deserializer<'de>,
    {
        HashMap::<String, ClusterNode>::deserialize(deserializer)?
            .into_iter()
            .map(|(k, v)| k.parse::<NodeId>().map(|i| (i, v)))
            .collect::<Result<Vec<(NodeId, ClusterNode)>, ParseIntError>>()
            .map_err(|err| {
                serde::de::Error::custom(format!(
                    "Found a key that couldn't be parsed as a NodeId: {err}"
                ))
            })?
            .into_iter()
            .collect::<HashMap<NodeId, ClusterNode>>()
            .pipe(Ok)
    }
}

impl Configuration {
    /// Obtain a [Configuration]
    ///
    /// Check the `INDIELINKS_TEST_CONFIG` environment variable; if defined & non-empty, attempt to
    /// parse a [Configuration] from the file named therein; else return a default instance.
    #[allow(dead_code)]
    pub fn new() -> Result<Configuration> {
        match env::var("INDIELINKS_TEST_CONFIG") {
            Ok(f) => fs::read_to_string(&f)
                .context(ReadSnafu { pth: f.clone() })?
                .pipe(|s| toml::from_str::<Configuration>(&s))
                .context(DeSnafu { pth: f.clone() }),
            Err(env::VarError::NotPresent) => Ok(Configuration::default()),
            Err(err) => Err(EnvSnafu.into_error(err)),
        }
    }
}

impl Default for Configuration {
    /// Default configuration
    ///
    /// When invoked with a bare `cargo test` (i.e. without `INDIELINKS_TEST_CONFIG` set), this is
    /// the configuration that will be used, so be sure the tests will pass with it.
    fn default() -> Self {
        Configuration {
            ops: Url::parse("http://127.0.0.1:20680").unwrap(/* known good */),
            scylla_env_file: None,
            local_state_base: "/tmp/indielinksd-cluster-".to_owned(),
            alternator_config_base: "../conf/indielinksd-alternator-".to_owned(),
            scylla_config_base: "../conf/indielinksd-scylla-".to_owned(),
            haproxy_id: "0".to_owned(),
            haproxy_port: 20673,
            raft_nodes: HashMap::from([(0, "127.0.0.1:20681".parse().unwrap(/* known good */))]),
            scylla: ScyllaConfig::default(),
            dynamo: DynamoConfig::default(),
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

    async fn setup(&self, configuration: &Self::Configuration) -> StdResult<(), Self::Error> {
        match self.id {
            FixtureId::DynamoDBCluster => {
                debug!("DynamoDBCluster setup...");
                setup_scylla(configuration.scylla_env_file.as_deref())?;
                setup_indielinks_cluster_alternator(
                    &configuration.alternator_config_base,
                    &configuration.local_state_base,
                    &configuration.haproxy_id,
                    configuration.haproxy_port,
                )?;
                debug!("DynamoDBCluster setup...done.");
            }
            _ => unimplemented!(),
        }
        Ok(())
    }

    async fn teardown(&self, configuration: &Self::Configuration) -> StdResult<(), Self::Error> {
        match self.id() {
            FixtureId::DynamoDBCluster => {
                debug!("DynamoDBCluster teardown...");
                teardown_indielinks_cluster(
                    &configuration.local_state_base,
                    &configuration.haproxy_id,
                )?;
                teardown_scylla(configuration.scylla_env_file.as_deref())?;
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
    sync_integration_test(
        TestConfiguration::<CacheFixture>::new_or_default("INDIELINKS_CACHE_TESTS_CONFIG")
            .context(IntegrationTestSnafu)?,
        inventory::iter::<CacheFixture>.into_iter(),
        inventory::iter::<CacheTest>,
    )
    .context(IntegrationTestSnafu)
}
