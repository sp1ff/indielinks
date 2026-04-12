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

//! # The indielinks Integration Test Framework
//!
//! # Introduction
//!
//! Code common to the indielinks integration test framework goes here. See [indielinks_test] for a
//! full description.
use std::{collections::HashMap, env, fs, process::Command, sync::Arc};

use libtest_mimic::Failed;
use reqwest::Url;
use serde::Deserialize;
use snafu::{prelude::*, Backtrace, IntoError};
use tap::Pipe;
use tracing::Level;

use indielinks_shared::entities::Username;

use indielinks_cache::types::{ClusterNode, NodeId};

use indielinks::{
    background_tasks::Backend as TasksBackend, peppers::Peppers, storage::Backend as StorageBackend,
};

use tests_indielinks::helper::{DynamoConfig, Helper, ScyllaConfig};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to run {command}; stdout was '{stdout}' & stderr was '{stderr}'."))]
    Command {
        command: String,
        code: Option<i32>,
        stdout: String,
        stderr: String,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to parse {pth}: {source}"))]
    De {
        pth: String,
        source: toml::de::Error,
    },
    #[snafu(display("Failed to read INDIELINKS_TEST_CONFIG: {source}"))]
    Env { source: std::env::VarError },
    #[snafu(display("Failed to execute {command}: {source}"))]
    Process {
        command: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to read {pth}: {source}"))]
    Read { pth: String, source: std::io::Error },
}

type Result<T> = std::result::Result<T, Error>;

const RUST_LOG: &str = "debug,aws_config=info,aws_runtime=info,aws_sdk_sts=info,aws_sigv4=info,\
                        aws_smithy_runtime=info,aws_smithy_runtime_api=info,hyper=info,scylla=info";

pub fn run(cmd: &str, args: &[&str]) -> Result<()> {
    let output = Command::new(cmd)
        .args(args.into_iter())
        .env("RUST_LOG", RUST_LOG)
        .output()
        .context(ProcessSnafu {
            command: cmd.to_string(),
        })?;
    if output.status.success() {
        Ok(())
    } else {
        CommandSnafu {
            command: cmd.to_string(),
            code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout),
            stderr: String::from_utf8_lossy(&output.stderr),
        }
        .fail()
    }
}

/// Common test configuration
///
/// Not sure about having all tests share one configuration format; coding speculatively.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
pub struct Configuration {
    #[serde(rename = "no-setup")]
    pub no_setup: bool,
    #[serde(rename = "no-teardown")]
    pub no_teardown: bool,
    /// The location at which the indielinks instance under test can be reached from this test
    pub indielinks: Url,
    /// The network location at which an operational interface can be reached
    pub ops: Url,
    /// gRPC endpoints for Raft configuration nodes, when run in cluster mode
    #[serde(rename = "raft-nodes", deserialize_with = "de_raft_nodes::deserialize")]
    pub raft_nodes: HashMap<NodeId, ClusterNode>,
    /// The username of the test user that comes "pre-configured" with our integration tests
    // I think I'd like to get rid of this altogether & just have tests create their own test users
    pub username: Username,
    /// The API key of test user that comes "pre-configured" with our integration tests
    // I think I'd like to get rid of this altogether & just have tests create their own test users,
    // but this is thoroughly woven into the test suite, at this point. I think I'd like to make
    // that a separate issue.
    #[serde(rename = "api-key")]
    pub api_key: String,
    pub pepper: Peppers,
    pub scylla: ScyllaConfig,
    pub dynamo: DynamoConfig,
    pub logging: bool,
    #[serde(deserialize_with = "de_level::deserialize", rename = "log-level")]
    pub log_level: Level,
}

mod de_level {
    use std::str::FromStr;

    use serde::{Deserialize, Deserializer};
    use tracing::Level;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Level, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Level::from_str(&s).map_err(|_| {
            serde::de::Error::custom(format!("{} cannot be interepreted as a log level", s))
        })
    }
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
            no_setup: false,
            no_teardown: false,
            username: Username::new("sp1ff").unwrap(/* known good */),
            indielinks: Url::parse("http://indiemark.local:20679").unwrap(/* known good */),
            ops: Url::parse("http://127.0.0.1:20680").unwrap(/* known good */),
            raft_nodes: HashMap::from([(0, "127.0.0.1:20681".parse().unwrap(/* known good */))]),
            api_key: "v1:5eb56ceebb7425aafe36eabca8b923054b4907d9375acd0b9950c51b57b201fb73e437428050f451b57632f99a3bbd5bed1c0f51cc0df752147090ed26e975f4".to_owned(),
            pepper: Peppers::default(),
            scylla: ScyllaConfig::default(),
            dynamo: DynamoConfig::default(),
            logging: false,
            log_level: Level::INFO,
        }
    }
}

#[allow(dead_code)] // not used by all test programs
pub struct IndielinksTest {
    pub name: &'static str,
    /// `test_fn` must be the address of an async function taking a copy of the test's
    /// [Configuration] along with a reference to a [Helper] implementation.
    pub test_fn: fn(
        Configuration,
        helper: Arc<dyn Helper + Send + Sync>,
    ) -> futures::future::BoxFuture<'static, std::result::Result<(), Failed>>,
}

inventory::collect!(IndielinksTest);

#[allow(dead_code)] // not used by all test programs
pub struct BackgroundTest {
    pub name: &'static str,
    pub test_fn: fn(
        Configuration,
        Arc<dyn TasksBackend + Send + Sync>,
        Arc<dyn StorageBackend + Send + Sync>,
    ) -> futures::future::BoxFuture<'static, std::result::Result<(), Failed>>,
}

inventory::collect!(BackgroundTest);
