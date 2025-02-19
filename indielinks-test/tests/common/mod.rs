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
use either::Either;
use indielinks::entities::Username;

use indielinks_test::Helper;
use libtest_mimic::Failed;
use reqwest::Url;
use serde::Deserialize;
use snafu::{prelude::*, Backtrace, IntoError};
use tap::Pipe;

use std::{env, fs, process::Command, sync::Arc};

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

pub fn run(cmd: &str, args: &[&str]) -> Result<()> {
    let output = Command::new(cmd)
        .args(args.into_iter())
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

#[derive(Clone, Debug, Deserialize)]
#[allow(dead_code)]
pub struct ScyllaConfig {
    /// ScyllaDB credentials, if authentication is to be used. Nb that I'm not using
    /// `secrecy::SecretString` here; I assume that if you're running this test suite, it's
    /// against a local, unsecured instance.
    pub credentials: Option<(String, String)>,
    /// ScyllaDB hosts; specify as "host:port"
    pub hosts: Vec<String>,
}

impl Default for ScyllaConfig {
    fn default() -> Self {
        ScyllaConfig {
            credentials: None,
            hosts: vec![String::from("localhost:9042")],
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[allow(dead_code)]
pub struct DynamoConfig {
    /// AWS credentials: key ID & secret key; you'll pretty-much always need to specify these
    /// when running against DDB, but one could be talking to a local SycllaDB over the
    /// Alternator interface locally and have the cluster be open. Nb that I'm not using
    /// `secrecy::SecretString` here; I assume that if you're running this test suite, it's
    /// against a local, unsecured instance.
    pub credentials: Option<(String, String)>,
    /// You can find DynamoDB in a few ways. If you're truly talking to DynamoDB in AWS, you can
    /// give a region. You can also specify an URL (like
    /// `https://dynamodb.us-west-2.amazonaws.com`). If you're talking to ScyllaDB over the
    /// Alternator interface, we're going to have to handle load-balancing on the client-side,
    /// so specify more than one.
    #[serde(with = "either::serde_untagged")]
    pub location: Either<String, Vec<Url>>,
}

impl Default for DynamoConfig {
    fn default() -> Self {
        DynamoConfig {
            credentials: None,
            location: Either::Right(vec![
                Url::parse("http://127.0.0.1:8042").unwrap(),
                Url::parse("http://127.0.0.1:8043").unwrap(),
                Url::parse("http://127.0.0.1:8044").unwrap(),
            ]),
        }
    }
}

/// Common test configuration
///
/// Not sure about having all tests share one configuration format, coding speculatively.
#[derive(Clone, Debug, Deserialize)]
#[allow(dead_code)]
pub struct Configuration {
    #[serde(rename = "no-setup")]
    pub no_setup: bool,
    #[serde(rename = "no-teardown")]
    pub no_teardown: bool,
    pub url: Url,
    pub domain: String,
    pub username: Username,
    pub api_key: String,
    pub scylla: ScyllaConfig,
    pub dynamo: DynamoConfig,
}

impl Configuration {
    /// Obtain a [Configuration]
    ///
    /// Check the `INDIELINKS_TEST_CONFIG` environment variable; if defined & non-empty, attempt to
    /// parse a [Configuration] from the file named therein; else return a default instance.
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
    fn default() -> Self {
        Configuration {
            no_setup: false,
            no_teardown: false,
            url: Url::parse("http://localhost:20673").unwrap(/* known good */),
            username: Username::new("sp1ff").unwrap(/* known good */),
            domain: "indiemark.sh".to_owned(),
            api_key: "6caf392688cc6b164fe88b786acb6ab6ed4eda6e4b1a0c1daf09aa9da3c89873".to_owned(),
            scylla: ScyllaConfig::default(),
            dynamo: DynamoConfig::default(),
        }
    }
}

pub struct Test {
    pub name: &'static str,
    /// `test_fn` must be the address of an async function taking a copy of the test's
    /// [Configuration] along with a reference to a [Helper] implementation.
    pub test_fn: fn(
        Configuration,
        helper: Arc<dyn Helper + Send + Sync>,
    ) -> futures::future::BoxFuture<'static, std::result::Result<(), Failed>>,
}

inventory::collect!(Test);
