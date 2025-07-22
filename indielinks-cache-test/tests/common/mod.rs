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

//! # The [indielinks-cache] Integration Test Framework

use std::{env, fs, process::Command};

use libtest_mimic::Failed;
use serde::Deserialize;
use snafu::{IntoError, ResultExt, Snafu};
use tap::Pipe;
use tracing::Level;

pub mod smoke;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to run {command}; stdout was '{stdout}' & stderr was '{stderr}'."))]
    Command {
        command: String,
        code: Option<i32>,
        stdout: String,
        stderr: String,
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
    },
    #[snafu(display("Failed to read {pth}: {source}"))]
    Read { pth: String, source: std::io::Error },
}

type Result<T> = std::result::Result<T, Error>;

pub fn run(cmd: &str, args: &[&str], rust_log: Option<&str>) -> Result<()> {
    let mut command = Command::new(cmd);
    command.args(args.into_iter());
    if let Some(rust_log) = rust_log {
        command.env("RUST_LOG", rust_log);
    }

    let output = command.output().context(ProcessSnafu {
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

/// Common [indielinks-cache] test configuration
#[derive(Clone, Debug, Deserialize)]
pub struct Configuration {
    #[serde(rename = "no-setup")]
    pub no_setup: bool,
    #[serde(rename = "no-teardown")]
    pub no_teardown: bool,
    pub logging: bool,
    #[serde(deserialize_with = "de_level::deserialize")]
    pub log_level: Level,
    pub base_port: u16,
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

impl Configuration {
    /// Obtain a [Configuration]
    ///
    /// Check the `INDIELINKS_CACHE_TEST_CONFIG` environment variable; if defined & non-empty,
    /// attempt to parse a [Configuration] from the file named therein; else return a default
    /// instance.
    pub fn new() -> Result<Configuration> {
        match env::var("INDIELINKS_CACHE_TEST_CONFIG") {
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
    /// When invoked with a bare `cargo test` (i.e. without `INDIELINKS_CACHE_TEST_CONFIG` set),
    /// this is the configuration that will be used, so be sure the tests will pass with it.
    fn default() -> Self {
        Configuration {
            no_setup: false,
            no_teardown: false,
            logging: false,
            log_level: Level::INFO,
            base_port: 20800,
        }
    }
}

pub struct Test {
    pub name: &'static str,
    pub test_fn: fn(Configuration) -> std::result::Result<(), Failed>,
}

inventory::collect!(Test);
