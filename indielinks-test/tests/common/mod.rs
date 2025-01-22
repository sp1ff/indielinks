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

use indielinks::entities::Username;

use libtest_mimic::Failed;
use reqwest::Url;
use serde::Deserialize;
use snafu::{prelude::*, Backtrace, IntoError};
use tap::Pipe;

use std::{env, fs, process::Command};

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

/// Common test configuration
///
/// Not sure about having all tests share one configuration format, coding speculatively.
#[derive(Clone, Debug, Deserialize)]
pub struct Configuration {
    #[serde(rename = "no-setup")]
    pub no_setup: bool,
    #[serde(rename = "no-teardown")]
    pub no_teardown: bool,
    pub url: Url,
    pub username: Username,
    pub api_key: String,
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
            api_key: "6caf392688cc6b164fe88b786acb6ab6ed4eda6e4b1a0c1daf09aa9da3c89873".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct Test {
    pub name: &'static str,
    pub test_fn: fn(cfg: Configuration) -> std::result::Result<(), Failed>,
}

inventory::collect!(Test);
