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

use std::process::Command;

use libtest_mimic::Failed;
use serde::Deserialize;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to run {command}; stdout was '{stdout}' & stderr was '{stderr}'."))]
    Command {
        command: String,
        code: Option<i32>,
        stdout: String,
        stderr: String,
    },
    #[snafu(display("Failed to execute {command}: {source}"))]
    Process {
        command: String,
        source: std::io::Error,
    },
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
pub struct Configuration;

pub struct InMemoryTest {
    pub name: &'static str,
    pub test_fn: fn(Configuration) -> std::result::Result<(), Failed>,
}

inventory::collect!(InMemoryTest);
