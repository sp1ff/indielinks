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
// You should have received a copy of the GNU General Public License along with indielinks.  If not,
// see <http://www.gnu.org/licenses/>.

//! # The [indielinks-cache] Integration Test Support

use std::{ffi::OsStr, process::Command};

use snafu::{Backtrace, ResultExt, Snafu};

pub mod smoke;

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
    #[snafu(display("Failed to execute {command}: {source}"))]
    Process {
        command: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn run<I, S>(cmd: &str, args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(cmd)
        .args(args)
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
