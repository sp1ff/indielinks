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

use indielinks_test::{delicious::delicious_smoke_test, test_healthcheck};

use common::{run, Configuration, Test};

use libtest_mimic::{Arguments, Trial};
use snafu::{prelude::*, Snafu};

use std::fmt::Display;

mod common;

#[derive(Snafu)]
enum Error {
    #[snafu(display("Failed to run {cmd}: {source}"))]
    Command { cmd: String, source: common::Error },
    #[snafu(display("Error obtaining test configuration: {source}"))]
    Configuration { source: common::Error },
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Command { cmd, source } => {
                write!(f, "Failed to run command {}: {}", cmd, source)
            }
            _ => Display::fmt(&self, f),
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

fn setup() -> Result<()> {
    teardown()?;
    run("../infra/scylla-up", &[]).context(CommandSnafu {
        cmd: "scylla-up".to_string(),
    })?;
    run("../infra/indielinks-up", &["indielinks-scylla.toml"]).context(CommandSnafu {
        cmd: "indielinks-up".to_string(),
    })
}

fn teardown() -> Result<()> {
    // Let's try & make this idempotent
    run("../infra/indielinks-down", &[]).context(CommandSnafu {
        cmd: "indielinks-down".to_string(),
    })?;
    run("../infra/scylla-down", &[]).context(CommandSnafu {
        cmd: "scylla-down".to_string(),
    })
}

inventory::submit!(Test {
    name: "test_healthcheck",
    test_fn: |cfg| {
        test_healthcheck(&cfg.url);
        Ok(())
    },
});

inventory::submit!(Test {
    name: "delicious_smoke_test",
    test_fn: |cfg| {
        delicious_smoke_test(&cfg.url, &cfg.username, &cfg.api_key);
        Ok(())
    },
});

fn main() -> Result<()> {
    let config = Configuration::new().context(ConfigurationSnafu)?;
    let args = Arguments::from_args();

    if !config.no_setup {
        setup()?;
    }

    // Nb. this program is always run from the root directory of the owning crate.
    let conclusion = libtest_mimic::run(
        &args,
        inventory::iter::<common::Test>
            .into_iter()
            .map(|trial| {
                Trial::test(trial.name, {
                    let cfg = config.clone();
                    move || (trial.test_fn)(cfg)
                })
            })
            .collect(),
    );

    if !config.no_teardown {
        let _ = teardown();
    }

    conclusion.exit();
}
