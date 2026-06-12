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

//! # [indielinks-cache] cluster integration tests
//!
//! Runs the test suite against three cluster configurations (fixtures):
//! in-memory log store, on-disk log store, and single-node.

use std::{process::ExitCode, result::Result as StdResult, str::FromStr};

use async_trait::async_trait;
use libtest_mimic::Failed;
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use tests_indielinks_cache::{
    admin, eviction, healthcheck, invalid_cache_id, multi_key, overwrite, slots, smoke,
};
use tests_support::{
    Fixture, IntegrationTest, SyncIntegrationTest, TestConfiguration, sync_integration_test,
};

mod common;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Error type                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to run {cmd}: {source}"))]
    Command { cmd: String, source: common::Error },
    #[snafu(display("Couldn't parse '{text}' as a fixture ID"))]
    FixtureId { text: String },
    #[snafu(display("{source}"))]
    IntegrationTest {
        #[snafu(source(from(tests_support::Error<CacheClusterFixture>, Box::new)))]
        source: Box<tests_support::Error<CacheClusterFixture>>,
    },
}

type Result<T> = StdResult<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                      Domain configuration                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheDomainConfiguration {
    #[serde(rename = "base-port")]
    pub base_port: u16,
}

impl Default for CacheDomainConfiguration {
    fn default() -> Self {
        Self { base_port: 20800 }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          Fixture ID                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CacheClusterId {
    InMemory,
    OnDisk,
    SingleNode,
}

impl FromStr for CacheClusterId {
    type Err = Error;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        match s {
            "in-memory" => Ok(CacheClusterId::InMemory),
            "on-disk" => Ok(CacheClusterId::OnDisk),
            "single-node" => Ok(CacheClusterId::SingleNode),
            _ => Err(FixtureIdSnafu { text: s.to_owned() }.build()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            Fixture                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CacheClusterFixture {
    id: CacheClusterId,
    name: &'static str,
}

impl CacheClusterFixture {
    pub const fn new(id: CacheClusterId, name: &'static str) -> Self {
        Self { id, name }
    }
}

inventory::collect!(CacheClusterFixture);

inventory::submit!(CacheClusterFixture::new(
    CacheClusterId::InMemory,
    "in-memory-cluster"
));
inventory::submit!(CacheClusterFixture::new(
    CacheClusterId::OnDisk,
    "on-disk-cluster"
));
inventory::submit!(CacheClusterFixture::new(
    CacheClusterId::SingleNode,
    "single-node-cluster"
));

#[async_trait]
impl Fixture for CacheClusterFixture {
    type Error = Error;
    type Backend = ();
    type Configuration = CacheDomainConfiguration;
    type Id = CacheClusterId;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn get_name(&self) -> String {
        self.name.to_owned()
    }

    async fn new_backend(&self, _: &Self::Configuration) -> StdResult<Self::Backend, Self::Error> {
        Ok(())
    }

    async fn setup(&self, config: &Self::Configuration) -> StdResult<(), Self::Error> {
        let port = format!("{}", config.base_port);
        match self.id {
            CacheClusterId::InMemory => {
                tracing::debug!("in-memory cluster up!");
                common::run("../infra/cache-test-cluster-down", ["in-memory"]).context(
                    CommandSnafu {
                        cmd: "cache-test-cluster-down",
                    },
                )?;
                common::run("../infra/cache-test-cluster-up", ["in-memory", &port]).context(
                    CommandSnafu {
                        cmd: "cache-test-cluster-up",
                    },
                )?;
            }
            CacheClusterId::OnDisk => {
                common::run("../infra/cache-test-cluster-down", ["on-disk"]).context(
                    CommandSnafu {
                        cmd: "cache-test-cluster-down",
                    },
                )?;
                common::run("../infra/cache-test-cluster-up", ["on-disk", &port]).context(
                    CommandSnafu {
                        cmd: "cache-test-cluster-up",
                    },
                )?;
            }
            CacheClusterId::SingleNode => {
                common::run("../infra/single-node-cluster-down", ["in-memory"]).context(
                    CommandSnafu {
                        cmd: "single-node-cluster-down",
                    },
                )?;
                common::run("../infra/single-node-cluster-up", ["in-memory", &port]).context(
                    CommandSnafu {
                        cmd: "single-node-cluster-up",
                    },
                )?;
            }
        }
        Ok(())
    }

    async fn teardown(&self, _: &Self::Configuration) -> StdResult<(), Self::Error> {
        match self.id {
            CacheClusterId::InMemory => {
                common::run("../infra/cache-test-cluster-down", ["in-memory"]).context(
                    CommandSnafu {
                        cmd: "cache-test-cluster-down",
                    },
                )?;
            }
            CacheClusterId::OnDisk => {
                common::run("../infra/cache-test-cluster-down", ["on-disk"]).context(
                    CommandSnafu {
                        cmd: "cache-test-cluster-down",
                    },
                )?;
            }
            CacheClusterId::SingleNode => {
                common::run("../infra/single-node-cluster-down", ["in-memory"]).context(
                    CommandSnafu {
                        cmd: "single-node-cluster-down",
                    },
                )?;
            }
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             Tests                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CacheClusterTest {
    name: &'static str,
    test_fn: fn(CacheDomainConfiguration) -> StdResult<(), Failed>,
    fixtures: Option<&'static [CacheClusterId]>,
}

impl CacheClusterTest {
    pub const fn new(
        name: &'static str,
        test_fn: fn(CacheDomainConfiguration) -> StdResult<(), Failed>,
        fixtures: Option<&'static [CacheClusterId]>,
    ) -> Self {
        Self {
            name,
            test_fn,
            fixtures,
        }
    }
}

inventory::collect!(CacheClusterTest);

// ---- 00: smoke ----

inventory::submit!(CacheClusterTest::new(
    "00_smoke",
    |config| smoke::test(config.base_port),
    Some(&[CacheClusterId::InMemory, CacheClusterId::OnDisk]),
));

inventory::submit!(CacheClusterTest::new(
    "00_single_node_smoke",
    |config| smoke::single_node(config.base_port),
    Some(&[CacheClusterId::SingleNode]),
));

// ---- 01: healthcheck ----

inventory::submit!(CacheClusterTest::new(
    "01_healthcheck",
    |config| healthcheck::test(config.base_port, 5),
    Some(&[CacheClusterId::InMemory, CacheClusterId::OnDisk]),
));

inventory::submit!(CacheClusterTest::new(
    "01_single_node_healthcheck",
    |config| healthcheck::single_node(config.base_port),
    Some(&[CacheClusterId::SingleNode]),
));

// ---- 01: multiple keys ----

inventory::submit!(CacheClusterTest::new(
    "01_multiple_keys",
    |config| multi_key::test(config.base_port),
    Some(&[CacheClusterId::InMemory, CacheClusterId::OnDisk]),
));

inventory::submit!(CacheClusterTest::new(
    "01_single_node_multiple_keys",
    |config| multi_key::single_node(config.base_port),
    Some(&[CacheClusterId::SingleNode]),
));

// ---- 02: overwrite ----

inventory::submit!(CacheClusterTest::new(
    "02_overwrite",
    |config| overwrite::test(config.base_port),
    Some(&[CacheClusterId::InMemory, CacheClusterId::OnDisk]),
));

inventory::submit!(CacheClusterTest::new(
    "02_single_node_overwrite",
    |config| overwrite::single_node(config.base_port),
    Some(&[CacheClusterId::SingleNode]),
));

// ---- 02: invalid cache ID ----

inventory::submit!(CacheClusterTest::new(
    "02_invalid_cache_id",
    |config| invalid_cache_id::test(config.base_port),
    None, // runs on all fixtures
));

// ---- 03: LRU eviction (single-node only) ----

inventory::submit!(CacheClusterTest::new(
    "03_lru_eviction",
    |config| eviction::test(config.base_port),
    Some(&[CacheClusterId::SingleNode]),
));

inventory::submit!(CacheClusterTest::new(
    "04_slots",
    |config| slots::test(config.base_port),
    Some(&[CacheClusterId::InMemory, CacheClusterId::OnDisk]),
));

// ---- 04: cluster membership shrink ----

inventory::submit!(CacheClusterTest::new(
    "100_remove_member",
    |config| admin::remove_member(config.base_port),
    Some(&[CacheClusterId::InMemory, CacheClusterId::OnDisk]),
));

impl IntegrationTest for CacheClusterTest {
    type F = CacheClusterFixture;

    fn germane(&self, id: CacheClusterId) -> bool {
        self.fixtures.map(|f| f.contains(&id)).unwrap_or(true)
    }

    fn name(&self) -> String {
        self.name.to_owned()
    }
}

impl SyncIntegrationTest for CacheClusterTest {
    fn run(&self, config: CacheDomainConfiguration, _backend: ()) -> StdResult<(), Failed> {
        (self.test_fn)(config)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             main()                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() -> Result<ExitCode> {
    sync_integration_test(
        TestConfiguration::<CacheClusterFixture>::new_or_default(
            "INDIELINKS_CACHE_CLUSTER_TESTS_CONFIG",
        )
        .context(IntegrationTestSnafu)?,
        inventory::iter::<CacheClusterFixture>.into_iter(),
        inventory::iter::<CacheClusterTest>,
    )
    .context(IntegrationTestSnafu)
}
