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

//! # Integration Tests for [indielinks] as a Web Service
//!
//! [indielinks]: ../indielinks/index.html
//!
//! ## Introduction
//!
//! This is an integration test for the [indielinks] web service. See the [tests-support]
//! documentation for a lengthy discussion of the [indielinks] integration testing strategy.
//!
//! [tests-support]: ../tests-support/index.html
//!
//! ## Running these Tests
//!
//! To run this integration tests as part of the overall suite, you don't hve to do anything special
//! by design: `cargo test` will execute it across all fixtures. The usual Cargo arguments apply, as
//! well. So, for instance, to execute _only_ this test, and only, say, the healthcheck test, but
//! across all fixtures with which it's registered, you'd say (from the workspace route):
//!
//! ```bash
//! cargo test -p tests-indielinks --test=smoke-tests healthcheck
//! ```
//!
//! Since the fixture name is integrated into the tests name (as seen by [libtest-mimic]), you can
//! only run it only for, say the DDB single node fixture (which is, in fact, the only fixture for
//! which it is registered) as:
//!
//! ```bash
//! cargo test -p tests-indielinks --test=smoke-tests dynamodb-single-node/000test_healthcheck
//! ```
//!
//! To select only certain fixtures, you'll have to set the `INDIELINKS_TEST_FIXTURES` environment
//! variable to a comma-separated list of fixture names. The fixture names are:
//!
//! - scylla-single-node
//! - scylla-single-node-pre-charged
//! - scylla-cluster
//! - dynamodb-single-node
//! - dynamodb-single-node-pre-charged
//! - dynamodb-cluster
//!
//! To select a non-default configuration, you'll need to place it in a TOML file and set the
//! `INDIELINKS_TEST_CONFIG` environment variable. Finally, the [tracing-subscriber] crate is used
//! for logging, so eht `RUST_LOG` environment variable is also respected.

use std::{
    collections::HashMap, process::ExitCode, result::Result as StdResult, str::FromStr, sync::Arc,
};

use async_trait::async_trait;
use aws_sdk_dynamodb::{error::SdkError, operation::put_item::PutItemError, types::AttributeValue};
use futures::future::BoxFuture;
use indielinks_shared::entities::Username;
use libtest_mimic::Failed;
use serde::Deserialize;
use snafu::{Backtrace, ResultExt, Snafu};
use tests_support::{async_integration_test, TestConfiguration};
use tracing::{instrument, Level};
use url::Url;

use indielinks::{peppers::Peppers, scylla::execute_cql};

use tests_indielinks::{
    activity_pub::{as_follower, context_with_mastodon, posting_creates_note, send_follow},
    delicious::{delicious_smoke_test, posts_all, posts_recent, tags_rename_and_delete},
    follow::accept_follow_smoke,
    helper::{DynamoConfig, DynamoDBHelper, Helper, ScyllaConfig, ScyllaHelper},
    test_healthcheck,
    users::{test_mint_key, test_signup},
    webfinger::webfinger_smoke,
};

use common::run;

mod common;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Error type                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to charge table {name}: {source:#?}"))]
    ChargeTable {
        name: String,
        #[snafu(source(from(SdkError<PutItemError, aws_sdk_dynamodb::config::http::HttpResponse>, Box::new)))]
        source: Box<SdkError<PutItemError, aws_sdk_dynamodb::config::http::HttpResponse>>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to run {cmd}: {source}"))]
    Command { cmd: String, source: common::Error },
    #[snafu(display("While obtaining test configuration, {source}"))]
    Configuration {
        #[snafu(source(from(tests_support::Error<Fixture>, Box::new)))]
        source: Box<tests_support::Error<Fixture>>,
    },
    #[snafu(display("Couldn't parse {text} as a FixtureId"))]
    FixtureId { text: String },
    #[snafu(display("Test helper: {source}"))]
    Helper {
        source: tests_indielinks::helper::Error,
    },
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
//                    utility functions for setting-up & tearing-down fixtures                    //
////////////////////////////////////////////////////////////////////////////////////////////////////

// Synchronous; these won't return until both the database & indielinks are healthchecking
// successfully

#[instrument(level = Level::DEBUG)]
fn setup_alternator_single_node() -> Result<()> {
    teardown_single_node()?;
    run("../infra/scylla-up", &[]).context(CommandSnafu {
        cmd: "scylla-up".to_string(),
    })?;
    run("../infra/indielinks-up", &["indielinksd-alternator.toml"]).context(CommandSnafu {
        cmd: "indielinks-up".to_string(),
    })
}

#[instrument(level = Level::DEBUG)]
fn setup_scylla_single_node() -> Result<()> {
    teardown_single_node()?;
    run("../infra/scylla-up", &[]).context(CommandSnafu {
        cmd: "scylla-up".to_string(),
    })?;
    run("../infra/indielinks-up", &["indielinksd-scylla.toml"]).context(CommandSnafu {
        cmd: "indielinks-up".to_string(),
    })
}

#[instrument(level = Level::DEBUG)]
fn teardown_single_node() -> Result<()> {
    run("../infra/indielinks-down", &[]).context(CommandSnafu {
        cmd: "indielinks-down".to_string(),
    })?;
    run("../infra/scylla-down", &[]).context(CommandSnafu {
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
    ScyllaSingleNodePreCharged,
    ScyllaCluster,
    DynamoDBSingleNode,
    DynamoDBSingleNodePreCharged,
    DynamoDBCluster,
}

impl FromStr for FixtureId {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "scylla-single-node" => Ok(FixtureId::ScyllaSingleNode),
            "scylla-single-node-pre-charged" => Ok(FixtureId::ScyllaSingleNodePreCharged),
            "scylla-cluster" => Ok(FixtureId::ScyllaCluster),
            "dynamodb-single-node" => Ok(FixtureId::DynamoDBSingleNode),
            "dynamodb-single-node-pre-charged" => Ok(FixtureId::DynamoDBSingleNodePreCharged),
            "dynamodb-cluster" => Ok(FixtureId::DynamoDBCluster),
            _ => Err(FixtureIdSnafu { text: s.to_owned() }.build()),
        }
    }
}

/// Domain-specific configuration
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    /// The location at which the indielinks instance under test can be reached
    pub indielinks: Url,
    pub scylla: ScyllaConfig,
    pub dynamo: DynamoConfig,
    /// The username of the test user that comes "pre-configured" with our integration tests
    // I think I'd like to get rid of this altogether & just have tests create their own test users,
    // but it for now for the sake of older tests.
    pub username: Username,
    /// The API key of test user that comes "pre-configured" with our integration tests
    // I think I'd like to get rid of this altogether & just have tests create their own test users,
    // but this is thoroughly woven into the test suite, at this point. I think I'd like to make
    // that a separate issue.
    #[serde(rename = "api-key")]
    pub api_key: String,
    pub pepper: Peppers,
}

// I'm not sure I'm going to be able to keep this `Default`, but I'm going to start as if I can;
// that way, the user can just say `cargo test` with no config files, no environment variables.
impl Default for Configuration {
    fn default() -> Self {
        Self {
            // I'd love to get rid of the requirement to add "indiemark.local" to your `/etc/hosts`,
            // but one thing at a time.
            indielinks: Url::parse("http://indiemark.local:20679").unwrap(/* known good */),
            scylla: Default::default(),
            dynamo: Default::default(),
            username: Username::new("sp1ff").unwrap(/* known good */),
            api_key: "v1:5eb56ceebb7425aafe36eabca8b923054b4907d9375acd0b9950c51b57b201fb73e437428050f451b57632f99a3bbd5bed1c0f51cc0df752147090ed26e975f4".to_owned(),
            pepper: Peppers::default(),
        }
    }
}

#[derive(Debug)]
pub struct Fixture {
    id: FixtureId,
    name: &'static str,
}

const CQL: &str = include_str!("dev-charge.cql");

impl Fixture {
    async fn charge_tables_dynamodb(&self, client: &aws_sdk_dynamodb::Client) -> Result<()> {
        client
        .put_item()
        .table_name("users")
        .set_item(Some(HashMap::from([
            ("id".to_string(), AttributeValue::S("9a1df092-cd69-4c64-91f7-b8fb4022ea49".to_string())),
            ("username".to_string(), AttributeValue::S("sp1ff".to_string())),
            ("discoverable".to_string(), AttributeValue::Bool(true)),
            ("display_name".to_string(), AttributeValue::S("sp1ff".to_string())),
            ("summary".to_string(), AttributeValue::S("Defender of the galaxy".to_string())),
            ("pub_key_pem".to_string(), AttributeValue::S("-----BEGIN RSA PUBLIC KEY-----MIICCgKCAgEAlpLzxYKh8aT90oMK6AeeKMCj220BhuWCozk06DsjF7KeOsCesiDxNwpKOuFvdljc8d6fhO1IWM75KplDs0vgPegdmxgMA/xwRpRt1L0x5rzOv8m2k6TRGgx8CquzimwAWG7M8pz2vTlb2HeRNHwsoyWd0hYtfFzrYfVQiBVI7MGul7dwyO3AIO94tW5cok7jfL8XkPo9bqrLTwLL/jw61vleuhcFtA7lf0H+chD6ikGcVqGD++aRmRdmnvVRZcS2ySo5btXQaT/THkouq2ZqWA1rpz0Ta645qE8LdfatqTBhPomOCQOViaT+sxrem6pEAUlJwP+/ibYO6ZOFGxZXAgH4WaEExPjIeJdOBP/flkx+YnvYb62e+Q7J+URVl6Y92ZMGmWBNz88zLu6uODD75p2Lyo0kG1Gr6qDChtqmH4fdKMZOXKxTQzwtN68NZmjUYR5ZVZYn6sTmzLT9RPiSj4NFzB28z7auNVRbROpNpSKpUonp3Bb6hy7aEfl1iaOeijjIQw26fZgxEJO624ZbpLLuLY+A/4pDNlawbyTK8WOYCZLUYn2w6IolpHVKh7/eP7qDy4TNbX439W0DLBRoCzA+8Vv5SLU8pT2coiXM65Dc3L6NGOwIjuoId5+Ei9SSP29GU5eu5rVb8JzM3lkmIujFVwqxOrdHu6CSrQcuf+MCAwEAAQ==-----END RSA PUBLIC KEY-----".to_string())),
            ("priv_key_pem".to_string(), AttributeValue::S("-----BEGIN RSA PRIVATE KEY-----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-----END RSA PRIVATE KEY-----".to_string())),
            ("api_keys".to_string(),
             AttributeValue::M(
                 HashMap::from(
                     [("One".to_string(),
                       AttributeValue::M(HashMap::from([
                           ("expiry".to_string(), AttributeValue::Null(true)),
                           ("version".to_string(), AttributeValue::S("1".to_string())),
                           ("key_material_hash".to_string(), AttributeValue::L(vec![
                               AttributeValue::N("18".to_string()),
                               AttributeValue::N("149".to_string()),
                               AttributeValue::N("37".to_string()),
                               AttributeValue::N("96".to_string()),
                               AttributeValue::N("225".to_string()),
                               AttributeValue::N("143".to_string()),
                               AttributeValue::N("211".to_string()),
                               AttributeValue::N("193".to_string()),
                               AttributeValue::N("123".to_string()),
                               AttributeValue::N("75".to_string()),
                               AttributeValue::N("85".to_string()),
                               AttributeValue::N("183".to_string()),
                               AttributeValue::N("244".to_string()),
                               AttributeValue::N("48".to_string()),
                               AttributeValue::N("52".to_string()),
                               AttributeValue::N("136".to_string()),
                               AttributeValue::N("147".to_string()),
                               AttributeValue::N("19".to_string()),
                               AttributeValue::N("233".to_string()),
                               AttributeValue::N("7".to_string()),
                               AttributeValue::N("156".to_string()),
                               AttributeValue::N("48".to_string()),
                               AttributeValue::N("144".to_string()),
                               AttributeValue::N("249".to_string()),
                               AttributeValue::N("223".to_string()),
                               AttributeValue::N("1".to_string()),
                               AttributeValue::N("93".to_string()),
                               AttributeValue::N("249".to_string()),
                           ]))
                       ])))]))
            ),
            ("password_hash".to_string(), AttributeValue::S("$argon2id$v=19$m=19456,t=2,p=1$P2VPm95xh/Pb5hBbokpHTg$TbheNsNWEk8OKL17u/GYhnLwgo8DCxnrzm0SJ+R/AUM".to_string())),
            ("pepper_version".to_string(), AttributeValue::S("pepper-ver:20250213".to_string())),
            ])))
        .send()
        .await
        .context(ChargeTableSnafu { name: "users".to_string()})?;
        client
            .put_item()
            .table_name("unique_usernames")
            .set_item(Some(HashMap::from([
                (
                    "username".to_string(),
                    AttributeValue::S("sp1ff".to_string()),
                ),
                (
                    "id".to_string(),
                    AttributeValue::S("9a1df092-cd69-4c64-91f7-b8fb4022ea49".to_string()),
                ),
            ])))
            .send()
            .await
            .context(ChargeTableSnafu {
                name: "unique_usernames".to_string(),
            })
            .map(|_| ())
    }
    async fn charge_tables_scylla(&self, session: &scylla::client::session::Session) -> Result<()> {
        execute_cql(session, CQL)
            .await
            .context(ScyllaSnafu)
            .map(|_| ())
    }
}

#[async_trait]
impl tests_support::Fixture for Fixture {
    type Error = Error;
    type Backend = Arc<dyn Helper + Send + Sync>;
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
            FixtureId::ScyllaSingleNode
            | FixtureId::ScyllaSingleNodePreCharged
            | FixtureId::ScyllaCluster => {
                // This is really kind of lame; it would be nicer to do this as part of `setup()`,
                // but because `Fixture` instances are immutable, we have to do it here.
                let backend = Arc::new(
                    ScyllaHelper::new(&config.scylla)
                        .await
                        .context(HelperSnafu)?,
                );
                if self.id == FixtureId::ScyllaSingleNodePreCharged {
                    self.charge_tables_scylla(backend.get_client()).await?;
                }
                backend as Arc<dyn Helper + Send + Sync>
            }
            FixtureId::DynamoDBSingleNode
            | FixtureId::DynamoDBSingleNodePreCharged
            | FixtureId::DynamoDBCluster => {
                // This is really kind of lame; it would be nicer to do this as part of `setup()`,
                // but because `Fixture` instances are immutable, we have to do it here.
                let backend = Arc::new(
                    DynamoDBHelper::new(&config.dynamo)
                        .await
                        .context(HelperSnafu)?,
                );
                if self.id == FixtureId::DynamoDBSingleNodePreCharged {
                    self.charge_tables_dynamodb(backend.get_client()).await?;
                }
                backend as Arc<dyn Helper + Send + Sync>
            }
        };
        Ok(backend)
    }

    async fn setup(&self, _config: &Self::Configuration) -> StdResult<(), Self::Error> {
        match self.id {
            FixtureId::ScyllaSingleNode | FixtureId::ScyllaSingleNodePreCharged => {
                setup_scylla_single_node()
            }
            FixtureId::DynamoDBSingleNode | FixtureId::DynamoDBSingleNodePreCharged => {
                setup_alternator_single_node()
            }
            _ => unimplemented!(),
        }
    }

    async fn teardown(&self, _config: &Self::Configuration) -> StdResult<(), Self::Error> {
        match self.id {
            FixtureId::ScyllaSingleNode
            | FixtureId::DynamoDBSingleNode
            | FixtureId::ScyllaSingleNodePreCharged
            | FixtureId::DynamoDBSingleNodePreCharged => teardown_single_node(),
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
    id: FixtureId::ScyllaSingleNodePreCharged,
    name: "Scylla (single-node, pre-charged)"
});

inventory::submit!(Fixture {
    id: FixtureId::DynamoDBSingleNode,
    name: "Alternator (single-node)"
});

inventory::submit!(Fixture {
    id: FixtureId::DynamoDBSingleNodePreCharged,
    name: "Alternator (single-node, pre-charged)"
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
        Arc<dyn Helper + Send + Sync>,
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
        backend: Arc<dyn Helper + Send + Sync>,
    ) -> StdResult<(), Failed> {
        (self.test_fn)(config, backend).await
    }
}

inventory::collect!(Test);

inventory::submit!(Test {
    name: "000test_healthcheck",
    test_fn: |cfg, _| { Box::pin(test_healthcheck(cfg.indielinks)) },
    // I don't think there's much point in running this in more than one fixture
    fixtures: Some(&[FixtureId::DynamoDBSingleNode])
});

inventory::submit!(Test {
    name: "001delicious_smoke_test",
    test_fn: |cfg, helper| {
        Box::pin(delicious_smoke_test(
            cfg.indielinks,
            cfg.username,
            cfg.api_key,
            helper,
        ))
    },
    fixtures: Some(&[
        FixtureId::ScyllaSingleNodePreCharged,
        FixtureId::DynamoDBSingleNodePreCharged
    ]),
});

inventory::submit!(Test {
    name: "010delicious_posts_recent",
    test_fn: |cfg, helper| {
        Box::pin(posts_recent(
            cfg.indielinks, /*url*/
            cfg.username,
            cfg.api_key,
            helper,
        ))
    },
    fixtures: Some(&[
        FixtureId::ScyllaSingleNodePreCharged,
        FixtureId::DynamoDBSingleNodePreCharged
    ]),
});

inventory::submit!(Test {
    name: "011delicious_posts_all",
    test_fn: |cfg, helper| {
        Box::pin(posts_all(
            cfg.indielinks, /*url*/
            cfg.username,
            cfg.api_key,
            helper,
        ))
    },
    fixtures: Some(&[
        FixtureId::ScyllaSingleNodePreCharged,
        FixtureId::DynamoDBSingleNodePreCharged
    ]),
});

inventory::submit!(Test {
    name: "012delicious_tags_rename_and_delete",
    test_fn: |cfg: Configuration, helper| {
        Box::pin(tags_rename_and_delete(
            cfg.indielinks,
            cfg.username,
            cfg.api_key,
            helper,
        ))
    },
    fixtures: Some(&[
        FixtureId::ScyllaSingleNodePreCharged,
        FixtureId::DynamoDBSingleNodePreCharged
    ]),
});

inventory::submit!(Test {
    name: "020user_test_signup",
    test_fn: |cfg: Configuration, helper| { Box::pin(test_signup(cfg.indielinks, helper)) },
    fixtures: None,
});

inventory::submit!(Test {
    name: "030webfinger_smoke",
    test_fn: |cfg: Configuration, _helper| {
        Box::pin(webfinger_smoke(
            cfg.indielinks.clone(),
            cfg.username,
            cfg.indielinks.clone().try_into().unwrap(/* Fail the test if this fails */),
        ))
    },
    fixtures: Some(&[
        FixtureId::ScyllaSingleNodePreCharged,
        FixtureId::DynamoDBSingleNodePreCharged
    ]),
});

inventory::submit!(Test {
    name: "040follow_smoke",
    test_fn: |cfg: Configuration, _helper| {
        Box::pin(accept_follow_smoke(
            cfg.indielinks.clone(),
            cfg.username,
            cfg.indielinks.try_into().unwrap(),
        ))
    },
    fixtures: Some(&[
        FixtureId::ScyllaSingleNodePreCharged,
        FixtureId::DynamoDBSingleNodePreCharged
    ]),
});

inventory::submit!(Test {
    name: "050post_creates_note",
    test_fn: |cfg: Configuration, helper| {
        let (version, pepper) = cfg.pepper.current_pepper().unwrap();
        Box::pin(posting_creates_note(
            cfg.indielinks,
            version,
            pepper,
            helper,
        ))
    },
    fixtures: None,
});

inventory::submit!(Test {
    name: "060send_follow",
    test_fn: |cfg: Configuration, helper| {
        let (version, pepper) = cfg.pepper.current_pepper().unwrap();
        Box::pin(send_follow(cfg.indielinks, version, pepper, helper))
    },
    fixtures: None,
});

inventory::submit!(Test {
    name: "070as_follower",
    test_fn: |cfg: Configuration, helper| {
        let (version, pepper) = cfg.pepper.current_pepper().unwrap();
        Box::pin(as_follower(cfg.indielinks, version, pepper, helper))
    },
    fixtures: None,
});

inventory::submit!(Test {
    name: "080user_test_mint_key",
    test_fn: |cfg: Configuration, helper| { Box::pin(test_mint_key(cfg.indielinks, helper)) },
    fixtures: None,
});

inventory::submit!(Test {
    name: "090context_with_mastodon",
    test_fn: |cfg: Configuration, helper| {
        let (version, pepper) = cfg.pepper.current_pepper().unwrap();
        Box::pin(context_with_mastodon(
            cfg.indielinks,
            version,
            pepper,
            helper,
        ))
    },
    fixtures: None,
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
