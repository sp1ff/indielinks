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

//! # delicious-alternator
//!
//! Integration tests run against an indielinks configured with the DynamoDB storage back-end.
use common::{run, Configuration, IndielinksTest};
use indielinks::{
    dynamodb::{add_followers, add_following, add_user, create_client},
    entities::{FollowId, User},
    peppers::{Pepper, Version as PepperVersion},
};
use indielinks_shared::{Post, StorUrl, UserEmail, UserId, Username};
use indielinks_test::{
    activity_pub::{as_follower, posting_creates_note, send_follow},
    delicious::{delicious_smoke_test, posts_all, posts_recent, tags_rename_and_delete},
    follow::accept_follow_smoke,
    test_healthcheck,
    users::{test_mint_key, test_signup},
    webfinger::webfinger_smoke,
    Helper,
};

use async_trait::async_trait;
use aws_sdk_dynamodb::{
    error::SdkError,
    operation::put_item::PutItemError,
    types::{AttributeValue, DeleteRequest, Select, WriteRequest},
};
use itertools::Itertools;
use libtest_mimic::{Arguments, Failed, Trial};
use secrecy::SecretString;
use serde_dynamo::aws_sdk_dynamodb_1::from_items;
use snafu::{prelude::*, Backtrace, Snafu};
use tap::Pipe;
use tokio::runtime::Runtime;
use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter, Registry};

use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    fmt::Display,
    io,
    sync::Arc,
};

mod common;

#[derive(Snafu)]
enum Error {
    #[snafu(display("Failed to charge table {name}: {source:#?}"))]
    ChargeTable {
        name: String,
        #[snafu(source(from(SdkError<PutItemError, aws_sdk_dynamodb::config::http::HttpResponse>, Box::new)))]
        source: Box<SdkError<PutItemError, aws_sdk_dynamodb::config::http::HttpResponse>>,
        backtrace: Backtrace,
    },
    #[snafu(display("While creating the DDB client, {source}"))]
    Client { source: indielinks::dynamodb::Error },
    #[snafu(display("Failed to run {cmd}: {source}"))]
    Command { cmd: String, source: common::Error },
    #[snafu(display("Error obtaining test configuration: {source}"))]
    Configuration { source: common::Error },
    #[snafu(display("Failed to deserialize a Post: {source}"))]
    DePost {
        source: serde_dynamo::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a User: {source}"))]
    DeUser {
        source: serde_dynamo::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to parse RUST_LOG: {source}"))]
    Filter {
        source: tracing_subscriber::filter::FromEnvError,
    },
    #[snafu(display("User {username} appears more than once in the database"))]
    MultipleUsers {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("No endpoint URLs specified"))]
    NoEndpoints { backtrace: Backtrace },
    #[snafu(display("No user {username}"))]
    NoSuchUser {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("DynamoDB query failed: {source}"))]
    Query {
        source: aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_dynamodb::operation::query::QueryError,
            aws_sdk_dynamodb::config::http::HttpResponse,
        >,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to set the global tracing subscriber: {source}"))]
    SetGlobalDefault {
        source: tracing::subscriber::SetGlobalDefaultError,
    },
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

type StdResult<T, E> = std::result::Result<T, E>;

fn setup() -> Result<()> {
    teardown()?;
    run("../infra/scylla-up", &[]).context(CommandSnafu {
        cmd: "scylla-up".to_string(),
    })?;
    run("../infra/indielinks-up", &["indielinksd-alternator.toml"]).context(CommandSnafu {
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

/// Application state shared across all tests
struct State {
    client: ::aws_sdk_dynamodb::Client,
}

impl State {
    pub async fn new(cfg: &Configuration) -> Result<State> {
        Ok(State {
            client: create_client(&cfg.dynamo.location, &cfg.dynamo.credentials)
                .await
                .context(ClientSnafu)?,
        })
    }
    async fn id_for_username(&self, username: &Username) -> Result<Option<UserId>> {
        Ok(self
            .client
            .query()
            .table_name("users")
            .index_name("users_by_username")
            .key_condition_expression("username=:u")
            .expression_attribute_values(":u", AttributeValue::S(username.to_string()))
            .send()
            .await
            .context(QuerySnafu)?
            .items()
            .to_vec()
            .pipe(from_items::<User>)
            .context(DeUserSnafu)?
            .into_iter()
            .at_most_one()
            .map_err(|_| {
                MultipleUsersSnafu {
                    username: username.clone(),
                }
                .build()
            })?
            .map(|user| user.id().clone()))
    }
}

#[async_trait]
impl Helper for State {
    async fn clear_posts(&self, username: &Username) -> std::result::Result<(), Failed> {
        let user_id = self
            .id_for_username(username)
            .await?
            .context(NoSuchUserSnafu {
                username: username.clone(),
            })?;

        // Arrrrrgghhhhh... DDB provides no "truncate" operation. We need to `BatchWrite` (or drop &
        // recreate the table). Hopefully, no test will create so many posts that we can't do this
        // in-memory:
        let mut posts = self
            .client
            .query()
            .table_name("posts")
            .select(Select::AllAttributes)
            .key_condition_expression("user_id=:id")
            .expression_attribute_values(":id", AttributeValue::S(user_id.to_string()))
            .send()
            .await
            .context(QuerySnafu)?
            .items()
            .to_vec()
            .pipe(from_items::<Post>)
            .context(DePostSnafu)?
            .into_iter()
            .map(|post| {
                DeleteRequest::builder()
                    .key("user_id", AttributeValue::S(post.user_id().to_string()))
                    .key("url", AttributeValue::S(post.url().to_string()))
                    .build()
            })
            .collect::<StdResult<Vec<DeleteRequest>, _>>()?
            .into_iter()
            .map(|dreq| WriteRequest::builder().delete_request(dreq).build())
            .collect::<Vec<WriteRequest>>();

        while !posts.is_empty() {
            let this_batch: Vec<_> = posts.drain(..min(posts.len(), 25)).collect();
            self.client
                .batch_write_item()
                .request_items("posts", this_batch)
                .send()
                .await?;
        }

        self.client
            .update_item()
            .table_name("users")
            .key("id", AttributeValue::S(user_id.to_string()))
            .update_expression("set first_update=:F, last_update=:L")
            .expression_attribute_values(":F", AttributeValue::Null(true))
            .expression_attribute_values(":L", AttributeValue::Null(true))
            .send()
            .await?;

        Ok(())
    }
    async fn create_user(
        &self,
        pepper_version: &PepperVersion,
        pepper_key: &Pepper,
        username: &Username,
        password: &SecretString,
        followers: &HashSet<StorUrl>,
        following: &HashSet<(StorUrl, FollowId)>,
    ) -> std::result::Result<String, Failed> {
        use crypto_common::rand_core::{OsRng, RngCore};
        let mut api_key = [0u8; 64];
        OsRng.fill_bytes(api_key.as_mut_slice());
        let textual_api_key = format!("v1:{}", hex::encode(&api_key));
        let user = User::new(
            pepper_version,
            pepper_key,
            username,
            password,
            &UserEmail::new(&format!("{}@example.com", username)).unwrap(/* known good */),
            Some(Box::new(api_key)),
            None,
            None,
            None,
        )?;

        add_user(&self.client, &user).await?;
        add_followers(&self.client, &user, followers).await?;
        add_following(&self.client, &user, following).await?;

        Ok(textual_api_key)
    }
    async fn remove_user(&self, username: &Username) -> std::result::Result<(), Failed> {
        let user_id = self.id_for_username(username).await?;
        if let Some(user_id) = user_id {
            self.client
                .delete_item()
                .table_name("users")
                .key("id", AttributeValue::S(user_id.to_string()))
                .send()
                .await?;
            self.client
                .delete_item()
                .table_name("unique_usernames")
                .key("username", AttributeValue::S(username.to_string()))
                .send()
                .await?;
        }
        Ok(())
    }
}

inventory::submit!(IndielinksTest {
    name: "000test_healthcheck",
    test_fn: |cfg, _helper| { Box::pin(test_healthcheck(cfg.indielinks)) },
});

inventory::submit!(IndielinksTest {
    name: "001delicious_smoke_test",
    test_fn: |cfg, helper| {
        Box::pin(delicious_smoke_test(
            cfg.indielinks, /*url*/
            cfg.username,
            cfg.api_key,
            helper,
        ))
    },
});

inventory::submit!(IndielinksTest {
    name: "010delicious_posts_recent",
    test_fn: |cfg, helper| {
        Box::pin(posts_recent(
            cfg.indielinks, /*url*/
            cfg.username,
            cfg.api_key,
            helper,
        ))
    },
});

inventory::submit!(IndielinksTest {
    name: "011delicious_posts_all",
    test_fn: |cfg, helper| {
        Box::pin(posts_all(
            cfg.indielinks, /*url*/
            cfg.username,
            cfg.api_key,
            helper,
        ))
    }
});

inventory::submit!(IndielinksTest {
    name: "012delicious_tags_rename_and_delete",
    test_fn: |cfg: Configuration, helper| {
        Box::pin(tags_rename_and_delete(
            cfg.indielinks,
            cfg.username,
            cfg.api_key,
            helper,
        ))
    },
});

inventory::submit!(IndielinksTest {
    name: "020user_test_signup",
    test_fn: |cfg: Configuration, helper| { Box::pin(test_signup(cfg.indielinks, helper)) },
});

inventory::submit!(IndielinksTest {
    name: "030webfinger_smoke",
    test_fn: |cfg: Configuration, _helper| {
        Box::pin(webfinger_smoke(
            cfg.indielinks.clone(),
            cfg.username,
            cfg.indielinks.clone().try_into().unwrap(/* Fail the test if this fails */),
        ))
    },
});

inventory::submit!(IndielinksTest {
    name: "040follow_smoke",
    test_fn: |cfg: Configuration, _helper| {
        Box::pin(accept_follow_smoke(
            cfg.indielinks.clone(),
            cfg.username,
            cfg.indielinks.try_into().unwrap(),
        ))
    },
});

inventory::submit!(IndielinksTest {
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
});

inventory::submit!(IndielinksTest {
    name: "060send_follow",
    test_fn: |cfg: Configuration, helper| {
        let (version, pepper) = cfg.pepper.current_pepper().unwrap();
        Box::pin(send_follow(cfg.indielinks, version, pepper, helper))
    },
});

inventory::submit!(IndielinksTest {
    name: "070as_follower",
    test_fn: |cfg: Configuration, helper| {
        let (version, pepper) = cfg.pepper.current_pepper().unwrap();
        Box::pin(as_follower(cfg.indielinks, version, pepper, helper))
    },
});

inventory::submit!(IndielinksTest {
    name: "080user_test_mint_key",
    test_fn: |cfg: Configuration, helper| { Box::pin(test_mint_key(cfg.indielinks, helper)) },
});

async fn charge_tables(client: &::aws_sdk_dynamodb::Client) -> Result<()> {
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
        })?;
    Ok(())
}

fn main() -> Result<()> {
    // Regrettably, the Scylla API has forced us to use async Rust. This is inconvenient as the
    // libtest-mimic crate expects *synchronous* test functions. Using `#[tokio::main]` leaves us
    // with no way to get a reference, or a "handle" to the Tokio runtime in which we're running, so
    // I eschew that here and create it myself:
    let rt = Arc::new(Runtime::new().expect("Failed to build a tokio multi-threaded runtime"));

    // We have no way to augment the set of command-line arguments this program will accept, so
    // we'll examine an environment variable to determine where to get our configuration:
    let config = Configuration::new().context(ConfigurationSnafu)?;

    let mut args = Arguments::from_args();

    if config.logging {
        let filter = EnvFilter::builder()
            .with_default_directive(config.log_level.into())
            .from_env()
            .context(FilterSnafu)?;
        tracing::subscriber::set_global_default(
            Registry::default()
                .with(fmt::Layer::default().compact().with_writer(io::stdout))
                .with(filter),
        )
        .context(SetGlobalDefaultSnafu)?;
    }

    if !config.no_setup {
        setup()?;
    }

    let state = Arc::new(rt.block_on(async { State::new(&config).await })?);

    // I'm not happy about this: charging the database with some initial data is absolutely part of
    // fixture setup. Thing is, I can't do this without a client, and I can't create a client until
    // I've spun-up the ScyllaDB cluster. I need to re-think my fixture setup logic, but I'm going
    // to wait on that until I re-organize it more generally along the lines of `cache-tests`.
    let _ = rt.block_on(async { charge_tables(&state.client).await })?;

    // Nb. this program is always run from the root directory of the owning crate.
    // This, together with prefixing my function names with numbers, is a hopefully temporary
    // workaround to the fact that my tests can't be run out-of-order or simultaneously.
    if !matches!(args.test_threads, Some(1)) {
        eprintln!("Temporarily overriding --test-threads to 1.");
        args.test_threads = Some(1);
    }

    let conclusion = libtest_mimic::run(
        &args,
        inventory::iter::<common::IndielinksTest>
            .into_iter()
            .sorted_by_key(|t| t.name)
            .map(|test| {
                // See delicious-scylla.rs for detailed remarks on this:
                Trial::test(test.name, {
                    let rt = rt.clone();
                    let cfg = config.clone();
                    let state = state.clone();
                    move || rt.block_on(async { (test.test_fn)(cfg, state).await })
                })
            })
            .collect(),
    );

    if !config.no_teardown {
        let _ = teardown();
    }

    conclusion.exit();
}
