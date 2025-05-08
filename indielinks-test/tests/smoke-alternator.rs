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
    dynamodb::{add_followers, add_user},
    entities::{Post, User, UserEmail, UserId, UserUrl, Username},
    peppers::{Pepper, Version as PepperVersion},
};
use indielinks_test::{
    activity_pub::{posting_creates_note, send_follow},
    delicious::{delicious_smoke_test, posts_all, posts_recent, tags_rename_and_delete},
    follow::accept_follow_smoke,
    test_healthcheck,
    users::test_signup,
    webfinger::webfinger_smoke,
    Helper,
};

use async_trait::async_trait;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region};
use aws_sdk_dynamodb::{
    config::Credentials,
    types::{AttributeValue, DeleteRequest, Select, WriteRequest},
};
use either::Either;
use itertools::Itertools;
use libtest_mimic::{Arguments, Failed, Trial};
use secrecy::SecretString;
use serde_dynamo::aws_sdk_dynamodb_1::from_items;
use snafu::{prelude::*, Backtrace, Snafu};
use tap::Pipe;
use tokio::runtime::Runtime;
use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter, Registry};

use std::{cmp::min, collections::HashSet, fmt::Display, io, sync::Arc};

mod common;

#[derive(Snafu)]
enum Error {
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
    #[snafu(display("Failed to parse RUST_LOG: {source}"))]
    Filter {
        source: tracing_subscriber::filter::FromEnvError,
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
    run("../infra/indielinks-up", &["indielinks-alternator.toml"]).context(CommandSnafu {
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
        let creds = cfg
            .dynamo
            .credentials
            .as_ref()
            .map(|(id, secret)| Credentials::new(id, secret, None, None, "indielinks"));

        let config = match cfg.dynamo.location.as_ref() {
            Either::Left(region) => {
                let region_provider =
                    RegionProviderChain::first_try(Some(Region::new(region.clone())))
                        .or_default_provider()
                        .or_else(Region::new("us-west-2"));
                let mut loader = aws_config::from_env().region(region_provider);
                if let Some(creds) = creds {
                    loader = loader.credentials_provider(creds);
                }
                loader.load().await
            }
            Either::Right(endpoints) => {
                let ep_url = *endpoints
                    .iter()
                    .peekable()
                    .peek()
                    .ok_or(NoEndpointsSnafu {}.build())?;
                let mut loader = aws_config::defaults(BehaviorVersion::latest())
                    .endpoint_url((*ep_url).as_str());
                if let Some(creds) = creds {
                    loader = loader.credentials_provider(creds);
                }
                loader.load().await
            }
        };
        Ok(State {
            client: ::aws_sdk_dynamodb::Client::new(&config),
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
        followers: &HashSet<UserUrl>,
    ) -> std::result::Result<String, Failed> {
        use crypto_common::rand_core::{OsRng, RngCore};

        let mut api_key: Vec<u8> = Vec::with_capacity(32);
        OsRng.fill_bytes(api_key.as_mut_slice());
        let textual_api_key = hex::encode(&api_key);
        let user = User::new(
            pepper_version,
            pepper_key,
            username,
            password,
            &UserEmail::new(&format!("{}@example.com", username)).unwrap(/* known good */),
            Some(&api_key.into()),
            None,
            None,
            None,
        )?;

        add_user(&self.client, &user).await?;
        add_followers(&self.client, &user, followers).await?;

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
    test_fn: |cfg, _helper| {
        Box::pin(test_healthcheck(cfg.indielinks /*url*/))
    },
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
