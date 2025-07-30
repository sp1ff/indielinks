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

/// # delicious-scylla
///
/// Integration tests run against an indielinks configured with the Scylla storage back-end.
use common::{Configuration, IndielinksTest, run};
use crypto_common::rand_core::{OsRng, RngCore};
use indielinks::{
    entities::{FollowId, User, UserEmail, Username},
    origin::Origin,
    peppers::{Pepper, Version as PepperVersion},
    scylla::{add_followers, add_following, add_user},
};
use indielinks_shared::{StorUrl, UserId};
use indielinks_test::{
    Helper,
    activity_pub::{as_follower, posting_creates_note, send_follow},
    delicious::{delicious_smoke_test, posts_all, posts_recent, tags_rename_and_delete},
    follow::accept_follow_smoke,
    test_healthcheck,
    users::test_signup,
    webfinger::webfinger_smoke,
};

use async_trait::async_trait;
use itertools::Itertools;
use libtest_mimic::{Arguments, Failed, Trial};
use scylla::client::session_builder::SessionBuilder;
use secrecy::SecretString;
use snafu::{Backtrace, Snafu, prelude::*};
use tokio::runtime::Runtime;
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};

use std::{collections::HashSet, fmt::Display, io, sync::Arc};

mod common;

#[derive(Snafu)]
enum Error {
    #[snafu(display("Failed to run {cmd}: {source}"))]
    Command { cmd: String, source: common::Error },
    #[snafu(display("Error obtaining test configuration: {source}"))]
    Configuration { source: common::Error },
    #[snafu(display("Failed to deserialize a UserId: {source}"))]
    DeUserId {
        source: scylla::deserialize::DeserializationError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to set keyspace: {source}"))]
    Keyspace {
        source: scylla::errors::UseKeyspaceError,
        backtrace: Backtrace,
    },
    #[snafu(display("User {username} appears more than once in the database"))]
    MultipleUsers {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to create a ScyllaDB session: {source}"))]
    NewSession {
        source: scylla::errors::NewSessionError,
        backtrace: Backtrace,
    },
    #[snafu(display("No user {username}"))]
    NoSuchUser {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("ScyllaDB query failed: {source}"))]
    Query {
        source: scylla::errors::ExecutionError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to get typed rows: {source}"))]
    Rows {
        source: scylla::response::query_result::RowsError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to get a rows result: {source}"))]
    RowsResult {
        source: scylla::response::query_result::IntoRowsResultError,
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

/// Application state shared across all tests
struct State {
    session: ::scylla::client::session::Session,
}

impl State {
    pub async fn new(cfg: &Configuration) -> Result<State> {
        let mut builder = SessionBuilder::new().known_nodes(&cfg.scylla.hosts);
        if let Some((user, pass)) = &cfg.scylla.credentials {
            builder = builder.user(user, pass);
        }
        let session = builder.build().await.context(NewSessionSnafu)?;
        session
            .use_keyspace("indielinks", false)
            .await
            .context(KeyspaceSnafu)?;
        Ok(State { session })
    }
    async fn id_for_username(&self, username: &Username) -> Result<Option<UserId>> {
        Ok(self
            .session
            .query_unpaged("select id from users where username=?", (username,))
            .await
            .context(QuerySnafu)?
            .into_rows_result()
            .context(RowsResultSnafu)?
            .rows::<(UserId,)>()
            .context(RowsSnafu)?
            .collect::<StdResult<Vec<(UserId,)>, _>>()
            .context(DeUserIdSnafu)?
            .into_iter()
            .at_most_one()
            .map_err(|_| {
                MultipleUsersSnafu {
                    username: username.clone(),
                }
                .build()
            })?
            .map(|opt| opt.0))
    }
}

#[async_trait]
impl Helper for State {
    async fn clear_posts(&self, username: &Username) -> std::result::Result<(), Failed> {
        let userid = self
            .id_for_username(username)
            .await?
            .context(NoSuchUserSnafu {
                username: username.clone(),
            })?;
        self.session.query_unpaged("truncate posts", ()).await?;
        self.session
            .query_unpaged(
                "update users set first_update=null, last_update=null where id=?",
                (userid,),
            )
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

        add_user(&self.session, None, None, &user).await?;
        add_followers(&self.session, None, &user, followers, true).await?;
        add_following(&self.session, None, &user, following, true).await?;

        Ok(textual_api_key)
    }
    async fn remove_user(&self, username: &Username) -> std::result::Result<(), Failed> {
        let user_id = self.id_for_username(username).await?;
        if let Some(user_id) = user_id {
            self.session
                .query_unpaged("delete from unique_usernames where username=?", (username,))
                .await?;
            self.session
                .query_unpaged("delete from users where id=?", (user_id,))
                .await?;
        }
        Ok(())
    }
}

inventory::submit!(IndielinksTest {
    name: "000test_healthcheck",
    test_fn: |cfg: Configuration, _helper| { Box::pin(test_healthcheck(cfg.indielinks)) },
});

inventory::submit!(IndielinksTest {
    name: "001delicious_smoke_test",
    test_fn: |cfg, helper| {
        Box::pin(delicious_smoke_test(
            cfg.indielinks,
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
            cfg.indielinks,
            cfg.username,
            cfg.api_key,
            helper,
        ))
    },
});

inventory::submit!(IndielinksTest {
    name: "011delicious_posts_all",
    test_fn: |cfg: Configuration, helper| {
        Box::pin(posts_all(cfg.indielinks, cfg.username, cfg.api_key, helper))
    },
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
            TryInto::<Origin>::try_into(cfg.indielinks.clone()).unwrap(/* fail the test if this fails */),
        ))
    },
});

inventory::submit!(IndielinksTest {
    name: "040follow_smoke",
    test_fn: |cfg: Configuration, _helper| {
        Box::pin(accept_follow_smoke(
            cfg.indielinks.clone(),
            cfg.username,
            cfg.indielinks.clone().try_into().unwrap(),
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

    // This, together with prefixing my function names with numbers, is a hopefully temporary
    // workaround to the fact that my tests can't be run out-of-order or simultaneously.
    if !matches!(args.test_threads, Some(1)) {
        eprintln!("Temporarily overriding --test-threads to 1.");
        args.test_threads = Some(1);
    }

    // Nb. this program is always run from the root directory of the owning crate.
    let conclusion = libtest_mimic::run(
        &args,
        inventory::iter::<common::IndielinksTest>
            .into_iter()
            .sorted_by_key(|t| t.name)
            .map(|test| {
                // `Trial::test` takes two parameters, something that implements `Into<String>`, and
                // something that implements:
                //
                //     FnOnce() -> Result<(), Failed> + Send + 'static
                //
                // The first is easy,
                Trial::test(
                    test.name,
                    // the second is more complex. What I'm doing here is opening a block, setting
                    // up a thing that meets that trait bound, and yielding that thing at the end of
                    // the block.
                    {
                        // The scheme is to invoke each `IndielinksTest` function with everything a
                        // test might need-- the application configuration as well as a suite of
                        // utility functions (well, there's only one ATM, but I see this growing).
                        // Each test's `test_fn` can unpack the bits that particular test needs.
                        let rt = rt.clone();
                        let cfg = config.clone();
                        let state = state.clone();
                        // Yield a non-async closure that's moved `rt`, `cfg` & `state` into it,
                        // that can be invoked once, and that yields a `Result<(), Failed>`. I
                        // originally tried to use `futures::executor::block_on()`, but that creates
                        // its own, single-threaded runtime on which it drives the future, which
                        // gave unpredictable results (futures would appear to just get "stuck").
                        move || rt.block_on(async { (test.test_fn)(cfg, state).await })
                    },
                )
            })
            .collect(),
    );

    if !config.no_teardown {
        let _ = teardown();
    }

    conclusion.exit();
}
