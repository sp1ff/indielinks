// Copyright (C) 2024-2025 Michael Herstine <sp1ff@pobox.com>
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

//! # indielinks-schemas
//!
//! indielinks-schemas is a tool for creating & updating the datastore schemas required by [indielinks].
//!
//! ## Introduction
//!
//! Like any web service, [indielinks] needs to write down state in a persistent manner. For
//! generations, the default choice in this situation was a Relational Database Management System
//! (or RDBMS, for short). Over the last twenty years or so, there's been a Cambrian explosion of
//! alternatives. These alternatives are frequently grouped under the rubric "NoSQL", and while at
//! their inception at the dawn of internet-scale companies they were something obscure and only
//! found in closed-source codebases like Google & Twitter, by 2024 they were widely used, and
//! common in open source code.
//!
//! [indielinks] is designed to work with any "BigTable-style" wide-column store. At the time of
//! this writing, it supports AWS' DynamoDB and ScyllaDB (over both the native & Alternator
//! interfaces). We are therefore still confronted with the problem of schema definition &
//! migration. [indielinks-schemas] is a tool, intended to be run at installation, for both creating
//! the initial [indielinks] schema in the user's chosen data store as well as updating the schema
//! as part of subsequent upgrades.
//!
//! It would have been more in keeping with my design goal of operational simplicity to build this
//! functionality into the [indielinks] binary itself, but that presented difficulties when running
//! [indielinks] in a cluster-- which node should be responsible for this function? I suppose we
//! could have waited for the Raft cluster to be initialized, and then have the leader do it, but
//! Raft cluster initialization depends on having the data store up & the schema created!
//!
//! ## DynamoDB
//!
//! I could have scripted this using the AWS CLI, but I didn't want to introduce that dependency. I
//! could have scripted this using the Python `boto3` library, but didn't want to introduce Python
//! code to the project (on the occasions that it actually works, Python inevitably becomes a
//! maintenance headache).
//!
//! ## ScylalDB
//!
//! I could have used the `cqlsh` tool to script this, but `cqlsh` is regrettably implemented in
//! Python, and even when one can successfully install Python packages, they invariably become a
//! maintenance headache. The convention in the ScyllaDB community seems to be to define schemas in
//! cqlsh, but parse them in your application code and use whatever ScyllaDB library you're using
//! already to execute it (see [here], e.g.)
//!
//! [here]: https://github.com/scylladb/care-pet/blob/master/rust/src/database/migrate/mod.rs

use std::{fmt::Display, io, net::SocketAddr, ops::Deref, sync::Arc};

use clap::{crate_authors, crate_version, value_parser, Arg, ArgAction, Command};
use futures::{future::BoxFuture, stream::iter, StreamExt};
use snafu::{prelude::*, Backtrace};
use tracing::{info, Level};
use tracing_subscriber::{
    fmt::{self},
    layer::SubscriberExt,
    EnvFilter, Layer, Registry,
};

use indielinks::{
    dynamodb::{
        create_client as create_dynamodb_client,
        get_current_schema_version as get_current_dynamodb_schema_version,
        Location as DynamoLocation,
    },
    dynamodb_schemas::create_schema as create_dynamodb_schema,
    scylla::{
        create_client as create_scylla_client, create_schema as create_scylladb_schema,
        get_current_schema_version as get_current_scylla_schema_version,
    },
    util::Credentials,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        crate error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Application error type
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("While creating the schema, {source}"))]
    CreateSchema { source: indielinks::scylla::Error },
    #[snafu(display("When creating the DDB client, {source}"))]
    DdbClient { source: indielinks::dynamodb::Error },
    #[snafu(display("While updating the DynamoDB schema, {source:#?}"))]
    DdbSchemaUpdate {
        source: indielinks::dynamodb_schemas::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While fetching the current schema version, {source:#?}"))]
    DdbSchemaVersion { source: indielinks::dynamodb::Error },
    #[snafu(display("Failed to parse RUST_LOG: {source}"))]
    EnvFilter {
        source: tracing_subscriber::filter::FromEnvError,
    },
    #[snafu(display("No endpoint URLs specified"))]
    NoEndpoints { backtrace: Backtrace },
    #[snafu(display("No sub-command given; try --help"))]
    NoSubCommand,
    #[snafu(display("When creating the Scylla client, {source}"))]
    ScyllaClient { source: indielinks::scylla::Error },
    #[snafu(display("While updating the schema, {source:#?}"))]
    ScyllaSchemaUpdate { source: indielinks::scylla::Error },
    #[snafu(display("While fetching the current schema version, {source:#?}"))]
    ScyllaSchemaVersion { source: indielinks::scylla::Error },
    #[snafu(display("Failed to set the tracing subscriber: {source}"))]
    Subscriber {
        source: tracing::subscriber::SetGlobalDefaultError,
    },
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self::Display::fmt(&self, f)
    }
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////

fn configure_logging(debug: bool, verbose: bool, quiet: bool, plain: bool) -> Result<()> {
    let level = match (debug, verbose, quiet) {
        (true, _, _) => Level::TRACE,
        (false, true, _) => Level::DEBUG,
        (false, false, true) => Level::ERROR,
        _ => Level::INFO,
    };
    let filter = EnvFilter::builder()
        .with_default_directive(level.into())
        .from_env()
        .context(EnvFilterSnafu)?;
    let formatter: Box<dyn Layer<Registry> + Send + Sync> = if plain {
        Box::new(
            fmt::Layer::default()
                .compact()
                .with_ansi(false)
                .with_writer(io::stdout),
        )
    } else {
        Box::new(fmt::Layer::default().json().with_writer(io::stdout))
    };
    tracing::subscriber::set_global_default(Registry::default().with(formatter).with(filter))
        .context(SubscriberSnafu)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                   schema version management                                    //
////////////////////////////////////////////////////////////////////////////////////////////////////

const SCHEMA_VERSION: u32 = 0;

// Each function is expected to update `schema_migrations` on successful completion
// Can be implemented as:
//     fn test2(_: aws_sdk_dynamodb::Client) -> BoxFuture<'static, Result<()>> {
//         ...
//     }
pub type ScyllaDbSchemaUpdate =
    fn(Arc<scylla::client::session::Session>) -> BoxFuture<'static, Result<()>>;

const CQL_SCHEMAS: &[ScyllaDbSchemaUpdate] = &[|session| {
    Box::pin(async move {
        create_scylladb_schema(session, include_str!("../../schemas/0.cql"))
            .await
            .context(CreateSchemaSnafu)
    })
}];

// Each function is expected to update `schema_versions` on successful completion
// Can be written as:
//     fn test2(_: aws_sdk_dynamodb::Client) -> BoxFuture<'static, Result<()>> {
//         ...
//     }
type DynamoDbSchemaUpdate = fn(aws_sdk_dynamodb::Client) -> BoxFuture<'static, Result<()>>;

const DDB_FNS: &[DynamoDbSchemaUpdate] = &[|client| {
    Box::pin(async move {
        create_dynamodb_schema(client)
            .await
            .context(DdbSchemaUpdateSnafu)
    })
}];

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                              main                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::main]
async fn main() -> Result<()> {
    // Later: move these to be static asserts?
    assert!(CQL_SCHEMAS.len() == SCHEMA_VERSION as usize + 1, "There is a mismatch between the number of CQL schema files and the current schema version. This should have been a compilation error.");
    assert!(DDB_FNS.len() == SCHEMA_VERSION as usize + 1, "There is a mismatch between the number of DDB update functions and the current schema version. This should have been a compilation error.");

    let mut matches = Command::new("indielinks-schemas")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Creating the schema for bookmarks in the Fediverse.")
        .long_about("Create or update the indielinks datastore schema")
        .arg(
            Arg::new("debug")
                .short('D')
                .long("debug")
                .num_args(0)
                .action(ArgAction::SetTrue)
                .env("INDIELINKS_SCHEMAS_DEBUG")
                .help("produce debug output"),
        )
        .arg(
            Arg::new("plain")
                .short('p')
                .long("plain")
                .num_args(0)
                .action(ArgAction::SetTrue)
                .env("INDIELINKS_SCHEMAS_PLAIN")
                .help("log in human-readable format, not JSON/structured logging"),
        )
        .arg(
            Arg::new("quiet")
                .short('q')
                .long("quiet")
                .num_args(0)
                .action(ArgAction::SetTrue)
                .env("INDIELINKS_SCHEMAS_QUIET")
                .help("produce only error output"),
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .num_args(0)
                .action(ArgAction::SetTrue)
                .env("INDIELINKS_SCHEMAS_VERBOSE")
                .help("produce prolix output"),
        )
        .subcommand(
            Command::new("ddb")
                .about("Create the DynamoDB tables & secondary indicies needed by indielinks")
                .arg(
                    Arg::new("creds")
                        .short('c')
                        .long("creds")
                        .num_args(1)
                        .env("INDIELINKS_SCHEMAS_CREDS")
                        .value_parser(value_parser!(Credentials)),
                )
                .arg(
                    Arg::new("location")
                        .index(1)
                        .value_parser(value_parser!(DynamoLocation))
                        .required(true)
                        .help("Network location of the DynamoDB cluster")
                        .long_help(
                            "Network location of the DynamoDB/ScyllaDB cluster.

Specify as either an AWS region ('us-west-2', e.g.) or as an URL ('http://localhost:8042, e.g.)",
                        )
                        .env("INDIELINKS_SCHEMAS_LOCATION"),
                ),
        )
        .subcommand(
            Command::new("scylla")
                .about("Create the ScyllaDB keyspace, tables, indicies & materialized views needed by indielinks")
                .arg(
                    Arg::new("creds")
                        .short('c')
                        .long("creds")
                        .num_args(1)
                        .env("INDIELINKS_SCHEMAS_CREDS")
                        .value_parser(value_parser!(Credentials)),
                )
                .arg(
                    Arg::new("host")
                        .num_args(1..)
                        .value_parser(value_parser!(SocketAddr))
                        .required(true))
        )
        .get_matches();

    configure_logging(
        matches.get_flag("debug"),
        matches.get_flag("verbose"),
        matches.get_flag("quiet"),
        matches.get_flag("plain"),
    )?;

    info!("indielinks-schemas {}", crate_version!());

    match matches.remove_subcommand() {
        Some((name, mut matches)) => match name.as_str() {
            "ddb" => {
                let creds = matches.remove_one::<Credentials>("creds");
                let location =
                    matches.remove_one::<DynamoLocation>("location").unwrap(/* required */);
                let client = create_dynamodb_client(&location, &creds)
                    .await
                    .context(DdbClientSnafu)?;
                let to_apply = get_current_dynamodb_schema_version(&client)
                    .await
                    .context(DdbSchemaVersionSnafu)?
                    .map(|i| i + 1)
                    .unwrap_or(0) as usize;
                let _ = iter(DDB_FNS[to_apply..].iter())
                    .then(|f| f(client.clone()))
                    .collect::<Vec<StdResult<_, _>>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;
                info!("DynamoDB configured.");
                Ok(())
            }
            "scylla" => {
                let creds = matches.remove_one::<Credentials>("creds");
                let hosts = matches.remove_many::<SocketAddr>("host").unwrap(/* required */);
                let client = Arc::new(
                    create_scylla_client(hosts, &creds)
                        .await
                        .context(ScyllaClientSnafu)?,
                );
                // Since the keyspace won't exist in a new database, attempt to set it in
                // `get_current_scylla_schema_version()
                let to_apply = get_current_scylla_schema_version(client.deref())
                    .await
                    .context(ScyllaSchemaVersionSnafu)?
                    .map(|i| i + 1)
                    .unwrap_or(0) as usize;
                let _: StdResult<Vec<_>, Error> = iter(CQL_SCHEMAS[to_apply..].iter())
                    .then(|f| f(client.clone()))
                    .collect::<Vec<StdResult<_, _>>>()
                    .await
                    .into_iter()
                    .collect::<StdResult<Vec<_>, _>>();
                info!("ScyllaDB configured.");
                Ok(())
            }
            _ => unimplemented!(/* impossible */),
        },
        None => NoSubCommandSnafu.fail(),
    }
}
