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

//! # indielinks-ddb
//!
//! Setup the indielinks DynamoDB schema & charge it with initial data for development purposes.
//!
//! I could have scripted this using the AWS CLI, but I didn't want to introduce that dependency. I
//! could have scripted this using the Python `boto3` library, but didn't want to introduce Python
//! code to the project (on the occasions that it actually works, Python inevitably becomes a
//! maintenance headache). Since I'm considering moving this functionality into the indielinks
//! binary itself, I decided to just write it in Rust.
//!
//! This kind of stinks because now I have to keep this code synchronized with my `.cql` files that
//! create the indielinks schema for SycllaDB, but this will hopefully be temporary.

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::{
    Client,
    config::Region,
    error::SdkError,
    operation::{create_table::CreateTableError, put_item::PutItemError},
    primitives::Blob,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement,
        KeyType, LocalSecondaryIndex, ScalarAttributeType,
    },
};
use clap::{Arg, ArgAction, ArgMatches, Command, crate_authors, crate_version, value_parser};
use either::Either;
use indielinks::util::exactly_two;
use itertools::Itertools;
use snafu::{Backtrace, prelude::*};
use tap::Pipe;
use tracing::{Level, debug, info};
use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    fmt::{self},
    layer::SubscriberExt,
};
use url::Url;

use std::{collections::HashMap, fmt::Display, io};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        crate error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Application error type
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("Failed to charge table {name}: {source:#?}"))]
    ChargeTable {
        name: String,
        source: SdkError<PutItemError, aws_sdk_dynamodb::config::http::HttpResponse>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to create table: {source}"))]
    CreateTable {
        source: SdkError<CreateTableError, aws_sdk_dynamodb::config::http::HttpResponse>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to parse RUST_LOG: {source}"))]
    EnvFilter {
        source: tracing_subscriber::filter::FromEnvError,
    },
    #[snafu(display("Failed to build {name}: {source}"))]
    GenericBuildFailure {
        name: String,
        source: aws_sdk_dynamodb::error::BuildError,
        backtrace: Backtrace,
    },
    #[snafu(display("No endpoint URLs specified"))]
    NoEndpoints { backtrace: Backtrace },
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

////////////////////////////////////////////////////////////////////////////////////////////////////
//                         Implement parsing a pair of String for `clap`                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Newtype to work around Rust's orphan traits rule
#[derive(Clone, Debug)]
struct Credentials((String, String));

impl clap::builder::ValueParserFactory for Credentials {
    type Parser = CredentialsParser;

    fn value_parser() -> Self::Parser {
        CredentialsParser
    }
}

#[derive(Clone, Debug)]
struct CredentialsParser;

impl clap::builder::TypedValueParser for CredentialsParser {
    type Value = Credentials;

    fn parse_ref(
        &self,
        _cmd: &clap::Command,
        _arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> std::result::Result<Self::Value, clap::Error> {
        use clap::error::ErrorKind;
        value
            .to_str()
            .ok_or(clap::Error::new(ErrorKind::InvalidValue))?
            .split(',')
            .collect::<Vec<&str>>()
            .into_iter()
            .pipe(exactly_two)
            .map_err(|_| clap::Error::new(ErrorKind::WrongNumberOfValues))?
            .pipe(|p| (p.0.to_string(), p.1.to_string())) // OMFG-- I can't map over a tuple
            .pipe(Credentials)
            .pipe(Ok)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                finding DynamoDB on the network                                 //
////////////////////////////////////////////////////////////////////////////////////////////////////

// The network location of our DynamoDB (or ScyllaDB/Alternator) instance can be given
// as either a region, or a list of Urls.
/// Newtype to work around Rust's orphan traits rule
#[derive(Clone, Debug)]
struct DynamoLocation(Either<Region, Vec<Url>>);

impl clap::builder::ValueParserFactory for DynamoLocation {
    type Parser = DynamoLocationParser;

    fn value_parser() -> Self::Parser {
        DynamoLocationParser
    }
}

#[derive(Clone, Debug)]
struct DynamoLocationParser;

impl clap::builder::TypedValueParser for DynamoLocationParser {
    type Value = DynamoLocation;

    fn parse_ref(
        &self,
        _cmd: &clap::Command,
        _arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> std::result::Result<Self::Value, clap::Error> {
        use clap::error::ErrorKind;
        let vals = value
            .to_str()
            .ok_or(clap::Error::new(ErrorKind::InvalidValue))?
            .split(',')
            .collect::<Vec<&str>>();
        match vals.iter().exactly_one() {
            Ok(s) => Ok(DynamoLocation(match Url::parse(s) {
                Ok(url) => Either::Right(vec![url]),
                Err(_) => Either::Left(Region::new(s.to_string())),
            })),
            Err(_) => Ok(DynamoLocation(Either::Right(
                vals.iter()
                    .cloned()
                    .map(Url::parse)
                    .collect::<std::result::Result<Vec<Url>, _>>()
                    .map_err(|_| clap::Error::new(ErrorKind::InvalidValue))?,
            ))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             schema                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

async fn get_client(location: DynamoLocation, creds: &Option<Credentials>) -> Result<Client> {
    let creds = creds.as_ref().map(|Credentials((id, secret))| {
        aws_sdk_dynamodb::config::Credentials::new(id, secret, None, None, "indielinks-ddb")
    });
    // This duplicates the logic in `dynamodb.rs`-- should re-factor.
    let config = match location.0 {
        Either::Left(region) => {
            let mut loader = aws_config::from_env().region(region);
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
            let mut loader =
                aws_config::defaults(BehaviorVersion::latest()).endpoint_url((*ep_url).as_str());
            if let Some(creds) = creds {
                loader = loader.credentials_provider(creds);
            }
            loader.load().await
        }
    };
    Ok(Client::new(&config))
}

macro_rules! table_attr {
    ($col_name:expr, $ty:ident) => {
        AttributeDefinition::builder()
            .attribute_name($col_name)
            .attribute_type(ScalarAttributeType::$ty)
            .build()
            .context(GenericBuildFailureSnafu {
                name: $col_name.to_string(),
            })?
    };
}

async fn create_users(client: &Client) -> Result<()> {
    let out = client
        .create_table()
        .table_name("users")
        // This is what the ScyllaDB/Alternator example uses-- not sure this is suitable for
        // DynamoDB
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![table_attr!("id", S), table_attr!("username", S)]))
        .set_key_schema(Some(vec![
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "id".to_string(),
                })?,
        ]))
        .global_secondary_indexes(
            //
            GlobalSecondaryIndex::builder()
                .index_name("users_by_username")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("username")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "username".to_string(),
                })?,
        )
        .send()
        .await
        .context(CreateTableSnafu)?;
    debug!("create users: {:#?}", out);
    let out = client
        .create_table()
        .table_name("unique_usernames")
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![table_attr!("username", S)]))
        .set_key_schema(Some(vec![
            KeySchemaElement::builder()
                .attribute_name("username")
                .key_type(KeyType::Hash)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "username".to_string(),
                })?,
        ]))
        .send()
        .await
        .context(CreateTableSnafu);
    debug!("create unique_usernames: {:#?}", out);
    out.map(|_| ())
}

async fn create_following(client: &Client) -> Result<()> {
    let out = client
        .create_table()
        .table_name("following")
        // This is what the ScyllaDB/Alternator example uses-- not sure this is suitable for
        // DynamoDB
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![
            table_attr!("user_id", S),  // partition key
            table_attr!("actor_id", S), // sort key
        ]))
        .set_key_schema(Some(vec![
            KeySchemaElement::builder()
                .attribute_name("user_id")
                .key_type(KeyType::Hash)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "user_id".to_string(),
                })?,
            KeySchemaElement::builder()
                .attribute_name("actor_id")
                .key_type(KeyType::Range)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "actor_id".to_string(),
                })?,
        ]))
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("following_by_actor_id")
                .set_key_schema(Some(vec![
                    KeySchemaElement::builder()
                        .attribute_name("actor_id")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                ]))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .context(CreateTableSnafu);
    debug!("create following: {:#?}", out);
    out.map(|_| ())
}

async fn create_followers(client: &Client) -> Result<()> {
    let out = client
        .create_table()
        .table_name("followers")
        // This is what the ScyllaDB/Alternator example uses-- not sure this is suitable for
        // DynamoDB
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![
            table_attr!("user_id", S),  // partition key
            table_attr!("actor_id", S), // sort key
        ]))
        .set_key_schema(Some(vec![
            KeySchemaElement::builder()
                .attribute_name("user_id")
                .key_type(KeyType::Hash)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "user_id".to_string(),
                })?,
            KeySchemaElement::builder()
                .attribute_name("actor_id")
                .key_type(KeyType::Range)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "actor_id".to_string(),
                })?,
        ]))
        .send()
        .await
        .context(CreateTableSnafu);
    debug!("create followers: {:#?}", out);
    out.map(|_| ())
}

async fn create_posts(client: &Client) -> Result<()> {
    let out = client
        .create_table()
        .table_name("posts")
        // This is what the ScyllaDB/Alternator example uses-- not sure this is suitable for
        // DynamoDB
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![
            table_attr!("user_id", S), // partition key
            table_attr!("url", S),     // sort key
            table_attr!("posted", S),  // sort key for first LSI
            table_attr!("day", S),     // sort key for second LSI
            table_attr!("id", S),      // partition key for the GSI
        ]))
        .set_key_schema(Some(vec![
            KeySchemaElement::builder()
                .attribute_name("user_id")
                .key_type(KeyType::Hash)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "user_id".to_string(),
                })?,
            KeySchemaElement::builder()
                .attribute_name("url")
                .key_type(KeyType::Range)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "url".to_string(),
                })?,
        ]))
        .local_secondary_indexes(
            LocalSecondaryIndex::builder()
                .index_name("posts_by_posted")
                .set_key_schema(Some(vec![
                    KeySchemaElement::builder()
                        .attribute_name("user_id")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                    KeySchemaElement::builder()
                        .attribute_name("posted")
                        .key_type(KeyType::Range)
                        .build()
                        .context(GenericBuildFailureSnafu {
                            name: "posted".to_string(),
                        })?,
                ]))
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "posts_by_posted".to_string(),
                })?,
        )
        .local_secondary_indexes(
            LocalSecondaryIndex::builder()
                .index_name("posts_by_day")
                .set_key_schema(Some(vec![
                    KeySchemaElement::builder()
                        .attribute_name("user_id")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                    KeySchemaElement::builder()
                        .attribute_name("day")
                        .key_type(KeyType::Range)
                        .build()
                        .unwrap(),
                ]))
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("posts_by_id")
                .set_key_schema(Some(vec![
                    KeySchemaElement::builder()
                        .attribute_name("id")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                ]))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .context(CreateTableSnafu);
    debug!("create posts: {:#?}", out);
    out.map(|_| ())
}

async fn create_likes(client: &Client) -> Result<()> {
    let out = client
        .create_table()
        .table_name("likes")
        // This is what the ScyllaDB/Alternator example uses-- not sure this is suitable for
        // DynamoDB
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![
            table_attr!("user_id_and_url", S), // partition key
            table_attr!("like_id", S),         // sort key
        ]))
        .set_key_schema(Some(vec![
            KeySchemaElement::builder()
                .attribute_name("user_id_and_url")
                .key_type(KeyType::Hash)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "user_id_and_url".to_string(),
                })?,
            KeySchemaElement::builder()
                .attribute_name("like_id")
                .key_type(KeyType::Range)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "like_id".to_string(),
                })?,
        ]))
        .send()
        .await
        .context(CreateTableSnafu);
    debug!("create likes: {:#?}", out);
    out.map(|_| ())
}

async fn create_replies(client: &Client) -> Result<()> {
    let out = client
        .create_table()
        .table_name("replies")
        // This is what the ScyllaDB/Alternator example uses-- not sure this is suitable for
        // DynamoDB
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![
            table_attr!("user_id_and_url", S), // partition key
            table_attr!("reply_id", S),        // sort key
        ]))
        .set_key_schema(Some(vec![
            KeySchemaElement::builder()
                .attribute_name("user_id_and_url")
                .key_type(KeyType::Hash)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "user_id_and_url".to_string(),
                })?,
            KeySchemaElement::builder()
                .attribute_name("reply_id")
                .key_type(KeyType::Range)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "like_id".to_string(),
                })?,
        ]))
        .send()
        .await
        .context(CreateTableSnafu);
    debug!("create replies: {:#?}", out);
    out.map(|_| ())
}

async fn create_shares(client: &Client) -> Result<()> {
    let out = client
        .create_table()
        .table_name("shares")
        // This is what the ScyllaDB/Alternator example uses-- not sure this is suitable for
        // DynamoDB
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![
            table_attr!("user_id_and_url", S), // partition key
            table_attr!("share_id", S),        // sort key
        ]))
        .set_key_schema(Some(vec![
            KeySchemaElement::builder()
                .attribute_name("user_id_and_url")
                .key_type(KeyType::Hash)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "user_id_and_url".to_string(),
                })?,
            KeySchemaElement::builder()
                .attribute_name("share_id")
                .key_type(KeyType::Range)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "share_id".to_string(),
                })?,
        ]))
        .send()
        .await
        .context(CreateTableSnafu);
    debug!("create shares: {:#?}", out);
    out.map(|_| ())
}

async fn create_activity_pub_posts(client: &Client) -> Result<()> {
    let out = client
        .create_table()
        .table_name("activity_pub_posts")
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![
            table_attr!("user_id", S),
            table_attr!("post_id", S),
            table_attr!("posted", S), // Sort key for the LSI
        ]))
        .set_key_schema(Some(vec![
            KeySchemaElement::builder()
                .attribute_name("user_id")
                .key_type(KeyType::Hash)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "user_id".to_string(),
                })?,
            KeySchemaElement::builder()
                .attribute_name("post_id")
                .key_type(KeyType::Range)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "post_id".to_string(),
                })?,
        ]))
        .local_secondary_indexes(
            LocalSecondaryIndex::builder()
                .index_name("activity_pub_posts_by_posted")
                .set_key_schema(Some(vec![
                    KeySchemaElement::builder()
                        .attribute_name("user_id")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                    KeySchemaElement::builder()
                        .attribute_name("posted")
                        .key_type(KeyType::Range)
                        .build()
                        .context(GenericBuildFailureSnafu {
                            name: "posted".to_string(),
                        })?,
                ]))
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "activity_pub_posts_by_posted".to_string(),
                })?,
        )
        .send()
        .await
        .context(CreateTableSnafu);
    debug!("create activity_pub_posts: {:#?}", out);
    out.map(|_| ())
}

async fn create_tasks(client: &Client) -> Result<()> {
    let out = client
        .create_table()
        .table_name("tasks")
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![
            table_attr!("id", S), // partition key
        ]))
        .set_key_schema(Some(vec![
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "id".to_string(),
                })?,
        ]))
        .send()
        .await
        .context(CreateTableSnafu);
    debug!("create tasks: {:#?}", out);
    out.map(|_| ())
}

async fn create_raft_log(client: &Client) -> Result<()> {
    let out = client
        .create_table()
        .table_name("raft_log")
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![
            table_attr!("node_id", N),
            table_attr!("log_id", N),
        ]))
        .set_key_schema(Some(vec![
            KeySchemaElement::builder()
                .attribute_name("node_id")
                .key_type(KeyType::Hash)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "node_id".to_string(),
                })?,
            KeySchemaElement::builder()
                .attribute_name("log_id")
                .key_type(KeyType::Range)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "log_id".to_string(),
                })?,
        ]))
        .send()
        .await
        .context(CreateTableSnafu);
    debug!("create raft_log: {:#?}", out);
    out.map(|_| ())
}

async fn create_raft_metadata(client: &Client) -> Result<()> {
    let out = client
        .create_table()
        .table_name("raft_metadata")
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![
            table_attr!("node_id", N),
            table_attr!("flavor", S),
        ]))
        .set_key_schema(Some(vec![
            KeySchemaElement::builder()
                .attribute_name("node_id")
                .key_type(KeyType::Hash)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "node_id".to_string(),
                })?,
            KeySchemaElement::builder()
                .attribute_name("flavor")
                .key_type(KeyType::Range)
                .build()
                .context(GenericBuildFailureSnafu {
                    name: "flavor".to_string(),
                })?,
        ]))
        .send()
        .await
        .context(CreateTableSnafu);
    debug!("create raft_metadata: {:#?}", out);
    out.map(|_| ())
}

async fn create_tables(client: &Client) -> Result<()> {
    create_users(client).await?;
    create_following(client).await?;
    create_followers(client).await?;
    create_posts(client).await?;
    create_likes(client).await?;
    create_replies(client).await?;
    create_shares(client).await?;
    create_activity_pub_posts(client).await?;
    create_tasks(client).await?;
    create_raft_log(client).await?;
    create_raft_metadata(client).await?;
    Ok(())
}

async fn charge_tables(client: &Client) -> Result<()> {
    let out = client
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
            ("api_key".to_string(), AttributeValue::B(Blob::new(vec![0x6cu8, 0xafu8, 0x39u8, 0x26u8, 0x88u8, 0xccu8, 0x6bu8, 0x16u8, 0x4fu8, 0xe8u8, 0x8bu8, 0x78u8, 0x6au8, 0xcbu8, 0x6au8, 0xb6u8, 0xedu8, 0x4eu8, 0xdau8, 0x6eu8, 0x4bu8, 0x1au8, 0x0cu8, 0x1du8, 0xafu8, 0x09u8, 0xaau8, 0x9du8, 0xa3u8, 0xc8u8, 0x98u8, 0x73u8]))),
            ("password_hash".to_string(), AttributeValue::S("$argon2id$v=19$m=19456,t=2,p=1$P2VPm95xh/Pb5hBbokpHTg$TbheNsNWEk8OKL17u/GYhnLwgo8DCxnrzm0SJ+R/AUM".to_string())),
            ("pepper_version".to_string(), AttributeValue::S("pepper-ver:20250213".to_string())),
            ])))
        .send()
        .await
        .context(ChargeTableSnafu { name: "users".to_string()})?;
    debug!("{:#?}", out);
    let out = client
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
    debug!("{:#?}", out);
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                              main                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

fn configure_logging(matches: &ArgMatches) -> Result<()> {
    let level = match (
        matches.get_flag("debug"),
        matches.get_flag("verbose"),
        matches.get_flag("quiet"),
    ) {
        (true, _, _) => Level::TRACE,
        (false, true, _) => Level::DEBUG,
        (false, false, true) => Level::ERROR,
        _ => Level::INFO,
    };
    let filter = EnvFilter::builder()
        .with_default_directive(level.into())
        .from_env()
        .context(EnvFilterSnafu)?;
    let formatter: Box<dyn Layer<Registry> + Send + Sync> = if matches.get_flag("plain") {
        Box::new(fmt::Layer::default().compact().with_writer(io::stdout))
    } else {
        Box::new(fmt::Layer::default().json().with_writer(io::stdout))
    };
    tracing::subscriber::set_global_default(Registry::default().with(formatter).with(filter))
        .context(SubscriberSnafu)
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut matches = Command::new("indielinks-ddb")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Creating the schema for bookmarks in the Fediverse.")
        .long_about("Create the indielinks schema for DynamoDB; charge the database with some initial data.")
        .arg(
            Arg::new("debug")
                .short('D')
                .long("debug")
                .num_args(0)
                .action(ArgAction::SetTrue)
                .env("INDIELINKS_DDB_DEBUG")
                .help("produce debug output"),
        )
        .arg(
            Arg::new("plain")
                .short('p')
                .long("plain")
                .num_args(0)
                .action(ArgAction::SetTrue)
                .env("INDIELINKS_DDB_PLAIN")
                .help("log in human-readable format, not JSON/structured logging"),
        )
        .arg(
            Arg::new("quiet")
                .short('q')
                .long("quiet")
                .num_args(0)
                .action(ArgAction::SetTrue)
                .env("INDIELINKS_DDB_QUIET")
                .help("produce only error output"),
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .num_args(0)
                .action(ArgAction::SetTrue)
                .env("INDIELINKS_DDB_VERBOSE")
                .help("produce prolix output"),
        )
        .arg(
            Arg::new("creds")
                .short('c')
                .long("creds")
                .num_args(1)
                .env("INDIELINKS_DDB_CREDS")
                .value_parser(value_parser!(Credentials))
        )
        .arg(
            Arg::new("location")
                .index(1)
                .value_parser(value_parser!(DynamoLocation))
                .required(true)
                .help("Network location of the DynamoDB cluster")
                .long_help("Network location of the DynamoDB/ScyllaDB cluster.

Specify as either an AWS region ('us-west-2', e.g.) or as an URL ('http://localhost:8042, e.g.)")
                .env("INDIELINKS_DDB_LOCATION")
        )
        .get_matches();
    configure_logging(&matches)?;

    info!("indielinks-ddb {}", crate_version!());

    let creds = matches.remove_one::<Credentials>("creds");
    let location = matches.remove_one::<DynamoLocation>("location").unwrap(/* required */);

    let client = get_client(location, &creds).await?;
    create_tables(&client).await?;
    charge_tables(&client).await?;
    Ok(())
}
