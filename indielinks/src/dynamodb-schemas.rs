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

//! # DynamoDB Schema Management

use aws_sdk_dynamodb::{
    config::http::HttpResponse,
    error::SdkError,
    operation::{create_table::CreateTableError, put_item::PutItemError},
    types::{
        AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement,
        KeyType, LocalSecondaryIndex, ScalarAttributeType,
    },
    Client,
};
use chrono::Utc;
use snafu::{Backtrace, ResultExt, Snafu};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create table: {source}"))]
    CreateTable {
        #[snafu(source(from(SdkError<CreateTableError, aws_sdk_dynamodb::config::http::HttpResponse>, Box::new)))]
        source: Box<SdkError<CreateTableError, aws_sdk_dynamodb::config::http::HttpResponse>>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to build {name}: {source}"))]
    GenericBuildFailure {
        name: String,
        source: aws_sdk_dynamodb::error::BuildError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to write the schema version to the database: {source}"))]
    SchemaVersion {
        #[snafu(source(from(SdkError<PutItemError, HttpResponse>, Box::new)))]
        source: Box<SdkError<PutItemError, HttpResponse>>,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         initial schema                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

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
    client
        .create_table()
        .table_name("users")
        // This is what the ScyllaDB/Alternator example uses-- not sure this is suitable for
        // DynamoDB
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![table_attr!("id", S), table_attr!("username", S)]))
        .set_key_schema(Some(vec![KeySchemaElement::builder()
            .attribute_name("id")
            .key_type(KeyType::Hash)
            .build()
            .context(GenericBuildFailureSnafu {
                name: "id".to_string(),
            })?]))
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
    client
        .create_table()
        .table_name("unique_usernames")
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![table_attr!("username", S)]))
        .set_key_schema(Some(vec![KeySchemaElement::builder()
            .attribute_name("username")
            .key_type(KeyType::Hash)
            .build()
            .context(GenericBuildFailureSnafu {
                name: "username".to_string(),
            })?]))
        .send()
        .await
        .context(CreateTableSnafu)
        .map(|_| ())
}

async fn create_following(client: &Client) -> Result<()> {
    client
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
                .set_key_schema(Some(vec![KeySchemaElement::builder()
                    .attribute_name("actor_id")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap()]))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .context(CreateTableSnafu)
        .map(|_| ())
}

async fn create_followers(client: &Client) -> Result<()> {
    client
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
        .context(CreateTableSnafu)
        .map(|_| ())
}

async fn create_posts(client: &Client) -> Result<()> {
    client
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
                .set_key_schema(Some(vec![KeySchemaElement::builder()
                    .attribute_name("id")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap()]))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .context(CreateTableSnafu)
        .map(|_| ())
}

async fn create_likes(client: &Client) -> Result<()> {
    client
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
        .context(CreateTableSnafu)
        .map(|_| ())
}

async fn create_replies(client: &Client) -> Result<()> {
    client
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
        .context(CreateTableSnafu)
        .map(|_| ())
}

async fn create_shares(client: &Client) -> Result<()> {
    client
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
        .context(CreateTableSnafu)
        .map(|_| ())
}

async fn create_activity_pub_posts(client: &Client) -> Result<()> {
    client
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
        .context(CreateTableSnafu)
        .map(|_| ())
}

async fn create_tasks(client: &Client) -> Result<()> {
    client
        .create_table()
        .table_name("tasks")
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![
            table_attr!("id", S), // partition key
        ]))
        .set_key_schema(Some(vec![KeySchemaElement::builder()
            .attribute_name("id")
            .key_type(KeyType::Hash)
            .build()
            .context(GenericBuildFailureSnafu {
                name: "id".to_string(),
            })?]))
        .send()
        .await
        .context(CreateTableSnafu)
        .map(|_| ())
}

async fn create_raft_log(client: &Client) -> Result<()> {
    client
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
        .context(CreateTableSnafu)
        .map(|_| ())
}

async fn create_raft_metadata(client: &Client) -> Result<()> {
    client
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
        .context(CreateTableSnafu)
        .map(|_| ())
}

async fn create_schema_migrations(client: &Client) -> Result<()> {
    client
        .create_table()
        .table_name("schema_migrations")
        .billing_mode(BillingMode::PayPerRequest)
        .set_attribute_definitions(Some(vec![table_attr!("version", N)]))
        .set_key_schema(Some(vec![KeySchemaElement::builder()
            .attribute_name("version")
            .key_type(KeyType::Hash)
            .build()
            .context(GenericBuildFailureSnafu {
                name: "verseion".to_string(),
            })?]))
        .send()
        .await
        .context(CreateTableSnafu)
        .map(|_| ())
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
    create_schema_migrations(client).await?;
    Ok(())
}

pub async fn create_schema(client: Client) -> Result<()> {
    create_tables(&client).await?;

    client
        .put_item()
        .table_name("schema_migrations")
        .item("version".to_owned(), AttributeValue::N("0".to_owned()))
        .item(
            "applied".to_owned(),
            AttributeValue::S(format!("{}", Utc::now().timestamp())),
        )
        .send()
        .await
        .context(SchemaVersionSnafu)
        .map(|_| ())
}
