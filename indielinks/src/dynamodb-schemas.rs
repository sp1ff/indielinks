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

//! # DynamoDB Schema Management

use std::time::Duration;

use aws_sdk_dynamodb::{
    client::Waiters,
    config::http::HttpResponse,
    error::SdkError,
    operation::{
        create_table::CreateTableError, delete_table::DeleteTableError, get_item::GetItemError,
        put_item::PutItemError,
    },
    primitives::Blob,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement,
        KeyType, LocalSecondaryIndex, Projection, ProjectionType, ScalarAttributeType,
    },
    waiters::{
        table_exists::WaitUntilTableExistsError, table_not_exists::WaitUntilTableNotExistsError,
    },
    Client,
};
use chrono::Utc;
use indielinks_shared::instance_state::InstanceStateV0;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create table: {source}"))]
    CreateTable {
        name: String,
        #[snafu(source(from(SdkError<CreateTableError, aws_sdk_dynamodb::config::http::HttpResponse>, Box::new)))]
        source: Box<SdkError<CreateTableError, aws_sdk_dynamodb::config::http::HttpResponse>>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize instance state from MessagePack: {source}"))]
    DeInstanceState {
        source: rmp_serde::decode::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("When deserializing instance state, got an AttributeValue of {attr_val:#?}"))]
    DeInstanceStateAttrVal {
        attr_val: AttributeValue,
        backtrace: Backtrace,
    },
    #[snafu(display("While deleting {table_name}: {source}"))]
    DeleteTable {
        table_name: String,
        #[snafu(source(from(SdkError<DeleteTableError, HttpResponse>, Box::new)))]
        source: Box<SdkError<DeleteTableError, HttpResponse>>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to build {name}: {source}"))]
    GenericBuildFailure {
        name: String,
        source: aws_sdk_dynamodb::error::BuildError,
        backtrace: Backtrace,
    },
    #[snafu(display("While fetching instance state: {source}"))]
    GetInstanceState {
        #[snafu(source(from(SdkError<GetItemError, HttpResponse>, Box::new)))]
        source: Box<SdkError<GetItemError, HttpResponse>>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to create the instance state: {source}"))]
    InstanceState {
        source: indielinks_shared::instance_state::Error,
    },
    #[snafu(display(
        "The request to fetch instance state succeeded, but had no instance_state field"
    ))]
    InvalidInstanceState { backtrace: Backtrace },
    #[snafu(display("The request to fetch instance state succeeded, but returned None"))]
    MissingInstanceState { backtrace: Backtrace },
    #[snafu(display("The schema_migrations table failed to become ready: {source}"))]
    SchemaMigrationsExists {
        #[snafu(source(from(WaitUntilTableExistsError, Box::new)))]
        source: Box<WaitUntilTableExistsError>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to write the schema version to the database: {source}"))]
    SchemaVersion {
        #[snafu(source(from(SdkError<PutItemError, HttpResponse>, Box::new)))]
        source: Box<SdkError<PutItemError, HttpResponse>>,
        backtrace: Backtrace,
    },
    #[snafu(display("While serializing the instance state to MessagePack: {source}"))]
    SerInstanceState {
        source: rmp_serde::encode::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Table {table_name} failed to delete: {source}"))]
    TableNotExists {
        table_name: String,
        #[snafu(source(from(WaitUntilTableNotExistsError, Box::new)))]
        source: Box<WaitUntilTableNotExistsError>,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        schema utilities                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

// Table creation for DynamoDB/ScyllaDB (Alternator) involves _lot_ of boilerplate. Let's see if we
// can't wrap that up in a declarative macro. This is still kinda lame in that it doesn't capture
// all the required relationships among its arguments (e.g. if a column is used as a partition- or a
// sort-key in the table or in a global or local secondary index, it must have an `attr_defn`).
// Still. The API doesn't capture that, either, and this macro removes a *lot* of boilerplate.
macro_rules! create_table {
    (client = $client:expr,
     table_name = $table_name:expr,
     $(attr_defn = ($col_name:expr, $col_ty:ident)),+,
     pk = $pk_name:expr
     $(, sk = $sk_name:expr)?
     $(, gsi : (name = $gsi_name:expr, pk = $gsi_pk:expr $(, sk = $gsi_sk:expr)?))*
     $(, lsi : (name = $lsi_name:expr, pk = $lsi_pk:expr $(, sk = $lsi_sk:expr)?))*
    ) => {
        $client
            .create_table()
            .table_name($table_name)
            .billing_mode(BillingMode::PayPerRequest)
            .set_attribute_definitions(Some(vec![
                $(
                    AttributeDefinition::builder()
                        .attribute_name($col_name)
                        .attribute_type(ScalarAttributeType::$col_ty)
                        .build()
                        .context(GenericBuildFailureSnafu {
                            name: $col_name.to_owned(),
                        })?
                ),+
            ]))
            .set_key_schema(Some(vec![
                KeySchemaElement::builder()
                    .attribute_name($pk_name)
                    .key_type(KeyType::Hash)
                    .build()
                    .context(GenericBuildFailureSnafu {
                        name: $pk_name.to_owned(),
                    })?
                $(,
                KeySchemaElement::builder()
                    .attribute_name($sk_name)
                    .key_type(KeyType::Range)
                    .build()
                    .context(GenericBuildFailureSnafu {
                        name: $sk_name.to_owned(),
                    })?
                )?
            ]))
            $(
                .global_secondary_indexes(
                    GlobalSecondaryIndex::builder()
                        .index_name($gsi_name)
                        .set_key_schema(Some(vec![
                            KeySchemaElement::builder()
                                .attribute_name($gsi_pk)
                                .key_type(KeyType::Hash)
                                .build()
                                .unwrap()
                                $(,
                                  KeySchemaElement::builder()
                                  .attribute_name($gsi_sk)
                                  .key_type(KeyType::Range)
                                  .build()
                                  .context(GenericBuildFailureSnafu {
                                      name: $gsi_sk.to_owned(),
                                  })?
                                )?
                        ]))
                        .projection(
                            Projection::builder()
                                .projection_type(ProjectionType::All)
                                .build(),
                        )
                        .build()
                        .context(GenericBuildFailureSnafu { name: $gsi_name })?,
                )
            )*
            $(
                .local_secondary_indexes(
                    LocalSecondaryIndex::builder()
                        .index_name($lsi_name)
                        .set_key_schema(Some(vec![
                            KeySchemaElement::builder()
                                .attribute_name($lsi_pk)
                                .key_type(KeyType::Hash)
                                .build()
                                .unwrap()
                                $(,
                                  KeySchemaElement::builder()
                                  .attribute_name($lsi_sk)
                                  .key_type(KeyType::Range)
                                  .build()
                                  .context(GenericBuildFailureSnafu {
                                      name: $lsi_sk.to_owned(),
                                  })?
                                )?
                        ]))
                        .projection(
                            Projection::builder()
                                .projection_type(ProjectionType::All)
                                .build(),
                        )
                        .build()
                        .context(GenericBuildFailureSnafu {
                            name: $lsi_name.to_owned(),
                        })?,
                )

            )*
            .send()
            .await
            .context(CreateTableSnafu { name: $table_name })
    };
}

macro_rules! _delete_table {
    ($client:expr, $table_name:expr, $timeout:expr) => {
        $client
            .delete_table()
            .table_name($table_name)
            .send()
            .await
            .context(DeleteTableSnafu {
                table_name: $table_name,
            })?;
        $client
            .wait_until_table_not_exists()
            .table_name($table_name)
            .wait(Duration::from_secs(60))
            .await
            .context(TableNotExistsSnafu {
                table_name: $table_name,
            })?;
    };
}

async fn update_schema_migrations(client: &Client, schema_version: i64) -> Result<()> {
    let instance_state = if schema_version == 0 {
        InstanceStateV0::new().context(InstanceStateSnafu)?
    } else {
        match client
            .get_item()
            .table_name("schema_migrations")
            .key(
                "version".to_owned(),
                AttributeValue::N(format!("{}", schema_version - 1)),
            )
            .send()
            .await
            .context(GetInstanceStateSnafu)?
            .item()
            .context(MissingInstanceStateSnafu)?
            .get("instance_state")
            .context(InvalidInstanceStateSnafu)?
        {
            AttributeValue::B(blob) => rmp_serde::from_slice::<InstanceStateV0>(blob.as_ref())
                .context(DeInstanceStateSnafu)?,
            attr_val => {
                return DeInstanceStateAttrValSnafu {
                    attr_val: attr_val.clone(),
                }
                .fail();
            }
        }
    };

    let buf = rmp_serde::to_vec_named(&instance_state).context(SerInstanceStateSnafu)?;

    client
        .put_item()
        .table_name("schema_migrations")
        .item(
            "version".to_owned(),
            AttributeValue::N(format!("{schema_version}")),
        )
        .item(
            "instance_state".to_owned(),
            AttributeValue::B(Blob::new(buf)),
        )
        .item(
            "applied".to_owned(),
            AttributeValue::S(format!("{}", Utc::now().timestamp())),
        )
        .send()
        .await
        .context(SchemaVersionSnafu)
        .map(|_| ())
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         initial schema                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_users(client: &Client) -> Result<()> {
    create_table!(
        client = client,
        table_name = "users",
        attr_defn = ("id", S),
        attr_defn = ("username", S),
        pk = "id",
        gsi: (name = "users_by_username", pk = "username")
    )?;
    create_table!(
        client = client,
        table_name = "unique_usernames",
        attr_defn = ("username", S),
        pk = "username"
    )
    .map(|_| ())
}

async fn create_posts(client: &Client) -> Result<()> {
    create_table!(
        client = client,
        table_name = "posts",
        attr_defn = ("user_id", S), // partition key
        attr_defn = ("url", S),     // sort key
        attr_defn = ("posted", S),  // sort key for first LSI
        attr_defn = ("day", S),     // sort key for second LSI
        attr_defn = ("id", S),      // partition key for the GSI
        pk = "user_id",
        sk = "url",
        gsi: (name = "posts_by_id", pk = "id"),
        lsi: (name = "posts_by_posted", pk = "user_id", sk = "posted"),
        lsi: (name = "posts_by_day", pk = "user_id", sk = "day")
    )
    .map(|_| ())
}

async fn create_tables(client: &Client) -> Result<()> {
    create_users(client).await?;
    create_table!(
        client = client,
        table_name = "following",
        attr_defn = ("user_id", S),
        attr_defn = ("actor_id", S),
        pk = "user_id",
        sk = "actor_id",
        gsi: (name = "following_by_actor_id", pk = "actor_id")
    )?;
    create_table!(
        client = client,
        table_name = "followers",
        attr_defn = ("user_id", S),  // partition key
        attr_defn = ("actor_id", S), // sort key
        pk = "user_id",
        sk = "actor_id",
        gsi: (name = "follows_by_actor_id", pk = "actor_id")
    )?;
    create_posts(client).await?;
    create_table!(
        client = client,
        table_name = "tasks",
        attr_defn = ("id", S),
        pk = "id"
    )?;
    create_table!(
        client = client,
        table_name = "raft_log",
        attr_defn = ("node_id", N),
        attr_defn = ("log_id", N),
        pk = "node_id",
        sk = "log_id"
    )?;
    create_table!(
        client = client,
        table_name = "raft_metadata",
        attr_defn = ("node_id", N),
        attr_defn = ("flavor", N),
        pk = "node_id",
        sk = "flavor"
    )?;
    create_table!(
        client = client,
        table_name = "schema_migrations",
        attr_defn = ("version", N),
        pk = "version"
    )?;
    create_table!(
        client = client,
        table_name = "likes_replies_shares",
        attr_defn = ("user_id", S),
        attr_defn = ("posted_and_id", S),
        attr_defn = ("id", S),
        pk = "user_id",
        sk = "posted_and_id",
        gsi: (name = "likes_replies_shares_by_id", pk = "id")
    )?;
    create_table!(
        client = client,
        table_name = "incoming_likes_replies_shares",
        attr_defn = ("user_id", S),
        attr_defn = ("received_and_ap_id", S),
        attr_defn = ("in_reply_to", S),
        pk = "user_id",
        sk = "received_and_ap_id",
        // This is a misnomer-- we're not indexing by ActivityPub ID, but just Post ID
        gsi: (name = "incoming_likes_replies_shares_by_ap_id", pk = "in_reply_to")
    )
    .map(|_| ())
}

pub async fn create_schema(client: Client) -> Result<()> {
    create_tables(&client).await?;
    // Even when a `CreateTable` request has returned success, the table is not available for
    // immediate use; it can take several seconds before the new table is ready to receive traffic.
    client
        .wait_until_table_exists()
        .table_name("schema_migrations")
        .wait(Duration::from_secs(60))
        .await
        .context(SchemaMigrationsExistsSnafu)?;
    update_schema_migrations(&client, 0).await
}
