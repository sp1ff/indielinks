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

use std::{
    borrow::Borrow, cmp, collections::HashMap, net::SocketAddr, result::Result as StdResult,
    time::Duration,
};

use snafu::{ResultExt, Snafu};

use indielinks_shared::{
    credentials::Credentials,
    dynamodb::{create_client, Location as DynamoLocation},
    scylla::create_client as create_scylla_client,
};

use aws_sdk_dynamodb::{
    config::http::HttpResponse,
    error::SdkError,
    operation::{batch_write_item::BatchWriteItemError, scan::ScanError},
    types::{AttributeValue, DeleteRequest, Select, WriteRequest},
};

use tokio::time::sleep;
use tracing::{debug, info};

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    #[snafu(display("Failed to build a DynamoDB DeleteRequest for {table}: {source}"))]
    BuildDeleteRequestDynamoDb {
        source: aws_sdk_dynamodb::error::BuildError,
        table: String,
    },
    #[snafu(display("Failed to connect to DynamoDB/Alternator: {source}"))]
    ConnectDynamoDb {
        source: indielinks_shared::dynamodb::Error,
    },
    #[snafu(display("Failed to connect to ScyllaDB: {source}"))]
    ConnectScylla {
        source: indielinks_shared::scylla::Error,
    },
    #[snafu(display("DynamoDB BatchWriteItem failed for {table}: {source}"))]
    BatchWriteDynamoDb {
        #[snafu(source(from(SdkError<BatchWriteItemError, HttpResponse>, Box::new)))]
        source: Box<SdkError<BatchWriteItemError, HttpResponse>>,
        table: String,
    },
    #[snafu(display("DynamoDB Scan failed for {table}: {source}"))]
    ScanDynamoDb {
        #[snafu(source(from(SdkError<ScanError, HttpResponse>, Box::new)))]
        source: Box<SdkError<ScanError, HttpResponse>>,
        table: String,
    },
    #[snafu(display("Retries exhausted while deleting from {table}"))]
    RetryExhaustedDynamoDb { table: String },
    #[snafu(display("Failed to select the indielinks keyspace: {source}"))]
    SelectKeyspaceScylla {
        #[snafu(source(from(scylla::errors::UseKeyspaceError, Box::new)))]
        source: Box<scylla::errors::UseKeyspaceError>,
    },
    #[snafu(display("Failed to execute CQL `{statement}`: {source}"))]
    ExecuteCqlScylla {
        #[snafu(source(from(scylla::errors::ExecutionError, Box::new)))]
        source: Box<scylla::errors::ExecutionError>,
        statement: String,
    },
}

pub type Result<T> = StdResult<T, Error>;

/// Maximum number of `BatchWriteItem` items in a single request.
const DYNAMODB_BATCH_SIZE: usize = 25;

/// Initial retry delay for unprocessed `BatchWriteItem` items.
const DYNAMODB_RETRY_INITIAL: Duration = Duration::from_millis(25);

/// Maximum retry delay for unprocessed `BatchWriteItem` items.
const DYNAMODB_RETRY_MAX: Duration = Duration::from_secs(2);

/// Maximum number of attempts to delete a batch before giving up.
const DYNAMODB_RETRY_ATTEMPTS: usize = 10;

/// Purge all rows from the DynamoDB/Alternator `raft_log` and `raft_metadata` tables.
///
/// The caller must ensure that every `indielinksd` process is stopped before calling this
/// function; the purge is deliberately non-atomic and retries on `UnprocessedItems` are
/// bounded.
pub async fn clean_dynamodb_raft_logs(
    location: &DynamoLocation,
    credentials: Option<&Credentials>,
) -> Result<()> {
    let client = create_client(location, credentials)
        .await
        .context(ConnectDynamoDbSnafu)?;

    info!("Purging DynamoDB/Alternator raft_log table");
    purge_dynamodb_table(&client, "raft_log", "node_id, log_id").await?;

    info!("Purging DynamoDB/Alternator raft_metadata table");
    purge_dynamodb_table(&client, "raft_metadata", "node_id, flavor").await?;

    Ok(())
}

/// Scan every key in one DynamoDB table and delete them in batches.
async fn purge_dynamodb_table(
    client: &aws_sdk_dynamodb::Client,
    table: &str,
    projection: &str,
) -> Result<()> {
    let mut exclusive_start_key = None;
    loop {
        let output = client
            .scan()
            .table_name(table)
            .select(Select::SpecificAttributes)
            .projection_expression(projection)
            .set_exclusive_start_key(exclusive_start_key.take())
            .send()
            .await
            .context(ScanDynamoDbSnafu { table })?;

        let keys: Vec<HashMap<String, AttributeValue>> = output.items().to_vec();
        exclusive_start_key = output.last_evaluated_key;

        delete_dynamodb_keys(client, table, keys).await?;

        if exclusive_start_key.is_none() {
            break;
        }
    }

    Ok(())
}

/// Delete a sequence of key maps from one DynamoDB table in batches, retrying unprocessed items.
async fn delete_dynamodb_keys(
    client: &aws_sdk_dynamodb::Client,
    table: &str,
    mut keys: Vec<HashMap<String, AttributeValue>>,
) -> Result<()> {
    while !keys.is_empty() {
        let batch: Vec<_> = keys
            .drain(..cmp::min(keys.len(), DYNAMODB_BATCH_SIZE))
            .collect();
        let requests: Vec<WriteRequest> = batch
            .into_iter()
            .map(|key| {
                Ok(WriteRequest::builder()
                    .delete_request(
                        DeleteRequest::builder()
                            .set_key(Some(key))
                            .build()
                            .context(BuildDeleteRequestDynamoDbSnafu { table })?,
                    )
                    .build())
            })
            .collect::<Result<Vec<_>>>()?;

        batch_write_dynamodb_deletes(client, table, requests).await?;
    }

    Ok(())
}

/// Submit a batch of `DeleteRequest`s to one DynamoDB table and retry any unprocessed items.
async fn batch_write_dynamodb_deletes(
    client: &aws_sdk_dynamodb::Client,
    table: &str,
    mut requests: Vec<WriteRequest>,
) -> Result<()> {
    let mut attempts = 0;
    let mut delay = DYNAMODB_RETRY_INITIAL;

    while !requests.is_empty() {
        if attempts >= DYNAMODB_RETRY_ATTEMPTS {
            return RetryExhaustedDynamoDbSnafu {
                table: table.to_string(),
            }
            .fail();
        }
        if attempts > 0 {
            debug!(
                "{table}: {count} unprocessed BatchWriteItem items; sleeping {delay:?} before retry {attempts}",
                count = requests.len()
            );
            sleep(delay).await;
            delay = cmp::min(delay * 2, DYNAMODB_RETRY_MAX);
        }
        attempts += 1;

        let output = client
            .batch_write_item()
            .request_items(table, requests)
            .send()
            .await
            .context(BatchWriteDynamoDbSnafu { table })?;

        requests = output
            .unprocessed_items
            .and_then(|mut map| map.remove(table))
            .unwrap_or_default();
    }

    Ok(())
}

/// Purge all rows from the ScyllaDB `raft_log` and `raft_metadata` tables.
///
/// The caller must ensure that every `indielinksd` process is stopped before calling this
/// function. The purge executes `TRUNCATE TABLE` for each Raft table.
pub async fn clean_scylla_raft_logs(
    hosts: impl IntoIterator<Item = impl Borrow<SocketAddr>>,
    credentials: Option<&Credentials>,
    translations: Option<impl IntoIterator<Item = (SocketAddr, SocketAddr)>>,
) -> Result<()> {
    let session = create_scylla_client(hosts, credentials, translations)
        .await
        .context(ConnectScyllaSnafu)?;

    session
        .use_keyspace("indielinks", false)
        .await
        .context(SelectKeyspaceScyllaSnafu)?;

    for statement in ["TRUNCATE TABLE raft_log", "TRUNCATE TABLE raft_metadata"] {
        info!("Executing CQL: {statement}");
        session
            .query_unpaged(statement, ())
            .await
            .context(ExecuteCqlScyllaSnafu {
                statement: statement.to_string(),
            })?;
    }

    Ok(())
}
