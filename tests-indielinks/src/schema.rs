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

//! # schema checks
//!
//! ## Introduction
//!
//! Integration tests verifying properties of the DynamoDB/Alternator schema itself; registered
//! only against the Alternator fixtures (the ScyllaDB backend has no notion of billing modes).

use std::result::Result as StdResult;

use aws_sdk_dynamodb::types::BillingMode;
use futures::stream::{self, StreamExt, TryStreamExt};
use libtest_mimic::Failed;

use indielinks::dynamodb::create_client;

use crate::helper::DynamoConfig;

/// Every table created by `create_schema` in [indielinks::dynamodb_schemas]
///
/// [indielinks::dynamodb_schemas]: ../indielinks/dynamodb_schemas/index.html
const TABLES: [&str; 11] = [
    "users",
    "unique_usernames",
    "posts",
    "following",
    "followers",
    "tasks",
    "raft_log",
    "raft_metadata",
    "schema_migrations",
    "likes_replies_shares",
    "incoming_likes_replies_shares",
];

/// Verify that every table was created with on-demand (`PAY_PER_REQUEST`) billing
///
/// The AWS deployment standardizes on on-demand billing for all tables & their global secondary
/// indexes (task APP-004 of the AWS deployment plan). Global secondary indexes on an on-demand
/// table inherit its billing mode, so checking each table suffices.
pub async fn tables_use_on_demand_billing(cfg: DynamoConfig) -> StdResult<(), Failed> {
    let client = create_client(&cfg.location, &cfg.credentials).await?;
    stream::iter(TABLES)
        .then(|table| {
            let client = &client;
            async move {
                let billing_mode = client
                    .describe_table()
                    .table_name(table)
                    .send()
                    .await?
                    .table
                    .ok_or_else(|| Failed::from(format!("table {table} should exist")))?
                    .billing_mode_summary
                    .and_then(|summary| summary.billing_mode);
                assert_eq!(
                    Some(BillingMode::PayPerRequest),
                    billing_mode,
                    "table {table} should use on-demand billing"
                );
                Ok::<(), Failed>(())
            }
        })
        .try_collect::<Vec<()>>()
        .await
        .map(|_| ())
}
