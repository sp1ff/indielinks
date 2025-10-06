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

//! # dynamodb
//!
//! [Storage] implementation for DynamoDB, along with assorted DDB-related utilities.
//!
//! [Storage]: crate::storage

use crate::{
    background_tasks::{Backend as TasksBackend, Error as BckError, FlatTask},
    cache::{
        to_storage_io_err, Backend as CacheBackend, Flavor, LogIndex, RaftLog, RaftMetadata, NID,
    },
    entities::{ActivityPubPost, ApiKeys, FollowId, Follower, Following, Like, Reply, Share, User},
    storage::{self, DateRange, UsernameClaimedSnafu},
    util::{Credentials, UpToThree},
};

use indielinks_shared::{
    entities::{Post, PostDay, PostId, StorUrl, Tagname, UserId, Username},
    instance_state::InstanceStateV0,
};

use async_trait::async_trait;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_dynamodb::{
    config::Region as AwsSdkRegion,
    operation::{
        batch_write_item::BatchWriteItemError, get_item::GetItemError, put_item::PutItemError,
        query::QueryOutput, update_item::UpdateItemError,
    },
    types::{AttributeValue, DeleteRequest, PutRequest, ReturnValue, Select, WriteRequest},
};
use aws_smithy_async::future::pagination_stream::PaginationStream;
use chrono::{DateTime, Duration, Utc};
use either::Either;
use futures::{stream::BoxStream, Stream};
use indielinks_cache::types::{NodeId, TypeConfig};
use itertools::Itertools;
use openraft::{Entry, ErrorSubject, ErrorVerb, LogId, LogState, StorageError, Vote};
use pin_project::pin_project;
use rmp_serde::{from_slice, to_vec};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_dynamo::{
    aws_sdk_dynamodb_1::{from_items, to_item},
    from_item,
};
use snafu::{Backtrace, IntoError, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use tracing::error;
use url::Url;
use uuid::Uuid;

use std::{
    borrow::Cow,
    cmp::min,
    collections::{HashMap, HashSet, VecDeque},
    ops::Bound,
    pin::Pin,
    task::Poll,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to add follows {follows:?}: {source}"))]
    AddFollow {
        follows: Box<HashSet<StorUrl>>,
        #[snafu(source(from(SdkError<UpdateItemError, aws_smithy_runtime_api::http::Response>, Box::new)))]
        source: Box<SdkError<UpdateItemError, aws_smithy_runtime_api::http::Response>>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to add followers {followers:?}: {source}"))]
    AddFollower {
        followers: HashSet<StorUrl>,
        #[snafu(source(from(SdkError<UpdateItemError, aws_smithy_runtime_api::http::Response>, Box::new)))]
        source: Box<SdkError<UpdateItemError, aws_smithy_runtime_api::http::Response>>,
        backtrace: Backtrace,
    },
    #[snafu(display("A query was expected to prodce at most one row & did not."))]
    AtMostOneRow { backtrace: Backtrace },
    #[snafu(display("Bad TagId {raw_tagid} read from DDB: {source}"))]
    BadTagId {
        raw_tagid: String,
        source: uuid::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Read bad ID in UpdateItemOutput: {text} ({source})"))]
    BadUpdateItemOutput {
        text: String,
        source: uuid::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Unexpected AttributeValue variant in UpdateItemOutput"))]
    BadAttrTypeUpdateItemOutput { backtrace: Backtrace },
    #[snafu(display("Bad metadata type {flavor} for {node_id}"))]
    BadMetadataType {
        node_id: NodeId,
        flavor: crate::cache::Flavor,
        backtrace: Backtrace,
    },
    #[snafu(display("A batch write failed: {source}"))]
    BatchWrite {
        #[snafu(source(from(SdkError<BatchWriteItemError, HttpResponse>, Box::new)))]
        source: Box<SdkError<BatchWriteItemError, HttpResponse>>,
        backtrace: Backtrace,
    },
    #[snafu(display("When counting items: {source}"))]
    Count {
        #[snafu(source(from(SdkError<QueryError, HttpResponse>, Box::new)))]
        source: Box<SdkError<QueryError, HttpResponse>>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to delete a post: {source}"))]
    DeletePosts {
        #[snafu(source(from(aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_dynamodb::operation::delete_item::DeleteItemError,
            aws_sdk_dynamodb::config::http::HttpResponse,
        >, Box::new)))]
        source: Box<
            aws_smithy_runtime_api::client::result::SdkError<
                aws_sdk_dynamodb::operation::delete_item::DeleteItemError,
                aws_sdk_dynamodb::config::http::HttpResponse,
            >,
        >,
    },
    #[snafu(display("Failed to deserialize items: {source}"))]
    DeItems {
        source: serde_dynamo::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a tag: {source}"))]
    DeTag {
        source: serde_dynamo::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("{username} is already claimed"))]
    DuplicateUsername {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to serialize a follower: {source}"))]
    FollowerSer {
        source: serde_dynamo::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to serialize a follow: {source}"))]
    FollowingSer {
        source: serde_dynamo::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to read last-purged from the database: {source}"))]
    LastPurgedGet {
        #[snafu(source(from(SdkError<GetItemError, HttpResponse>, Box::new)))]
        source: Box<SdkError<GetItemError, HttpResponse>>,
    },
    #[snafu(display("Read {in_count} tags in, produced {out_count} TagIds"))]
    MismatchedTagCounts {
        in_count: usize,
        out_count: usize,
        backtrace: Backtrace,
    },
    #[snafu(display(
        "The returned TagIds were not unique ({tag_ids}/{unique}); this is likely a bug"
    ))]
    MismatchedTagIdCounts {
        tag_ids: usize,
        unique: usize,
        backtrace: Backtrace,
    },
    #[snafu(display("Missing {flavor} for {node_id}"))]
    MissingMetadata {
        node_id: NodeId,
        flavor: crate::cache::Flavor,
        backtrace: Backtrace,
    },
    #[snafu(display("The `version` field is missing from the query results; this is a bug and should be reported."))]
    MissingVersion { backtrace: Backtrace },
    #[snafu(display(
        "The schema version query didn't return anything; this is a bug and should be reported."
    ))]
    MissingVersions { backtrace: Backtrace },
    #[snafu(display("{userid}'s post count has gone negative ({count})"))]
    NegativePostCount {
        userid: UserId,
        count: i32,
        backtrace: Backtrace,
    },
    #[snafu(display("The count attribute wasn't returned with UpdateItemOutput"))]
    NoCountWithUpdateItemOutput { backtrace: Backtrace },
    #[snafu(display("The ID attribute wasn't returned with UpdateItemOutput"))]
    NoIdWithUpdateItemOutput { backtrace: Backtrace },
    #[snafu(display("No endpoint URLs specified"))]
    NoEndpoints { backtrace: Backtrace },
    #[snafu(display("No tags attribute in QueryOutput"))]
    NoTagsWithQueryOutput { backtrace: Backtrace },
    #[snafu(display("The UpdateItemOutput was missing"))]
    NoUpdateItemOutput { backtrace: Backtrace },
    #[snafu(display("Failed to build an Operation: {source}"))]
    OpBuild {
        source: aws_sdk_dynamodb::error::BuildError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a Post: {source}"))]
    PostDe {
        source: serde_dynamo::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("DynamoDB query failed: {source}"))]
    Query {
        #[snafu(source(from(aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_dynamodb::operation::query::QueryError,
            aws_sdk_dynamodb::config::http::HttpResponse,
        >, Box::new)))]
        source: Box<
            aws_smithy_runtime_api::client::result::SdkError<
                aws_sdk_dynamodb::operation::query::QueryError,
                aws_sdk_dynamodb::config::http::HttpResponse,
            >,
        >,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a Raft log entry: {source}"))]
    RaftLogDecode {
        source: rmp_serde::decode::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to serialize a Raft log entry: {source}"))]
    RaftLogEncode {
        source: rmp_serde::encode::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to serialize a Raft log entry to a DDB Item: {source}"))]
    RaftLogSer {
        source: serde_dynamo::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a Raft log entry from a DDB Item: {source}"))]
    RaftLogDe {
        source: serde_dynamo::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize openraft metadata: {source}"))]
    RaftMetaDe {
        source: serde_dynamo::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to decode openraft metadata: {source}"))]
    RaftMetaDecode {
        source: rmp_serde::decode::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to encode openraft metadata: {source}"))]
    RaftMetaEncode {
        source: rmp_serde::encode::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to serialize openraft metadata: {source}"))]
    RaftMetaSer {
        source: serde_dynamo::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to write Raft metadata to the database: {source}"))]
    RaftMetaPut {
        #[snafu(source(from(SdkError<PutItemError, HttpResponse>, Box::new)))]
        source: Box<SdkError<PutItemError, HttpResponse>>,
        backtrace: Backtrace,
    },
    #[snafu(display("A scan failed: {source}"))]
    Scan {
        #[snafu(source(from(SdkError<ScanError, HttpResponse>, Box::new)))]
        source: Box<SdkError<ScanError, HttpResponse>>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to serialize API keys {keys:?} for {user:?}: {source}"))]
    SerApiKeys {
        user: Box<User>,
        keys: Box<ApiKeys>,
        #[snafu(source(from(serde_dynamo::Error, Box::new)))]
        source: Box<serde_dynamo::Error>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to serialize to an AttributeValue: {source}"))]
    TAV {
        source: serde_dynamo::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to update post counts: {source}"))]
    UpdatePostCounts {
        #[snafu(source(from(aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_dynamodb::operation::update_item::UpdateItemError,
            aws_sdk_dynamodb::config::http::HttpResponse,
        >, Box::new)))]
        source: Box<
            aws_smithy_runtime_api::client::result::SdkError<
                aws_sdk_dynamodb::operation::update_item::UpdateItemError,
                aws_sdk_dynamodb::config::http::HttpResponse,
            >,
        >,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to update a post time: {source}"))]
    UpdatePostTime {
        #[snafu(source(from(aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_dynamodb::operation::update_item::UpdateItemError,
            aws_sdk_dynamodb::config::http::HttpResponse,
        >, Box::new)))]
        source: Box<
            aws_smithy_runtime_api::client::result::SdkError<
                aws_sdk_dynamodb::operation::update_item::UpdateItemError,
                aws_sdk_dynamodb::config::http::HttpResponse,
            >,
        >,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to update a tag: {source}"))]
    UpdateTag {
        #[snafu(source(from(aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_dynamodb::operation::update_item::UpdateItemError,
            aws_sdk_dynamodb::config::http::HttpResponse,
        >, Box::new)))]
        source: Box<
            aws_smithy_runtime_api::client::result::SdkError<
                aws_sdk_dynamodb::operation::update_item::UpdateItemError,
                aws_sdk_dynamodb::config::http::HttpResponse,
            >,
        >,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to update API keys {keys:?} for {user:?}: {source}"))]
    UpdateUserApiKeys {
        user: Box<User>,
        keys: Box<ApiKeys>,
        #[snafu(source(from(SdkError<
            aws_sdk_dynamodb::operation::update_item::UpdateItemError,
            aws_sdk_dynamodb::config::http::HttpResponse,
        >, Box::new)))]
        source: Box<
            SdkError<
                aws_sdk_dynamodb::operation::update_item::UpdateItemError,
                aws_sdk_dynamodb::config::http::HttpResponse,
            >,
        >,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to write user {user:?} to the database: {source}"))]
    User {
        user: Box<User>,
        #[snafu(source(from(SdkError<PutItemError, HttpResponse>, Box::new)))]
        source: Box<SdkError<PutItemError, HttpResponse>>,
    },
    #[snafu(display("Failed to check whether the username was claimed: {source}"))]
    Username {
        username: Username,
        #[snafu(source(from(SdkError<PutItemError, HttpResponse>, Box::new)))]
        source: Box<SdkError<PutItemError, HttpResponse>>,
    },
    #[snafu(display("{text} could not be parsed as a schema version ({source}); this is a bug."))]
    Version {
        text: String,
        source: std::num::ParseIntError,
        backtrace: Backtrace,
    },
    #[snafu(display(
        "The schema version came back with an unexpected DDB AttributeType; this is a bug."
    ))]
    VersionType { backtrace: Backtrace },
    #[snafu(display("Failed to read a vote from the database: {source}"))]
    VoteGet {
        #[snafu(source(from(SdkError<GetItemError, HttpResponse>, Box::new)))]
        source: Box<SdkError<GetItemError, HttpResponse>>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to write vote {vote:?} to the database: {source}"))]
    VotePut {
        vote: Vote<NodeId>,
        #[snafu(source(from(SdkError<PutItemError, HttpResponse>, Box::new)))]
        source: Box<SdkError<PutItemError, HttpResponse>>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to serialize a Raft Vote: {source}"))]
    VoteEncode {
        source: rmp_serde::encode::Error,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

use storage::Error as StorError;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                   DynamoDB-related utilities                                   //
////////////////////////////////////////////////////////////////////////////////////////////////////

// Many of these have been factored-out into standalone functions & types because they're used by
// the integration test suite as well as by the Scylla `Storage` implementation.

/// Count the number of items for a given partition key
pub async fn count_range(
    client: &::aws_sdk_dynamodb::Client,
    table_name: &str,
    pk_name: &str,
    pk_value: &str,
    index_name: Option<String>,
) -> Result<usize> {
    Ok(client
        .query()
        .table_name(table_name)
        .set_index_name(index_name)
        .key_condition_expression(format!("{} = :pk", pk_name))
        .expression_attribute_values(":pk", AttributeValue::S(pk_value.to_string()))
        .select(Select::Count)
        .send()
        .await
        .context(CountSnafu)?
        .count as usize)
}

/// Write a user into the database
///
/// This logic has been factored out because the integration tests make use of it, as well. Returns
/// true on success, false if the username was already claimed.
pub async fn add_user(client: &::aws_sdk_dynamodb::Client, user: &User) -> Result<bool> {
    use aws_sdk_dynamodb::{
        error::SdkError, operation::put_item::PutItemError::ConditionalCheckFailedException,
    };
    let claimed = match client
        .put_item()
        .table_name("unique_usernames")
        .item("username", AttributeValue::S(user.username().to_string()))
        .item("id", AttributeValue::S(user.id().to_string()))
        .condition_expression("attribute_not_exists(username)")
        .send()
        .await
    {
        Ok(_) => false,
        Err(err) => {
            if matches!(err, SdkError::ServiceError(ref inner) if matches!(inner.err(), ConditionalCheckFailedException(_)))
            {
                true
            } else {
                return Err(UsernameSnafu {
                    username: user.username().clone(),
                }
                .into_error(err));
            }
        }
    };
    if claimed {
        return Ok(false);
    }

    // I first thought to serialize the `User` via `serde_dynamo::to_item`, but serde_dynamo makes
    // some... odd choices (serializing an empty `HashSet<String>` to `L: []`, for instance). I want
    // to use `to_attribute_value()` wherever I can to avoid exposing implementation details and
    // duplicating serialization logic. That said, I'd like to reconsider this:

    use serde_dynamo::to_attribute_value as tav;
    let item = HashMap::from([
        ("id".to_string(), tav(user.id()).context(TAVSnafu)?),
        (
            "username".to_string(),
            tav(user.username()).context(TAVSnafu)?,
        ),
        (
            "discoverable".to_string(),
            tav(user.discoverable()).context(TAVSnafu)?,
        ),
        (
            "display_name".to_string(),
            tav(user.display_name()).context(TAVSnafu)?,
        ),
        (
            "summary".to_string(),
            tav(user.summary()).context(TAVSnafu)?,
        ),
        (
            "pub_key_pem".to_string(),
            tav(user.pub_key()).context(TAVSnafu)?,
        ),
        (
            "priv_key_pem".to_string(),
            tav(user.priv_key()).context(TAVSnafu)?,
        ),
        (
            "api_keys".to_string(),
            tav(user.api_keys()).context(TAVSnafu)?,
        ),
        (
            "password_hash".to_string(),
            tav(user.password_hash()).context(TAVSnafu)?,
        ),
        (
            "pepper_version".to_string(),
            tav(user.pepper_version()).context(TAVSnafu)?,
        ),
    ]);

    client
        .put_item()
        .table_name("users")
        .set_item(Some(item))
        .send()
        .await
        .context(UserSnafu {
            user: Box::new(user.clone()),
        })?;
    Ok(true)
}

/// Add a follower to the database
///
/// This logic has been factored out because the integration tests make use of it, as well.
pub async fn add_followers(
    client: &::aws_sdk_dynamodb::Client,
    user: &User,
    followers: &HashSet<StorUrl>,
) -> Result<()> {
    // Build-up a vector of `WriteRequest`; we'll batch 'em, below.
    let mut write_reqs = followers
        .iter()
        .map(|follow| {
            WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .set_item(Some(
                            to_item(Follower::new(user, follow)).context(FollowerSerSnafu)?,
                        ))
                        .build()
                        .context(OpBuildSnafu)?,
                )
                .build()
                .pipe(Ok)
        })
        .collect::<Result<Vec<WriteRequest>>>()?;

    // `BatchWrite` can only handle 25 items at a time.
    while !write_reqs.is_empty() {
        let this_batch: Vec<_> = write_reqs.drain(..min(write_reqs.len(), 25)).collect();
        client
            .batch_write_item()
            .request_items("followers", this_batch)
            .send()
            .await
            .context(BatchWriteSnafu)?;
    }

    Ok(())
}

/// Add a follow to the database
///
/// This logic has been factored out because the integration tests make use of it, as well.
pub async fn add_following(
    client: &::aws_sdk_dynamodb::Client,
    user: &User,
    follows: &HashSet<(StorUrl, FollowId)>,
) -> Result<()> {
    // Build-up a vector of `WriteRequest`; we'll batch 'em, below.
    let mut write_reqs = follows
        .iter()
        .map(|follow| {
            WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .set_item(Some(
                            to_item(Following::new_with_id(user, &follow.0, &follow.1))
                                .context(FollowingSerSnafu)?,
                        ))
                        .build()
                        .context(OpBuildSnafu)?,
                )
                .build()
                .pipe(Ok)
        })
        .collect::<Result<Vec<WriteRequest>>>()?;

    // `BatchWrite` can only handle 25 items at a time.
    while !write_reqs.is_empty() {
        let this_batch: Vec<_> = write_reqs.drain(..min(write_reqs.len(), 25)).collect();
        client
            .batch_write_item()
            .request_items("following", this_batch)
            .send()
            .await
            .context(BatchWriteSnafu)?;
    }

    Ok(())
}

/// A [Stream] implementation on top of a DDB [PaginationStream]
#[pin_project]
pub struct PagedResultsStream<T> {
    #[pin]
    stream: Option<PaginationStream<StdResult<QueryOutput, SdkError<QueryError, HttpResponse>>>>,
    count: usize,
    #[pin]
    curr: VecDeque<T>,
}

impl<'a, T> PagedResultsStream<T>
where
    T: serde::Deserialize<'a>,
{
    pub async fn new(
        client: &::aws_sdk_dynamodb::Client,
        table_name: &str,
        pk_name: &str,
        pk_value: &str,
        count: usize,
        index_name: Option<String>,
    ) -> Result<PagedResultsStream<T>> {
        Ok(PagedResultsStream {
            stream: Some(
                client
                    .query()
                    .table_name(table_name)
                    .set_index_name(index_name)
                    .key_condition_expression(format!("{} = :pk", pk_name))
                    .expression_attribute_values(":pk", AttributeValue::S(pk_value.to_string()))
                    .into_paginator()
                    .page_size(512)
                    .send(),
            ),
            count,
            curr: VecDeque::new(),
        })
    }

    // Utility method; convert a DDB `QueryOutput` to a vector of `Following`
    fn get_items(query_output: QueryOutput) -> Result<Vec<T>> {
        query_output
            .items()
            .to_vec()
            .pipe(from_items::<T>)
            .context(DeItemsSnafu)?
            .pipe(Ok)
    }
}

impl<'a, T> Stream for PagedResultsStream<T>
where
    T: serde::Deserialize<'a> + Unpin,
{
    type Item = StdResult<T, StorError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();
        loop {
            match this.curr.pop_front() {
                Some(item) => return Poll::Ready(Some(Ok(item))),
                None => {
                    // This implementation was patched-up after I generalized `PagedResultsStream`
                    // (back when it was specific to follows, it was a lot simpler). It should
                    // probably be re-implemented from scratch-- this shouldn't be this complex.
                    match this.stream.as_mut().get_mut() {
                        Some(stream) => match stream.poll_next(cx) {
                            Poll::Ready(Some(Ok(query_output))) => {
                                match Self::get_items(query_output) {
                                    Ok(next_page) => {
                                        // Not sure how to handle this; not sure it can even happen.
                                        // Certainly, I haven't seen it. I suppose if we get an
                                        // empty response, we treat the stream as exhausted.
                                        match next_page.first() {
                                            Some(_) => {
                                                *this.curr.as_mut().get_mut() = next_page.into()
                                            }
                                            None => {
                                                *this.stream.as_mut().get_mut() = None;
                                                return Poll::Ready(None);
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        self.stream = None;
                                        return Poll::Ready(Some(Err(StorError::new(err))));
                                    }
                                }
                            }
                            Poll::Ready(Some(Err(err))) => {
                                // Yield the error, then return None foreveer
                                self.stream = None;
                                return Poll::Ready(Some(Err(StorError::new(err))));
                            }
                            Poll::Ready(None) => return Poll::Ready(None),
                            Poll::Pending => return Poll::Pending,
                        },
                        None => return Poll::Ready(None),
                    }
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.count, Some(self.count))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                   creating DynamoDB clients                                    //
////////////////////////////////////////////////////////////////////////////////////////////////////

// Let's tighten-up the types for this, a bit.

/// A serializable [Region](aws_sdk_dynamodb::config::Region). Implemented as a newtype around the
/// AWS SDK's [Region] to work around Rust's orphan traits rule
// Not making this sortable since the AWS SDK doesn't
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Region(AwsSdkRegion);

impl Region {
    /// Creates a new `Region` from the given string.
    pub fn new(region: impl Into<Cow<'static, str>>) -> Self {
        Self(AwsSdkRegion::new(region))
    }

    /// Const function that creates a new `Region` from a static str.
    pub const fn from_static(region: &'static str) -> Self {
        Self(AwsSdkRegion::from_static(region))
    }
}

impl std::fmt::Display for Region {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::Deref for Region {
    type Target = AwsSdkRegion;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::convert::AsRef<AwsSdkRegion> for Region {
    fn as_ref(&self) -> &AwsSdkRegion {
        &self.0
    }
}

impl std::convert::AsRef<str> for Region {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl<'de> Deserialize<'de> for Region {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        Ok(Region::new(s))
    }
}

impl Serialize for Region {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.0.as_ref())
    }
}

/// Where to find DynamoDB on the network. Again implemented as a newtype due to orphan traits
/// rules. [Location] is not only serializable, but also parsable by [clap].
// Not making this sortable since `Region` isn't
#[derive(Clone, Debug, Deserialize, Hash, Eq, PartialEq, Serialize)]
pub struct Location(#[serde(with = "either::serde_untagged")] Either<Region, Vec<Url>>);

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Either::Left(region) => write!(f, "{region}"),
            Either::Right(urls) => write!(
                f,
                "[{}]",
                urls.iter()
                    .map(Url::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

impl std::ops::Deref for Location {
    type Target = Either<Region, Vec<Url>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Region> for Location {
    fn from(value: Region) -> Self {
        Location(Either::Left(value))
    }
}

impl From<Vec<Url>> for Location {
    fn from(value: Vec<Url>) -> Self {
        Location(Either::Right(value))
    }
}

impl std::convert::AsRef<Either<Region, Vec<Url>>> for Location {
    fn as_ref(&self) -> &Either<Region, Vec<Url>> {
        &self.0
    }
}

impl clap::builder::ValueParserFactory for Location {
    type Parser = LocationParser;

    fn value_parser() -> Self::Parser {
        LocationParser
    }
}

#[derive(Clone, Debug)]
pub struct LocationParser;

impl clap::builder::TypedValueParser for LocationParser {
    type Value = Location;

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
            Ok(s) => Ok(Location(match Url::parse(s) {
                Ok(url) => Either::Right(vec![url]),
                Err(_) => Either::Left(Region::new(s.to_string())),
            })),
            Err(_) => Ok(Location(Either::Right(
                vals.iter()
                    .cloned()
                    .map(Url::parse)
                    .collect::<std::result::Result<Vec<Url>, _>>()
                    .map_err(|_| clap::Error::new(ErrorKind::InvalidValue))?,
            ))),
        }
    }
}

/// Create an AWS SDK DynamoDB Client
pub async fn create_client(
    location: &Location,
    credentials: &Option<Credentials>,
) -> Result<aws_sdk_dynamodb::Client> {
    use secrecy::ExposeSecret;

    let credentials = credentials.as_ref().map(|Credentials((id, secret))| {
        aws_sdk_dynamodb::config::Credentials::new(
            id.expose_secret(),
            secret.expose_secret(),
            None,
            None,
            "indielinks",
        )
    });

    let config = match &location.0 {
        Either::Left(region) => {
            let region_provider = RegionProviderChain::first_try(Some(region.0.clone()))
                .or_default_provider()
                .or_else(AwsSdkRegion::new("us-west-2"));
            let mut loader = aws_config::from_env().region(region_provider);
            if let Some(credentials) = credentials {
                loader = loader.credentials_provider(credentials);
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
            if let Some(credentials) = credentials {
                loader = loader.credentials_provider(credentials);
            }
            loader.load().await
        }
    };

    Ok(aws_sdk_dynamodb::Client::new(&config))
}

/// Retrieve the current schema version (or None, if the database doesn't exist)
pub async fn get_current_schema_version(client: &aws_sdk_dynamodb::Client) -> Result<Option<u32>> {
    fn convert(map: HashMap<String, AttributeValue>) -> Result<u32> {
        match map.get("version").context(MissingVersionSnafu)? {
            AttributeValue::N(s) => s
                .parse::<u32>()
                .context(VersionSnafu { text: s.to_owned() })?
                .pipe(Ok),
            _ => VersionTypeSnafu.fail(),
        }
    }

    // It's unfortunate that we have to scan the table to do this, but what the hell: the table
    // should be very small, and this should never be run on a hot path.
    match client
        .scan()
        .table_name("schema_migrations")
        .projection_expression("version")
        .select(Select::SpecificAttributes)
        .send()
        .await
    {
        Ok(scan_output) => {
            let mut versions = scan_output
                .items
                .context(MissingVersionsSnafu)?
                .into_iter()
                .map(convert)
                .collect::<Result<Vec<_>>>()?;
            versions.sort();
            Ok(versions.pop())
        }
        Err(err) => {
            // SdkError<ScanError, HttpResponse>
            //              E           R
            if matches!(
                err.as_service_error(),
                Some(&ScanError::ResourceNotFoundException(_))
            ) {
                Ok(None) // `schema_migrations` table not found! => return None
            } else {
                error!("get_current_schema_version: {err:#?}");
                Err(ScanSnafu.into_error(err))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                indielinks DynamoDB client type                                 //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct Client {
    client: ::aws_sdk_dynamodb::Client,
    // Raft node ID
    node_id: NodeId,
}

impl Client {
    pub async fn new(
        location: &Location,
        credentials: &Option<crate::util::Credentials>,
        node_id: NodeId,
    ) -> Result<Client> {
        Ok(Client {
            client: create_client(location, credentials).await?,
            node_id,
        })
    }
}

use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::operation::delete_item::DeleteItemError;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::operation::scan::ScanError;
use aws_smithy_runtime_api::client::result::SdkError;

// Use these if you don't want to attach any context to a failed request; the `StorError` will have
// a backtrace, so the operator should be able to figure-out where the error occurred. I should
// probably wrap this boilerplate up in a macro, but I want to be sure this is the way I want to go,
// first.

impl std::convert::From<SdkError<BatchWriteItemError, HttpResponse>> for StorError {
    fn from(value: SdkError<BatchWriteItemError, HttpResponse>) -> Self {
        StorError::new(value)
    }
}

impl std::convert::From<SdkError<DeleteItemError, HttpResponse>> for StorError {
    fn from(value: SdkError<DeleteItemError, HttpResponse>) -> Self {
        StorError::new(value)
    }
}

impl std::convert::From<SdkError<GetItemError, HttpResponse>> for StorError {
    fn from(value: SdkError<GetItemError, HttpResponse>) -> Self {
        StorError::new(value)
    }
}

impl std::convert::From<SdkError<PutItemError, HttpResponse>> for StorError {
    fn from(value: SdkError<PutItemError, HttpResponse>) -> Self {
        StorError::new(value)
    }
}

impl std::convert::From<SdkError<QueryError, HttpResponse>> for StorError {
    fn from(value: SdkError<QueryError, HttpResponse>) -> Self {
        StorError::new(value)
    }
}

impl std::convert::From<SdkError<ScanError, HttpResponse>> for StorError {
    fn from(value: SdkError<ScanError, HttpResponse>) -> Self {
        StorError::new(value)
    }
}

impl std::convert::From<SdkError<UpdateItemError, HttpResponse>> for StorError {
    fn from(value: SdkError<UpdateItemError, HttpResponse>) -> Self {
        StorError::new(value)
    }
}

impl std::convert::From<serde_dynamo::Error> for StorError {
    fn from(value: serde_dynamo::Error) -> Self {
        StorError::new(value)
    }
}

impl std::convert::From<aws_sdk_dynamodb::error::BuildError> for StorError {
    fn from(value: aws_sdk_dynamodb::error::BuildError) -> Self {
        StorError::new(value)
    }
}

impl std::convert::From<SdkError<PutItemError, HttpResponse>> for BckError {
    fn from(value: SdkError<PutItemError, HttpResponse>) -> Self {
        BckError::new(value)
    }
}

impl std::convert::From<SdkError<ScanError, HttpResponse>> for BckError {
    fn from(value: SdkError<ScanError, HttpResponse>) -> Self {
        BckError::new(value)
    }
}

impl std::convert::From<SdkError<UpdateItemError, HttpResponse>> for BckError {
    fn from(value: SdkError<UpdateItemError, HttpResponse>) -> Self {
        BckError::new(value)
    }
}

impl std::convert::From<serde_dynamo::Error> for BckError {
    fn from(value: serde_dynamo::Error) -> Self {
        BckError::new(value)
    }
}

// These are used with queries involving projections (i.e. selecting only certain attributes).
// Again, should probably wrap them up in a macro.

#[derive(Debug, serde::Deserialize)]
struct Days {
    pub day: PostDay,
}

#[derive(Debug, serde::Deserialize)]
struct Tags {
    pub tags: Vec<Tagname>,
}

#[async_trait]
impl storage::Backend for Client {
    async fn add_activity_pub_post(&self, post: &ActivityPubPost) -> StdResult<(), StorError> {
        self.client
            .put_item()
            .table_name("activity_pub_posts")
            .set_item(Some(serde_dynamo::to_item(post)?))
            .send()
            .await?;
        Ok(())
    }
    async fn add_follower(&self, user: &User, follower: &StorUrl) -> StdResult<(), StorError> {
        add_followers(&self.client, user, &HashSet::from([follower.clone()]))
            .await
            .map_err(StorError::new)
    }

    async fn add_following(
        &self,
        user: &User,
        follow: &StorUrl,
        id: &FollowId,
    ) -> StdResult<(), StorError> {
        add_following(&self.client, user, &HashSet::from([(follow.clone(), *id)]))
            .await
            .map_err(StorError::new)
    }

    async fn add_like(&self, like: &Like) -> StdResult<(), StorError> {
        self.client
            .put_item()
            .table_name("likes")
            .item(
                "user_id_and_url",
                AttributeValue::S(format!("{}#{}", like.user_id(), like.url())),
            )
            .item("id", AttributeValue::S(like.id().to_string()))
            .item("like_id", AttributeValue::S(like.like_id().to_string()))
            .item("created", AttributeValue::S(format!("{}", like.created())))
            .send()
            .await?;
        Ok(())
    }

    // Return true if an insert/upsert occurred, false if the insert/upsert failed because the post
    // already existed and `replace` was false, Err otherwise.
    async fn add_post(
        &self,
        user: &User,
        replace: bool,
        uri: &StorUrl,
        id: &PostId,
        title: &str,
        dt: &DateTime<Utc>,
        notes: &Option<String>,
        shared: bool,
        to_read: bool,
        tags: &HashSet<Tagname>,
    ) -> std::result::Result<bool, StorError> {
        let day: PostDay = dt.into();
        let post = Post::new(
            uri,
            id,
            user.id(),
            dt,
            &day,
            title,
            notes,
            tags,
            shared,
            to_read,
        );
        let item = to_item(post).unwrap();

        let mut builder = self
            .client
            .put_item()
            .table_name("posts")
            .set_item(Some(item));

        if !replace {
            builder = builder.condition_expression(
                "attribute_not_exists(user_id) and attribute_not_exists(url)",
            );
        };

        use aws_sdk_dynamodb::{
            error::SdkError, operation::put_item::PutItemError::ConditionalCheckFailedException,
        };
        match builder.send().await {
            Ok(_) => Ok(true),
            Err(err) => {
                // If we failed, it could be because `replace` was false and the conditional
                // expression failed (i.e. the item already existed). The DynamoDB SDK expresses
                // that with a `ServiceError` wrapping a `ConditionalCheckFailedException`:
                if !replace
                    && matches!(err, SdkError::ServiceError(ref inner) if matches!(inner.err(), ConditionalCheckFailedException(_)))
                {
                    Ok(false)
                } else {
                    Err(StorError::new(err))
                }
            }
        }
    }

    async fn add_reply(&self, reply: &Reply) -> StdResult<(), StorError> {
        let mut item: HashMap<String, AttributeValue> = serde_dynamo::to_item(reply)?;
        item.remove("user_id");
        item.remove("url");
        item.insert(
            "user_id_and_url".to_owned(),
            AttributeValue::S(format!("{}#{}", reply.user_id(), reply.url())),
        );

        self.client
            .put_item()
            .table_name("replies")
            .set_item(Some(item))
            .send()
            .await?;
        Ok(())
    }

    async fn add_share(&self, share: &Share) -> StdResult<(), StorError> {
        let mut item: HashMap<String, AttributeValue> = serde_dynamo::to_item(share)?;
        item.remove("user_id");
        item.remove("url");
        item.insert(
            "user_id_and_url".to_owned(),
            AttributeValue::S(format!("{}#{}", share.user_id(), share.url())),
        );

        self.client
            .put_item()
            .table_name("shares")
            .set_item(Some(item))
            .send()
            .await?;

        Ok(())
    }

    async fn add_user(&self, user: &User) -> StdResult<(), StorError> {
        if add_user(&self.client, user).await.map_err(StorError::new)? {
            Ok(())
        } else {
            UsernameClaimedSnafu {
                username: user.username().clone(),
            }
            .fail()
        }
    }

    async fn confirm_following(
        &self,
        user: &User,
        following: &StorUrl,
    ) -> StdResult<(), StorError> {
        self.client
            .update_item()
            .table_name("following")
            .key("user_id", AttributeValue::S(user.id().to_string()))
            .key("actor_id", AttributeValue::S(following.to_string()))
            .update_expression("set confirmed = :c")
            .expression_attribute_values(":c", AttributeValue::Bool(true))
            .send()
            .await
            .map_err(StorError::new)?;
        Ok(())
    }

    async fn delete_post(&self, user: &User, url: &StorUrl) -> StdResult<bool, StorError> {
        self.client
            .delete_item()
            .table_name("posts")
            .key("user_id", AttributeValue::S(user.id().to_string()))
            .key("url", AttributeValue::S(url.to_string()))
            // Asking for the old attributes lets us determine whether a deletion actually occurred; if
            // it did not, `attributes()`, below, will be None.
            .return_values(ReturnValue::AllOld)
            .send()
            .await?
            .attributes()
            .is_some()
            .pipe(Ok)
    }

    async fn delete_tag(&self, user: &User, tag: &Tagname) -> StdResult<(), StorError> {
        // This logic is awfully similar to that in `rename_tag`-- consider re-factoring?
        let mut write_reqs = self
            .client
            .query()
            .table_name("posts")
            .key_condition_expression("user_id=:id")
            .expression_attribute_values(":id", AttributeValue::S(user.id().to_string()))
            .send()
            .await?
            .items()
            .to_vec()
            .pipe(from_items::<Post>)?
            .into_iter()
            // We now have an iterator over `Post`s; map over that to get a collection of
            // `StdResult<HashMap<String,AttributeValue>,_>`...
            .map(|mut post| {
                post.delete_tag(tag);
                to_item(post)
            })
            .collect::<StdResult<Vec<HashMap<String, AttributeValue>>, _>>()?
            // Now map over *that* to convert the `HashMap`s into `Result<PutRequest,_>`s
            .into_iter()
            .map(|item| PutRequest::builder().set_item(Some(item)).build())
            .collect::<StdResult<Vec<PutRequest>, _>>()?
            // Arrrgghhhh... map once again to turn 'm into `WriteRequest`s!
            .into_iter()
            .map(|put_req| WriteRequest::builder().put_request(put_req).build())
            .collect::<Vec<WriteRequest>>();

        // `BatchWrite` can only handle 25 items at a time.
        while !write_reqs.is_empty() {
            let this_batch: Vec<_> = write_reqs.drain(..min(write_reqs.len(), 25)).collect();
            self.client
                .batch_write_item()
                .request_items("posts", this_batch)
                .send()
                .await?;
        }

        Ok(())
    }

    async fn followers_for_actor<'a>(
        &'a self,
        actor_id: &StorUrl,
    ) -> StdResult<BoxStream<'a, StdResult<Following, StorError>>, StorError> {
        let count = count_range(
            &self.client,
            "followers",
            "actor_id",
            actor_id,
            Some("following_by_actor_id".to_owned()),
        )
        .await
        .map_err(StorError::new)?;
        Ok(Box::pin(
            PagedResultsStream::new(
                &self.client,
                "followers",
                "actor_id",
                actor_id,
                count,
                Some("following_by_actor_id".to_owned()),
            )
            .await
            .map_err(StorError::new)?,
        ))
    }

    async fn get_followers<'a>(
        &'a self,
        user: &User,
    ) -> StdResult<BoxStream<'a, StdResult<Follower, StorError>>, StorError> {
        let count = count_range(
            &self.client,
            "followers",
            "user_id",
            &user.id().to_string(),
            None,
        )
        .await
        .map_err(StorError::new)?;
        Ok(Box::pin(
            PagedResultsStream::new(
                &self.client,
                "followers",
                "user_id",
                &user.id().to_string(),
                count,
                None,
            )
            .await
            .map_err(StorError::new)?,
        ))
    }

    async fn get_following<'a>(
        &'a self,
        user: &User,
    ) -> StdResult<BoxStream<'a, StdResult<Following, StorError>>, StorError> {
        let count = count_range(
            &self.client,
            "following",
            "user_id",
            &user.id().to_string(),
            None,
        )
        .await
        .map_err(StorError::new)?;
        Ok(Box::pin(
            PagedResultsStream::new(
                &self.client,
                "following",
                "user_id",
                &user.id().to_string(),
                count,
                None,
            )
            .await
            .map_err(StorError::new)?,
        ))
    }

    async fn get_posts(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
        day: &PostDay,
        uri: &Option<StorUrl>,
    ) -> StdResult<Vec<Post>, StorError> {
        let mut query = self
            .client
            .query()
            .table_name("posts")
            .key_condition_expression("user_id=:id")
            .expression_attribute_values(":id", AttributeValue::S(user.id().to_string()));

        match (uri, tags) {
            (None, UpToThree::None) => {
                query = query
                    .filter_expression("day=:day")
                    .expression_attribute_values(":day", AttributeValue::S(day.to_string()))
            }
            (None, UpToThree::One(tag)) => {
                query = query
                    .filter_expression("day=:day and contains(tags,:tag)")
                    .expression_attribute_values(":day", AttributeValue::S(day.to_string()))
                    .expression_attribute_values(":tag", AttributeValue::S(tag.to_string()))
            }
            (None, UpToThree::Two(tag0, tag1)) => {
                query = query
                    .filter_expression("day=:day and contains(tags,:tag0) and contains(tags,:tag1)")
                    .expression_attribute_values(":day", AttributeValue::S(day.to_string()))
                    .expression_attribute_values(":tag0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":tag1", AttributeValue::S(tag1.to_string()))
            }
            (None, UpToThree::Three(tag0, tag1, tag2)) => {
                query = query
                    .filter_expression("day=:day and contains(tags,:tag0) and contains(tags,:tag1) and contains(tags,:tag2)")
                    .expression_attribute_values(":day", AttributeValue::S(day.to_string()))
                    .expression_attribute_values(":tag0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":tag1", AttributeValue::S(tag1.to_string()))
                    .expression_attribute_values(":tag2", AttributeValue::S(tag2.to_string()))
            },
            (Some(uri), _) => {
                query = query
                    .key_condition_expression("url=:url")
                    .expression_attribute_values(":url", AttributeValue::S(uri.to_string()))
            }
        }

        query
            .send()
            .await?
            .items()
            .to_vec()
            .pipe(from_items::<Post>)?
            .pipe(Ok)
    }

    async fn get_post_by_id(&self, id: &PostId) -> StdResult<Option<Post>, StorError> {
        self.client
            .query()
            .table_name("posts")
            .index_name("posts_by_id")
            .select(aws_sdk_dynamodb::types::Select::AllAttributes)
            .key_condition_expression("id=:id")
            .expression_attribute_values(":id", AttributeValue::S(id.to_string()))
            .send()
            .await?
            .items()
            .to_vec()
            .pipe(from_items::<Post>)?
            .into_iter()
            .at_most_one()
            .map_err(StorError::new)?
            .pipe(Ok)
    }

    async fn get_posts_by_day(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
    ) -> StdResult<Vec<(PostDay, usize)>, StorError> {
        let mut query = self
            .client
            .query()
            .table_name("posts")
            .key_condition_expression("user_id=:id")
            .projection_expression("day")
            .expression_attribute_values(":id", AttributeValue::S(user.id().to_string()));
        match tags {
            UpToThree::None => {}
            UpToThree::One(tag) => {
                query = query
                    .filter_expression("contains(tags,:tag)")
                    .expression_attribute_values(":tag", AttributeValue::S(tag.to_string()));
            }
            UpToThree::Two(tag0, tag1) => {
                query = query
                    .filter_expression("contains(tags,:tag0) and contains(tags,:tag1)")
                    .expression_attribute_values(":tag0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":tag1", AttributeValue::S(tag1.to_string()));
            }
            UpToThree::Three(tag0, tag1, tag2) => {
                query = query
                    .filter_expression(
                        "contains(tags,:tag0) and contains(tags,:tag1) and contains(tags,:tag2)",
                    )
                    .expression_attribute_values(":tag0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":tag1", AttributeValue::S(tag1.to_string()))
                    .expression_attribute_values(":tag2", AttributeValue::S(tag2.to_string()));
            }
        }
        query
            .send()
            .await?
            .items()
            .to_vec()
            .pipe(from_items::<Days>)?
            .into_iter()
            .map(|x| x.day)
            .counts()
            .into_iter()
            .sorted_by_key(|(d, _n)| d.clone())
            .collect::<Vec<(PostDay, usize)>>()
            .pipe(Ok)
    }

    async fn get_all_posts(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
        dates: &DateRange,
        unread: bool,
    ) -> StdResult<Vec<Post>, StorError> {
        let mut query = self
            .client
            .query()
            .table_name("posts")
            .index_name("posts_by_posted")
            .key_condition_expression("user_id=:id")
            .expression_attribute_values(":id", AttributeValue::S(user.id().to_string()))
            .scan_index_forward(false);

        let mut filter_expression = String::new();
        if unread {
            filter_expression.push_str("unread=:u");
            query = query.expression_attribute_values(":u", AttributeValue::Bool(true));
        }

        fn append_clause(mut filter_expression: String, clause: &str) -> String {
            if filter_expression.is_empty() {
                clause.to_owned()
            } else {
                filter_expression.push_str(" and ");
                filter_expression.push_str(clause);
                filter_expression
            }
        }

        match (tags, dates) {
            (UpToThree::None, DateRange::None) => {}
            (UpToThree::None, DateRange::Begins(b)) => {
                filter_expression = append_clause(filter_expression, "posted>=:b");
                query = query.expression_attribute_values(":b", AttributeValue::S(b.to_string()));
            }
            (UpToThree::None, DateRange::Ends(e)) => {
                filter_expression = append_clause(filter_expression, "posted<:e");
                query = query.expression_attribute_values(":e", AttributeValue::S(e.to_string()));
            }
            (UpToThree::None, DateRange::Both(b, e)) => {
                filter_expression = append_clause(filter_expression, "posted>:b and posted<:e");
                query = query
                    .expression_attribute_values(":b", AttributeValue::S(b.to_string()))
                    .expression_attribute_values(":e", AttributeValue::S(e.to_string()));
            }
            (UpToThree::One(tag), DateRange::None) => {
                filter_expression = append_clause(filter_expression, "contains(tags,:t0)");
                query =
                    query.expression_attribute_values(":t0", AttributeValue::S(tag.to_string()));
            }
            (UpToThree::One(tag), DateRange::Begins(b)) => {
                filter_expression =
                    append_clause(filter_expression, "posted>=:b and contains(tags,:t0)");
                query = query
                    .expression_attribute_values(":b", AttributeValue::S(b.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag.to_string()));
            }
            (UpToThree::One(tag), DateRange::Ends(e)) => {
                filter_expression =
                    append_clause(filter_expression, "posted<:e and contains(tags,:t0)");
                query = query
                    .expression_attribute_values(":e", AttributeValue::S(e.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag.to_string()));
            }
            (UpToThree::One(tag), DateRange::Both(b, e)) => {
                filter_expression = append_clause(
                    filter_expression,
                    "posted>:b and posted<:e and contains(tags,:t0)",
                );
                query = query
                    .expression_attribute_values(":b", AttributeValue::S(b.to_string()))
                    .expression_attribute_values(":e", AttributeValue::S(e.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag.to_string()));
            }
            (UpToThree::Two(tag0, tag1), DateRange::None) => {
                filter_expression = append_clause(
                    filter_expression,
                    "contains(tags,:t0) and contains(tags,:t1)",
                );
                query = query
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()));
            }
            (UpToThree::Two(tag0, tag1), DateRange::Begins(b)) => {
                filter_expression
                    .push_str("posted>=:b and contains(tags,:t0) and contains(tags,:t1)");
                query = query
                    .expression_attribute_values(":b", AttributeValue::S(b.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()));
            }
            (UpToThree::Two(tag0, tag1), DateRange::Ends(e)) => {
                filter_expression
                    .push_str("posted<:e and contains(tags,:t0) and contains(tags,:t1)");
                query = query
                    .expression_attribute_values(":e", AttributeValue::S(e.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()));
            }
            (UpToThree::Two(tag0, tag1), DateRange::Both(b, e)) => {
                filter_expression = append_clause(
                    filter_expression,
                    "posted>:b and posted<:e and contains(tags,:t0) and contains(tags,:t1)",
                );
                query = query
                    .expression_attribute_values(":b", AttributeValue::S(b.to_string()))
                    .expression_attribute_values(":e", AttributeValue::S(e.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()));
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::None) => {
                filter_expression
                    .push_str("contains(tags,:t0) and contains(tags,:t1) and contains(tags, :t2)");
                query = query
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()))
                    .expression_attribute_values(":t2", AttributeValue::S(tag2.to_string()));
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::Begins(b)) => {
                filter_expression = append_clause(
                    filter_expression,
                    "posted>=:b and contains(tags,:t0) and contains(tags,:t1) and contains(tags, :t2)",
                );
                query = query
                    .expression_attribute_values(":b", AttributeValue::S(b.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()))
                    .expression_attribute_values(":t2", AttributeValue::S(tag2.to_string()));
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::Ends(e)) => {
                filter_expression = append_clause(
                    filter_expression,
                    "posted<:e and contains(tags,:t0) and contains(tags,:t1) and contains(tags, :t2)",
                );
                query = query
                    .expression_attribute_values(":e", AttributeValue::S(e.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()))
                    .expression_attribute_values(":t2", AttributeValue::S(tag2.to_string()));
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::Both(b, e)) => {
                filter_expression = append_clause(
                    filter_expression,
                    "posted>:b and posted<:e and contains(tags,:t0) and contains(tags,:t1) and contains(tags, :t2)",
                );
                query = query
                    .expression_attribute_values(":b", AttributeValue::S(b.to_string()))
                    .expression_attribute_values(":e", AttributeValue::S(e.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()))
                    .expression_attribute_values(":t2", AttributeValue::S(tag2.to_string()));
            }
        }

        if !filter_expression.is_empty() {
            query = query.filter_expression(filter_expression);
        }

        query
            .send()
            .await?
            .items()
            .to_vec()
            .pipe(from_items::<Post>)?
            .pipe(Ok)
    }

    async fn get_recent_posts(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
        count: usize,
    ) -> StdResult<Vec<Post>, StorError> {
        let mut query = self
            .client
            .query()
            .table_name("posts")
            .index_name("posts_by_posted")
            .key_condition_expression("user_id=:id")
            .expression_attribute_values(":id", AttributeValue::S(user.id().to_string()))
            .limit(count as i32)
            .scan_index_forward(false);
        match tags {
            UpToThree::None => {}
            UpToThree::One(tag) => {
                query = query
                    .filter_expression("contains(tags,:tag)")
                    .expression_attribute_values(":tag", AttributeValue::S(tag.to_string()));
            }
            UpToThree::Two(tag0, tag1) => {
                query = query
                    .filter_expression("contains(tags,:tag0) and contains(tags,:tag1)")
                    .expression_attribute_values(":tag0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":tag1", AttributeValue::S(tag1.to_string()));
            }
            UpToThree::Three(tag0, tag1, tag2) => {
                query = query
                    .filter_expression(
                        "contains(tags,:tag0) and contains(tags,:tag1) and contains(tags,:tag2)",
                    )
                    .expression_attribute_values(":tag0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":tag1", AttributeValue::S(tag1.to_string()))
                    .expression_attribute_values(":tag2", AttributeValue::S(tag2.to_string()));
            }
        }

        query
            .send()
            .await?
            .items()
            .to_vec()
            .pipe(from_items::<Post>)?
            .pipe(Ok)
    }

    async fn get_tag_cloud(&self, user: &User) -> StdResult<HashMap<Tagname, usize>, StorError> {
        self.client
            .query()
            .table_name("posts")
            .projection_expression("tags")
            .key_condition_expression("user_id=:id")
            .expression_attribute_values(":id", AttributeValue::S(user.id().to_string()))
            .send()
            .await?
            .items()
            .to_vec() // [{"tags":L(["a","b"])},..]
            .pipe(from_items::<Tags>)?
            .into_iter()
            .flat_map(|x| x.tags)
            .counts()
            .pipe(Ok)
    }

    async fn get_user_by_id(&self, id: &UserId) -> StdResult<Option<User>, StorError> {
        self.client
            .get_item()
            .table_name("users")
            .key("id", AttributeValue::S(id.to_string()))
            .send()
            .await?
            .item
            .map(from_item)
            .transpose()?
            .pipe(Ok)
    }

    async fn rename_tag(
        &self,
        user: &User,
        from: &Tagname,
        to: &Tagname,
    ) -> StdResult<(), StorError> {
        // OK: here's the plan. We whack-down the writes needed by first selecting only the posts
        // containing `from`. Using that we'll construct a `BatchWrite` request. This of course
        // leaves us open to a `posts/add` for a `Post` with the tag being renamed "sneaking in"
        // between our two queries, but such is life in the NoSQL world. I can't see a way to detect
        // that, it's a corner case, and it won't leave the system in an invalid state, so I'm
        // prepared to live with it.
        let mut write_reqs = self
            .client
            .query()
            .table_name("posts")
            .key_condition_expression("user_id=:id")
            .expression_attribute_values(":id", AttributeValue::S(user.id().to_string()))
            .send()
            .await?
            .items()
            .to_vec()
            .pipe(from_items::<Post>)?
            .into_iter()
            // We now have an iterator over `Post`s; map over that to get a collection of
            // `StdResult<HashMap<String,AttributeValue>,_>`...
            .map(|mut post| {
                post.rename_tag(from, to);
                to_item(post)
            })
            .collect::<StdResult<Vec<HashMap<String, AttributeValue>>, _>>()?
            // Now map over *that* to convert the `HashMap`s into `Result<PutRequest,_>`s
            .into_iter()
            .map(|item| PutRequest::builder().set_item(Some(item)).build())
            .collect::<StdResult<Vec<PutRequest>, _>>()?
            // Arrrgghhhh... map once again to turn 'm into `WriteRequest`s!
            .into_iter()
            .map(|put_req| WriteRequest::builder().put_request(put_req).build())
            .collect::<Vec<WriteRequest>>();

        // `BatchWrite` can only handle 25 items at a tiem.
        while !write_reqs.is_empty() {
            let this_batch: Vec<_> = write_reqs.drain(..min(write_reqs.len(), 25)).collect();
            self.client
                .batch_write_item()
                .request_items("posts", this_batch)
                .send()
                .await?;
        }

        Ok(())
    }

    async fn update_user_api_keys(&self, user: &User, keys: &ApiKeys) -> StdResult<(), StorError> {
        self.client
            .update_item()
            .table_name("users")
            .key("id", AttributeValue::S(user.id().to_string()))
            .update_expression("set api_keys=:k")
            .expression_attribute_values(
                ":k",
                AttributeValue::M(to_item(keys).map_err(|err| {
                    StorError::new(
                        SerApiKeysSnafu {
                            user: user.clone(),
                            keys: keys.clone(),
                        }
                        .into_error(err),
                    )
                })?),
            )
            .send()
            .await
            .map_err(|err| {
                StorError::new(
                    UpdateUserApiKeysSnafu {
                        user: user.clone(),
                        keys: keys.clone(),
                    }
                    .into_error(err),
                )
            })
            .map(|_| ())
    }

    async fn update_user_post_times(
        &self,
        user: &User,
        dt: &DateTime<Utc>,
    ) -> StdResult<(), StorError> {
        if user.first_update().is_none() {
            self.client
                .update_item()
                .table_name("users")
                .key("id", AttributeValue::S(user.id().to_string()))
                .update_expression("set first_update=:t")
                .expression_attribute_values(
                    ":t",
                    AttributeValue::S(dt.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string()),
                )
                .send()
                .await
                .map_err(|err| StorError::new(UpdatePostTimeSnafu.into_error(err)))?;
        }
        self.client
            .update_item()
            .table_name("users")
            .key("id", AttributeValue::S(user.id().to_string()))
            .update_expression("set last_update=:t")
            .expression_attribute_values(
                ":t",
                AttributeValue::S(dt.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string()),
            )
            .send()
            .await
            .map_err(|err| StorError::new(UpdatePostTimeSnafu.into_error(err)))?;

        Ok(())
    }

    async fn user_for_name(&self, name: &str) -> std::result::Result<Option<User>, StorError> {
        // An "Item" seems to be a HashMap<String, AttributeValue>. The DynamoDB interface is really
        // irritating because we get an Option<Vec<Item>> meaning we have to handle the "None" case
        // for the outer option as well as the zero length case for the inner Vec. It's not clear to
        // me what the semantic difference is between the two, if any.

        // I tried writing this logic solely in terms of the `Iterator` interface & `tap` primitives (pipe, mostly),
        // but it turned out to be fairly opaque; I prefer this:
        let gio = self
            .client
            .query()
            .table_name("users")
            .index_name("users_by_username")
            .key_condition_expression("username = :val")
            .expression_attribute_values(":val", AttributeValue::S(name.to_string()))
            .send()
            .await
            .map_err(StorError::new)?;

        match gio.items {
            Some(items) => from_items::<User>(items)
                .map_err(StorError::new)?
                .into_iter()
                .at_most_one()
                .map_err(StorError::new)?
                .pipe(Ok),
            None => Ok(None),
        }
    }

    async fn validate_schema_version(
        &self,
        expected_version: u32,
    ) -> StdResult<InstanceStateV0, StorError> {
        match self
            .client
            .scan()
            .table_name("schema_migrations")
            .projection_expression("instance_state")
            .filter_expression("version=:version")
            .expression_attribute_values(":version", AttributeValue::N(format!("{expected_version}")))
            .send()
            .await?
            .items()
            .iter()
            .exactly_one()
            .map_err(|_| crate::storage::SchemaSnafu.build())?
            .get("instance_state")
            .unwrap(/* known good */)
        {
            AttributeValue::B(blob) => {
                rmp_serde::from_slice::<InstanceStateV0>(blob.as_ref()).map_err(StorError::new)
            }
            _ => crate::storage::SchemaSnafu.fail(),
        }
    }
}

#[async_trait]
impl TasksBackend for Client {
    async fn write_task(&self, tag: &Uuid, buf: &[u8]) -> StdResult<(), BckError> {
        let task = FlatTask {
            id: Uuid::new_v4(),
            created: Utc::now(),
            task: buf.to_vec(),
            tag: *tag,
            lease_expires: chrono::DateTime::UNIX_EPOCH,
            done: false,
        };
        self.client
            .put_item()
            .table_name("tasks")
            .set_item(Some(to_item(task)?))
            .send()
            .await?;
        Ok(())
    }
    async fn lease_task(&self) -> StdResult<Option<(Uuid, Uuid, Vec<u8>)>, BckError> {
        let mut tasks = self
            .client
            .scan()
            .table_name("tasks")
            .filter_expression("done=:f and lease_expires<:t")
            .expression_attribute_values(
                ":t",
                AttributeValue::S(Utc::now().format("%Y-%m-%dT%H:%M:%S%.fZ").to_string()),
            )
            .expression_attribute_values(":f", AttributeValue::Bool(false))
            .send()
            .await?
            .items()
            .to_vec()
            .pipe(from_items::<FlatTask>)?;
        // Next, sort 'em by creation time...
        tasks.sort_by(|lhs, rhs| lhs.created.cmp(&rhs.created));

        async fn take_lease(client: &::aws_sdk_dynamodb::Client, t: &FlatTask) -> Result<bool> {
            let new_lease = Utc::now() + Duration::seconds(60);
            client
                .update_item()
                .table_name("tasks")
                .key("id", AttributeValue::S(t.id.to_string()))
                .condition_expression("lease_expires = :d")
                .update_expression("set lease_expires = :e")
                .expression_attribute_values(
                    ":d",
                    AttributeValue::S(t.lease_expires.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string()),
                )
                .expression_attribute_values(
                    ":e",
                    AttributeValue::S(new_lease.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string()),
                )
                .return_values(ReturnValue::AllNew) // UpdatedNew?
                .send()
                .await
                .is_ok()
                .pipe(Ok)
        }

        let mut task: Option<FlatTask> = None;
        for t in tasks {
            if take_lease(&self.client, &t).await.unwrap_or(false) {
                task = Some(t);
                break;
            }
        }

        match task {
            None => Ok(None),
            Some(task) => Ok(Some((task.tag, task.id, task.task))),
        }
    }
    async fn close_task(&self, uuid: &Uuid) -> StdResult<(), BckError> {
        let _ = self
            .client
            .update_item()
            .table_name("tasks")
            .key("id", AttributeValue::S(uuid.to_string()))
            .update_expression("set done = :t")
            .expression_attribute_values(":t", AttributeValue::Bool(true))
            .send()
            .await?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl CacheBackend for Client {
    /// Append log entries
    #[tracing::instrument(skip(self))]
    async fn append(&self, entries: Vec<Entry<TypeConfig>>) -> StdResult<(), StorageError<NodeId>> {
        async fn append1(
            client: &::aws_sdk_dynamodb::Client,
            node_id: NodeId,
            entries: Vec<Entry<TypeConfig>>,
        ) -> Result<()> {
            // Map `entries` to a `Vec<WriteRequest>` which we'll submit below:
            let mut write_reqs = entries
                .into_iter()
                .map(|entry| {
                    WriteRequest::builder()
                        .put_request(
                            PutRequest::builder()
                                .set_item(Some(
                                    to_item(&RaftLog {
                                        node_id: NID(node_id),
                                        log_id: LogIndex(entry.log_id.index),
                                        entry: to_vec(&entry).context(RaftLogEncodeSnafu)?,
                                    })
                                    .context(RaftLogSerSnafu)?,
                                ))
                                .build()
                                .context(OpBuildSnafu)?,
                        )
                        .build()
                        .pipe(Ok)
                })
                .collect::<Result<Vec<WriteRequest>>>()?;

            // `BatchWrite` can only handle 25 items at a time.
            while !write_reqs.is_empty() {
                let this_batch: Vec<_> = write_reqs.drain(..min(write_reqs.len(), 25)).collect();
                client
                    .batch_write_item()
                    .request_items("raft_log", this_batch)
                    .send()
                    .await
                    .context(BatchWriteSnafu)?;
            }

            Ok(())
        }

        append1(&self.client, self.node_id, entries)
            .await
            .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::Logs, ErrorVerb::Write, &err))
    }
    /// Drop all rows in `raft_log` and `raft_metadata`
    #[tracing::instrument(skip(self))]
    async fn drop_all_rows(&self) -> StdResult<(), StorageError<NodeId>> {
        // DynamoDB doesn't have a truncate operation, so I'll just scan & batch write
        // (again, not atomic) At this point, this is only for use with testing, so maybe not a big
        // deal.
        async fn drop1(client: &::aws_sdk_dynamodb::Client) -> Result<()> {
            let mut drop_reqs = client
                .scan()
                .table_name("raft_log")
                .send()
                .await
                .context(ScanSnafu)?
                .items()
                .to_vec()
                .into_iter()
                .map(from_item)
                .collect::<StdResult<Vec<RaftLog>, _>>()
                .context(RaftLogDeSnafu)?
                .into_iter()
                .map(|log| {
                    WriteRequest::builder()
                        .delete_request(
                            DeleteRequest::builder()
                                .key("node_id", AttributeValue::N(log.node_id.to_string()))
                                .key("log_id", AttributeValue::N(log.log_id.to_string()))
                                .build()
                                .context(OpBuildSnafu)?,
                        )
                        .build()
                        .pipe(Ok)
                })
                .collect::<Result<Vec<WriteRequest>>>()?;

            // `BatchWrite` can only handle 25 items at a time.
            while !drop_reqs.is_empty() {
                let this_batch: Vec<_> = drop_reqs.drain(..min(drop_reqs.len(), 25)).collect();
                client
                    .batch_write_item()
                    .request_items("raft_log", this_batch)
                    .send()
                    .await
                    .context(BatchWriteSnafu)?;
            }

            let mut drop_reqs = client
                .scan()
                .table_name("raft_metadata")
                .send()
                .await
                .unwrap()
                .items()
                .to_vec()
                .into_iter()
                .map(from_item)
                .collect::<StdResult<Vec<RaftMetadata>, _>>()
                .unwrap()
                .into_iter()
                .map(|log| {
                    WriteRequest::builder()
                        .delete_request(
                            DeleteRequest::builder()
                                .key("node_id", AttributeValue::N(log.node_id.to_string()))
                                .key("flavor", AttributeValue::S(log.flavor.to_string()))
                                .build()
                                .context(OpBuildSnafu)?,
                        )
                        .build()
                        .pipe(Ok)
                })
                .collect::<Result<Vec<WriteRequest>>>()?;

            // `BatchWrite` can only handle 25 items at a time.
            while !drop_reqs.is_empty() {
                let this_batch: Vec<_> = drop_reqs.drain(..min(drop_reqs.len(), 25)).collect();
                client
                    .batch_write_item()
                    .request_items("raft_metadata", this_batch)
                    .send()
                    .await
                    .context(BatchWriteSnafu)?;
            }

            Ok(())
        }

        drop1(&self.client)
            .await
            .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::None, ErrorVerb::Delete, &err))
    }
    /// Returns the last deleted log id and the last log id.
    #[tracing::instrument(skip(self))]
    async fn get_log_state(&self) -> StdResult<LogState<TypeConfig>, StorageError<NodeId>> {
        async fn get_log_state1(
            client: &::aws_sdk_dynamodb::Client,
            node_id: NodeId,
        ) -> Result<LogState<TypeConfig>> {
            // The logic below _still_ seems awfully prolix; is there no way to refactor the logic
            // "I've read a thing, I expect it to be zero or one rows of T; resolve the response to
            // an Option<T>?"
            let last_purged_log_id = client
                .get_item()
                .table_name("raft_metadata")
                .key("node_id", AttributeValue::N(node_id.to_string()))
                .key("flavor", AttributeValue::S(Flavor::LastPurged.to_string()))
                .send()
                .await
                .context(LastPurgedGetSnafu)?
                .item
                .map(from_item)
                .transpose()
                .context(RaftMetaDeSnafu)?
                .map(|meta: RaftMetadata| {
                    from_slice::<LogId<NodeId>>(&meta.data).context(RaftMetaDecodeSnafu)
                })
                .transpose()?;

            let last_log_id = client
                .query()
                .table_name("raft_log")
                .key_condition_expression("#pk = :pk")
                .expression_attribute_names("#pk", "node_id")
                .expression_attribute_values(":pk", AttributeValue::N(node_id.to_string()))
                .scan_index_forward(false) // i.e. descending order
                .limit(1)
                .send()
                .await
                .context(QuerySnafu)?
                .items()
                .to_vec()
                .into_iter()
                .at_most_one()
                .map_err(|_| AtMostOneRowSnafu.build())?
                .map(from_item::<HashMap<String, AttributeValue>, RaftLog>)
                .transpose()
                .context(RaftLogDeSnafu)?
                .map(|log| from_slice::<Entry<TypeConfig>>(&log.entry))
                .transpose()
                .context(RaftLogDecodeSnafu)?
                .map(|entry| entry.log_id)
                .or(last_purged_log_id);

            Ok(LogState {
                last_purged_log_id,
                last_log_id,
            })
        }

        get_log_state1(&self.client, self.node_id)
            .await
            .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::Logs, ErrorVerb::Read, &err))
    }
    /// Purge logs upto log_id, inclusive
    // This is particularly painful because, AFAIK, DDB offers no "range delete" operation
    #[tracing::instrument(skip(self))]
    async fn purge(&self, log_id: LogId<NodeId>) -> StdResult<(), StorageError<NodeId>> {
        async fn purge1(
            client: &::aws_sdk_dynamodb::Client,
            node_id: NodeId,
            log_id: LogId<NodeId>,
        ) -> Result<()> {
            // First step; ask for all the items whose hash key is `node_id` and whose sort key is
            // <= `log_id.index`
            let mut to_be_rmd = client
                .query()
                .table_name("raft_log")
                .key_condition_expression("#pk = :pk and #sk <= :log_id")
                .expression_attribute_names("#pk", "node_id")
                .expression_attribute_names("#sk", "log_id")
                .expression_attribute_values(":pk", AttributeValue::N(node_id.to_string()))
                .expression_attribute_values(":log_id", AttributeValue::N(log_id.index.to_string()))
                .scan_index_forward(true) // i.e. ascending order
                .send()
                .await
                .context(QuerySnafu)?
                .items()
                .to_vec()
                .into_iter()
                .map(|item| {
                    from_item::<HashMap<String, AttributeValue>, RaftLog>(item)
                        .context(RaftLogSerSnafu)
                })
                .collect::<Result<Vec<RaftLog>>>()?
                .into_iter()
                // .map(|log| log.log_idx)
                // .collect::<Vec<LogIndex>>(); // At long last, these are the indicies we'll delete
                .map(|log| {
                    WriteRequest::builder()
                        .delete_request(
                            DeleteRequest::builder()
                                .key("node_id", AttributeValue::N(node_id.to_string()))
                                .key("log_id", AttributeValue::N(log.log_id.to_string()))
                                .build()
                                .context(OpBuildSnafu)?,
                        )
                        .build()
                        .pipe(Ok)
                })
                .collect::<Result<Vec<WriteRequest>>>()?;
            // At long last, this 👆 is the collection of deletion requests we need to send.

            // `BatchWrite` can only handle 25 items at a time.
            while !to_be_rmd.is_empty() {
                let this_batch: Vec<_> = to_be_rmd.drain(..min(to_be_rmd.len(), 25)).collect();
                client
                    .batch_write_item()
                    .request_items("raft_log", this_batch)
                    .send()
                    .await
                    .context(BatchWriteSnafu)?;
            }

            // Wheh! But we're still not done-- we need to scribble down `log_id`
            client
                .put_item()
                .table_name("raft_metadata")
                .set_item(Some(
                    to_item(RaftMetadata {
                        node_id: NID(node_id),
                        flavor: Flavor::LastPurged,
                        data: to_vec(&log_id).context(RaftMetaEncodeSnafu)?,
                    })
                    .context(RaftMetaSerSnafu)?,
                ))
                .send()
                .await
                .context(RaftMetaPutSnafu)
                .map(|_| ())
        }

        purge1(&self.client, self.node_id, log_id)
            .await
            .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::Logs, ErrorVerb::Delete, &err))
    }
    #[tracing::instrument(skip(self))]
    async fn read_vote(&self) -> StdResult<Option<Vote<NodeId>>, StorageError<NodeId>> {
        async fn read_vote1(
            client: &::aws_sdk_dynamodb::Client,
            node_id: NodeId,
        ) -> Result<Option<Vote<NodeId>>> {
            client
                .get_item()
                .table_name("raft_metadata")
                .key("node_id", AttributeValue::N(node_id.to_string()))
                .key("flavor", AttributeValue::S("Vote".to_owned()))
                .send()
                .await
                .context(VoteGetSnafu)?
                .item
                .map(from_item)
                .transpose()
                .context(RaftMetaDeSnafu)?
                .map(|meta: RaftMetadata| {
                    from_slice::<Vote<NodeId>>(meta.data.as_slice()).context(RaftMetaDecodeSnafu)
                })
                .transpose()?
                .pipe(Ok)
        }

        read_vote1(&self.client, self.node_id)
            .await
            .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::Vote, ErrorVerb::Read, &err))
    }
    /// Save vote to storage
    #[tracing::instrument(skip(self))]
    async fn save_vote(&self, vote: &Vote<NodeId>) -> StdResult<(), StorageError<NodeId>> {
        async fn save_vote1(
            client: &::aws_sdk_dynamodb::Client,
            node_id: NodeId,
            vote: &Vote<NodeId>,
        ) -> Result<()> {
            client
                .put_item()
                .table_name("raft_metadata")
                .set_item(Some(
                    to_item(&RaftMetadata {
                        node_id: NID(node_id),
                        flavor: Flavor::Vote,
                        data: to_vec(vote).context(RaftMetaEncodeSnafu)?,
                    })
                    .context(RaftMetaSerSnafu)?,
                ))
                .send()
                .await
                .map(|_| ())
                .context(VotePutSnafu { vote: *vote })
        }

        save_vote1(&self.client, self.node_id, vote)
            .await
            .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::Vote, ErrorVerb::Write, &err))
    }
    /// Truncate logs since log_id, inclusive
    #[tracing::instrument(skip(self))]
    async fn truncate(&self, log_id: LogId<NodeId>) -> StdResult<(), StorageError<NodeId>> {
        async fn truncate1(
            client: &::aws_sdk_dynamodb::Client,
            node_id: NodeId,
            log_id: LogId<NodeId>,
        ) -> Result<()> {
            // First step; ask for all the items whose hash key is `node_id` and whose sort key is
            // <= `log_id.index`
            let mut to_be_rmd = client
                .query()
                .table_name("raft_log")
                // Like... this is the only difference between this method and `purge1`
                .key_condition_expression("#pk = :pk and #sk >= :log_id")
                .expression_attribute_names("#pk", "node_id")
                .expression_attribute_names("#sk", "log_id")
                .expression_attribute_values(":pk", AttributeValue::N(node_id.to_string()))
                .expression_attribute_values(":log_id", AttributeValue::N(log_id.index.to_string()))
                .scan_index_forward(true) // i.e. ascending order
                .send()
                .await
                .context(QuerySnafu)?
                .items()
                .to_vec()
                .into_iter()
                .map(|item| {
                    from_item::<HashMap<String, AttributeValue>, RaftLog>(item)
                        .context(RaftLogSerSnafu)
                })
                .collect::<Result<Vec<RaftLog>>>()?
                .into_iter()
                .map(|log| {
                    WriteRequest::builder()
                        .delete_request(
                            DeleteRequest::builder()
                                .key("node_id", AttributeValue::N(node_id.to_string()))
                                .key("log_id", AttributeValue::N(log.log_id.to_string()))
                                .build()
                                .context(OpBuildSnafu)?,
                        )
                        .build()
                        .pipe(Ok)
                })
                .collect::<Result<Vec<WriteRequest>>>()?;
            // At long last, this 👆 is the collection of deletion requests we need to send.

            // `BatchWrite` can only handle 25 items at a time.
            while !to_be_rmd.is_empty() {
                let this_batch: Vec<_> = to_be_rmd.drain(..min(to_be_rmd.len(), 25)).collect();
                client
                    .batch_write_item()
                    .request_items("raft_log", this_batch)
                    .send()
                    .await
                    .context(BatchWriteSnafu)?;
            }

            Ok(())
        }

        truncate1(&self.client, self.node_id, log_id)
            .await
            .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::Logs, ErrorVerb::Delete, &err))
    }
    /// Get a series of log entries from storage.
    #[tracing::instrument(skip(self))]
    async fn try_get_log_entries(
        &self,
        lower_bound: Bound<&u64>,
        upper_bound: Bound<&u64>,
    ) -> StdResult<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        async fn try_get_log_entries1(
            client: &::aws_sdk_dynamodb::Client,
            node_id: NodeId,
            lower_bound: Bound<&u64>,
            upper_bound: Bound<&u64>,
        ) -> Result<Vec<Entry<TypeConfig>>> {
            let key_condition_expr = format!(
                "#pk = :node_id{}",
                match (lower_bound, upper_bound) {
                    (Bound::Included(_), Bound::Included(_)) => " and #sk between :i and :j",
                    (Bound::Included(_), Bound::Excluded(_)) => " and #sk between :i and :j",
                    (Bound::Included(_), Bound::Unbounded) => " and #sk >= :i",
                    (Bound::Excluded(_), Bound::Included(_)) => " and #sk between :i and :j",
                    (Bound::Excluded(_), Bound::Excluded(_)) => " and #sk between :i and :j",
                    (Bound::Excluded(_), Bound::Unbounded) => " and #sk > :i",
                    (Bound::Unbounded, Bound::Included(_)) => " and #sk <= :j",
                    (Bound::Unbounded, Bound::Excluded(_)) => " and #sk < :j",
                    (Bound::Unbounded, Bound::Unbounded) => "",
                }
            );

            let mut builder = client
                .query()
                .table_name("raft_log")
                .key_condition_expression(key_condition_expr)
                .expression_attribute_names("#pk", "node_id")
                .expression_attribute_names("#sk", "log_id")
                .expression_attribute_values(":node_id", AttributeValue::N(node_id.to_string()));

            builder = match lower_bound {
                Bound::Included(i) => {
                    builder.expression_attribute_values(":i", AttributeValue::N(i.to_string()))
                }
                Bound::Excluded(i) => builder
                    .expression_attribute_values(":i", AttributeValue::N((i + 1).to_string())),
                Bound::Unbounded => builder,
            };

            builder = match upper_bound {
                Bound::Included(j) => {
                    builder.expression_attribute_values(":j", AttributeValue::N(j.to_string()))
                }
                Bound::Excluded(j) => builder
                    .expression_attribute_values(":j", AttributeValue::N((j - 1).to_string())),
                Bound::Unbounded => builder,
            };

            builder
                .send()
                .await
                .context(QuerySnafu)?
                .items()
                .to_vec()
                .into_iter()
                .map(from_item)
                .collect::<StdResult<Vec<RaftLog>, _>>()
                .context(RaftLogSerSnafu)?
                .into_iter()
                .map(|log| from_slice::<Entry<TypeConfig>>(&log.entry))
                .collect::<StdResult<Vec<Entry<TypeConfig>>, _>>()
                .context(RaftLogDecodeSnafu)
        }

        try_get_log_entries1(&self.client, self.node_id, lower_bound, upper_bound)
            .await
            .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::Logs, ErrorVerb::Read, &err))
    }
}
