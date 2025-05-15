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
    entities::{
        FollowId, Follower, Following, Like, Post, PostDay, PostId, PostUri, Reply, Share, StorUrl,
        Tagname, User, UserId, Username,
    },
    storage::{self, DateRange, UsernameClaimedSnafu},
    util::UpToThree,
};

use async_trait::async_trait;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region};
use aws_sdk_dynamodb::{
    config::Credentials,
    operation::{
        batch_write_item::BatchWriteItemError, get_item::GetItemError, put_item::PutItemError,
        query::QueryOutput, update_item::UpdateItemError,
    },
    types::{AttributeValue, PutRequest, ReturnValue, Select, WriteRequest},
};
use aws_smithy_async::future::pagination_stream::PaginationStream;
use chrono::{DateTime, Duration, Utc};
use either::Either;
use futures::{stream::BoxStream, Stream};
use itertools::Itertools;
use pin_project::pin_project;
use secrecy::SecretString;
use serde_dynamo::{
    aws_sdk_dynamodb_1::{from_items, to_item},
    from_item,
};
use snafu::{Backtrace, IntoError, ResultExt, Snafu};
use tap::Pipe;
use url::Url;
use uuid::Uuid;

use std::{
    cmp::min,
    collections::{HashMap, HashSet, VecDeque},
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
        follows: HashSet<StorUrl>,
        source: SdkError<UpdateItemError, aws_smithy_runtime_api::http::Response>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to add followers {followers:?}: {source}"))]
    AddFollower {
        followers: HashSet<StorUrl>,
        source: SdkError<UpdateItemError, aws_smithy_runtime_api::http::Response>,
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
    #[snafu(display("A batch write failed: {source}"))]
    BatchWrite {
        source: SdkError<BatchWriteItemError, HttpResponse>,
        backtrace: Backtrace,
    },
    #[snafu(display("When counting items: {source}"))]
    Count {
        source: SdkError<QueryError, HttpResponse>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to delete a post: {source}"))]
    DeletePosts {
        source: aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_dynamodb::operation::delete_item::DeleteItemError,
            aws_sdk_dynamodb::config::http::HttpResponse,
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
        source: aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_dynamodb::operation::query::QueryError,
            aws_sdk_dynamodb::config::http::HttpResponse,
        >,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to serialize to an AttributeValue: {source}"))]
    TAV {
        source: serde_dynamo::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to update post counts: {source}"))]
    UpdatePostCounts {
        source: aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_dynamodb::operation::update_item::UpdateItemError,
            aws_sdk_dynamodb::config::http::HttpResponse,
        >,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to update a post time: {source}"))]
    UpdatePostTime {
        source: aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_dynamodb::operation::update_item::UpdateItemError,
            aws_sdk_dynamodb::config::http::HttpResponse,
        >,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to update a tag: {source}"))]
    UpdateTag {
        source: aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_dynamodb::operation::update_item::UpdateItemError,
            aws_sdk_dynamodb::config::http::HttpResponse,
        >,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to write user {user:?} to the database: {source}"))]
    User {
        user: Box<User>,
        source: SdkError<PutItemError, HttpResponse>,
    },
    #[snafu(display("Failed to check whether the username was claimed: {source}"))]
    Username {
        username: Username,
        source: SdkError<PutItemError, HttpResponse>,
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
) -> Result<usize> {
    Ok(client
        .query()
        .table_name(table_name)
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
    // duplicating serialization logic.

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
            "api_key".to_string(),
            tav(user.api_key()).context(TAVSnafu)?,
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
    ) -> Result<PagedResultsStream<T>> {
        Ok(PagedResultsStream {
            stream: Some(
                client
                    .query()
                    .table_name(table_name)
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
//                                indielinks DynamoDB client type                                 //
////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Client {
    client: ::aws_sdk_dynamodb::Client,
}

impl Client {
    pub async fn new(
        location: &Either<String, Vec<Url>>,
        credentials: &Option<(SecretString, SecretString)>,
    ) -> Result<Client> {
        use secrecy::ExposeSecret;
        let creds = credentials.as_ref().map(|(id, secret)| {
            Credentials::new(
                id.expose_secret(),
                secret.expose_secret(),
                None,
                None,
                "indielinks",
            )
        });

        let config = match location {
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
        Ok(Client {
            client: ::aws_sdk_dynamodb::Client::new(&config),
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

    async fn add_like(&self, reply: &Like) -> StdResult<(), StorError> {
        self.client
            .put_item()
            .table_name("likes")
            .item(
                "user_id_and_url",
                AttributeValue::S(format!("{}#{}", reply.user_id(), reply.url())),
            )
            .item("id", AttributeValue::S(reply.id().to_string()))
            .item("like_id", AttributeValue::S(reply.like_id().to_string()))
            .item("created", AttributeValue::S(format!("{}", reply.created())))
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
        uri: &PostUri,
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

    async fn add_reply(
        &self,
        user: &User,
        url: &PostUri,
        reply: &Reply,
    ) -> StdResult<(), StorError> {
        // It would be nice to be able to store `Reply`-s in a Set, so as to prevent duplication at
        // the data layer. Regrettably, DDB Sets can only hold numbers, strings & binary types.
        let item: HashMap<String, AttributeValue> =
            serde_dynamo::to_item(reply).map_err(StorError::new)?;
        let _ = self
            .client
            .update_item()
            .table_name("posts")
            .key("user_id", AttributeValue::S(user.id().to_string()))
            .key("url", AttributeValue::S(url.to_string()))
            .update_expression("set #r = list_append(#r, :val)")
            .expression_attribute_names("#r", "replies".to_owned())
            .expression_attribute_values(":val", AttributeValue::L(vec![AttributeValue::M(item)]))
            .send()
            .await?;
        // Nb. Unlike in `delete_post()`, this query will error-out if the key doesn't name an
        // extant Post ("UpdateExpression: list_append() given a non-list")
        Ok(())
    }

    async fn add_share(
        &self,
        user: &User,
        url: &PostUri,
        share: &Share,
    ) -> StdResult<(), StorError> {
        let item = serde_dynamo::to_item(share).map_err(StorError::new)?;
        let _ = self
            .client
            .update_item()
            .table_name("posts")
            .key("user_id", AttributeValue::S(user.id().to_string()))
            .key("url", AttributeValue::S(url.to_string()))
            .update_expression("set #s = list_append(#s, :val)")
            .expression_attribute_names("#s", "shares".to_owned())
            .expression_attribute_values(":val", AttributeValue::L(vec![AttributeValue::M(item)]))
            .send()
            .await?;
        // Nb. Unlike in `delete_post()`, this query will error-out if the key doesn't name an
        // extant Post ("UpdateExpression: list_append() given a non-list")
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

    async fn delete_post(&self, user: &User, url: &PostUri) -> StdResult<bool, StorError> {
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

    async fn get_followers<'a>(
        &'a self,
        user: &User,
    ) -> StdResult<BoxStream<'a, StdResult<Follower, StorError>>, StorError> {
        let count = count_range(&self.client, "followers", "user_id", &user.id().to_string())
            .await
            .map_err(StorError::new)?;
        Ok(Box::pin(
            PagedResultsStream::new(
                &self.client,
                "followers",
                "user_id",
                &user.id().to_string(),
                count,
            )
            .await
            .map_err(StorError::new)?,
        ))
    }

    async fn get_following<'a>(
        &'a self,
        user: &User,
    ) -> StdResult<BoxStream<'a, StdResult<Following, StorError>>, StorError> {
        let count = count_range(&self.client, "following", "user_id", &user.id().to_string())
            .await
            .map_err(StorError::new)?;
        Ok(Box::pin(
            PagedResultsStream::new(
                &self.client,
                "following",
                "user_id",
                &user.id().to_string(),
                count,
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
        uri: &Option<PostUri>,
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
    ) -> StdResult<Vec<Post>, StorError> {
        let mut query = self
            .client
            .query()
            .table_name("posts")
            .index_name("posts_by_posted")
            .key_condition_expression("user_id=:id")
            .expression_attribute_values(":id", AttributeValue::S(user.id().to_string()))
            .scan_index_forward(false);

        match (tags, dates) {
            (UpToThree::None, DateRange::None) => {}
            (UpToThree::None, DateRange::Begins(b)) => {
                query = query
                    .filter_expression("posted>=:b")
                    .expression_attribute_values(":b", AttributeValue::S(b.to_string()));
            }
            (UpToThree::None, DateRange::Ends(e)) => {
                query = query
                    .filter_expression("posted<:e")
                    .expression_attribute_values(":e", AttributeValue::S(e.to_string()));
            }
            (UpToThree::None, DateRange::Both(b, e)) => {
                query = query
                    .filter_expression("posted>:b and posted<:e")
                    .expression_attribute_values(":b", AttributeValue::S(b.to_string()))
                    .expression_attribute_values(":e", AttributeValue::S(e.to_string()));
            }
            (UpToThree::One(tag), DateRange::None) => {
                query = query
                    .filter_expression("contains(tags,:t0)")
                    .expression_attribute_values(":t0", AttributeValue::S(tag.to_string()));
            }
            (UpToThree::One(tag), DateRange::Begins(b)) => {
                query = query
                    .filter_expression("posted>=:b and contains(tags,:t0)")
                    .expression_attribute_values(":b", AttributeValue::S(b.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag.to_string()));
            }
            (UpToThree::One(tag), DateRange::Ends(e)) => {
                query = query
                    .filter_expression("posted<:e and contains(tags,:t0)")
                    .expression_attribute_values(":e", AttributeValue::S(e.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag.to_string()));
            }
            (UpToThree::One(tag), DateRange::Both(b, e)) => {
                query = query
                    .filter_expression("posted>:b and posted<:e and contains(tags,:t0)")
                    .expression_attribute_values(":b", AttributeValue::S(b.to_string()))
                    .expression_attribute_values(":e", AttributeValue::S(e.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag.to_string()));
            }
            (UpToThree::Two(tag0, tag1), DateRange::None) => {
                query = query
                    .filter_expression("contains(tags,:t0) and contains(tags,:t1)")
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()));
            }
            (UpToThree::Two(tag0, tag1), DateRange::Begins(b)) => {
                query = query
                    .filter_expression("posted>=:b and contains(tags,:t0) and contains(tags,:t1)")
                    .expression_attribute_values(":b", AttributeValue::S(b.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()));
            }
            (UpToThree::Two(tag0, tag1), DateRange::Ends(e)) => {
                query = query
                    .filter_expression("posted<:e and contains(tags,:t0) and contains(tags,:t1)")
                    .expression_attribute_values(":e", AttributeValue::S(e.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()));
            }
            (UpToThree::Two(tag0, tag1), DateRange::Both(b, e)) => {
                query = query
                    .filter_expression(
                        "posted>:b and posted<:e and contains(tags,:t0) and contains(tags,:t1)",
                    )
                    .expression_attribute_values(":b", AttributeValue::S(b.to_string()))
                    .expression_attribute_values(":e", AttributeValue::S(e.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()));
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::None) => {
                query = query
                    .filter_expression(
                        "contains(tags,:t0) and contains(tags,:t1) and contains(tags, :t2)",
                    )
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()))
                    .expression_attribute_values(":t2", AttributeValue::S(tag2.to_string()));
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::Begins(b)) => {
                query = query
                    .filter_expression("posted>=:b and contains(tags,:t0) and contains(tags,:t1) and contains(tags, :t2)")
                    .expression_attribute_values(":b", AttributeValue::S(b.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()))
                    .expression_attribute_values(":t2", AttributeValue::S(tag2.to_string()));
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::Ends(e)) => {
                query = query
                    .filter_expression("posted<:e and contains(tags,:t0) and contains(tags,:t1) and contains(tags, :t2)")
                    .expression_attribute_values(":e", AttributeValue::S(e.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()))
                    .expression_attribute_values(":t2", AttributeValue::S(tag2.to_string()));
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::Both(b, e)) => {
                query = query
                    .filter_expression(
                        "posted>:b and posted<:e and contains(tags,:t0) and contains(tags,:t1) and contains(tags, :t2)",
                    )
                    .expression_attribute_values(":b", AttributeValue::S(b.to_string()))
                    .expression_attribute_values(":e", AttributeValue::S(e.to_string()))
                    .expression_attribute_values(":t0", AttributeValue::S(tag0.to_string()))
                    .expression_attribute_values(":t1", AttributeValue::S(tag1.to_string()))
                    .expression_attribute_values(":t2", AttributeValue::S(tag2.to_string()));
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
