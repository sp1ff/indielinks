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
//! [Storage] implementation for DynamoDB.
//!
//! [Storage]: crate::storage

use crate::{
    entities::{Post, PostDay, PostUri, Tagname, User, UserId, Username},
    storage::{self, DateRange, UsernameClaimedSnafu},
    util::UpToThree,
};

use async_trait::async_trait;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region};
use aws_sdk_dynamodb::{
    config::Credentials,
    operation::{batch_write_item::BatchWriteItemError, put_item::PutItemError},
    types::{AttributeValue, PutRequest, ReturnValue, WriteRequest},
};
use chrono::{DateTime, Utc};
use either::Either;
use itertools::Itertools;
use secrecy::SecretString;
use serde_dynamo::aws_sdk_dynamodb_1::{from_items, to_item};
use snafu::{Backtrace, IntoError, Snafu};
use tap::Pipe;
use url::Url;

use std::{
    cmp::min,
    collections::{HashMap, HashSet},
};

#[derive(Debug, Snafu)]
pub enum Error {
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
    #[snafu(display("Failed to delete a post: {source}"))]
    DeletePosts {
        source: aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_dynamodb::operation::delete_item::DeleteItemError,
            aws_sdk_dynamodb::config::http::HttpResponse,
        >,
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
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

use storage::Error as StorError;

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
use aws_smithy_runtime_api::client::result::SdkError;

// Use these if you don't want to attach any context to a failed DeleteItem request; the `StorError`
// will have a backtrace, so the operator should be able to figure-out where the error occurred. I
// should probably wrap this boilerplate up in a macro, but I want to be sure this is the way I
// want to go, first.

impl std::convert::From<SdkError<DeleteItemError, HttpResponse>> for StorError {
    fn from(value: SdkError<DeleteItemError, HttpResponse>) -> Self {
        StorError::new(value)
    }
}

impl std::convert::From<SdkError<QueryError, HttpResponse>> for StorError {
    fn from(value: SdkError<QueryError, HttpResponse>) -> Self {
        StorError::new(value)
    }
}

impl std::convert::From<SdkError<BatchWriteItemError, HttpResponse>> for StorError {
    fn from(value: SdkError<BatchWriteItemError, HttpResponse>) -> Self {
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

impl std::convert::From<SdkError<PutItemError, HttpResponse>> for StorError {
    fn from(value: SdkError<PutItemError, HttpResponse>) -> Self {
        StorError::new(value)
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
    // Return true if an insert/upsert occurred, false if the insert/upsert failed because the post
    // already existed and `replace` was false, Err otherwise.
    async fn add_post(
        &self,
        user: &User,
        replace: bool,
        uri: &PostUri,
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
            &user.id(),
            dt,
            &day,
            title,
            notes,
            tags,
            shared,
            to_read,
        );
        let item = to_item(post).expect("to_item failed");

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

    async fn add_user(&self, user: &User) -> StdResult<(), StorError> {
        use aws_sdk_dynamodb::{
            error::SdkError, operation::put_item::PutItemError::ConditionalCheckFailedException,
        };
        let claimed = match self
            .client
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
                    return UsernameClaimedSnafu {
                        username: user.username(),
                    }
                    .fail();
                }
            }
        };
        if claimed {
            return UsernameClaimedSnafu {
                username: user.username(),
            }
            .fail();
        }
        self.client
            .put_item()
            .table_name("users")
            .set_item(Some(serde_dynamo::to_item(user.clone())?))
            .send()
            .await?;
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
