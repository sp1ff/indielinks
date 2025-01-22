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
    entities::{Post, PostDay, PostUri, Tag, TagId, Tagname, User, UserId},
    storage,
};

use async_trait::async_trait;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region};
use aws_sdk_dynamodb::{
    config::Credentials,
    operation::update_item::UpdateItemOutput,
    types::{AttributeValue, ReturnValue},
};
use chrono::{DateTime, Utc};
use either::Either;
use futures::{stream, StreamExt};
use itertools::Itertools;
use secrecy::SecretString;
use serde_dynamo::aws_sdk_dynamodb_1::{from_item, from_items};
use snafu::{Backtrace, IntoError, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use tracing::debug;
use url::Url;
use uuid::Uuid;

use std::collections::{HashMap, HashSet};

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

#[async_trait]
impl storage::Backend for Client {
    async fn user_for_name(&self, name: &str) -> std::result::Result<Option<User>, StorError> {
        // An "Item" seems to be a HashMap<String, AttributeValue>. The DynamoDB interface is really
        // irritating because we get an Option<Vec<Item>> meaning we have to handle the "None" case
        // for the outer option as well as the zero length case for the inner Vec. It's not clear to
        // me what the semantic difference is between the two, if any.

        // I tried writing this logic solely in terms of the `Iterator` interface & `tap` primitives (pipe, mostly),
        // but it turned out to be fairly opaque; I prefer this:
        debug!("DynamoDB backend looking-up user {}", name);
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
        tags: &HashSet<TagId>,
    ) -> std::result::Result<bool, StorError> {
        let day: String = dt.format("%Y-%m-%d").to_string();
        let mut builder = self
            .client
            .put_item()
            .table_name("posts")
            .item(
                "PK",
                AttributeValue::S(format!("{}#{}", user.id().to_raw_string(), day.clone())),
            )
            .item(
                "IK",
                AttributeValue::S(format!("{}#{}", user.id().to_raw_string(), uri)),
            )
            .item(
                "id",
                AttributeValue::S(format!("{}", Uuid::new_v4().as_simple())),
            )
            .item("url", AttributeValue::S(format!("{}", uri)))
            .item("user_id", AttributeValue::S(user.id().to_raw_string()))
            .item(
                "posted",
                AttributeValue::S(dt.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string()),
            )
            .item("day", AttributeValue::S(day.clone()))
            .item("title", AttributeValue::S(title.to_string()))
            .item("public", AttributeValue::Bool(shared))
            .item("unread", AttributeValue::Bool(to_read))
            .item(
                "tags",
                AttributeValue::Ss(
                    tags.iter()
                        .map(|tagid| tagid.to_raw_string())
                        .collect::<Vec<String>>(),
                ),
            );

        if let Some(notes) = notes {
            builder = builder.item("notes", AttributeValue::S(notes.clone()))
        }

        if !replace {
            builder = builder
                .condition_expression("attribute_not_exists(PK) and attribute_not_exists(url)");
        };

        debug!("add_post sending request.");

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

    async fn delete_posts(&self, posts: &[Post]) -> StdResult<(), StorError> {
        stream::iter(posts.iter())
            .then(|post| async move {
                self.client
                    .delete_item()
                    .table_name("posts")
                    .key(
                        "PK",
                        AttributeValue::S(format!(
                            "{}#{}",
                            post.user_id().to_raw_string(),
                            post.day()
                        )),
                    )
                    .key("id", AttributeValue::S(post.id().to_raw_string()))
                    .send()
                    .await
            })
            .collect::<Vec<StdResult<_, _>>>()
            .await
            .into_iter()
            .collect::<StdResult<Vec<_>, _>>()
            .map_err(|err| StorError::new(DeletePostsSnafu.into_error(err)))?;
        // I've got a Vec<DeleteItemOutput>... what to do with it?
        Ok(())
    }

    async fn get_posts_by_day(
        &self,
        userid: &UserId,
        day: &PostDay,
    ) -> StdResult<Vec<Post>, StorError> {
        self.client
            .query()
            .table_name("posts")
            .key_condition_expression(format!("PK={}#{}", userid.to_raw_string(), day))
            .projection_expression("id,url,user_id,posted,day,title,notes,tags,public,unread")
            .send()
            .await
            .map_err(|err| StorError::new(QuerySnafu.into_error(err)))?
            .items()
            .to_vec()
            .pipe(from_items)
            .map_err(|err| StorError::new(PostDeSnafu.into_error(err)))?
            .pipe(Ok)
    }

    async fn get_posts_by_uri(
        &self,
        userid: &UserId,
        uri: &PostUri,
    ) -> StdResult<Vec<Post>, StorError> {
        self.client
            .query()
            .table_name("posts")
            .index_name("posts_by_user_and_url")
            .key_condition_expression("IK = :ik")
            .expression_attribute_values(
                ":ik",
                AttributeValue::S(format!("{}#{}", userid.to_raw_string(), uri)),
            )
            .projection_expression("id,url,user_id,posted,day,title,notes,tags,public,unread")
            .send()
            .await
            .map_err(|err| StorError::new(QuerySnafu.into_error(err)))?
            .items()
            .to_vec()
            .pipe(from_items)
            .map_err(|err| StorError::new(PostDeSnafu.into_error(err)))?
            .into_iter()
            .collect::<Vec<Post>>()
            .pipe(Ok)
    }

    async fn get_tag_cloud(&self, user: &User) -> StdResult<HashMap<Tagname, usize>, StorError> {
        self.client
            .query()
            .table_name("tags")
            .key_condition_expression("user_id = :id")
            .expression_attribute_values(":id", AttributeValue::S(user.id().to_raw_string()))
            .send()
            .await
            .map_err(|err| StorError::new(QuerySnafu.into_error(err)))?
            .items()
            .to_vec()
            .pipe(from_items::<Tag>)
            .map_err(|err| StorError::new(DeTagSnafu.into_error(err)))?
            .into_iter()
            .map(|tag| (tag.name(), tag.count()))
            .collect::<HashMap<Tagname, usize>>()
            .pipe(Ok)
    }

    async fn get_tag_cloud_for_uri(
        &self,
        userid: &UserId,
        uri: &PostUri,
    ) -> StdResult<HashMap<TagId, usize>, StorError> {
        Ok(self
            .client
            .query()
            .table_name("posts")
            .index_name("posts_by_user_and_url")
            .key_condition_expression("IK = :ik")
            .expression_attribute_values(
                ":ik",
                AttributeValue::S(format!("{}#{}", userid.to_raw_string(), uri)),
            )
            .projection_expression("tags")
            .send()
            .await
            .map_err(|err| StorError::new(QuerySnafu.into_error(err)))?
            .items()
            .iter()
            .map(|m| match m.get("tags") {
                Some(AttributeValue::Ss(v)) => v
                    .iter()
                    .map(|s| {
                        TagId::from_raw_string(s).map_err(|err| {
                            BadTagIdSnafu {
                                raw_tagid: s.to_string(),
                            }
                            .into_error(err)
                        })
                    })
                    .collect::<StdResult<Vec<TagId>, _>>(),
                _ => Err(NoTagsWithQueryOutputSnafu.build()),
            })
            .collect::<Result<Vec<Vec<TagId>>>>()
            .map_err(StorError::new)?
            .into_iter()
            .flatten()
            .collect::<Vec<TagId>>()
            .into_iter()
            .counts())
    }

    async fn update_tag_cloud_on_add(
        &self,
        userid: &UserId,
        tags: &HashSet<Tagname>,
    ) -> StdResult<HashSet<TagId>, StorError> {
        // I cycled through several implementation ideas as I learned the DynamoDB interface. Here's
        // the plan:
        //
        // 1. For each tag, issue an carefully crafted UpdateItem request that will update the
        //    count if the tag exists, or create it if it doesn't, atomically.
        //
        // 2. Accumulate the returned [TagId]s
        //
        // The careful reader will note that in the absence of transactions, other requests could
        // slip in while we're doing this. Creation seems safe to me: if another request is also
        // trying to create the same tag, one will "win", and the other will simply see it as
        // already existing. Problems could arise in if another request deletes a tag out from under
        // us. But even there, the result will be that this request will leave a new post with a
        // "dead" `TagId` in it... which is perfectly normal for tag deletion. Besides, if a given
        // user is simultaneously adding posts & deleting tags named in those posts... I don't
        // really feel bad for them.

        // I need to refine this implementation once it's validated. I built this out as I was
        // understanding what the DDB API could do, and it seems ripe to me for the application of
        // the adage "parse don't validate".
        let tag_ids = stream::iter(tags.iter())
            .then(|tag| async move {
                let tagid = TagId::new();
                self.client
                    .update_item()
                    .table_name("tags")
                    .key("user_id", AttributeValue::S(userid.to_raw_string()))
                    .key("name", AttributeValue::S(tag.to_string()))
                    .update_expression(
                        "SET count = if_not_exists(count, :s) + :i, id = if_not_exists(id, :id)",
                    )
                    .expression_attribute_values(":s", AttributeValue::N("0".to_string()))
                    .expression_attribute_values(":i", AttributeValue::N("1".to_string()))
                    .expression_attribute_values(":id", AttributeValue::S(tagid.to_raw_string()))
                    .return_values(ReturnValue::UpdatedNew)
                    .send()
                    .await
            })
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<std::result::Result<Vec<UpdateItemOutput>, _>>()
            .context(UpdateTagSnafu)
            .map_err(StorError::new)?
            .into_iter()
            .map(|out| {
                match out
                    .attributes()
                    .context(NoUpdateItemOutputSnafu)?
                    .get("id")
                    .context(NoIdWithUpdateItemOutputSnafu)?
                {
                    AttributeValue::S(s) => {
                        TagId::from_raw_string(s).context(BadUpdateItemOutputSnafu {
                            text: s.to_string(),
                        })
                    }
                    _ => BadAttrTypeUpdateItemOutputSnafu.fail(),
                }
            })
            .collect::<Result<Vec<TagId>>>()
            .map_err(StorError::new)?;
        // Verify that we got a TagId for each tag
        if tags.len() != tag_ids.len() {
            return Err(StorError::new(
                MismatchedTagCountsSnafu {
                    in_count: tags.len(),
                    out_count: tag_ids.len(),
                }
                .build(),
            ));
        }

        // Verify that they're unique
        let unique_tag_ids = tag_ids.iter().cloned().collect::<HashSet<TagId>>();
        if tag_ids.len() != unique_tag_ids.len() {
            return Err(StorError::new(
                MismatchedTagIdCountsSnafu {
                    tag_ids: tag_ids.len(),
                    unique: unique_tag_ids.len(),
                }
                .build(),
            ));
        }

        debug!("update_tag_cloud :=> {:?}", unique_tag_ids);

        Ok(unique_tag_ids)
    }

    async fn update_tag_cloud_on_delete(
        &self,
        userid: &UserId,
        counts: &HashMap<TagId, usize>,
    ) -> StdResult<(), StorError> {
        // This is really unfortunate-- I have `TagId`, but I need the *name*. Going to make two
        // passes:
        //
        // 1. one to match a name to the `TagId` (using the global secondary index)
        //
        // 2. a second to update the count; taking care to check that the Id hasn't changed (to
        // guard against race conditions)
        async fn lookup_name(
            client: &::aws_sdk_dynamodb::Client,
            tagid: &TagId,
        ) -> Result<Option<Tagname>> {
            client
                .query()
                .table_name("tags")
                .index_name("tags_by_id")
                .key_condition_expression("id = :id")
                .expression_attribute_values(":id", AttributeValue::S(tagid.to_raw_string()))
                .send()
                .await
                .context(QuerySnafu)?
                .items()
                .iter()
                .cloned()
                .at_most_one()
                .map_err(|_| AtMostOneRowSnafu.build())?
                .map(from_item::<Tag>)
                .transpose()
                .context(DeTagSnafu)?
                .map(|tag| tag.name())
                .pipe(Ok)
        }
        // Collect our lookups into a vector of `Option<(TagId, Tagname)`. If the `Option` is None,
        // that means someone deleted the `TagId` out from underneath us-- that's fine, just skip
        // it.
        let lookups = stream::iter(counts.iter())
            .then(|(tagid, _delta)| async move {
                lookup_name(&self.client, tagid)
                    .await
                    .map(|o| o.map(|n| (*tagid, n)))
            })
            .collect::<Vec<Result<Option<(TagId, Tagname)>>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<Option<(TagId, Tagname)>>>>()
            .map_err(StorError::new)?;

        let _ = stream::iter(lookups.iter())
            .then(|lookup| async move {
                match lookup {
                    Some((tagid, name)) => {
                        Some(
                            self.client
                                .update_item()
                                .table_name("tags")
                                .key("user_id", AttributeValue::S(userid.to_raw_string()))
                                .key("name", AttributeValue::S(name.to_string()))
                                .update_expression("SET count = count - :inc")
                                .condition_expression("id = :id")
                                .expression_attribute_values(
                                    ":inc",
                                    AttributeValue::N(format!(
                                        "{}",
                                        counts.get(tagid).unwrap(/* known good*/)
                                    )),
                                )
                                .expression_attribute_values(
                                    ":id",
                                    AttributeValue::S(tagid.to_raw_string()),
                                )
                                .return_values(ReturnValue::UpdatedNew)
                                .send()
                                .await
                                .map_err(|err| UpdateTagSnafu.into_error(err)),
                        )
                    }
                    None => None,
                }
            })
            .collect::<Vec<Option<_>>>()
            .await
            .into_iter()
            .map(|x| x.transpose())
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<Result<Vec<Option<UpdateItemOutput>>>>()
            .map_err(StorError::new)?
            .into_iter()
            .flatten()
            .collect::<Vec<UpdateItemOutput>>();

        Ok(())
        // I have a `Vec<UpdateItemOutput>`... how to process that?
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
                .key("id", AttributeValue::S(user.id().to_raw_string()))
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
            .key("id", AttributeValue::S(user.id().to_raw_string()))
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
}
