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
// You should have received a copy of the GNU General Public License along with mpdpopm.  If not,
// see <http://www.gnu.org/licenses/>.

//! # dynamodb
//!
//! [Storage] implementation for DynamoDB.
//!
//! [Storage]: crate::storage

use std::collections::HashSet;

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
use serde_dynamo::aws_sdk_dynamodb_1::from_items;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use tracing::debug;
use url::Url;
use uuid::Uuid;

use crate::{
    entities::{PostUri, TagId, User, UserId},
    storage,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Read bad ID in UpdateItemOutput: {text} ({source})"))]
    BadUpdateItemOutput {
        text: String,
        source: uuid::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Unexpected AttributeValue variant in UpdateItemOutput"))]
    BadAttrTypeUpdateItemOutput { backtrace: Backtrace },
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
    #[snafu(display("The UpdateItemOutput was missing"))]
    NoUpdateItemOutput { backtrace: Backtrace },
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

    /// Update the user's tag cloud. Internally, create the new tags if they're not already there,
    /// with a count of one. If they're there, increment their counts. Return their TagIds.
    ///
    /// Ideally, this function would take an iterator over [String] along with a proof that the
    /// yielded [String]s are unique. The closest approximation of which I'm aware in Rust is to
    /// just take a [HashSet]. Returning a [HashSet] expresses this method's commitment that the
    /// resulting collection of [TagId]s is also unique.
    pub async fn update_tag_cloud(
        &self,
        userid: &UserId,
        tags: &HashSet<String>,
    ) -> Result<HashSet<TagId>> {
        // This is the one place (so far) I couldn't wriggle out of performing a join in application
        // logic. We need to ensure that each tag exists in the database & increment their counts, I
        // cycled through several implementation ideas as I learned the DynamoDB interface. Here's
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
                    .key("name", AttributeValue::S(tag.clone()))
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
            .context(UpdateTagSnafu)?
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
            .collect::<Result<Vec<TagId>>>()?;
        // Verify that we got a TagId for each tag
        if tags.len() != tag_ids.len() {
            return MismatchedTagCountsSnafu {
                in_count: tags.len(),
                out_count: tag_ids.len(),
            }
            .fail();
        }

        // Verify that they're unique
        let unique_tag_ids = tag_ids.iter().cloned().collect::<HashSet<TagId>>();
        if tag_ids.len() != unique_tag_ids.len() {
            return MismatchedTagIdCountsSnafu {
                tag_ids: tag_ids.len(),
                unique: unique_tag_ids.len(),
            }
            .fail();
        }

        debug!("update_tag_cloud :=> {:?}", unique_tag_ids);

        Ok(unique_tag_ids)
    }
}

#[async_trait]
impl storage::Backend for Client {
    async fn user_for_name(&self, name: &str) -> std::result::Result<Option<User>, storage::Error> {
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
            .map_err(storage::Error::new)?;
        match gio.items {
            Some(items) => from_items::<User>(items)
                .map_err(storage::Error::new)?
                .into_iter()
                .at_most_one()
                .map_err(storage::Error::new)?
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
        tags: &Option<HashSet<String>>,
    ) -> std::result::Result<bool, storage::Error> {
        let day: String = dt.format("%Y-%m-%d").to_string();
        let mut builder = self
            .client
            .put_item()
            .table_name("posts")
            .item(
                "PK",
                AttributeValue::S(format!("{}#{}", user.id(), day.clone())),
            )
            .item(
                "id",
                AttributeValue::S(format!("{}", Uuid::new_v4().as_simple())),
            )
            .item("url", AttributeValue::S(format!("{}", uri)))
            .item("user_id", AttributeValue::S(format!("{}", user.id())))
            .item("posted", AttributeValue::N(format!("{}", dt.timestamp())))
            .item("day", AttributeValue::S(day.clone()))
            .item("title", AttributeValue::S(title.to_string()))
            .item("public", AttributeValue::Bool(shared))
            .item("unread", AttributeValue::Bool(to_read));

        if let Some(notes) = notes {
            builder = builder.item("notes", AttributeValue::S(notes.clone()))
        }

        if let Some(tags) = tags {
            builder = builder.item(
                "tags",
                AttributeValue::Ss(
                    self.update_tag_cloud(&user.id(), tags)
                        .await
                        .map_err(storage::Error::new)?
                        .iter()
                        .map(|tagid| tagid.to_raw_string())
                        .collect::<Vec<String>>(),
                ),
            );
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
                    Err(storage::Error::new(err))
                }
            }
        }
    }
}
