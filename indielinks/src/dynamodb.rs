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

use async_trait::async_trait;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region};
use aws_sdk_dynamodb::{config::Credentials, types::AttributeValue};
use either::Either;
use itertools::Itertools;
use secrecy::SecretString;
use serde_dynamo::aws_sdk_dynamodb_1::from_items;
use snafu::{Backtrace, Snafu};
use tap::Pipe;
use url::Url;

use crate::{entities::User, storage};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No endpoint URLs specified"))]
    NoEndpoints { backtrace: Backtrace },
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
            .index_name("username")
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
}
