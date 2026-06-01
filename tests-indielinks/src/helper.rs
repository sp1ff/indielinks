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

use std::{cmp::min, collections::HashSet, net::SocketAddr, result::Result as StdResult};

use async_trait::async_trait;
use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, Select, WriteRequest};
use crypto_common::rand_core::{OsRng, RngCore};
use itertools::Itertools;
use libtest_mimic::Failed;
use secrecy::SecretString;
use serde::Deserialize;
use serde_dynamo::aws_sdk_dynamodb_1::{from_items, to_item};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use url::Url;

use indielinks_shared::entities::{Post, StorUrl, UserEmail, UserId, Username};

use indielinks::{
    dynamodb::{
        add_followers as add_ddb_followers, add_following as add_ddb_following,
        add_user as add_ddb_user, create_client as create_ddb_client, Location,
    },
    entities::{
        FollowId, LikeReplyShareRef, OutgoingLike, OutgoingReply, OutgoingShare, ReplyId, User,
        Visibility,
    },
    peppers::{Pepper, Version as PepperVersion},
    scylla::{
        add_followers as add_scylla_followers, add_following as add_scylla_following,
        add_outgoing_like_reply_share as add_scylla_outgoing_lrs, add_user as add_scylla_user,
        create_client as create_scylla_client,
    },
    util::Credentials,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("While creating the DDB client, {source}"))]
    Client { source: indielinks::dynamodb::Error },
    #[snafu(display("DynamoDB query failed: {source}"))]
    DdbQuery {
        source: aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_dynamodb::operation::query::QueryError,
            aws_sdk_dynamodb::config::http::HttpResponse,
        >,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a Post: {source}"))]
    DePost {
        source: serde_dynamo::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a User: {source}"))]
    DeUser {
        source: serde_dynamo::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a UserId: {source}"))]
    DeUserId {
        source: scylla::deserialize::DeserializationError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to set keyspace: {source}"))]
    Keyspace {
        source: scylla::errors::UseKeyspaceError,
        backtrace: Backtrace,
    },
    #[snafu(display("User {username} appears more than once in the database"))]
    MultipleUsers {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to create a ScyllaDB session: {source}"))]
    NewSession {
        source: indielinks::scylla::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("No user {username}"))]
    NoSuchUser {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to get typed rows: {source}"))]
    Rows {
        source: scylla::response::query_result::RowsError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to get a rows result: {source}"))]
    RowsResult {
        source: scylla::response::query_result::IntoRowsResultError,
        backtrace: Backtrace,
    },
    #[snafu(display("ScyllaDB query failed: {source}"))]
    ScyllaQuery {
        source: scylla::errors::ExecutionError,
        backtrace: Backtrace,
    },
}

type Result<T> = StdResult<T, Error>;

#[derive(Clone, Debug, Deserialize)]
#[allow(dead_code)]
pub struct ScyllaConfig {
    /// ScyllaDB credentials, if authentication is to be used. Nb that I'm not using
    /// `secrecy::SecretString` here; I assume that if you're running this test suite, it's
    /// against a local, unsecured instance.
    pub credentials: Option<Credentials>,
    /// ScyllaDB hosts; specify as "host:port"
    pub hosts: Vec<SocketAddr>,
    /// Scylla address translations
    pub translations: Option<Vec<(SocketAddr, SocketAddr)>>,
}

// Regrettably, if we want the tests to run without needing a configuration file, we have to ensure
// that the default implementation of this struct works with the test setup.
impl Default for ScyllaConfig {
    fn default() -> Self {
        ScyllaConfig {
            credentials: None,
            hosts: vec!["127.0.0.1:9043".parse::<SocketAddr>().unwrap(/* known good */)],
            translations: Some(vec![
                (
                    "172.11.0.2:9043".parse::<SocketAddr>().unwrap(),
                    "127.0.0.1:9043".parse::<SocketAddr>().unwrap(),
                ),
                (
                    "172.11.0.3:9043".parse::<SocketAddr>().unwrap(),
                    "127.0.0.1:9044".parse::<SocketAddr>().unwrap(),
                ),
                (
                    "172.11.0.4:9043".parse::<SocketAddr>().unwrap(),
                    "127.0.0.1:9045".parse::<SocketAddr>().unwrap(),
                ),
            ]),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[allow(dead_code)]
pub struct DynamoConfig {
    /// AWS credentials: key ID & secret key; you'll pretty-much always need to specify these
    /// when running against DDB, but one could be talking to a local SycllaDB over the
    /// Alternator interface locally and have the cluster be open. Nb that I'm not using
    /// `secrecy::SecretString` here; I assume that if you're running this test suite, it's
    /// against a local, unsecured instance.
    pub credentials: Option<Credentials>,
    /// You can find DynamoDB in a few ways. If you're truly talking to DynamoDB in AWS, you can
    /// give a region. You can also specify an URL (like
    /// `https://dynamodb.us-west-2.amazonaws.com`). If you're talking to ScyllaDB over the
    /// Alternator interface, we're going to have to handle load-balancing on the client-side,
    /// so specify more than one.
    pub location: Location,
}

impl Default for DynamoConfig {
    fn default() -> Self {
        DynamoConfig {
            credentials: None,
            location: vec![
                Url::parse("http://127.0.0.1:8043").unwrap(/* known good */),
                Url::parse("http://127.0.0.1:8044").unwrap(/* known good */),
                Url::parse("http://127.0.0.1:8045").unwrap(/* known good */),
            ]
            .into(),
        }
    }
}

/// Implementations of this trait will be passed to each test function to enable them to do things
/// like talk directly to the back-end, ensure a known starting point for the test, & so on. Each
/// fixture will need to provide an implemetnation.
#[async_trait]
pub trait Helper {
    /// Remove all posts belonging to `username`
    async fn clear_posts(&self, username: &Username) -> StdResult<(), Failed>;
    /// Create an indielinks user with given followers on federated servers
    async fn create_user(
        &self,
        pepper_version: &PepperVersion,
        pepper_key: &Pepper,
        username: &Username,
        password: &SecretString,
        followers: &HashSet<StorUrl>,
        following: &HashSet<(StorUrl, FollowId)>,
    ) -> StdResult<String, Failed>;
    /// Insert an outgoing like directly into storage (bypasses background tasks)
    async fn add_outgoing_like(&self, username: &Username, url: &Url) -> StdResult<(), Failed>;
    /// Insert an outgoing reply directly into storage (bypasses background tasks)
    async fn add_outgoing_reply(
        &self,
        username: &Username,
        url: &Url,
        text: &str,
    ) -> StdResult<(), Failed>;
    /// Insert an outgoing share directly into storage (bypasses background tasks)
    async fn add_outgoing_share(&self, username: &Username, url: &Url) -> StdResult<(), Failed>;
    /// Remove a user
    async fn remove_user(&self, username: &Username) -> StdResult<(), Failed>;
    /// Retrieve the public address at which tests can reach indielinks, whether it's clustered or
    /// single-node
    fn indielinks(&self) -> Url;
    /// Retrieve the nodes comprising the indielinks cluster under test.
    // It would be nicer to return `impl Iterator<Item = ...>`, but that's not dyn compatible
    fn nodes(&self) -> Vec<(Url, Url, SocketAddr)>;
}

/// Application state shared across all tests
pub struct DynamoDBHelper {
    client: ::aws_sdk_dynamodb::Client,
    indielinks: Url,
    nodes: Vec<(Url, Url, SocketAddr)>,
}

impl DynamoDBHelper {
    pub async fn new(
        indielinks: Url,
        nodes: impl Iterator<Item = (Url, Url, SocketAddr)>,
        cfg: &DynamoConfig,
    ) -> Result<DynamoDBHelper> {
        Ok(DynamoDBHelper {
            client: create_ddb_client(&cfg.location, &cfg.credentials)
                .await
                .context(ClientSnafu)?,
            indielinks,
            nodes: nodes.collect(),
        })
    }
    pub fn get_client(&self) -> &aws_sdk_dynamodb::Client {
        &self.client
    }
    async fn id_for_username(&self, username: &Username) -> Result<Option<UserId>> {
        Ok(self
            .client
            .query()
            .table_name("users")
            .index_name("users_by_username")
            .key_condition_expression("username=:u")
            .expression_attribute_values(":u", AttributeValue::S(username.to_string()))
            .send()
            .await
            .context(DdbQuerySnafu)?
            .items()
            .to_vec()
            .pipe(from_items::<User>)
            .context(DeUserSnafu)?
            .into_iter()
            .at_most_one()
            .map_err(|_| {
                MultipleUsersSnafu {
                    username: username.clone(),
                }
                .build()
            })?
            .map(|user| *user.id()))
    }
}

#[async_trait]
impl Helper for DynamoDBHelper {
    async fn clear_posts(&self, username: &Username) -> StdResult<(), Failed> {
        let user_id = self
            .id_for_username(username)
            .await?
            .context(NoSuchUserSnafu {
                username: username.clone(),
            })?;

        // Arrrrrgghhhhh... DDB provides no "truncate" operation. We need to `BatchWrite` (or drop &
        // recreate the table). Hopefully, no test will create so many posts that we can't do this
        // in-memory:
        let mut posts = self
            .client
            .query()
            .table_name("posts")
            .select(Select::AllAttributes)
            .key_condition_expression("user_id=:id")
            .expression_attribute_values(":id", AttributeValue::S(user_id.to_string()))
            .send()
            .await
            .context(DdbQuerySnafu)?
            .items()
            .to_vec()
            .pipe(from_items::<Post>)
            .context(DePostSnafu)?
            .into_iter()
            .map(|post| {
                DeleteRequest::builder()
                    .key("user_id", AttributeValue::S(post.user_id().to_string()))
                    .key("url", AttributeValue::S(post.url().to_string()))
                    .build()
            })
            .collect::<StdResult<Vec<DeleteRequest>, _>>()?
            .into_iter()
            .map(|dreq| WriteRequest::builder().delete_request(dreq).build())
            .collect::<Vec<WriteRequest>>();

        while !posts.is_empty() {
            let this_batch: Vec<_> = posts.drain(..min(posts.len(), 25)).collect();
            self.client
                .batch_write_item()
                .request_items("posts", this_batch)
                .send()
                .await?;
        }

        self.client
            .update_item()
            .table_name("users")
            .key("id", AttributeValue::S(user_id.to_string()))
            .update_expression("set first_update=:F, last_update=:L")
            .expression_attribute_values(":F", AttributeValue::Null(true))
            .expression_attribute_values(":L", AttributeValue::Null(true))
            .send()
            .await?;

        Ok(())
    }
    async fn create_user(
        &self,
        pepper_version: &PepperVersion,
        pepper_key: &Pepper,
        username: &Username,
        password: &SecretString,
        followers: &HashSet<StorUrl>,
        following: &HashSet<(StorUrl, FollowId)>,
    ) -> std::result::Result<String, Failed> {
        use crypto_common::rand_core::{OsRng, RngCore};
        let mut api_key = [0u8; 64];
        OsRng.fill_bytes(api_key.as_mut_slice());
        let textual_api_key = format!("v1:{}", hex::encode(api_key));
        let user = User::new(
            pepper_version,
            pepper_key,
            username,
            password,
            &UserEmail::new(&format!("{username}@example.com")).unwrap(/* known good */),
            Some(Box::new(api_key)),
            None,
            None,
            None,
        )?;

        add_ddb_user(&self.client, &user).await?;
        add_ddb_followers(&self.client, &user, followers).await?;
        add_ddb_following(&self.client, &user, following).await?;

        Ok(textual_api_key)
    }
    async fn add_outgoing_like(&self, username: &Username, url: &Url) -> StdResult<(), Failed> {
        let user_id = self
            .id_for_username(username)
            .await?
            .context(NoSuchUserSnafu {
                username: username.clone(),
            })?;
        let like = OutgoingLike::new(user_id, url);
        self.client
            .put_item()
            .table_name("likes_replies_shares")
            .set_item(Some(to_item(LikeReplyShareRef::Like(&like))?))
            .send()
            .await?;
        Ok(())
    }
    async fn add_outgoing_reply(
        &self,
        username: &Username,
        url: &Url,
        text: &str,
    ) -> StdResult<(), Failed> {
        let user_id = self
            .id_for_username(username)
            .await?
            .context(NoSuchUserSnafu {
                username: username.clone(),
            })?;
        let reply = OutgoingReply::new(
            user_id,
            ReplyId::default(),
            url.clone(),
            Visibility::Public,
            text.to_owned(),
        );
        self.client
            .put_item()
            .table_name("likes_replies_shares")
            .set_item(Some(to_item(LikeReplyShareRef::Reply(&reply))?))
            .send()
            .await?;
        Ok(())
    }
    async fn add_outgoing_share(&self, username: &Username, url: &Url) -> StdResult<(), Failed> {
        let user_id = self
            .id_for_username(username)
            .await?
            .context(NoSuchUserSnafu {
                username: username.clone(),
            })?;
        let share = OutgoingShare::new(user_id, url, Visibility::Public, String::new());
        self.client
            .put_item()
            .table_name("likes_replies_shares")
            .set_item(Some(to_item(LikeReplyShareRef::Share(&share))?))
            .send()
            .await?;
        Ok(())
    }
    async fn remove_user(&self, username: &Username) -> std::result::Result<(), Failed> {
        let user_id = self.id_for_username(username).await?;
        if let Some(user_id) = user_id {
            self.client
                .delete_item()
                .table_name("users")
                .key("id", AttributeValue::S(user_id.to_string()))
                .send()
                .await?;
            self.client
                .delete_item()
                .table_name("unique_usernames")
                .key("username", AttributeValue::S(username.to_string()))
                .send()
                .await?;
        }
        Ok(())
    }
    fn indielinks(&self) -> Url {
        self.indielinks.clone()
    }
    fn nodes(&self) -> Vec<(Url, Url, SocketAddr)> {
        self.nodes.clone()
    }
}

pub struct ScyllaHelper {
    session: ::scylla::client::session::Session,
    indielinks: Url,
    nodes: Vec<(Url, Url, SocketAddr)>,
}

impl ScyllaHelper {
    pub async fn new(
        indielinks: Url,
        nodes: impl Iterator<Item = (Url, Url, SocketAddr)>,
        cfg: &ScyllaConfig,
    ) -> Result<ScyllaHelper> {
        let session = create_scylla_client(
            &cfg.hosts,
            cfg.credentials.as_ref(),
            cfg.translations.as_ref().cloned(),
        )
        .await
        .context(NewSessionSnafu)?;
        session
            .use_keyspace("indielinks", false)
            .await
            .context(KeyspaceSnafu)?;
        Ok(ScyllaHelper {
            session,
            indielinks,
            nodes: nodes.collect(),
        })
    }
    pub fn get_client(&self) -> &scylla::client::session::Session {
        &self.session
    }
    async fn id_for_username(&self, username: &Username) -> Result<Option<UserId>> {
        Ok(self
            .session
            .query_unpaged("select id from users where username=?", (username,))
            .await
            .context(ScyllaQuerySnafu)?
            .into_rows_result()
            .context(RowsResultSnafu)?
            .rows::<(UserId,)>()
            .context(RowsSnafu)?
            .collect::<StdResult<Vec<(UserId,)>, _>>()
            .context(DeUserIdSnafu)?
            .into_iter()
            .at_most_one()
            .map_err(|_| {
                MultipleUsersSnafu {
                    username: username.clone(),
                }
                .build()
            })?
            .map(|opt| opt.0))
    }
}

#[async_trait]
impl Helper for ScyllaHelper {
    async fn clear_posts(&self, username: &Username) -> StdResult<(), Failed> {
        let userid = self
            .id_for_username(username)
            .await?
            .context(NoSuchUserSnafu {
                username: username.clone(),
            })?;
        self.session.query_unpaged("truncate posts", ()).await?;
        self.session
            .query_unpaged(
                "update users set first_update=null, last_update=null where id=?",
                (userid,),
            )
            .await?;
        Ok(())
    }
    async fn create_user(
        &self,
        pepper_version: &PepperVersion,
        pepper_key: &Pepper,
        username: &Username,
        password: &SecretString,
        followers: &HashSet<StorUrl>,
        following: &HashSet<(StorUrl, FollowId)>,
    ) -> std::result::Result<String, Failed> {
        let mut api_key = [0u8; 64];
        OsRng.fill_bytes(api_key.as_mut_slice());
        let textual_api_key = format!("v1:{}", hex::encode(api_key));
        let user = User::new(
            pepper_version,
            pepper_key,
            username,
            password,
            &UserEmail::new(&format!("{username}@example.com")).unwrap(/* known good */),
            Some(Box::new(api_key)),
            None,
            None,
            None,
        )?;

        add_scylla_user(&self.session, None, None, &user).await?;
        add_scylla_followers(&self.session, None, &user, followers, true).await?;
        add_scylla_following(&self.session, None, &user, following, true).await?;

        Ok(textual_api_key)
    }
    async fn add_outgoing_like(&self, username: &Username, url: &Url) -> StdResult<(), Failed> {
        let user_id = self
            .id_for_username(username)
            .await?
            .context(NoSuchUserSnafu {
                username: username.clone(),
            })?;
        let like = OutgoingLike::new(user_id, url);
        add_scylla_outgoing_lrs(&self.session, None, &LikeReplyShareRef::Like(&like)).await?;
        Ok(())
    }
    async fn add_outgoing_reply(
        &self,
        username: &Username,
        url: &Url,
        text: &str,
    ) -> StdResult<(), Failed> {
        let user_id = self
            .id_for_username(username)
            .await?
            .context(NoSuchUserSnafu {
                username: username.clone(),
            })?;
        let reply = OutgoingReply::new(
            user_id,
            ReplyId::default(),
            url.clone(),
            Visibility::Public,
            text.to_owned(),
        );
        add_scylla_outgoing_lrs(&self.session, None, &LikeReplyShareRef::Reply(&reply)).await?;
        Ok(())
    }
    async fn add_outgoing_share(&self, username: &Username, url: &Url) -> StdResult<(), Failed> {
        let user_id = self
            .id_for_username(username)
            .await?
            .context(NoSuchUserSnafu {
                username: username.clone(),
            })?;
        let share = OutgoingShare::new(user_id, url, Visibility::Public, String::new());
        add_scylla_outgoing_lrs(&self.session, None, &LikeReplyShareRef::Share(&share)).await?;
        Ok(())
    }
    async fn remove_user(&self, username: &Username) -> std::result::Result<(), Failed> {
        let user_id = self.id_for_username(username).await?;
        if let Some(user_id) = user_id {
            self.session
                .query_unpaged("delete from unique_usernames where username=?", (username,))
                .await?;
            self.session
                .query_unpaged("delete from users where id=?", (user_id,))
                .await?;
        }
        Ok(())
    }
    fn indielinks(&self) -> Url {
        self.indielinks.clone()
    }
    fn nodes(&self) -> Vec<(Url, Url, SocketAddr)> {
        self.nodes.clone()
    }
}
