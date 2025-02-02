// Copyright (C) 2024-2025 Michael Herstine <sp1ff@pobox.com>
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

//! # scylla
//!
//! [Storage] implementation for ScyallaDB.
//!
//! [Storage]: crate::storage

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use enum_map::{Enum, EnumMap};
use futures::stream;
use itertools::Itertools;
use scylla::{
    prepared_statement::PreparedStatement, transport::errors::QueryError, SessionBuilder,
};
use secrecy::{ExposeSecret, SecretString};
use snafu::{Backtrace, IntoError, ResultExt, Snafu};
use tap::Pipe;

use crate::{
    entities::{Post, PostDay, PostUri, Tagname, User, UserId},
    storage,
    util::UpToThree,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The add_post query failed: {source}"))]
    AddPost {
        source: scylla::transport::errors::QueryError,
        backtrace: Backtrace,
    },
    #[snafu(display("A query was expected to prodce at most one row & did not."))]
    AtMostOneRow { backtrace: Backtrace },
    #[snafu(display(
        "The number of prepared statements isn't consistent; this is a bug & should be reported!"
    ))]
    BadPreparedStatementCount { backtrace: Backtrace },
    #[snafu(display("On conversion, {count} was too large to be converted to an i32: {source}"))]
    CountOOR {
        count: usize,
        source: <i32 as TryInto<usize>>::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("The query succeeded, but returned zero rows"))]
    EmptyQueryResult { backtrace: Backtrace },
    #[snafu(display("Failed to deserialize the first row: {source}"))]
    FirstRow {
        source: scylla::transport::query_result::FirstRowError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to query tag cloud: {source}"))]
    GetTagCloud {
        source: scylla::transport::errors::QueryError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed inserting tag {tag}: {source}"))]
    InsertTag {
        tag: String,
        source: scylla::transport::errors::QueryError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to convert to a RowsResult: {source}"))]
    IntoRowsResult {
        source: scylla::transport::query_result::IntoRowsResultError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to set keyspace: {source}"))]
    Keyspace {
        source: scylla::transport::errors::QueryError,
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
    #[snafu(display(
        "TagID {tag} has a mistmatched tag count: current is {current} and delta is {delta}"
    ))]
    MismatchedTagUseCounts {
        current: usize,
        delta: usize,
        tag: Tagname,
        backtrace: Backtrace,
    },
    #[snafu(display("{userid}'s post count has gone negative ({count})"))]
    NegativePostCount {
        userid: UserId,
        count: i32,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to create a ScyllaDB session: {source}"))]
    NewSession {
        source: scylla::transport::errors::NewSessionError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a post count: {source}"))]
    PostCountDe {
        source: scylla::deserialize::DeserializationError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a Post: {source}"))]
    PostDe {
        source: scylla::deserialize::DeserializationError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to prepare statement: {stmt}: {source}"))]
    Prepare {
        stmt: String,
        source: scylla::transport::errors::QueryError,
        backtrace: Backtrace,
    },
    #[snafu(display("ScyllaDB query failed: {source}"))]
    Query {
        source: QueryError,
        backtrace: Backtrace,
    },
    #[snafu(display("Expected rows: {source}"))]
    ResultNotRows {
        source: scylla::transport::query_result::IntoRowsResultError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a tag count: {source}"))]
    TagCountDe {
        source: scylla::deserialize::DeserializationError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a Tag: {source}"))]
    TagDe {
        source: scylla::deserialize::DeserializationError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a tag ID: {source}"))]
    TagIdDe {
        source: scylla::deserialize::DeserializationError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to type a RowResult: {source}"))]
    TypedRows {
        source: scylla::transport::query_result::RowsError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed inserting tag {tag}: {source}"))]
    UpdateTag {
        tag: String,
        source: scylla::transport::errors::QueryError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a User: {source}"))]
    UserDe {
        source: scylla::deserialize::DeserializationError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to lookup user {username}: {source}"))]
    UserQuery {
        username: String,
        source: scylla::transport::errors::QueryError,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                indielinks SycllaDB session type                                //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// The set of prepared statements used by indielinks
///
/// I began this implementation by simply giving each prepared statement a field in the [Session]
/// struct, but this quickly became unwieldy. This enum is intended to be used as both a mnemonic
/// tag identifying prepared statements and as the key type in a mapping from said tags to
/// the actual [PreparedStatement]s.
///
/// The [Enum] interface below may be unfamiliar to the reader; that is defined in the [enum_map]
/// crate & will be used below. It will require us to provide a slice of [PreparedStatement] of
/// length exactly equal to the number of variants in this enumeration.
#[derive(Clone, Debug, Enum, Eq, PartialEq)]
enum PreparedStatements {
    SelectUser,
    UpdateFirstPost,
    UpdateLastPost,
    TagCloud,
    InsertPost0,
    InsertPost1,
    DeletePost,
    GetPostsByDay0,
    GetPostsByDay1,
    GetPostsByDay2,
    GetPostsByDay3,
    GetLastPosted,
    GetPosts1,
    GetPosts5,
    GetPosts6,
    GetPosts7,
    GetPosts8,
}

/// `indielinks`-specific ScyllaDB Session type
///
/// Instantiate this via [Session::new] with connection info & credentials if need be, when dropped
/// the ScyllaDB session will be terminated.
pub struct Session {
    session: ::scylla::Session,
    /// An [EnumMap] is a map whose keys are enum values where all values are guaranteed to be
    /// represented. As a result, the index operator is guaranteed to succeed-- no need to unwrap
    /// [Option]s or [Result]s or some such.
    prepared_statements: EnumMap<PreparedStatements, PreparedStatement>,
}

impl Session {
    /// Prepare a statement
    async fn prepare(scylla: &::scylla::Session, stmt: &str) -> Result<PreparedStatement> {
        scylla.prepare(stmt).await.context(PrepareSnafu {
            stmt: stmt.to_owned(),
        })
    }

    /// [Session] constructor
    ///
    /// Construct with a collection of SycllaDB hosts. The `Item`s are regrettably typed as `&str`,
    /// but they need to be parsable as `IpAddress`es. `credentials`, if non-None, should be a pair
    /// of string consisting of the username & password.
    pub async fn new(
        hosts: impl IntoIterator<Item = impl AsRef<str>>,
        credentials: &Option<(SecretString, SecretString)>,
    ) -> Result<Session> {
        let mut builder = SessionBuilder::new().known_nodes(hosts);
        if let Some((user, pass)) = credentials {
            builder = builder.user(user.expose_secret(), pass.expose_secret())
        }
        let scylla = builder.build().await.context(NewSessionSnafu)?;
        scylla
            .use_keyspace("indielinks", false)
            .await
            .context(KeyspaceSnafu)?;

        use futures::stream::StreamExt;
        let prepared_statements = stream::iter(vec![
            // Ho-kay: here's the deal. We list here all the prepared statements we want to use, in
            // the same order as [PreparedStatements].
            "select id,username,discoverable,display_name,summary,pub_key_pem,priv_key_pem,api_key,first_update,last_update from users where username=?",
            "update users set first_update=? where id=?",
            "update users set last_update=? where id=?", // UpdateLoastPost
            "select tags from posts where user_id=?", // TagCloud
            "insert into posts (user_id,url,posted,day,title,notes,tags,public,unread) values (?,?,?,?,?,?,?,?,?)",
            "insert into posts (user_id,url,posted,day,title,notes,tags,public,unread) values (?,?,?,?,?,?,?,?,?) if not exists",
            "delete from posts where user_id=? and url=? if exists", // DeletePost
            "select day from posts where user_id=?",
            "select day from posts where user_id=? and tags contains ? allow filtering",
            "select day from posts where user_id=? and tags contains ? and tags contains ? allow filtering",
            "select day from posts where user_id=? and tags contains ? and tags contains ? and tags contains ? allow filtering",
            "select posted from posts where user_id=? limit 1 allow filtering",
            "select url,day,title,notes,tags,user_id,posted,day,public,unread from posts where user_id=? and url=?", // GetPosts1
            "select url,title,notes,tags,user_id,posted,day,public,unread from posts where user_id=? and day=? allow filtering", // GetPosts5
            "select url,title,notes,tags,user_id,posted,day,public,unread from posts where user_id=? and day=? and tags contains ? allow filtering", // GetPosts6
            "select url,title,notes,tags,user_id,posted,day,public,unread from posts where user_id=? and day=? and tags contains ? and tags contains ? allow filtering",
            "select url,title,notes,tags,user_id,posted,day,public,unread from posts where user_id=? and day=? and tags contains ? and tags contains ? and tags contains ? allow filtering",
        ])
            // Then (see what I did there?), we actually prepare them with the Scylla database to
            // get futures yielding `Result<PreparedStatement>`...
            .then(|s|  async { Self::prepare(&scylla, s).await })
            // which we collect into a single `Future`...
            .collect::<Vec<_>>()
            // and then resolve to a `Vec<Result<PreparedStatement>>`...
            .await
            // and then move into an iterator...
            .into_iter()
            // and, finally, collect into a `Result<Vec<PreparedStatement>>:`
            .collect::<Result<Vec<PreparedStatement>>>()?;
        // Now: in order to create an `EnumMap`, we need a slice of `PreparedStatement` of
        // *precisely the right length*, and in the right order. We can't test for the latter, but
        // we can for the former: this will fail at compile time if we don't have a prepared
        // statement corresponding to each element of `PreparedStatements`.
        let prepared_statements: [PreparedStatement; 17] = prepared_statements
            .try_into()
            .map_err(|_| BadPreparedStatementCountSnafu.build())?;

        Ok(Session {
            session: scylla,
            prepared_statements: EnumMap::from_array(prepared_statements),
        })
    }
}

use storage::Error as StorError;

// Use these if you don't want to add any context to a failed query... should probably wrap this up
// in a macro, but I'm not sure this is the way I want to go, just yet.
impl std::convert::From<scylla::transport::errors::QueryError> for StorError {
    fn from(value: scylla::transport::errors::QueryError) -> Self {
        StorError::new(value)
    }
}

impl std::convert::From<scylla::transport::query_result::IntoRowsResultError> for StorError {
    fn from(value: scylla::transport::query_result::IntoRowsResultError) -> Self {
        StorError::new(value)
    }
}

impl std::convert::From<scylla::transport::query_result::RowsError> for StorError {
    fn from(value: scylla::transport::query_result::RowsError) -> Self {
        StorError::new(value)
    }
}

impl std::convert::From<scylla::deserialize::DeserializationError> for StorError {
    fn from(value: scylla::deserialize::DeserializationError) -> Self {
        StorError::new(value)
    }
}

impl std::convert::From<scylla::transport::query_result::FirstRowError> for StorError {
    fn from(value: scylla::transport::query_result::FirstRowError) -> Self {
        StorError::new(value)
    }
}

#[async_trait]
impl storage::Backend for Session {
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
    ) -> StdResult<bool, StorError> {
        // Unlike in SQL, INSERT INTO does not check the prior existence of the row by default: the
        // row is created if none existed before, and updated otherwise. This behavior can be
        // changed by using ScyllaDB’s Lightweight Transaction IF NOT EXISTS or IF EXISTS clauses.
        let day: PostDay = dt.into();
        let result = self
            .session
            .execute_unpaged(
                if replace {
                    &self.prepared_statements[PreparedStatements::InsertPost0]
                } else {
                    &self.prepared_statements[PreparedStatements::InsertPost1]
                },
                (
                    &user.id(),
                    uri,
                    &dt,
                    &day,
                    title,
                    notes,
                    tags,
                    shared,
                    to_read,
                ),
            )
            .await?;
        // If the insert happened, the resulting `QueryResult` will have no rows (if it did not, it will):
        Ok(!result.is_rows())
    }

    async fn delete_post(&self, user: &User, url: &PostUri) -> StdResult<bool, StorError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::DeletePost],
                (user.id(), url),
            )
            .await?
            .into_rows_result()?
            .first_row::<(
                bool,
                Option<UserId>,
                Option<PostUri>,
                Option<PostDay>,
                Option<String>,
                Option<DateTime<Utc>>,
                Option<bool>,
                Option<HashSet<Tagname>>,
                Option<String>,
                Option<bool>,
            )>()?
            .0
            .pipe(Ok)
    }

    async fn get_posts_by_day(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
    ) -> StdResult<Vec<(PostDay, usize)>, StorError> {
        // Use `execute_paged`?
        match tags {
            UpToThree::None => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::GetPostsByDay0],
                        (user.id().to_raw_string(),),
                    )
                    .await
            }
            UpToThree::One(tag) => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::GetPostsByDay1],
                        (user.id().to_raw_string(), tag),
                    )
                    .await
            }
            UpToThree::Two(tag0, tag1) => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::GetPostsByDay2],
                        (user.id().to_raw_string(), tag0, tag1),
                    )
                    .await
            }
            UpToThree::Three(tag0, tag1, tag2) => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::GetPostsByDay3],
                        (user.id().to_raw_string(), tag0, tag1, tag2),
                    )
                    .await
            }
        }?
        .into_rows_result()?
        .rows::<(PostDay,)>()?
        .map(|x| x.map(|y| y.0))
        .collect::<StdResult<Vec<PostDay>, _>>()?
        .into_iter()
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
        match (uri, tags) {
            (None, UpToThree::None) => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::GetPosts5],
                        (user.id(), day),
                    )
                    .await
            }
            (None, UpToThree::One(tag)) => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::GetPosts6],
                        (user.id(), day, tag),
                    )
                    .await
            }
            (None, UpToThree::Two(tag0, tag1)) => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::GetPosts7],
                        (user.id(), day, tag0, tag1),
                    )
                    .await
            }
            (None, UpToThree::Three(tag0, tag1, tag2)) => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::GetPosts8],
                        (user.id(), day, tag0, tag1, tag2),
                    )
                    .await
            }
            (Some(uri), _) => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::GetPosts1],
                        (user.id(), uri),
                    )
                    .await
            }
        }?
        .into_rows_result()?
        .rows::<Post>()?
        .collect::<StdResult<Vec<Post>, _>>()?
        .pipe(Ok)
    }

    async fn get_tag_cloud(&self, user: &User) -> StdResult<HashMap<Tagname, usize>, StorError> {
        self.session
            // Use `execute_paged`?
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::TagCloud],
                (user.id(),),
            )
            .await?
            .into_rows_result()?
            .rows::<(Vec<Tagname>,)>()?
            .map(|x| x.map(|y| y.0))
            .collect::<StdResult<Vec<Vec<Tagname>>, _>>()?
            .into_iter()
            .flatten()
            .counts()
            .pipe(Ok)
    }

    async fn update_user_post_times(
        &self,
        user: &User,
        dt: &DateTime<Utc>,
    ) -> StdResult<(), StorError> {
        // This brings up an interesting question: should I update the `User` instance
        // to reflect the new state in the database?
        if user.first_update().is_none() {
            self.session
                .execute_unpaged(
                    &self.prepared_statements[PreparedStatements::UpdateFirstPost],
                    (dt, user.id()),
                )
                .await
                .map_err(|err| StorError::new(QuerySnafu.into_error(err)))?;
        }
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::UpdateLastPost],
                (dt, user.id()),
            )
            .await
            .map_err(|err| StorError::new(QuerySnafu.into_error(err)))?;
        Ok(())
    }

    async fn user_for_name(&self, name: &str) -> StdResult<Option<User>, StorError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::SelectUser],
                (name,),
            )
            .await
            .map_err(|err| {
                name.to_string()
                    .pipe(|s| UserQuerySnafu { username: s })
                    .pipe(|e| e.into_error(err))
                    .pipe(StorError::new)
            })?
            .into_rows_result()
            .map_err(|err| StorError::new(IntoRowsResultSnafu {}.into_error(err)))?
            .rows::<User>()
            .map_err(|err| StorError::new(TypedRowsSnafu {}.into_error(err)))?
            .at_most_one()
            .map_err(|_| StorError::new(AtMostOneRowSnafu.build()))?
            .transpose()
            .map_err(|err| StorError::new(UserDeSnafu {}.into_error(err)))?
            .pipe(Ok)
    }
}
