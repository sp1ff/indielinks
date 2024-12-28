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
// You should have received a copy of the GNU General Public License along with mpdpopm.  If not,
// see <http://www.gnu.org/licenses/>.

//! # scylla
//!
//! [Storage] implementation for ScyallaDB.
//!
//! [Storage]: crate::storage

use async_trait::async_trait;
use enum_map::{Enum, EnumMap};
use futures::stream;
use itertools::Itertools;
use scylla::{prepared_statement::PreparedStatement, SessionBuilder};
use secrecy::{ExposeSecret, SecretString};
use snafu::{Backtrace, ResultExt, Snafu};
use tap::Pipe;

use crate::{entities::User, storage};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("A query was expected to prodce at most one row & did not."))]
    AtMostOneRow { backtrace: Backtrace },
    #[snafu(display(
        "The number of prepared statements isn't consistent; this is a bug & should be reported!"
    ))]
    BadPreparedStatementCount { backtrace: Backtrace },
    #[snafu(display("The query succeeded, but returned zero rows"))]
    EmptyQueryResult { backtrace: Backtrace },
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
    #[snafu(display("Failed to create a ScyllaDB session: {source}"))]
    NewSession {
        source: scylla::transport::errors::NewSessionError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to prepare statement {stmt}: {source}"))]
    Prepare {
        stmt: String,
        source: scylla::transport::errors::QueryError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to type a RowResult: {source}"))]
    TypedRows {
        source: scylla::transport::query_result::RowsError,
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
    InsertPost,
    GetLastPosted,
    GetRecentPosts0,
    GetRecentPosts1,
    GetRecentPosts2,
    GetRecentPosts3,
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
            "insert into posts (url, user_id, posted, day, title) values (?,?,?,?,?) if not exists",
            "select posted from posts where user_id=? limit 1 allow filtering",
            "select url,title,notes,tags,user_id,posted from posts where user_id=? limit ? allow filtering",
            "select url,title,notes,tags,user_id,posted from posts where user_id=? and tags contains ? limit ? allow filtering",
            "select url,title,notes,tags,user_id,posted from posts where user_id=? and tags contains ? and tags contains ? limit ? allow filtering",
            "select url,title,notes,tags,user_id,posted from posts where user_id=? and tags contains ? and tags contains ? and tags contains ? limit ? allow filtering",
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
        let prepared_statements: [PreparedStatement; 7] = prepared_statements
            .try_into()
            .map_err(|_| BadPreparedStatementCountSnafu.build())?;

        Ok(Session {
            session: scylla,
            prepared_statements: EnumMap::from_array(prepared_statements),
        })
    }
}

#[async_trait]
impl storage::Backend for Session {
    async fn user_for_name(&self, name: &str) -> std::result::Result<Option<User>, storage::Error> {
        use snafu::IntoError;
        use storage::Error as StorError;
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
