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
use futures::{stream, StreamExt};
use itertools::Itertools;
use scylla::{
    prepared_statement::PreparedStatement, transport::errors::QueryError, QueryResult,
    SessionBuilder,
};
use secrecy::{ExposeSecret, SecretString};
use snafu::{Backtrace, IntoError, ResultExt, Snafu};
use tap::Pipe;

use crate::{
    entities::{Post, PostDay, PostId, PostUri, Tag, TagId, Tagname, User, UserId},
    storage,
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
    #[snafu(display("Failed to fetch tag {tag_id}: {source}"))]
    FetchTag {
        tag_id: TagId,
        source: scylla::transport::errors::QueryError,
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
    #[snafu(display("Failed to create a ScyllaDB session: {source}"))]
    NewSession {
        source: scylla::transport::errors::NewSessionError,
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
    TagCloud0,
    TagCloud1,
    TagCloud2,
    TagCloud3,
    TagCloud4,
    InsertPost0,
    InsertPost1,
    DeletePost,
    GetPostsByDay,
    GetPostsByUserAndUrl,
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
            "update users set first_update=? where id=?",
            "update users set last_update=? where id=?", // UpdateLoastPost
            "insert into tags (id,user_id,name,count) values (?,?,?,?) if not exists", // TagCloud0
            "select * from tags where user_id=?",
            "select * from posts_by_user_and_url where user_id=? and url=?", // TagCloud2
            "select * from tags where user_id=? and id=?", // TagCloud3
            "update tags set count=? where user_id=? and name=? if count=?", // TagCloud4
            "insert into posts (id,url,user_id,posted,day,title,notes,tags,public,unread) values (?,?,?,?,?,?,?,?,?,?)",
            "insert into posts (id,url,user_id,posted,day,title,notes,tags,public,unread) values (?,?,?,?,?,?,?,?,?,?) if not exists",
            "delete from posts where user_id=? and day=? and id=?", // DeletePost
            "select id,url,user_id,posted,day,title,notes,tags,public,unread from posts where user_id=? and day=?",
            "select id,url,user_id,posted,day,title,notes,tags,public,unread from posts_by_user_and_url where user_id=? and url=?",
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
        let prepared_statements: [PreparedStatement; 18] = prepared_statements
            .try_into()
            .map_err(|_| BadPreparedStatementCountSnafu.build())?;

        Ok(Session {
            session: scylla,
            prepared_statements: EnumMap::from_array(prepared_statements),
        })
    }
}

use storage::Error as StorError;

#[async_trait]
impl storage::Backend for Session {
    async fn user_for_name(&self, name: &str) -> StdResult<Option<User>, StorError> {
        use snafu::IntoError;
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
    ) -> StdResult<bool, StorError> {
        use snafu::IntoError;

        // Grrr... this is duplicated betweeen the Scylla & Dynamo implementations.
        let day: String = dt.format("%Y-%m-%d").to_string();
        let id = PostId::new();
        // Unlike in SQL, INSERT INTO does not check the prior existence of the row by default: the
        // row is created if none existed before, and updated otherwise. This behavior can be
        // changed by using ScyllaDB’s Lightweight Transaction IF NOT EXISTS or IF EXISTS clauses.
        let result = self
            .session
            .execute_unpaged(
                if replace {
                    &self.prepared_statements[PreparedStatements::InsertPost0]
                } else {
                    &self.prepared_statements[PreparedStatements::InsertPost1]
                },
                (
                    id,
                    format!("{}", uri),
                    user.id(),
                    dt,
                    day,
                    title.to_string(),
                    notes,
                    tags,
                    shared,
                    to_read,
                ),
            )
            .await
            .map_err(|err| StorError::new(AddPostSnafu.into_error(err)))?;

        // If the insert happened, the resulting `QueryResult` will have no rows (if it did not, it will):
        Ok(!result.is_rows())
    }

    async fn delete_posts(&self, posts: &[Post]) -> StdResult<(), StorError> {
        // A good developer would batch these to cut down on round-trips to the server, but for now
        // I'll let myself down & settle for this.
        stream::iter(posts.iter())
            .then(|post| async move {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::DeletePost],
                        (post.user_id(), post.day(), post.id()),
                    )
                    .await
                    .map_err(|err| QuerySnafu.into_error(err))
                    // Throw away the query result; in the case of a delete, I don't believe it carries
                    // any useful informatation
                    .map(|_| ())
            })
            .collect::<Vec<Result<()>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<()>>>()
            .map_err(StorError::new)?;
        Ok(())
    }

    async fn get_posts_by_day(
        &self,
        userid: &UserId,
        day: &PostDay,
    ) -> StdResult<Vec<Post>, StorError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::GetPostsByDay],
                (userid, day),
            )
            .await
            .map_err(|err| StorError::new(QuerySnafu.into_error(err)))?
            .into_rows_result()
            .map_err(|err| StorError::new(IntoRowsResultSnafu.into_error(err)))?
            .rows::<Post>()
            .map_err(|err| StorError::new(TypedRowsSnafu.into_error(err)))?
            .collect::<StdResult<Vec<Post>, _>>()
            .map_err(|err| StorError::new(PostDeSnafu.into_error(err)))?
            .pipe(Ok)
    }

    async fn get_posts_by_uri(
        &self,
        userid: &UserId,
        uri: &PostUri,
    ) -> StdResult<Vec<Post>, StorError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::GetPostsByUserAndUrl],
                (userid, uri),
            )
            .await
            .map_err(|err| StorError::new(QuerySnafu.into_error(err)))?
            .into_rows_result()
            .map_err(|err| StorError::new(IntoRowsResultSnafu.into_error(err)))?
            .rows::<Post>()
            .map_err(|err| StorError::new(TypedRowsSnafu.into_error(err)))?
            .collect::<StdResult<Vec<Post>, _>>()
            .map_err(|err| StorError::new(PostDeSnafu.into_error(err)))?
            .pipe(Ok)
    }

    async fn get_tag_cloud(&self, user: &User) -> StdResult<HashMap<Tagname, usize>, StorError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::TagCloud1],
                (user.id(),),
            )
            .await
            .map_err(|err| StorError::new(GetTagCloudSnafu.into_error(err)))?
            .into_rows_result()
            .map_err(|err| StorError::new(IntoRowsResultSnafu.into_error(err)))?
            .rows::<Tag>()
            .map_err(|err| StorError::new(TypedRowsSnafu.into_error(err)))?
            .collect::<StdResult<Vec<Tag>, _>>()
            .map_err(|err| StorError::new(TagDeSnafu.into_error(err)))?
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
            .session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::TagCloud2],
                (userid, uri),
            )
            .await
            .map_err(|err| StorError::new(QuerySnafu.into_error(err)))?
            .into_rows_result()
            .map_err(|err| StorError::new(IntoRowsResultSnafu.into_error(err)))?
            .rows::<Post>()
            .map_err(|err| StorError::new(TypedRowsSnafu.into_error(err)))?
            .map(|res| res.map(|post| post.tags()))
            .collect::<StdResult<Vec<HashSet<TagId>>, scylla::deserialize::DeserializationError>>()
            .map_err(|err| StorError::new(TagIdDeSnafu.into_error(err)))?
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
        // This has been unexpectedly painful with ScyllaDB. There's no "create-or-update"
        // primitive. You *can* send an "INSERT...IF NOT EXISTS" statement and examine the result
        // (if the insert didn't take place, the *extant* values will be returned to you. You can
        // then issue an "UPDATE...IF NOT EQUAL" statement (the "if not equal" portion being to
        // guard against another writer slipping in ahead of you, like a CAS operation)).
        //
        // So, for each tag in `tags`:
        //
        // 1. Insert a tag (with a newly minted `TagId` and a count of 1) if it doesn't exist; if
        // this succeeds, we're done.
        //
        // 1. Query by `user_id` and `name` (which we can do efficiently because we have a secondary
        // index on `name`).
        //
        // 2. Else, update the count, taking care to check that no one else got in between these
        // two queries
        //
        // I *think* this will be robust to race conditions: simultaneous creation should be fine,
        // since one will "win" and the other will see it as already existing & proceeed
        // accordingly. Things get interesting in the presence of deletions, but even if I see the
        // tag entry as existing, you delete, and I then try to update, the update will fail.

        // Consider re-factoring; similar to the DDB implementation, this is tough to read & I'd
        // like to apply the adage "parse don't validate":

        // Build a stream of futures yielding a `Result<TagId>`
        let tag_ids = stream::iter(tags.iter())
            .then(|tag| async move {
                let tagid = TagId::new();
                let result = self
                    .session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::TagCloud0],
                        (&tagid, &userid, tag.to_string(), 1),
                    )
                    .await
                    .context(InsertTagSnafu {
                        tag: tag.to_string(),
                    })?
                    .into_rows_result()
                    .context(ResultNotRowsSnafu)?;
                let row = result
                    .first_row::<(
                        bool,
                        Option<UserId>,
                        Option<&str>,
                        Option<i32>,
                        Option<TagId>,
                    )>()
                    .context(FirstRowSnafu)?;
                if row.0 {
                    return Ok::<TagId, Error>(tagid);
                }
                let cnt = row.3.unwrap();
                let tagid = row.4.unwrap();
                let _result = self
                    .session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::TagCloud4],
                        (cnt + 1, &userid, tag, cnt),
                    )
                    .await
                    .context(UpdateTagSnafu {
                        tag: tag.to_string(),
                    })?
                    .into_rows_result()
                    .context(ResultNotRowsSnafu)?;
                Ok(tagid)
            })
            .collect::<Vec<Result<TagId>>>()
            .await
            .into_iter()
            .collect::<StdResult<Vec<TagId>, _>>()
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

        Ok(unique_tag_ids)
    }

    async fn update_tag_cloud_on_delete(
        &self,
        userid: &UserId,
        deltas: &HashMap<TagId, usize>,
    ) -> StdResult<(), StorError> {
        // This is really unfortunate-- I have `TagId`, but I need the *name*. Going to make two
        // passes:
        //
        // 1. one to match a name to the `TagId` (using the global secondary index)
        //
        // 2. a second to update the count; taking care to check that the Id hasn't changed (to
        // guard against race conditions)
        async fn get_count(
            session: &::scylla::Session,
            pstate: &PreparedStatement,
            userid: &UserId,
            tagid: &TagId,
        ) -> Result<Option<(TagId, Tagname, usize)>> {
            match session
                .execute_unpaged(pstate, (userid, tagid))
                .await
                .context(FetchTagSnafu { tag_id: *tagid })?
                .into_rows_result()
                .context(ResultNotRowsSnafu)?
                .rows::<Tag>()
                .context(TypedRowsSnafu)?
                .at_most_one()
                .map_err(|_| AtMostOneRowSnafu.build())?
            {
                // We have a current count for `tagid`!
                Some(Ok(tag)) => Ok(Some((*tagid, tag.name(), tag.count()))),
                // Deserialization error-- pass it along
                Some(Err(err)) => Err(TagCountDeSnafu.into_error(err)),
                None => Ok(None),
            }
        }

        let current_counts = stream::iter(deltas.iter())
            .then(|(tagid, _count)| async move {
                get_count(
                    &self.session,
                    &self.prepared_statements[PreparedStatements::TagCloud3],
                    userid,
                    tagid,
                )
                .await
            })
            .collect::<Vec<Result<Option<(TagId, Tagname, usize)>>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<Option<(TagId, Tagname, usize)>>>>()
            .map_err(StorError::new)?
            .into_iter()
            .flatten(); // :=> Vec<(TagId, Tagname, usize)>... yeesh
                        // OK-- now adjust the counts

        async fn go(
            session: &::scylla::Session,
            statement: &PreparedStatement,
            userid: &UserId,
            tag: Tagname,
            current_count: usize,
            delta: usize,
        ) -> Result<QueryResult> {
            if delta > current_count {
                return MismatchedTagUseCountsSnafu {
                    tag,
                    current: current_count,
                    delta,
                }
                .fail();
            }
            let new_count = current_count - delta;
            let current_count = TryInto::<i32>::try_into(current_count).context(CountOORSnafu {
                count: current_count,
            })?;
            let new_count =
                TryInto::<i32>::try_into(new_count).context(CountOORSnafu { count: new_count })?;
            session
                .execute_unpaged(statement, (new_count, userid, tag, current_count))
                .await
                .context(QuerySnafu)
        }

        stream::iter(current_counts)
            .then(|(tagid, tag, current_count)| async move {
                go(
                    &self.session,
                    &self.prepared_statements[PreparedStatements::TagCloud4],
                    userid,
                    tag,
                    current_count,
                    *deltas.get(&tagid).unwrap(/* Known good */),
                )
                .await
            })
            .collect::<Vec<Result<QueryResult>>>()
            .await
            .into_iter()
            .collect::<StdResult<Vec<QueryResult>, _>>()
            .map_err(StorError::new)?;
        // Here, I now have a `Vec<QueryResult>`-- not sure how to process that.
        Ok(())
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
}
