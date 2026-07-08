// Copyright (C) 2024-2026 Michael Herstine <sp1ff@pobox.com>
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

//! # scylla: [Storage] implementation for ScyallaDB, along with other ScyllaDB-related utilities.
//!
//! [Storage]: crate::storage
//!
//! ## Introduction
//!
//! This is one of the oldest pieces of [indielinks], and to be honest it's overdue for a refactor.
//! This is the [Storage] [Backend] implementation for the Cassandra/native ScyllaDB interface.
//!
//! [indielinks]: ../indielinskd/index.html
//! [Backend]: crate::storage::Backend

use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    net::SocketAddr,
    ops::{Bound, Deref},
    pin::Pin,
    result::Result as StdResult,
    sync::Arc,
    task::Poll,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use enum_map::{Enum, EnumMap};
use futures::stream::iter;
use futures::{
    stream::{self, BoxStream},
    Stream,
};
use futures::{StreamExt, TryStreamExt};
use indielinks_cache::types::{NodeId, TypeConfig};
use indielinks_shared::instance_state::InstanceStateV0;
use indielinks_shared::nonempty_string::NonEmptyString;
use itertools::Itertools;
use num_bigint::BigInt;
use openraft::{
    Entry, ErrorSubject, ErrorVerb, LogId, LogState, RaftLogId, StorageError as RaftStorageError,
    StorageIOError, Vote,
};
use pin_project::pin_project;
use rmp_serde::{from_slice, to_vec};
use secrecy::ExposeSecret;
use snafu::{Backtrace, IntoError, ResultExt, Snafu};
use tap::Pipe;
use tracing::{debug, error, info};
use url::Url;
use uuid::Uuid;

use scylla::{
    client::{session::Session as InnerSession, session_builder::SessionBuilder},
    errors::TranslationError,
    policies::address_translator::UntranslatedPeer,
    response::{PagingState, PagingStateResponse},
    statement::{
        batch::{Batch, BatchStatement, BatchType},
        prepared::PreparedStatement,
        Statement,
    },
};

use indielinks_shared::entities::{Post, PostDay, PostId, StorUrl, Tagname, UserId, Username};

use crate::{
    background_tasks::{Backend as TasksBackend, Error as BackgroundTaskError, FlatTask},
    cache::{
        to_storage_io_err, Backend as CacheBackend, Flavor, LogIndex, RaftLog, RaftMetadata, NID,
    },
    entities::{
        ApiKeys, FollowId, Follower, FollowerId, Following, IncomingLike, IncomingLikeReplyShare,
        IncomingLikeReplyShareRef, IncomingReply, IncomingShare, LikeReplyShare, LikeReplyShareRef,
        OutgoingLike, OutgoingReply, OutgoingShare, User,
    },
    storage::{self, Counts, DateRange, SchemaSnafu, UsernameClaimedSnafu},
    util::{Credentials, UpToThree},
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("A query was expected to produce at most one row & did not."))]
    AtMostOneRow { backtrace: Backtrace },
    #[snafu(display(
        "The number of prepared statements isn't consistent; this is a bug & should be reported!"
    ))]
    BadPreparedStatementCount { backtrace: Backtrace },
    #[snafu(display(
        "Failed to deserialize a username claim query response for {username}: {source}"
    ))]
    ClaimDe {
        username: Username,
        source: scylla::deserialize::DeserializationError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize the following count: {source}"))]
    CountDe {
        source: scylla::deserialize::DeserializationError,
        backtrace: Backtrace,
    },
    #[snafu(display("On conversion, {count} was too large to be converted to an i32: {source}"))]
    CountOOR {
        count: usize,
        source: <i32 as TryInto<usize>>::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("{username} is already claimed"))]
    DuplicateUsername {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("The query succeeded, but returned zero rows"))]
    EmptyQueryResult { backtrace: Backtrace },
    #[snafu(display("Failed to deserialize an openraft log Entry: {source}"))]
    EntryDe {
        source: rmp_serde::decode::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("ScyllaDB query failed: {source}"))]
    Execution {
        #[snafu(source(from(scylla::errors::ExecutionError, Box::new)))]
        source: Box<scylla::errors::ExecutionError>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize the first row: {source}"))]
    FirstRow {
        source: scylla::response::query_result::FirstRowError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a page worth of followers: {source}"))]
    FollowDe {
        source: scylla::deserialize::DeserializationError,
        backtrace: Backtrace,
    },
    #[snafu(display("While creating instance state, {source}"))]
    InstanceState {
        source: indielinks_shared::instance_state::Error,
    },
    #[snafu(display("Failed to convert to a RowsResult: {source}"))]
    IntoRowsResult {
        #[snafu(source(from(scylla::response::query_result::IntoRowsResultError, Box::new)))]
        source: Box<scylla::response::query_result::IntoRowsResultError>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to set keyspace: {source}"))]
    Keyspace {
        #[snafu(source(from(scylla::errors::UseKeyspaceError, Box::new)))]
        source: Box<scylla::errors::UseKeyspaceError>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to serialize an openraft LogId: {source}"))]
    LogIdSer {
        source: rmp_serde::encode::Error,
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
    #[snafu(display("Mutliple hits for {username}"))]
    MultipleUsernames {
        username: Username,
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
        #[snafu(source(from(scylla::errors::NewSessionError, Box::new)))]
        source: Box<scylla::errors::NewSessionError>,
        backtrace: Backtrace,
    },
    #[snafu(display("While paging a query, {source}"))]
    Pager {
        #[snafu(source(from(scylla::errors::PagerExecutionError, Box::new)))]
        source: Box<scylla::errors::PagerExecutionError>,
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
        #[snafu(source(from(scylla::errors::PrepareError, Box::new)))]
        source: Box<scylla::errors::PrepareError>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a RaftLog: {source}"))]
    RaftLogDe {
        source: scylla::deserialize::DeserializationError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize Raft metadata: {source}"))]
    RaftMetaDe {
        source: scylla::deserialize::DeserializationError,
        backtrace: Backtrace,
    },
    #[snafu(display("Expected rows: {source}"))]
    ResultNotRows {
        #[snafu(source(from(scylla::response::query_result::IntoRowsResultError, Box::new)))]
        source: Box<scylla::response::query_result::IntoRowsResultError>,
        backtrace: Backtrace,
    },
    #[snafu(display("While converting to typed rows, {source}"))]
    RowType {
        source: scylla::deserialize::TypeCheckError,
        backtrace: Backtrace,
    },
    #[snafu(display("Deserialization error while validating schema: {source}"))]
    SchemaDe {
        source: scylla::deserialize::DeserializationError,
        backtrace: Backtrace,
    },
    #[snafu(display("Expected a single row; {source}"))]
    SingleRow {
        source: scylla::errors::SingleRowError,
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
        source: scylla::response::query_result::RowsError,
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
        #[snafu(source(from(scylla::errors::ExecutionError, Box::new)))]
        source: Box<scylla::errors::ExecutionError>,
        backtrace: Backtrace,
    },
    #[snafu(display("Expected UTF-8: {source}"))]
    Utf8 {
        source: std::string::FromUtf8Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize an openraft Vote: {source}"))]
    VoteDe {
        source: rmp_serde::decode::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to serialize an openraft Vote: {source}"))]
    VoteSer {
        source: rmp_serde::encode::Error,
        backtrace: Backtrace,
    },
}

type Result<T> = StdResult<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                   ScyllaDB-related utilities                                   //
////////////////////////////////////////////////////////////////////////////////////////////////////

// Many of these have been factored-out into standalone functions & types because they're used by
// the integration test suite as well as by the Scylla `Storage` implementation.

/// Add a [User] to the database
///
/// This logic has been factored out because it's used by the integration tests, as well.
pub async fn add_user(
    session: &::scylla::client::session::Session,
    prep_claim: Option<&PreparedStatement>,
    prep_insert: Option<&PreparedStatement>,
    user: &User,
) -> Result<bool> {
    let claimed = !match prep_claim {
        Some(prep) => session
            .execute_unpaged(prep, (user.username(), user.id()))
            .await
            .context(ExecutionSnafu)?,
        None => session
            .query_unpaged(
                "insert into unique_usernames (username, id) values (?, ?) if not exists",
                (user.username(), user.id()),
            )
            .await
            .context(ExecutionSnafu)?,
    }
    .into_rows_result()
    .context(IntoRowsResultSnafu)?
    .rows::<(bool, Option<Username>, Option<UserId>)>()
    .context(TypedRowsSnafu)?
    .exactly_one()
    .map_err(|_| {
        MultipleUsernamesSnafu {
            username: user.username().clone(),
        }
        .build()
    })?
    .context(ClaimDeSnafu {
        username: user.username().clone(),
    })?
    .pipe(|tup| tup.0);

    if claimed {
        return Ok(false);
    }

    let params = (
        user.id(),
        user.username(),
        user.discoverable(),
        user.display_name(),
        user.summary(),
        user.pub_key(),
        user.priv_key(),
        user.api_keys(),
        user.hash(),
        user.pepper_version(),
    );
    match prep_insert {
        Some(prep) => session.execute_unpaged(prep, params).await,
        None => {
            session
                .query_unpaged(
                    "insert into users (id, username, discoverable, display_name, \
                                       summary, pub_key_pem, priv_key_pem, api_keys, first_update, \
                                       last_update, password_hash, pepper_version) values \
                                       (?, ?, ?, ?, ?, ?, ?, ?, null, null, ?, ?)",
                    params,
                )
                .await
        }
    }
    .context(ExecutionSnafu)?;

    Ok(true)
}

/// Add a followers to the database
///
/// This logic has been factored out because the integration tests make use of it, as well.
pub async fn add_followers(
    session: &InnerSession,
    prep: Option<&PreparedStatement>,
    user: &User,
    followers: &HashSet<StorUrl>,
    confirmed: bool,
) -> Result<()> {
    let mut batch = Batch::default();
    let mut batch_values: Vec<(UserId, StorUrl, FollowerId, DateTime<Utc>, bool)> = Vec::new();
    followers.iter().for_each(|follower| {
        match prep {
            Some(prep) => batch.append_statement(prep.clone()),
            None => batch.append_statement(Statement::new(
                "insert into followers (user_id, actor_id, id, created, accepted) values (?, ?, ?, ?, ?)",
            )),
        };
        batch_values.push((
            *user.id(),
            follower.clone(),
            FollowerId::default(),
            Utc::now(),
            confirmed,
        ));
    });
    session
        .batch(&batch, batch_values)
        .await
        .context(ExecutionSnafu)?;
    Ok(())
}

pub async fn add_outgoing_like_reply_share(
    session: &InnerSession,
    prep: Option<&PreparedStatement>,
    lrs: &LikeReplyShareRef<'_>,
) -> Result<()> {
    match prep {
        Some(prepped) => session.execute_unpaged(prepped, lrs).await,
        None => session.query_unpaged("insert into likes_replies_shares (user_id, posted, id, content, in_reply_to, sort, visibility) values (?, ?, ?, ?, ?, ?, ?) if not exists", lrs).await,
    }
    .map(|_| ())
    .context(ExecutionSnafu)
}

/// Retrieve the number of actors following a given [User]
pub async fn get_followers_count(
    session: &InnerSession,
    prep: Option<&PreparedStatement>,
    user: &User,
) -> Result<usize> {
    let res = match prep {
        Some(prep) => session.execute_unpaged(prep, (user.id(),)).await,
        None => {
            session
                .query_unpaged(
                    "select count(*) from followers where user_id = ?",
                    (user.id(),),
                )
                .await
        }
    };

    Ok(res
       .context(ExecutionSnafu)?
       .into_rows_result()
       .context(IntoRowsResultSnafu)?
       .rows::<(i64,)>()
       .context(TypedRowsSnafu)?
       .exactly_one()
       .unwrap(/* known good */)
       .context(CountDeSnafu)?
       .0 as usize)
}

/// Note that a given user is now following one more more people
///
/// This logic has been factored out because the integration tests make use of it, as well.
// The logic is substantially the same as that of `add_followers()`-- may want to re-factor, at some
// point, if the two tables don't diverge.
pub async fn add_following(
    session: &InnerSession,
    prep: Option<&PreparedStatement>,
    user: &User,
    follows: &HashSet<(StorUrl, FollowId)>,
    confirmed: bool,
) -> Result<()> {
    let mut batch = Batch::default();
    let mut batch_values: Vec<(UserId, StorUrl, FollowId, DateTime<Utc>, bool)> = Vec::new();
    follows.iter().for_each(|follow| {
        match prep {
            Some(prep) => batch.append_statement(prep.clone()),
            None => batch.append_statement(Statement::new(
                "insert into following (user_id, actor_id, id, created, accepted) values (?, ?, ?, ?, ?)",
            )),
        };
        batch_values.push((*user.id(), follow.0.clone(), follow.1, Utc::now(), confirmed));
    });
    session
        .batch(&batch, batch_values)
        .await
        .context(ExecutionSnafu)?;
    Ok(())
}

/// Retrieve the number of actors followed by a given [User]
// Again, the logic is substantially the same as that of `get_followers_count()`-- may want to
// re-factor, at some point, if the two tables don't diverge.
pub async fn get_following_count(
    session: &InnerSession,
    prep: Option<&PreparedStatement>,
    user: &User,
) -> Result<usize> {
    let res = match prep {
        Some(prep) => session.execute_unpaged(prep, (user.id(),)).await,
        None => {
            session
                .query_unpaged(
                    "select count(*) from following where user_id = ?",
                    (user.id(),),
                )
                .await
        }
    };

    Ok(res
       .context(ExecutionSnafu)?
       .into_rows_result()
       .context(IntoRowsResultSnafu)?
       .rows::<(i64,)>()
       .context(TypedRowsSnafu)?
       .exactly_one()
       .unwrap(/* known good */)
       .context(CountDeSnafu)?
       .0 as usize)
}

pub async fn get_posts_count(
    session: &InnerSession,
    stmt: &PreparedStatement,
    user: &User,
) -> Result<usize> {
    Ok(session
        .execute_unpaged(stmt, (user.id(),))
        .await
        .context(ExecutionSnafu)?
        .into_rows_result()
        .context(IntoRowsResultSnafu)?
        .rows::<(i64,)>()
        .context(TypedRowsSnafu)?
        .exactly_one()
        .unwrap(/* known good */)
        .context(CountDeSnafu)?
        .0 as usize)
}

// re-factor with some of the above, `get_posts_count()`, at the least!
pub async fn get_likes_replies_shares_count(
    session: &InnerSession,
    stmt: &PreparedStatement,
    user: &User,
) -> Result<usize> {
    Ok(session
        .execute_unpaged(stmt, (user.id(),))
        .await
        .context(ExecutionSnafu)?
        .into_rows_result()
        .context(IntoRowsResultSnafu)?
        .rows::<(i64,)>()
        .context(TypedRowsSnafu)?
        .exactly_one()
        .unwrap(/* known good */)
        .context(CountDeSnafu)?
        .0 as usize)
}

// This is an internal macro that I'm using to reduce boilerplate in one method only:
// `get_all_posts()`.
#[macro_export]
macro_rules! all_posts_case {
    ($self:expr, $idx:expr, $params:expr) => {
        make_streaming_response(
            &$self.session,
            $self.get_all_posts_statements[$idx].clone(),
            $params,
        )
        .await
        .map_err(StorageError::new)?
        .boxed()
        .pipe(Ok)
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       PagedResultsStream                                       //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A [Stream] for enumerating a results from a paged ScyllaDB response
///
/// This is a utility type I whipped-up to make paged results out of ScyllaDB generic.
// I think I wrote this before I discovered `rows_stream()` (or maybe that was added later?) In any
// event, I think this can be removed, and callers can just use `make_streaming_response()`, below.
#[allow(clippy::type_complexity)]
#[pin_project]
pub struct PagedResultsStream<'a, T, P>
where
    T: for<'frame, 'metadata> scylla::deserialize::row::DeserializeRow<'frame, 'metadata>,
    P: scylla::serialize::row::SerializeRow + 'a,
{
    session: &'a InnerSession,
    params: P,
    count: usize,
    statement: &'a PreparedStatement,
    #[pin]
    fut: Option<
        Pin<Box<dyn Future<Output = Result<(VecDeque<T>, PagingStateResponse)>> + Send + 'a>>,
    >,
    #[pin]
    curr: VecDeque<T>,
}

impl<'a, T, P> PagedResultsStream<'a, T, P>
where
    T: for<'frame, 'metadata> scylla::deserialize::row::DeserializeRow<'frame, 'metadata> + 'a,
    P: scylla::serialize::row::SerializeRow + Clone + Send + Sync,
{
    pub async fn new(
        session: &'a InnerSession,
        statement: &'a PreparedStatement,
        params: P,
        count: usize,
    ) -> Result<PagedResultsStream<'a, T, P>> {
        Ok(PagedResultsStream {
            session,
            params: params.clone(),
            count,
            statement,
            fut: Some(Box::pin(Self::get_page(
                session,
                statement,
                params,
                PagingState::start(),
            ))),
            curr: VecDeque::new(),
        })
    }
    async fn get_page(
        session: &'a InnerSession,
        statement: &'a PreparedStatement,
        params: P,
        paging_state: PagingState,
    ) -> Result<(VecDeque<T>, PagingStateResponse)> {
        let (query_result, paging_state_response) = session
            .execute_single_page(statement, params, paging_state)
            .await
            .context(ExecutionSnafu)?;
        Ok((
            query_result
                .into_rows_result()
                .context(IntoRowsResultSnafu)?
                .rows::<T>()
                .context(TypedRowsSnafu)?
                .collect::<StdResult<VecDeque<T>, _>>()
                .context(FollowDeSnafu)?,
            paging_state_response,
        ))
    }
}

impl<'a, T, P> Stream for PagedResultsStream<'a, T, P>
where
    T: for<'frame, 'metadata> scylla::deserialize::row::DeserializeRow<'frame, 'metadata>
        + Unpin
        + 'a,
    P: scylla::serialize::row::SerializeRow + Clone + Send + Sync,
{
    type Item = StdResult<T, StorageError>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();
        loop {
            match this.curr.pop_front() {
                Some(row) => return Poll::Ready(Some(Ok(row))),
                None => match this.fut.as_mut().get_mut() {
                    // We're empty-- attempt to get more
                    Some(fut) => match fut.as_mut().poll(cx) {
                        Poll::Ready(Ok((next_page, paging_state_response))) => {
                            // // Not sure how to handle this, or whether it can even happen.
                            match next_page.front() {
                                Some(_) => {
                                    *this.curr.as_mut().get_mut() = next_page;
                                    match paging_state_response {
                                        PagingStateResponse::HasMorePages { state } => {
                                            *this.fut.as_mut().get_mut() =
                                                Some(Box::pin(Self::get_page(
                                                    this.session,
                                                    this.statement,
                                                    this.params.clone(),
                                                    state,
                                                )));
                                        }
                                        PagingStateResponse::NoMorePages => {
                                            *this.fut.as_mut().get_mut() = None
                                        }
                                    }
                                }
                                None => {
                                    self.fut = None;
                                    return Poll::Ready(None);
                                }
                            };
                        }
                        Poll::Ready(Err(err)) => {
                            // Return the error this time, then drop our Future
                            self.fut = None;
                            return Poll::Ready(Some(Err(StorageError::new(err))));
                        }
                        Poll::Pending => return Poll::Pending,
                    },
                    None => return Poll::Ready(None), // We're done
                },
            }
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.count, Some(self.count))
    }
}

/// Create a [Stream] yielding rows in response to a [PreparedStatement]
///
/// For some queries, the number of rows in the response may be large. Prepare such queries with
/// pagination enabled in the [Session] constructor, and use this function to build a [Stream]
/// implementation yielding a `Result<T, StorageError>` (where `T` is the row type). The choice of
/// `Err` variant is convenient in terms of implementing [Backend] trait methods.
///
/// Note that the resulting [Stream] implementation will have `'static` lifetime, meaning that
/// callers can hold on to instances that are in the process of yielding their results.
async fn make_streaming_response<T, P>(
    session: &InnerSession,
    statement: PreparedStatement,
    params: P,
) -> Result<impl Stream<Item = StdResult<T, StorageError>>>
where
    P: scylla::serialize::row::SerializeRow,
    T: for<'frame, 'metadata> scylla::deserialize::row::DeserializeRow<'frame, 'metadata>,
{
    session
        .execute_iter(statement, params)
        .await
        .context(PagerSnafu)?
        .rows_stream::<T>()
        .context(RowTypeSnafu)?
        .err_into::<StorageError>()
        .pipe(Ok)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          Translations                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// [Scylla] [AddressTranslator] implementation
///
/// Scylla allows for cluster members identify one another by different addresses than clients. I
/// gather this is intended to allow clients to work with clusters that are behind NAT (or similar),
/// but I find it handy for allowing integration tests to work with containerized clusters.
struct Translations {
    addresses: HashMap<SocketAddr, SocketAddr>,
}

impl Translations {
    pub fn new(addresses: impl IntoIterator<Item = (SocketAddr, SocketAddr)>) -> Self {
        Self {
            addresses: addresses.into_iter().collect(),
        }
    }
}

#[async_trait]
impl scylla::policies::address_translator::AddressTranslator for Translations {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> StdResult<SocketAddr, TranslationError> {
        self.addresses
            .get(&untranslated_peer.untranslated_address())
            .ok_or(TranslationError::NoRuleForAddress(
                untranslated_peer.untranslated_address(),
            ))
            .copied()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                 the Indielinks ScyllaDB client                                 //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Create an instance of the native ScyllaDB SDK client
pub async fn create_client(
    hosts: impl IntoIterator<Item = impl Borrow<SocketAddr>>,
    credentials: Option<&Credentials>,
    translations: Option<impl IntoIterator<Item = (SocketAddr, SocketAddr)>>,
) -> Result<InnerSession> {
    let mut builder = SessionBuilder::new().known_nodes_addr(hosts);
    if let Some(Credentials((user, pass))) = credentials {
        builder = builder.user(user.expose_secret(), pass.expose_secret())
    }
    if let Some(translations) = translations {
        builder = builder.address_translator(Arc::new(Translations::new(translations)))
    }
    builder.build().await.context(NewSessionSnafu)
}

/// Retrieve the current schema version (or None, if the database doesn't exist)
pub async fn get_current_schema_version(session: &InnerSession) -> Result<Option<u32>> {
    if let Err(err) = session.use_keyspace("indielinks", false).await {
        if matches!(
            err,
            scylla::errors::UseKeyspaceError::RequestError(
                scylla::errors::RequestAttemptError::DbError(scylla::errors::DbError::Invalid, _)
            )
        ) {
            // Keyspace doesn't exist
            info!(
                "The `indielinks` keyspace does not exist; assuming this database is unconfigured."
            );
            return Ok(None);
        } else {
            error!("get_current_schema_version: {err:#?}");
            return Err(KeyspaceSnafu.into_error(err));
        }
    }

    match session
        .query_unpaged("select version from schema_migrations;", ())
        .await
    {
        Ok(query_result) => {
            let mut versions = query_result
                .into_rows_result()
                .context(IntoRowsResultSnafu)?
                .rows::<(i64,)>()
                .context(TypedRowsSnafu)?
                .map(|res| res.map(|(i,)| i as u32))
                .collect::<StdResult<Vec<_>, _>>()
                .context(SchemaDeSnafu)?;
            versions.sort();
            Ok(versions.pop())
        }
        Err(err) => {
            // It would be weird to have a keyspace defined and not the `schema_versions` table, but still.
            if matches!(
                err,
                scylla::errors::ExecutionError::LastAttemptError(
                    scylla::errors::RequestAttemptError::DbError(
                        scylla::errors::DbError::Invalid,
                        _
                    )
                )
            ) {
                info!("The `schema_migrations` table doesn't exist; assuming this database is unconfigured.");
                Ok(None)
            } else {
                error!("get_current_schema_version: {err:#?}");
                Err(ExecutionSnafu.into_error(err))
            }
        }
    }
}

/// Execute arbitrary CQL.
pub async fn execute_cql<S: AsRef<str>>(session: &InnerSession, cql: S) -> Result<()> {
    // Strip comments first, since they might contain semicolons. There are a few ways to do this,
    // but the simplest and most efficient seemed to me to just do it by hand in a single pass.
    let mut output_buffer = Vec::new();
    enum State {
        Init,
        SawForwardSlash,
        InComment,
        SawAsterisk,
    }
    let mut state = State::Init;
    let text = cql.as_ref().as_bytes();
    for b in text {
        match state {
            State::Init => {
                // Looking for '/'; else copy
                if *b == b'/' {
                    state = State::SawForwardSlash;
                } else {
                    output_buffer.push(*b);
                }
            }
            State::SawForwardSlash => {
                // Looking for a '*'; if there, we're in a comment. Else push the preceedint '/'
                // _and_ the current char.
                if *b == b'*' {
                    state = State::InComment;
                } else {
                    output_buffer.push(b'/');
                    output_buffer.push(*b);
                    state = State::Init;
                }
            }
            State::InComment => {
                // Looking for a '*'. Skip text, in any event.
                if *b == b'*' {
                    state = State::SawAsterisk;
                }
            }
            State::SawAsterisk => {
                // Looking for a '/'; if there, shift back to init. Skip text, in any event.
                if *b == b'/' {
                    state = State::Init;
                } else {
                    state = State::InComment;
                }
            }
        }
    }

    iter(
        String::from_utf8(output_buffer)
            .context(Utf8Snafu)?
            .as_str()
            .split(";")
            .filter_map(|query| match query.trim() {
                "" => None,
                s => Some(s),
            }),
    )
    .then(|query| {
        debug!("Executing cql: {}", query);
        session.query_unpaged(query, ())
    })
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<StdResult<Vec<_>, _>>()
    .context(ExecutionSnafu)
    .map(|_| ())
}

pub async fn create_schema(
    session: Arc<InnerSession>,
    cql: &str,
    schema_version: i64,
) -> Result<()> {
    execute_cql(session.deref(), cql).await?;
    // This logic will have to be updated if we ever rev the "instance state". Until then, on create
    // we just write a default state, then copy it forward.
    let instance_state = if schema_version == 0 {
        InstanceStateV0::new().context(InstanceStateSnafu)?
    } else {
        session
            .query_unpaged(
                "select instance_state from schema_migrations where version = ?;",
                (schema_version - 1,),
            )
            .await
            .context(ExecutionSnafu)?
            .into_rows_result()
            .context(IntoRowsResultSnafu)?
            .single_row::<(InstanceStateV0,)>()
            .context(SingleRowSnafu)?
            .0
    };

    session
        .query_unpaged(
            "insert into schema_migrations (version, instance_state, applied) values (?, ?, ?);",
            (schema_version, instance_state, Utc::now()),
        )
        .await
        .context(ExecutionSnafu)
        .map(|_| ())
}

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
    ClaimUsername,
    InsertUser,
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
    RecentPosts0,
    RecentPosts1,
    RecentPosts2,
    RecentPosts3,
    GetAllPosts0,
    GetAllPosts1,
    GetAllPosts2,
    GetAllPosts3,
    GetAllPosts4,
    GetAllPosts5,
    GetAllPosts6,
    GetAllPosts7,
    GetAllPosts8,
    GetAllPosts9,
    GetAllPosts10,
    GetAllPosts11,
    GetAllPosts12,
    GetAllPosts13,
    GetAllPosts14,
    GetAllPosts15,

    GetAllPosts16,
    GetAllPosts17,
    GetAllPosts18,
    GetAllPosts19,
    GetAllPosts20,
    GetAllPosts21,
    GetAllPosts22,
    GetAllPosts23,
    GetAllPosts24,
    GetAllPosts25,
    GetAllPosts26,
    GetAllPosts27,
    GetAllPosts28,
    GetAllPosts29,
    GetAllPosts30,
    GetAllPosts31,

    GetPostsForTag,
    GetPostById,
    RenameTag,
    InsertTask,
    ScanTasks,
    TakeLease,
    FinishTask,
    GetUserById,
    AddFollows,
    ConfirmFollow,
    AddFollowers,
    CountFollowers,
    CountFollowing,
    CountFollowingByActor,
    InsertIntoRaftLog,
    TruncateRaftLog,
    TruncateRaftMeta,
    SelectRaftMeta1,
    SelectRaftLog1,
    DeleteRaftLog1,
    SelectRaftMeta2,
    InsertRaftMeta1,
    DeleteRaftLog2,
    GetRaftLogEntries1,
    GetRaftLogEntries2,
    GetRaftLogEntries3,
    GetRaftLogEntries4,
    GetRaftLogEntries5,
    GetRaftLogEntries6,
    GetRaftLogEntries7,
    GetRaftLogEntries8,
    GetRaftLogEntries9,
    UpdateApiKeys,
    AddOutgoingLikeReplyShare,
    AddIncomingLikeReplyShare,
    OutgoingLikeReplyShare,
    CountPosts,
    CountLikesRepliesShares,
    CountAllUsers,
    CountAllPosts,
}

/// `indielinks`-specific ScyllaDB Session type
///
/// Instantiate this via [Session::new] with connection info & credentials if need be, when dropped
/// the ScyllaDB session will be terminated.
// Nb. `InnerSession` (AKA scylla::client::session::Session) is *not* `Clone` (!)
pub struct Session {
    session: InnerSession,
    /// An [EnumMap] is a map whose keys are enum values where all values are guaranteed to be
    /// represented. As a result, the index operator is guaranteed to succeed-- no need to unwrap
    /// [Option]s or [Result]s or some such.
    prepared_statements: EnumMap<PreparedStatements, PreparedStatement>,
    /// Prepare statement with page size
    // Will probably need another `EnumMap`, but for now, just make it its own field
    following_statement: PreparedStatement,
    followers_statement: PreparedStatement,
    following_by_actor_statement: PreparedStatement,
    get_all_posts_statements: Vec<PreparedStatement>, // 32
    get_all_likes_replies_shares_statement: PreparedStatement,
    get_likes_replies_shares_statement: PreparedStatement,
    // Raft node ID
    node_id: NodeId,
}

impl Session {
    /// Prepare a statement
    async fn prepare(
        scylla: &::scylla::client::session::Session,
        stmt: &str,
    ) -> Result<PreparedStatement> {
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
        hosts: impl IntoIterator<Item = impl Borrow<SocketAddr>>,
        credentials: Option<&Credentials>,
        node_id: NodeId,
        translations: Option<impl IntoIterator<Item = (SocketAddr, SocketAddr)>>,
    ) -> Result<Session> {
        let scylla = create_client(hosts, credentials, translations).await?;
        scylla
            .use_keyspace("indielinks", false)
            .await
            .context(KeyspaceSnafu)?;

        use futures::stream::StreamExt;
        let prepared_statements = stream::iter(vec![
            // Ho-kay: here's the deal. We list here all the prepared statements we want to use, in
            // the same order as [PreparedStatements].
            "select * from users where username=?",
            "insert into unique_usernames (username, id) values (?, ?) if not exists",
            "insert into users (id, username, discoverable, display_name, summary, pub_key_pem, priv_key_pem, api_keys, first_update, last_update, password_hash, pepper_version) values (?, ?, ?, ?, ?, ?, ?, ?, null, null, ?, ?)",
            "update users set first_update=? where id=?",
            "update users set last_update=? where id=?", // UpdateLoastPost
            "select tags from posts where user_id=?", // TagCloud
            "insert into posts (user_id,url,id,posted,day,title,notes,tags,public,unread) values (?,?,?,?,?,?,?,?,?,?)", // InsertPost0
            "insert into posts (user_id,url,id,posted,day,title,notes,tags,public,unread) values (?,?,?,?,?,?,?,?,?,?) if not exists",
            "delete from posts where user_id=? and url=? if exists", // DeletePost
            "select day from posts where user_id=?",
            "select day from posts where user_id=? and tags contains ? allow filtering",
            "select day from posts where user_id=? and tags contains ? and tags contains ? allow filtering",
            "select day from posts where user_id=? and tags contains ? and tags contains ? and tags contains ? allow filtering",
            "select posted from posts where user_id=? limit 1 allow filtering",
            "select * from posts where user_id=? and url=?", // GetPosts1
            "select * from posts where user_id=? and day=? allow filtering", // GetPosts5
            "select * from posts where user_id=? and day=? and tags contains ? allow filtering", // GetPosts6
            "select * from posts where user_id=? and day=? and tags contains ? and tags contains ? allow filtering",
            "select * from posts where user_id=? and day=? and tags contains ? and tags contains ? and tags contains ? allow filtering",
            "select * from posts_by_posted where user_id=? limit ?",
            "select * from posts_by_posted where user_id=? and tags contains ? limit ? allow filtering",
            "select * from posts_by_posted where user_id=? and tags contains ? and tags contains ? limit ? allow filtering",
            "select * from posts_by_posted where user_id=? and tags contains ? and tags contains ? and tags contains ? limit ? allow filtering",

            "select * from posts_by_posted where user_id=? order by posted desc", // GetAllPosts0
            "select * from posts_by_posted where user_id=? and posted >= ? order by posted desc",
            "select * from posts_by_posted where user_id=? and posted < ? order by posted desc",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? order by posted desc",
            "select * from posts_by_posted where user_id=? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted < ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and tags contains ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and tags contains ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted < ? and tags contains ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? and tags contains ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and tags contains ? and tags contains ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and tags contains ? and tags contains ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted < ? and tags contains ? and tags contains ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? and tags contains ? and tags contains ? and tags contains ? order by posted desc allow filtering", // GetAllPost15

            "select * from posts_by_posted where user_id=? and unread=true order by posted desc allow filtering", // GetAllPosts16
            "select * from posts_by_posted where user_id=? and posted >= ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted < ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted < ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted < ? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and tags contains ? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and tags contains ? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted < ? and tags contains ? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? and tags contains ? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering", // GetAllPost31

            "select * from posts where user_id=? and tags contains ? allow filtering", // GetPostsForTag
            "select * from posts where id=?", // GetPostById
            // I hate to add the `if exists` clause here, making this an LWT, but if I don't I
            // expose myself to the possibility of a post being deleted out from under me while
            // renaming, which would leave the system in an invalid state.
            "update posts set tags=? where user_id=? and url=? if exists", // RenameTag
            "insert into tasks (id, created, task, tag, lease_expires, done) values (?, ?, ?, ?, ?, ?)", // InsertTask
            "select * from tasks where done=false and lease_expires < ? allow filtering", // ScanTasks
            "update tasks set lease_expires = ? where id = ? if lease_expires = ?", // TakeLease
            "update tasks set done=true where id=?", // FinishTasks
            "select * from users where id=?",
            "insert into following (user_id, actor_id, id, created, accepted) values (?, ?, ?, ?, ?) if not exists", // AddFollows
            "update following set accepted = true where user_id = ? and actor_id = ? if exists", // ConfirmFollow
            "insert into followers (user_id, actor_id, id, created, accepted) values (?, ?, ?, ?, ?) if not exists", // AddFollowerss
            "select count(*) from followers where user_id = ?", // CountFollowers
            "select count(*) from following where user_id = ?", // CountFollowing
            "select count(*) from following where actor_id = ?", // CountFollowingByActor
            "insert into raft_log (node_id, log_id, entry) values (?, ?, ?)",
            "truncate raft_log",
            "truncate raft_metadata",
            "select * from raft_metadata where node_id = ? and flavor = ?",
            "select * from raft_log where node_id = ? order by log_id desc limit 1",
            "delete from raft_log where node_id = ? and log_id <= ?",
            "select * from raft_metadata where node_id=? and flavor=?",
            "insert into raft_metadata (node_id, flavor, data) values (?, ?, ?)",
            "delete from raft_log where node_id = ? and log_id >= ?",
            "select * from raft_log where node_id = ? and log_id >= ? and log_id <= ?",
            "select * from raft_log where node_id = ? and log_id >= ? and log_id < ?",
            "select * from raft_log where node_id = ? and log_id >= ?",
            "select * from raft_log where node_id = ? and log_id > ? and log_id <= ?",
            "select * from raft_log where node_id = ? and log_id > ? and log_id < ?",
            "select * from raft_log where node_id = ? and log_id > ?",
            "select * from raft_log where node_id = ? and log_id <= ?",
            "select * from raft_log where node_id = ? and log_id < ?",
            "select * from raft_log where node_id = ?",
            "update users set api_keys=? where id=?",
            "insert into likes_replies_shares (user_id, posted, id, content, in_reply_to, sort, visibility) values (?, ?, ?, ?, ?, ?, ?) if not exists", // AddOutgoingLikeReplyShare
            "insert into incoming_likes_replies_shares (sort, user_id, received, ap_id, attributed_to, in_reply_to_sort, in_reply_to, visibility, content, replies, shares) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) if not exists", // AddIncomingLikeReplyShare,
            "select * from likes_replies_shares where id = ?", // OutgoingLikeReplyShare
            "select count(*) from posts where user_id = ?", // CountPosts
            "select count(*) from likes_replies_shares where user_id = ?", // CountLikesRepliesShares
            "select count(*) from users allow filtering",
            "select count(*) from posts allow filtering",
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
        let prepared_statements: [PreparedStatement; 95] = prepared_statements
            .try_into()
            .map_err(|_| BadPreparedStatementCountSnafu.build())?;

        // Ugly hack to be factored-out
        let followers_statement = "select * from followers where user_id = ?";
        let followers_statement = scylla
            .prepare(Statement::new(followers_statement).with_page_size(512))
            .await
            .context(PrepareSnafu {
                stmt: followers_statement.to_owned(),
            })?;
        let following_statement = "select * from following where user_id = ?";
        let following_statement = scylla
            .prepare(Statement::new(following_statement).with_page_size(512))
            .await
            .context(PrepareSnafu {
                stmt: following_statement.to_owned(),
            })?;
        let following_by_actor_statement = "select * from following where actor_id = ?";
        let following_by_actor_statement = scylla
            .prepare(Statement::new(following_by_actor_statement).with_page_size(512))
            .await
            .context(PrepareSnafu {
                stmt: following_by_actor_statement.to_owned(),
            })?;

        let get_all_posts_statements = [
            "select * from posts_by_posted where user_id=? order by posted desc", // GetAllPosts0
            "select * from posts_by_posted where user_id=? and posted >= ? order by posted desc",
            "select * from posts_by_posted where user_id=? and posted < ? order by posted desc",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? order by posted desc",
            "select * from posts_by_posted where user_id=? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted < ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and tags contains ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and tags contains ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted < ? and tags contains ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? and tags contains ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and tags contains ? and tags contains ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and tags contains ? and tags contains ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted < ? and tags contains ? and tags contains ? and tags contains ? order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? and tags contains ? and tags contains ? and tags contains ? order by posted desc allow filtering", // GetAllPost15

            "select * from posts_by_posted where user_id=? and unread=true order by posted desc allow filtering", // GetAllPosts16
            "select * from posts_by_posted where user_id=? and posted >= ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted < ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted < ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted < ? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and tags contains ? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and tags contains ? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted < ? and tags contains ? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering",
            "select * from posts_by_posted where user_id=? and posted >= ? and posted < ? and tags contains ? and tags contains ? and tags contains ? and unread=true order by posted desc allow filtering",
        ];
        let get_all_posts_statements = iter(get_all_posts_statements.iter())
            .then(|text| async {
                scylla
                    .prepare(Statement::new(*text).with_page_size(128))
                    .await
                    .context(PrepareSnafu {
                        stmt: text.to_owned(),
                    })
            })
            .collect::<Vec<StdResult<PreparedStatement, _>>>()
            .await
            .into_iter()
            .collect::<StdResult<Vec<PreparedStatement>, _>>()?;

        let get_all_likes_replies_shares_statement =
            // `select *` is dangerous-- should select by name, in order
            "select * from likes_replies_shares where user_id = ?";
        let get_all_likes_replies_shares_statement = scylla
            .prepare(Statement::new(get_all_likes_replies_shares_statement).with_page_size(512))
            .await
            .context(PrepareSnafu {
                stmt: get_all_likes_replies_shares_statement.to_owned(),
            })?;

        let get_likes_replies_shares_statement =
            "select sort, user_id, received, ap_id, attributed_to, in_reply_to_sort, in_reply_to, visibility, content, replies, shares from incoming_likes_replies_shares where in_reply_to=?";
        let get_likes_replies_shares_statement = scylla
            .prepare(Statement::new(get_likes_replies_shares_statement).with_page_size(512))
            .await
            .context(PrepareSnafu {
                stmt: get_likes_replies_shares_statement.to_owned(),
            })?;

        Ok(Session {
            session: scylla,
            prepared_statements: EnumMap::from_array(prepared_statements),
            followers_statement,
            following_statement,
            following_by_actor_statement,
            get_all_posts_statements,
            get_all_likes_replies_shares_statement,
            get_likes_replies_shares_statement,
            node_id,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                         The Big Tuna: storage::Backend implementation                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

// Due to a design decision I'm regretting, all our methods have to return a `storage::Error`.
use storage::Error as StorageError;

// Use these if you don't want to add any context to a failed query... should probably wrap this up
// in a macro, but I'm not sure this is the way I want to go, just yet.
impl std::convert::From<scylla::deserialize::DeserializationError> for StorageError {
    fn from(value: scylla::deserialize::DeserializationError) -> Self {
        StorageError::new(value)
    }
}

impl std::convert::From<scylla::errors::ExecutionError> for StorageError {
    fn from(value: scylla::errors::ExecutionError) -> Self {
        StorageError::new(value)
    }
}

impl std::convert::From<scylla::response::query_result::IntoRowsResultError> for StorageError {
    fn from(value: scylla::response::query_result::IntoRowsResultError) -> Self {
        StorageError::new(value)
    }
}

impl std::convert::From<scylla::response::query_result::RowsError> for StorageError {
    fn from(value: scylla::response::query_result::RowsError) -> Self {
        StorageError::new(value)
    }
}

impl std::convert::From<scylla::response::query_result::FirstRowError> for StorageError {
    fn from(value: scylla::response::query_result::FirstRowError) -> Self {
        StorageError::new(value)
    }
}

impl std::convert::From<scylla::deserialize::DeserializationError> for BackgroundTaskError {
    fn from(value: scylla::deserialize::DeserializationError) -> Self {
        BackgroundTaskError::new(value)
    }
}

impl std::convert::From<scylla::errors::ExecutionError> for BackgroundTaskError {
    fn from(value: scylla::errors::ExecutionError) -> Self {
        BackgroundTaskError::new(value)
    }
}

impl std::convert::From<scylla::response::query_result::RowsError> for BackgroundTaskError {
    fn from(value: scylla::response::query_result::RowsError) -> Self {
        BackgroundTaskError::new(value)
    }
}

impl std::convert::From<scylla::response::query_result::IntoRowsResultError>
    for BackgroundTaskError
{
    fn from(value: scylla::response::query_result::IntoRowsResultError) -> Self {
        BackgroundTaskError::new(value)
    }
}

impl std::convert::From<scylla::response::query_result::SingleRowError> for BackgroundTaskError {
    fn from(value: scylla::response::query_result::SingleRowError) -> Self {
        BackgroundTaskError::new(value)
    }
}

#[async_trait]
impl storage::Backend for Session {
    async fn add_follower(&self, user: &User, follower: &StorUrl) -> StdResult<(), StorageError> {
        add_followers(
            &self.session,
            Some(&self.prepared_statements[PreparedStatements::AddFollowers]),
            user,
            &HashSet::from([follower.clone()]),
            false,
        )
        .await
        .map_err(StorageError::new)
    }

    async fn add_following(
        &self,
        user: &User,
        follow: &StorUrl,
        id: &FollowId,
    ) -> StdResult<(), StorageError> {
        add_following(
            &self.session,
            Some(&self.prepared_statements[PreparedStatements::AddFollows]),
            user,
            &HashSet::from([(follow.clone(), *id)]),
            false,
        )
        .await
        .map_err(StorageError::new)
    }

    async fn add_outgoing_like(&self, like: &OutgoingLike) -> StdResult<(), StorageError> {
        add_outgoing_like_reply_share(
            &self.session,
            Some(&self.prepared_statements[PreparedStatements::AddOutgoingLikeReplyShare]),
            &LikeReplyShareRef::Like(like),
        )
        .await
        .map_err(StorageError::new)
    }

    async fn add_outgoing_reply(&self, reply: &OutgoingReply) -> StdResult<(), StorageError> {
        add_outgoing_like_reply_share(
            &self.session,
            Some(&self.prepared_statements[PreparedStatements::AddOutgoingLikeReplyShare]),
            &LikeReplyShareRef::Reply(reply),
        )
        .await
        .map_err(StorageError::new)
    }

    async fn add_outgoing_share(&self, share: &OutgoingShare) -> StdResult<(), StorageError> {
        add_outgoing_like_reply_share(
            &self.session,
            Some(&self.prepared_statements[PreparedStatements::AddOutgoingLikeReplyShare]),
            &LikeReplyShareRef::Share(share),
        )
        .await
        .map_err(StorageError::new)
    }

    async fn add_post(
        &self,
        user: &User,
        replace: bool,
        uri: &Url,
        id: &PostId,
        title: &str,
        dt: &DateTime<Utc>,
        notes: Option<&NonEmptyString>,
        shared: bool,
        to_read: bool,
        tags: &HashSet<Tagname>,
    ) -> StdResult<bool, StorageError> {
        // Unlike in SQL, INSERT INTO does not check the prior existence of the row by default: the
        // row is created if none existed before, and updated otherwise. This behavior can be
        // changed by using ScyllaDB’s Lightweight Transaction IF NOT EXISTS or IF EXISTS clauses.
        let day: PostDay = dt.into();
        // I hate the clone here, but the alternative, AFAIK, is unsafe code:
        let uri: StorUrl = uri.clone().into();
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
                    &id,
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

    async fn add_post_like(&self, like: &IncomingLike) -> StdResult<(), StorageError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::AddIncomingLikeReplyShare],
                &IncomingLikeReplyShareRef::Like(like),
            )
            .await?;
        // Unfortunately, this implementation gives us no way of knowing whether the statement had
        // any effect. See the comments in `delete_post()` for more on this, and how I plan to fix
        // that.
        Ok(())
    }

    async fn add_post_reply(&self, reply: &IncomingReply) -> StdResult<(), StorageError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::AddIncomingLikeReplyShare],
                &IncomingLikeReplyShareRef::Reply(reply),
            )
            .await?;
        // Unfortunately, this implementation gives us no way of knowing whether the statement had
        // any effect. See the comments in `delete_post()` for more on this, and how I plan to fix
        // that.
        Ok(())
    }

    async fn add_post_share(&self, share: &IncomingShare) -> StdResult<(), StorageError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::AddIncomingLikeReplyShare],
                &IncomingLikeReplyShareRef::Share(share),
            )
            .await?;
        // Unfortunately, this implementation gives us no way of knowing whether the statement had
        // any effect. See the comments in `delete_post()` for more on this, and how I plan to fix
        // that.
        Ok(())
    }

    async fn add_user(&self, user: &User) -> StdResult<(), StorageError> {
        if add_user(
            &self.session,
            Some(&self.prepared_statements[PreparedStatements::ClaimUsername]),
            Some(&self.prepared_statements[PreparedStatements::InsertUser]),
            user,
        )
        .await
        .map_err(StorageError::new)?
        {
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
    ) -> StdResult<bool, StorageError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::ConfirmFollow],
                (user.id(), following),
            )
            .await
            .map_err(StorageError::new)?;
        // This is unfortunate: "there is no way to know whether creation or update occurred."
        // <https://opensource.docs.scylladb.com/stable/cql/dml/update.html>
        Ok(true) // This seems like a bug.
    }

    // This implementation is bad, and I feel bad about it. The better solution would be to setup a
    // proper `counts` table and keep it up-to-date. For now, however, I just want to get this up &
    // running; if we ever get to a point where this is a performance bottleneck, well, that would
    // be a nice problem to have.
    async fn counts(&self) -> StdResult<Counts, StorageError> {
        let num_users = self.session
            .execute_unpaged(&self.prepared_statements[PreparedStatements::CountAllUsers], ())
            .await?
            .into_rows_result()?
            .rows::<(i64,)>()?
            .exactly_one()
            .unwrap(/* known good */)?
            .0 as usize;
        let num_posts = self.session
            .execute_unpaged(&self.prepared_statements[PreparedStatements::CountAllPosts], ())
            .await?
            .into_rows_result()?
            .rows::<(i64,)>()?
            .exactly_one()
            .unwrap(/* known good */)?
            .0 as usize;
        Ok(Counts {
            num_users,
            num_posts,
        })
    }

    async fn delete_post(&self, user: &User, url: &StorUrl) -> StdResult<bool, StorageError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::DeletePost],
                (user.id(), url),
            )
            .await?
            .into_rows_result()?
            // This is tedious & breaks everytime the `posts` schema changes. That said, it's nice
            // to know whether a given statement had any effect (see also `add_reply()` & `add_share()`). Once
            // the schema stabilizes a bit more, perhaps I can factor this out into one place.
            .first_row::<(
                bool,
                Option<UserId>,           // user_id
                Option<StorUrl>,          // url
                Option<PostDay>,          // day
                Option<PostId>,           // id
                Option<String>,           // title
                Option<DateTime<Utc>>,    // posted
                Option<bool>,             // public
                Option<HashSet<Tagname>>, // tags
                Option<String>,           // notes
                Option<bool>,             // unread
            )>()?
            .0
            .pipe(Ok)
    }

    async fn delete_tag(&self, user: &User, tag: &Tagname) -> StdResult<(), StorageError> {
        // OK: here's the plan. We whack-down the writes needed by first selecting only the posts
        // containing `from`. Using that we'll construct a `Batch` update. This of course leaves us
        // open to a `posts/add` for a `Post` with the tag being renamed "sneaking in" between our
        // two queries, but such is life in the NoSQL world. I can't see a way to detect that, it's
        // a corner case, and it won't leave the system in an invalid state, so I'm prepared to live
        // with it.
        let mut batch = Batch::default();
        let mut batch_values: Vec<(HashSet<Tagname>, UserId, StorUrl)> = Vec::new();

        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::GetPostsForTag],
                (user.id(), tag),
            )
            .await?
            .into_rows_result()?
            .rows::<Post>()?
            .collect::<StdResult<Vec<Post>, _>>()?
            .into_iter()
            .for_each(|mut post| {
                post.delete_tag(tag);
                batch.append_statement(
                    self.prepared_statements[PreparedStatements::RenameTag].clone(),
                );
                batch_values.push((
                    post.tags().cloned().collect::<HashSet<Tagname>>(),
                    *user.id(),
                    post.url().clone(),
                ));
            });

        self.session.batch(&batch, batch_values).await?;
        Ok(())
    }

    async fn followers_for_actor(
        &self,
        actor_id: &StorUrl,
    ) -> StdResult<BoxStream<'static, StdResult<Following, StorageError>>, StorageError> {
        make_streaming_response(
            &self.session,
            self.following_by_actor_statement.clone(),
            (actor_id.clone(),),
        )
        .await
        .map_err(StorageError::new)?
        .boxed()
        .pipe(Ok)
    }

    async fn get_all_likes_replies_and_shares(
        &self,
        user: &User,
    ) -> StdResult<BoxStream<'static, StdResult<LikeReplyShare, StorageError>>, StorageError> {
        make_streaming_response(
            &self.session,
            self.get_all_likes_replies_shares_statement.clone(),
            (*user.id(),),
        )
        .await
        .map_err(StorageError::new)?
        .boxed()
        .pipe(Ok)
    }

    async fn get_followers<'a>(
        &'a self,
        user: &User,
    ) -> StdResult<BoxStream<'a, StdResult<Follower, StorageError>>, StorageError> {
        let count = get_followers_count(
            &self.session,
            Some(&self.prepared_statements[PreparedStatements::CountFollowers]),
            user,
        )
        .await
        .map_err(StorageError::new)?;
        Ok(Box::pin(
            PagedResultsStream::new(
                &self.session,
                &self.followers_statement,
                (*user.id(),),
                count,
            )
            .await
            .map_err(StorageError::new)?,
        ))
    }

    async fn get_following<'a>(
        &'a self,
        user: &User,
    ) -> StdResult<BoxStream<'a, StdResult<Following, StorageError>>, StorageError> {
        let count = get_following_count(
            &self.session,
            Some(&self.prepared_statements[PreparedStatements::CountFollowing]),
            user,
        )
        .await
        .map_err(StorageError::new)?;
        Ok(Box::pin(
            PagedResultsStream::new(
                &self.session,
                &self.following_statement,
                (*user.id(),),
                count,
            )
            .await
            .map_err(StorageError::new)?,
        ))
    }

    async fn get_posts(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
        day: &PostDay,
        uri: &Option<StorUrl>,
    ) -> StdResult<Vec<Post>, StorageError> {
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

    async fn get_like_reply_share(
        &self,
        id: &Uuid,
    ) -> StdResult<Option<LikeReplyShare>, StorageError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::OutgoingLikeReplyShare],
                (id,),
            )
            .await?
            .into_rows_result()?
            .rows::<LikeReplyShare>()?
            .at_most_one()
            .map_err(|_| StorageError::new(AtMostOneRowSnafu.build()))?
            .transpose()?
            .pipe(Ok)
    }

    async fn get_likes_replies_shares(
        &self,
        id: &Uuid,
    ) -> StdResult<BoxStream<'static, StdResult<IncomingLikeReplyShare, StorageError>>, StorageError>
    {
        make_streaming_response(
            &self.session,
            self.get_likes_replies_shares_statement.clone(),
            (*id,),
        )
        .await
        .map_err(StorageError::new)?
        .boxed()
        .pipe(Ok)
    }

    async fn get_post(
        &self,
        userid: &UserId,
        uri: &StorUrl,
    ) -> StdResult<Option<Post>, StorageError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::GetPosts1],
                (userid, uri),
            )
            .await?
            .into_rows_result()?
            .rows::<Post>()?
            .at_most_one()
            .map_err(|_| StorageError::new(AtMostOneRowSnafu.build()))?
            .transpose()?
            .pipe(Ok)
    }

    async fn get_post_by_id(&self, id: &PostId) -> StdResult<Option<Post>, StorageError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::GetPostById],
                (id,),
            )
            .await?
            .into_rows_result()?
            .rows::<Post>()?
            .at_most_one()
            .map_err(|_| StorageError::new(AtMostOneRowSnafu.build()))?
            .transpose()?
            .pipe(Ok)
    }

    async fn get_posts_by_day(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
    ) -> StdResult<Vec<(PostDay, usize)>, StorageError> {
        // Use `execute_paged`?
        match tags {
            UpToThree::None => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::GetPostsByDay0],
                        (user.id(),),
                    )
                    .await
            }
            UpToThree::One(tag) => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::GetPostsByDay1],
                        (user.id(), tag),
                    )
                    .await
            }
            UpToThree::Two(tag0, tag1) => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::GetPostsByDay2],
                        (user.id(), tag0, tag1),
                    )
                    .await
            }
            UpToThree::Three(tag0, tag1, tag2) => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::GetPostsByDay3],
                        (user.id(), tag0, tag1, tag2),
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

    async fn get_all_posts(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
        dates: &DateRange,
        unread: bool,
    ) -> StdResult<BoxStream<'static, StdResult<Post, StorageError>>, StorageError> {
        match (tags, dates, unread) {
            (UpToThree::None, DateRange::None, false) => {
                all_posts_case!(self, 0, (*user.id(),))
            }
            (UpToThree::None, DateRange::Begins(dt), false) => {
                all_posts_case!(self, 1, (*user.id(), *dt))
            }
            (UpToThree::None, DateRange::Ends(dt), false) => {
                all_posts_case!(self, 2, (*user.id(), *dt))
            }
            (UpToThree::None, DateRange::Both(b, e), false) => {
                all_posts_case!(self, 3, (*user.id(), *b, *e))
            }
            (UpToThree::One(tag), DateRange::None, false) => {
                all_posts_case!(self, 4, (*user.id(), tag.clone()))
            }
            (UpToThree::One(tag), DateRange::Begins(dt), false) => {
                all_posts_case!(self, 5, (*user.id(), *dt, tag.clone()))
            }
            (UpToThree::One(tag), DateRange::Ends(dt), false) => {
                all_posts_case!(self, 6, (*user.id(), *dt, tag.clone()))
            }
            (UpToThree::One(tag), DateRange::Both(b, e), false) => {
                all_posts_case!(self, 7, (*user.id(), *b, *e, tag.clone()))
            }
            (UpToThree::Two(tag0, tag1), DateRange::None, false) => {
                all_posts_case!(self, 8, (*user.id(), tag0.clone(), tag1.clone()))
            }
            (UpToThree::Two(tag0, tag1), DateRange::Begins(dt), false) => {
                all_posts_case!(self, 9, (*user.id(), *dt, tag0.clone(), tag1.clone()))
            }
            (UpToThree::Two(tag0, tag1), DateRange::Ends(dt), false) => {
                all_posts_case!(self, 10, (*user.id(), *dt, tag0.clone(), tag1.clone()))
            }
            (UpToThree::Two(tag0, tag1), DateRange::Both(b, e), false) => {
                all_posts_case!(self, 11, (*user.id(), *b, *e, tag0.clone(), tag1.clone()))
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::None, false) => {
                all_posts_case!(
                    self,
                    12,
                    (*user.id(), tag0.clone(), tag1.clone(), tag2.clone())
                )
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::Begins(dt), false) => {
                all_posts_case!(
                    self,
                    13,
                    (*user.id(), *dt, tag0.clone(), tag1.clone(), tag2.clone())
                )
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::Ends(dt), false) => {
                all_posts_case!(
                    self,
                    14,
                    (*user.id(), *dt, tag0.clone(), tag1.clone(), tag2.clone())
                )
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::Both(b, e), false) => {
                all_posts_case!(
                    self,
                    15,
                    (*user.id(), *b, *e, tag0.clone(), tag1.clone(), tag2.clone())
                )
            }
            (UpToThree::None, DateRange::None, true) => {
                all_posts_case!(self, 16, (*user.id(),))
            }
            (UpToThree::None, DateRange::Begins(dt), true) => {
                all_posts_case!(self, 17, (*user.id(), *dt))
            }
            (UpToThree::None, DateRange::Ends(dt), true) => {
                all_posts_case!(self, 18, (*user.id(), *dt))
            }
            (UpToThree::None, DateRange::Both(b, e), true) => {
                all_posts_case!(self, 19, (*user.id(), *b, *e))
            }
            (UpToThree::One(tag), DateRange::None, true) => {
                all_posts_case!(self, 20, (*user.id(), tag.clone()))
            }
            (UpToThree::One(tag), DateRange::Begins(dt), true) => {
                all_posts_case!(self, 21, (*user.id(), *dt, tag.clone()))
            }
            (UpToThree::One(tag), DateRange::Ends(dt), true) => {
                all_posts_case!(self, 22, (*user.id(), *dt, tag.clone()))
            }
            (UpToThree::One(tag), DateRange::Both(b, e), true) => {
                all_posts_case!(self, 23, (*user.id(), *b, *e, tag.clone()))
            }
            (UpToThree::Two(tag0, tag1), DateRange::None, true) => {
                all_posts_case!(self, 24, (*user.id(), tag0.clone(), tag1.clone()))
            }
            (UpToThree::Two(tag0, tag1), DateRange::Begins(dt), true) => {
                all_posts_case!(self, 25, (*user.id(), *dt, tag0.clone(), tag1.clone()))
            }
            (UpToThree::Two(tag0, tag1), DateRange::Ends(dt), true) => {
                all_posts_case!(self, 26, (*user.id(), *dt, tag0.clone(), tag1.clone()))
            }
            (UpToThree::Two(tag0, tag1), DateRange::Both(b, e), true) => {
                all_posts_case!(self, 27, (*user.id(), *b, *e, tag0.clone(), tag1.clone()))
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::None, true) => {
                all_posts_case!(
                    self,
                    28,
                    (*user.id(), tag0.clone(), tag1.clone(), tag2.clone())
                )
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::Begins(dt), true) => {
                all_posts_case!(
                    self,
                    29,
                    (*user.id(), *dt, tag0.clone(), tag1.clone(), tag2.clone())
                )
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::Ends(dt), true) => {
                all_posts_case!(
                    self,
                    30,
                    (*user.id(), *dt, tag0.clone(), tag1.clone(), tag2.clone())
                )
            }
            (UpToThree::Three(tag0, tag1, tag2), DateRange::Both(b, e), true) => {
                all_posts_case!(
                    self,
                    31,
                    (*user.id(), *b, *e, tag0.clone(), tag1.clone(), tag2.clone())
                )
            }
        }
    }

    async fn get_recent_posts(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
        count: usize,
    ) -> StdResult<Vec<Post>, StorageError> {
        match tags {
            UpToThree::None => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::RecentPosts0],
                        (user.id(), count as i32),
                    )
                    .await
            }
            UpToThree::One(tag) => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::RecentPosts1],
                        (user.id(), tag, count as i32),
                    )
                    .await
            }
            UpToThree::Two(tag0, tag1) => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::RecentPosts2],
                        (user.id(), tag0, tag1, count as i32),
                    )
                    .await
            }
            UpToThree::Three(tag0, tag1, tag2) => {
                self.session
                    .execute_unpaged(
                        &self.prepared_statements[PreparedStatements::RecentPosts3],
                        (user.id(), tag0, tag1, tag2, count as i32),
                    )
                    .await
            }
        }?
        .into_rows_result()?
        .rows::<Post>()?
        .collect::<StdResult<Vec<Post>, _>>()?
        .pipe(Ok)
    }

    async fn get_tag_cloud(&self, user: &User) -> StdResult<HashMap<Tagname, usize>, StorageError> {
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

    async fn get_user_by_id(&self, id: &UserId) -> StdResult<Option<User>, StorageError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::GetUserById],
                (id,),
            )
            .await?
            .into_rows_result()?
            .rows::<User>()?
            .at_most_one()
            .map_err(|_| StorageError::new(AtMostOneRowSnafu.build()))?
            .transpose()?
            .pipe(Ok)
    }

    async fn rename_tag(
        &self,
        user: &User,
        from: &Tagname,
        to: &Tagname,
    ) -> StdResult<(), StorageError> {
        // OK: here's the plan. We whack-down the writes needed by first selecting only the posts
        // containing `from`. Using that we'll construct a `Batch` update. This of course leaves us
        // open to a `posts/add` for a `Post` with the tag being renamed "sneaking in" between our
        // two queries, but such is life in the NoSQL world. I can't see a way to detect that, it's
        // a corner case, and it won't leave the system in an invalid state, so I'm prepared to live
        // with it.
        let mut batch = Batch::default();
        let mut batch_values: Vec<(HashSet<Tagname>, UserId, StorUrl)> = Vec::new();

        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::GetPostsForTag],
                (user.id(), from),
            )
            .await?
            .into_rows_result()?
            .rows::<Post>()?
            .collect::<StdResult<Vec<Post>, _>>()?
            .into_iter()
            .for_each(|mut post| {
                post.rename_tag(from, to);
                batch.append_statement(
                    self.prepared_statements[PreparedStatements::RenameTag].clone(),
                );
                batch_values.push((
                    post.tags().cloned().collect::<HashSet<Tagname>>(),
                    *user.id(),
                    post.url().clone(),
                ));
            });

        self.session.batch(&batch, batch_values).await?;
        Ok(())
    }

    async fn update_user_api_keys(
        &self,
        user: &User,
        keys: &ApiKeys,
    ) -> StdResult<(), StorageError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::UpdateApiKeys],
                (keys, user.id()),
            )
            .await
            .map_err(|err| StorageError::new(ExecutionSnafu.into_error(err)))
            .map(|_| ())
    }

    async fn update_user_post_times(
        &self,
        user: &User,
        dt: &DateTime<Utc>,
    ) -> StdResult<(), StorageError> {
        // This brings up an interesting question: should I update the `User` instance
        // to reflect the new state in the database?
        if user.first_update().is_none() {
            self.session
                .execute_unpaged(
                    &self.prepared_statements[PreparedStatements::UpdateFirstPost],
                    (dt, user.id()),
                )
                .await
                .map_err(|err| StorageError::new(ExecutionSnafu.into_error(err)))?;
        }
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::UpdateLastPost],
                (dt, user.id()),
            )
            .await
            .map_err(|err| StorageError::new(ExecutionSnafu.into_error(err)))?;
        Ok(())
    }

    async fn user_for_name(&self, name: &str) -> StdResult<Option<User>, StorageError> {
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
                    .pipe(StorageError::new)
            })?
            .into_rows_result()
            .map_err(|err| StorageError::new(IntoRowsResultSnafu {}.into_error(err)))?
            .rows::<User>()
            .map_err(|err| StorageError::new(TypedRowsSnafu {}.into_error(err)))?
            .at_most_one()
            .map_err(|_| StorageError::new(AtMostOneRowSnafu.build()))?
            .transpose()
            .map_err(|err| StorageError::new(UserDeSnafu {}.into_error(err)))?
            .pipe(Ok)
    }

    async fn validate_schema_version(
        &self,
        expected_version: u32,
    ) -> StdResult<InstanceStateV0, StorageError> {
        let state = self
            .session
            .query_unpaged(
                "select instance_state from schema_migrations where version = ?;",
                (expected_version as i64,),
            )
            .await?
            .into_rows_result()?
            .rows::<(InstanceStateV0,)>()?
            .exactly_one()
            .map_err(|_| SchemaSnafu.build())?
            .map_err(|err| StorageError::new(SchemaDeSnafu.into_error(err)))?;
        Ok(state.0)
    }
}

#[async_trait]
impl TasksBackend for Session {
    async fn write_task(&self, tag: &Uuid, buf: &[u8]) -> StdResult<(), BackgroundTaskError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::InsertTask],
                (
                    Uuid::new_v4(),
                    Utc::now(),
                    buf,
                    tag,
                    DateTime::<Utc>::UNIX_EPOCH,
                    false,
                ),
            )
            .await?;
        Ok(())
    }
    // type tag, task id, messagepack-- should probably intro a newtype
    async fn lease_task(&self) -> StdResult<Option<(Uuid, Uuid, Vec<u8>)>, BackgroundTaskError> {
        // Start by grabbing all eligible tasks; those that are not done and that either don't have
        // leases at all, or expired leases.
        let mut tasks = self
            .session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::ScanTasks],
                (Utc::now(),),
            )
            .await?
            .into_rows_result()?
            .rows::<FlatTask>()?
            .collect::<StdResult<Vec<FlatTask>, _>>()?;
        // Next, sort 'em by creation time...
        tasks.sort_by_key(|lhs| lhs.created);
        // and walk the list, trying to get a lease. There may be other writers grabbing leases, as
        // well, so just keep trying.
        async fn take_lease(
            session: &::scylla::client::session::Session,
            statement: &PreparedStatement,
            t: &FlatTask,
        ) -> StdResult<bool, BackgroundTaskError> {
            session
                .execute_unpaged(
                    statement,
                    (Utc::now() + Duration::seconds(60), t.id, t.lease_expires),
                )
                .await?
                .into_rows_result()?
                .single_row::<(bool, DateTime<Utc>)>()?
                .0
                .pipe(Ok)
        }
        // I shot part of an afternoon trying to do this more elegantly using streams, to no avail.
        // If I try to call `filter(...).next()` on a streams iterator (on the understanding that
        // `filter` is lazy), I get a stern compiler error about the closure passed to `filter()`
        // being pinned (or something).
        let mut task: Option<FlatTask> = None;
        for t in tasks {
            if take_lease(
                &self.session,
                &self.prepared_statements[PreparedStatements::TakeLease],
                &t,
            )
            .await
            .unwrap_or(false)
            {
                task = Some(t);
                break;
            }
        }

        match task {
            // No task => no task
            None => Ok(None),
            Some(task) => Ok(Some((task.tag, task.id, task.task))),
        }
    }
    async fn close_task(&self, uuid: &Uuid) -> StdResult<(), BackgroundTaskError> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::FinishTask],
                (uuid,),
            )
            .await?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

// In the implementation of `CacheBackend`, we need to convert a variety of `Error`s into
// `StorageError<NodeId>`. The natural move is to implement `From<...> for StorageError<NodeId`.
// Unfortunately, this module generally defines neither the source nor the target for such a
// conversion, meaning that Rust's orphaned trait rules preclude us from doing this.
//
// For now, I'm just going to write a sequence of infallible free functions to handle the
// conversion. Later on, it might make sense to define a new trait (say, `IntoStorageError`) or,
// perhaps better, if I wind-up keeping `CacheBackend` around, define it in terms of an error type
// defined in the `cache` module.

fn from_vec_error(
    log_id: LogId<NodeId>,
    err: rmp_serde::encode::Error,
) -> RaftStorageError<NodeId> {
    RaftStorageError::<NodeId>::IO {
        source: StorageIOError::<NodeId>::new(
            ErrorSubject::<NodeId>::Apply(log_id),
            ErrorVerb::Write,
            &err,
        ),
    }
}

#[async_trait]
impl CacheBackend for Session {
    /// Append log entries
    #[tracing::instrument(skip(self))]
    #[allow(clippy::result_large_err)]
    async fn append(
        &self,
        entries: Vec<Entry<TypeConfig>>,
    ) -> StdResult<(), RaftStorageError<NodeId>> {
        // Make one pass; produce both a vector of `BatchStatement` and a vector of tuples
        let (batch, logs): (Vec<BatchStatement>, Vec<RaftLog>) = entries
            .into_iter()
            .map(|entry| match to_vec(&entry) {
                Ok(buf) => Ok((
                    BatchStatement::PreparedStatement(
                        self.prepared_statements[PreparedStatements::InsertIntoRaftLog].clone(),
                    ),
                    RaftLog {
                        node_id: NID(self.node_id),
                        log_id: LogIndex(entry.log_id.index),
                        entry: buf,
                    },
                )),
                Err(err) => Err(from_vec_error(entry.log_id, err)),
            })
            .collect::<StdResult<Vec<_>, _>>()?
            .into_iter()
            .unzip();

        // Submit 'em both:
        self.session
            .batch(&Batch::new_with_statements(BatchType::Logged, batch), logs)
            .await
            .map(|_| ())
            .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::Logs, ErrorVerb::Write, &err))
    }
    /// Truncate the `raft_log` & `raft_metadata` tables
    #[tracing::instrument(skip(self))]
    async fn drop_all_rows(&self) -> StdResult<(), RaftStorageError<NodeId>> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::TruncateRaftLog],
                (),
            )
            .await
            .map_err(|err| {
                to_storage_io_err(ErrorSubject::<NodeId>::Logs, ErrorVerb::Delete, &err)
            })?;

        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::TruncateRaftMeta],
                (),
            )
            .await
            .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::None, ErrorVerb::Delete, &err))
            .map(|_| ())
    }
    /// Returns the last deleted log id and the last log id
    #[tracing::instrument(skip(self))]
    async fn get_log_state(&self) -> StdResult<LogState<TypeConfig>, RaftStorageError<NodeId>> {
        async fn get_log_state1(
            session: &InnerSession,
            prepared_statements: &EnumMap<PreparedStatements, PreparedStatement>,
            node_id: NodeId,
        ) -> Result<LogState<TypeConfig>> {
            let last_purged_log_id = session
                .execute_unpaged(
                    &prepared_statements[PreparedStatements::SelectRaftMeta1],
                    (Into::<BigInt>::into(node_id), Flavor::LastPurged),
                )
                .await
                .context(ExecutionSnafu)?
                .into_rows_result()
                .context(IntoRowsResultSnafu)?
                .rows::<RaftMetadata>()
                .context(TypedRowsSnafu)?
                .collect::<StdResult<Vec<RaftMetadata>, _>>()
                .context(RaftMetaDeSnafu)?
                .into_iter()
                .at_most_one()
                .map_err(|_| AtMostOneRowSnafu.build())?
                .map(|meta| from_slice::<LogId<NodeId>>(&meta.data).context(EntryDeSnafu))
                .transpose()?;

            let last_log_id = session
                .execute_unpaged(
                    &prepared_statements[PreparedStatements::SelectRaftLog1],
                    (Into::<BigInt>::into(node_id),),
                )
                .await
                .context(ExecutionSnafu)?
                .into_rows_result()
                .context(IntoRowsResultSnafu)?
                .rows::<RaftLog>()
                .context(TypedRowsSnafu)?
                .collect::<StdResult<Vec<RaftLog>, _>>()
                .context(RaftLogDeSnafu)?
                .into_iter()
                .at_most_one()
                .map_err(|_| AtMostOneRowSnafu.build())?
                .map(|log| {
                    from_slice::<Entry<TypeConfig>>(&log.entry)
                        .map(|entry| *entry.get_log_id())
                        .context(EntryDeSnafu)
                })
                .transpose()?
                .or(last_purged_log_id);

            debug!("get_log_state(): => {last_purged_log_id:?}|{last_log_id:?}");

            Ok(LogState {
                last_purged_log_id,
                last_log_id,
            })
        }

        get_log_state1(&self.session, &self.prepared_statements, self.node_id)
            .await
            .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::Logs, ErrorVerb::Read, &err))
    }
    /// Purge logs upto log_id, inclusive
    #[tracing::instrument(skip(self))]
    async fn purge(&self, log_id: LogId<NodeId>) -> StdResult<(), RaftStorageError<NodeId>> {
        async fn purge1(
            session: &InnerSession,
            prepared_statements: &EnumMap<PreparedStatements, PreparedStatement>,
            node_id: NodeId,
            log_id: LogId<NodeId>,
        ) -> Result<()> {
            session
                .execute_unpaged(
                    &prepared_statements[PreparedStatements::DeleteRaftLog1],
                    (
                        Into::<BigInt>::into(node_id),
                        Into::<BigInt>::into(log_id.index),
                    ),
                )
                .await
                .context(ExecutionSnafu)?;
            // This is kind of lame, since if the next write fails, we can't undo the delete-- I
            // guess we could keep it in memory?
            session
                .query_unpaged(
                    "insert into raft_metadata (node_id, flavor, data) values (?, ?, ?)",
                    RaftMetadata {
                        node_id: NID(node_id),
                        flavor: Flavor::LastPurged,
                        data: to_vec(&log_id).context(LogIdSerSnafu)?,
                    },
                )
                .await
                .context(ExecutionSnafu)
                .map(|_| ())
        }

        purge1(
            &self.session,
            &self.prepared_statements,
            self.node_id,
            log_id,
        )
        .await
        .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::Logs, ErrorVerb::Delete, &err))
    }
    /// Return the last saved vote by [Self::save_vote] (if any)
    #[tracing::instrument(skip(self))]
    async fn read_vote(&self) -> StdResult<Option<Vote<NodeId>>, RaftStorageError<NodeId>> {
        async fn read_vote1(
            session: &InnerSession,
            prepared_statements: &EnumMap<PreparedStatements, PreparedStatement>,
            node_id: NodeId,
        ) -> Result<Option<Vote<NodeId>>> {
            session
                .execute_unpaged(
                    &prepared_statements[PreparedStatements::SelectRaftMeta2],
                    (Into::<BigInt>::into(node_id), Flavor::Vote),
                )
                .await
                .context(ExecutionSnafu)?
                .into_rows_result()
                .context(IntoRowsResultSnafu)?
                .rows::<RaftMetadata>()
                .context(TypedRowsSnafu)?
                .collect::<StdResult<Vec<RaftMetadata>, _>>()
                .context(RaftMetaDeSnafu)?
                .into_iter()
                .at_most_one()
                .map_err(|_| AtMostOneRowSnafu.build())?
                .map(|meta| from_slice::<Vote<NodeId>>(&meta.data).context(VoteDeSnafu))
                .transpose()?
                .pipe(Ok)
        }

        read_vote1(&self.session, &self.prepared_statements, self.node_id)
            .await
            .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::Vote, ErrorVerb::Read, &err))
    }
    /// Save vote to storage
    #[tracing::instrument(skip(self))]
    async fn save_vote(&self, vote: &Vote<NodeId>) -> StdResult<(), RaftStorageError<NodeId>> {
        async fn save_vote1(
            session: &InnerSession,
            prepared_statements: &EnumMap<PreparedStatements, PreparedStatement>,
            node_id: NodeId,
            vote: &Vote<NodeId>,
        ) -> Result<()> {
            session
                .execute_unpaged(
                    &prepared_statements[PreparedStatements::InsertRaftMeta1],
                    RaftMetadata {
                        node_id: NID(node_id),
                        flavor: Flavor::Vote,
                        data: to_vec(&vote).context(VoteSerSnafu)?,
                    },
                )
                .await
                .context(ExecutionSnafu)
                .map(|_| ())
        }

        save_vote1(&self.session, &self.prepared_statements, self.node_id, vote)
            .await
            .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::Vote, ErrorVerb::Write, &err))
    }
    /// Truncate logs since log_id, inclusive
    #[tracing::instrument(skip(self))]
    async fn truncate(&self, log_id: LogId<NodeId>) -> StdResult<(), RaftStorageError<NodeId>> {
        self.session
            .execute_unpaged(
                &self.prepared_statements[PreparedStatements::DeleteRaftLog2],
                (
                    Into::<BigInt>::into(self.node_id),
                    Into::<BigInt>::into(log_id.index),
                ),
            )
            .await
            .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::Logs, ErrorVerb::Delete, &err))
            .map(|_| ())
    }
    /// Get a series of log entries from storage.
    #[tracing::instrument(skip(self))]
    async fn try_get_log_entries(
        &self,
        lower_bound: Bound<&u64>,
        upper_bound: Bound<&u64>,
    ) -> StdResult<Vec<Entry<TypeConfig>>, RaftStorageError<NodeId>> {
        async fn try_get_log_entries1(
            session: &InnerSession,
            prepared_statements: &EnumMap<PreparedStatements, PreparedStatement>,
            node_id: NodeId,
            lower_bound: Bound<&u64>,
            upper_bound: Bound<&u64>,
        ) -> Result<Vec<Entry<TypeConfig>>> {
            let node_id = Into::<BigInt>::into(node_id);
            // This seems prolix... see if I can tighten this up when I move to prepared statements
            match (lower_bound, upper_bound) {
                (Bound::Included(i), Bound::Included(j)) => {
                    let i = Into::<BigInt>::into(*i);
                    let j = Into::<BigInt>::into(*j);
                    session
                        .execute_unpaged(
                            &prepared_statements[PreparedStatements::GetRaftLogEntries1],
                            (node_id, i, j),
                        )
                        .await
                }
                (Bound::Included(i), Bound::Excluded(j)) => {
                    let i = Into::<BigInt>::into(*i);
                    let j = Into::<BigInt>::into(*j);
                    session
                        .execute_unpaged(
                            &prepared_statements[PreparedStatements::GetRaftLogEntries2],
                            (node_id, i, j),
                        )
                        .await
                }
                (Bound::Included(i), Bound::Unbounded) => {
                    let i = Into::<BigInt>::into(*i);
                    session
                        .execute_unpaged(
                            &prepared_statements[PreparedStatements::GetRaftLogEntries3],
                            (node_id, i),
                        )
                        .await
                }
                (Bound::Excluded(i), Bound::Included(j)) => {
                    let i = Into::<BigInt>::into(*i);
                    let j = Into::<BigInt>::into(*j);
                    session
                        .execute_unpaged(
                            &prepared_statements[PreparedStatements::GetRaftLogEntries4],
                            (node_id, i, j),
                        )
                        .await
                }
                (Bound::Excluded(i), Bound::Excluded(j)) => {
                    let i = Into::<BigInt>::into(*i);
                    let j = Into::<BigInt>::into(*j);
                    session
                        .execute_unpaged(
                            &prepared_statements[PreparedStatements::GetRaftLogEntries5],
                            (node_id, i, j),
                        )
                        .await
                }
                (Bound::Excluded(i), Bound::Unbounded) => {
                    let i = Into::<BigInt>::into(*i);
                    session
                        .execute_unpaged(
                            &prepared_statements[PreparedStatements::GetRaftLogEntries6],
                            (node_id, i),
                        )
                        .await
                }
                (Bound::Unbounded, Bound::Included(j)) => {
                    let j = Into::<BigInt>::into(*j);
                    session
                        .execute_unpaged(
                            &prepared_statements[PreparedStatements::GetRaftLogEntries7],
                            (node_id, j),
                        )
                        .await
                }
                (Bound::Unbounded, Bound::Excluded(j)) => {
                    let j = Into::<BigInt>::into(*j);
                    session
                        .execute_unpaged(
                            &prepared_statements[PreparedStatements::GetRaftLogEntries8],
                            (node_id, j),
                        )
                        .await
                }
                (Bound::Unbounded, Bound::Unbounded) => {
                    session
                        .execute_unpaged(
                            &prepared_statements[PreparedStatements::GetRaftLogEntries9],
                            (node_id,),
                        )
                        .await
                }
            }
            .context(ExecutionSnafu)?
            .into_rows_result()
            .context(IntoRowsResultSnafu)?
            .rows::<RaftLog>()
            .context(TypedRowsSnafu)?
            // At this point, we have an iterator over `Result<RaftLog, DeserializationError>`, and
            // I need to fallibly deserialize the `entry` field to an `Entry<TypeConfig>`. I guess
            // I'd prefer to do one pass, at the cost of a more complex lambda:
            .map(|res| {
                res.context(RaftLogDeSnafu).and_then(|log| {
                    from_slice::<Entry<TypeConfig>>(log.entry.as_slice()).context(EntryDeSnafu)
                })
            })
            .collect::<Result<Vec<Entry<TypeConfig>>>>()
        }

        try_get_log_entries1(
            &self.session,
            &self.prepared_statements,
            self.node_id,
            lower_bound,
            upper_bound,
        )
        .await
        .map_err(|err| to_storage_io_err(ErrorSubject::<NodeId>::Logs, ErrorVerb::Read, &err))
    }
}
