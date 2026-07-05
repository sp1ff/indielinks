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

//! # storage: Abstractions for the indielinks storage layer
//!
//! ## Introduction
//!
//! This is one of the oldest pieces of [indielinks]: the "storage" abstraction. The primary product
//! of this module is the [Backend] trait: an object-safe trait meant to be implemented by multiple
//! backends: the native Cassandra/CQL API for ScyllaDB and the DynamoDB/ScyllaDB Alternator APIs at
//! least, if not more for testing purposes.
//!
//! [indielinks]: ../indielinskd/index.html

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use snafu::{prelude::*, Backtrace, IntoError};
use url::Url;
use uuid::Uuid;

use indielinks_shared::{
    entities::{Post, PostDay, PostId, StorUrl, Tagname, UserId, Username},
    instance_state::InstanceStateV0,
    nonempty_string::NonEmptyString,
};

use crate::{
    entities::{
        ApiKeys, FollowId, Follower, Following, IncomingLike, IncomingReply, IncomingShare,
        LikeReplyShare, OutgoingLike, OutgoingReply, OutgoingShare, User,
    },
    util::UpToThree,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("While yielding the next row in a Stream over a paged result, {source}"))]
    NextRow {
        #[snafu(source(from(scylla::errors::NextRowError, Box::new)))]
        source: Box<scylla::errors::NextRowError>,
        backtrace: Backtrace,
    },
    #[snafu(display("Schema validation error"))]
    Schema { backtrace: Backtrace },
    #[snafu(display("{source}"))]
    Storage {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        backtrace: Backtrace,
    },
    #[snafu(display("Username {username} is already claimed"))]
    UsernameClaimed {
        username: Username,
        backtrace: Backtrace,
    },
}

impl Error {
    pub fn new(err: impl std::error::Error + Send + Sync + 'static) -> Error {
        Error::Storage {
            source: Box::new(err),
            backtrace: Backtrace::capture(),
        }
    }
}

// This impl allows us to use the `TryStreamExt::err_into()` combinator in the scylla module for
// building `Stream`s on the paginated results of queries.
impl From<scylla::errors::NextRowError> for Error {
    fn from(value: scylla::errors::NextRowError) -> Self {
        NextRowSnafu.into_error(value)
    }
}

// Not sure why I didn't use [Range] here (perhaps at the time I was unaware of its existence).
pub enum DateRange {
    None,
    Begins(DateTime<Utc>),
    Ends(DateTime<Utc>),
    Both(DateTime<Utc>, DateTime<Utc>),
}

impl DateRange {
    pub fn new(from: Option<DateTime<Utc>>, to: Option<DateTime<Utc>>) -> DateRange {
        match (from, to) {
            (None, None) => DateRange::None,
            (None, Some(d)) => DateRange::Ends(d),
            (Some(d), None) => DateRange::Begins(d),
            (Some(b), Some(e)) => DateRange::Both(b, e),
        }
    }
}

/// Assorted counts from the backend data store
// This will hopefully grow in the future, and right now the implementations are detestable. The
// right way to address this is to setup dedicated "counts" tables that are kept up-to-date by
// application logic. For now, I'll just live with full table scans. In particular, tags can't be
// counted, this way (at least, not without some fairly odios application logic).
#[derive(Clone, Debug)]
pub struct Counts {
    pub num_users: usize,
    pub num_posts: usize,
}

/// Storage backend
///
/// This (dyn compatible) trait must be implemented by each storage backend (ScyllaDB, DynamoDB &c).
// Possibly poor design choice, here: having these methods return *this* module's `Error` type,
// rather than allowing implementors to specify their own through an associated type. The trait (at
// the time of this writing) has 33 methods... might be feasible to change? Maybe give it to an LLM?
#[async_trait]
pub trait Backend {
    /// Add a follower to a user's collection
    async fn add_follower(&self, user: &User, follower: &StorUrl) -> Result<(), Error>;
    /// Add a follow to a user's collection
    async fn add_following(
        &self,
        user: &User,
        following: &StorUrl,
        id: &FollowId,
    ) -> Result<(), Error>;
    /// Add a like that originated on this intance
    async fn add_outgoing_like(&self, like: &OutgoingLike) -> Result<(), Error>;
    /// Add a reply that originated on this intance
    async fn add_outgoing_reply(&self, reply: &OutgoingReply) -> Result<(), Error>;
    /// Add a share that originated on this instance
    async fn add_outgoing_share(&self, share: &OutgoingShare) -> Result<(), Error>;
    /// Add a like for a post that originated on this instance
    async fn add_post_like(&self, reply: &IncomingLike) -> Result<(), Error>;
    /// Add a Post for `user`; return true if a new post was actually created, false if the post
    /// already existed and `replace` was set to false.
    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<bool, Error>;
    /// Add a Reply for an existing post
    // Rename this method?
    async fn add_post_reply(&self, reply: &IncomingReply) -> Result<(), Error>;
    /// Add a Share for an existing post
    async fn add_post_share(&self, share: &IncomingShare) -> Result<(), Error>;
    /// Add a new user
    async fn add_user(&self, user: &User) -> Result<(), Error>;
    /// Confirm a follow for a user
    async fn confirm_following(&self, user: &User, following: &StorUrl) -> Result<bool, Error>;
    /// Retrieve counts of assorted entities
    async fn counts(&self) -> Result<Counts, Error>;
    /// Remove a post-- return true if a [Post] was actually removed, false else
    async fn delete_post(&self, user: &User, url: &StorUrl) -> Result<bool, Error>;
    /// Delete a tag for a user; since we've denormalized the tags (i.e. we store them along with the
    /// posts to avoid a join), this is likely to be an inefficient operation. I might want to consider
    /// special logic here for rate-limiting.
    async fn delete_tag(&self, user: &User, tag: &Tagname) -> Result<(), Error>;
    /// Return a stream of all [User]s following a given AP actor
    async fn followers_for_actor(
        &self,
        actor_id: &StorUrl,
    ) -> Result<BoxStream<'static, Result<Following, Error>>, Error>;
    /// Return a stream yielding all of a [User]'s likes, replies & shares
    async fn get_all_likes_replies_and_shares(
        &self,
        user: &User,
    ) -> Result<BoxStream<'static, Result<LikeReplyShare, Error>>, Error>;
    // I considered using `TryStream`, but not sure I see the advantage, yet. Also, there's no handy
    // type alias `BoxTryStream` (tho of course I could define it)
    async fn get_followers<'a>(
        &'a self,
        user: &User,
    ) -> Result<BoxStream<'a, Result<Follower, Error>>, Error>;
    async fn get_following<'a>(
        &'a self,
        user: &User,
    ) -> Result<BoxStream<'a, Result<Following, Error>>, Error>;
    async fn get_like_reply_share(&self, id: &Uuid) -> Result<Option<LikeReplyShare>, Error>;
    /// Retrieve all incoming likes, replies, and/or shares for a given post
    async fn get_likes_replies_shares(
        &self,
        id: &PostId,
    ) -> Result<BoxStream<'static, Result<LikeReplyShare, Error>>, Error>;
    async fn get_post(&self, userid: &UserId, uri: &StorUrl) -> Result<Option<Post>, Error>;
    async fn get_post_by_id(&self, id: &PostId) -> Result<Option<Post>, Error>;
    /// Retrieve full posts with various filtering options
    async fn get_posts(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
        day: &PostDay,
        uri: &Option<StorUrl>,
    ) -> Result<Vec<Post>, Error>;
    async fn get_posts_by_day(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
    ) -> Result<Vec<(PostDay, usize)>, Error>;
    /// Retrieve all of a user's posts, optionally filtering by time & tags. The implementation shall
    /// return the [Post]s in reverse chronological order.
    async fn get_all_posts(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
        dates: &DateRange,
        unread: bool,
    ) -> Result<BoxStream<'static, Result<Post, Error>>, Error>;
    /// Retrieve recent posts
    async fn get_recent_posts(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
        count: usize,
    ) -> Result<Vec<Post>, Error>;
    /// Retrieve the user's tag cloud
    async fn get_tag_cloud(&self, user: &User) -> Result<HashMap<Tagname, usize>, Error>;
    async fn get_user_by_id(&self, id: &UserId) -> Result<Option<User>, Error>;
    /// Rename a tag for a user; since we've denormalized the tags (i.e. we store them along with the
    /// posts to avoid a join), this is likely to be an inefficient operation. I might want to consider
    /// special logic here for rate-limiting.
    async fn rename_tag(&self, user: &User, from: &Tagname, to: &Tagname) -> Result<(), Error>;
    /// Update the `api_keys` fields for the given user
    async fn update_user_api_keys(&self, user: &User, keys: &ApiKeys) -> Result<(), Error>;
    /// Update the `first_update` and `last_update` for the given user
    async fn update_user_post_times(&self, user: &User, dt: &DateTime<Utc>) -> Result<(), Error>;
    /// Retrieve a [User] instance given a textual username. None means there is no user by that
    /// name.
    async fn user_for_name(&self, name: &str) -> Result<Option<User>, Error>;
    async fn validate_schema_version(&self, schema_version: u32) -> Result<InstanceStateV0, Error>;
}
