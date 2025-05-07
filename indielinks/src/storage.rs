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

//! # storage
//!
//! Abstractions for the indielinks storage layer.

use crate::{
    entities::{
        FollowId, Post, PostDay, PostId, PostUri, Reply, Share, Tagname, User, UserId, UserUrl,
        Username,
    },
    util::UpToThree,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use snafu::{prelude::*, Backtrace};

use std::collections::{HashMap, HashSet};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
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

#[async_trait]
pub trait Backend {
    /// Add a follower to a user's collection
    async fn add_follower(&self, user: &User, follower: &url::Url) -> Result<(), Error>;
    /// Add a follow to a user's collection
    async fn add_following(
        &self,
        user: &User,
        following: &UserUrl,
        id: &FollowId,
    ) -> Result<(), Error>;
    /// Add a Post for `user`; return true if a new post was actually created, false if the post
    /// already existed and `replace` was set to false.
    #[allow(clippy::too_many_arguments)]
    async fn add_post(
        &self,
        user: &User,
        replace: bool,
        uri: &PostUri,
        id: &PostId,
        title: &str,
        dt: &DateTime<Utc>,
        notes: &Option<String>,
        shared: bool,
        to_read: bool,
        tags: &HashSet<Tagname>,
    ) -> Result<bool, Error>;
    /// Add a Reply for an existing post-- the backend is responsibile for verifying that [User]
    /// `user` actually made a post with [PostId] `postid`! This is to preserve the possibility for
    /// the implementation to optimize that implementation (by doing it a single request with a
    /// "WHERE" clause, for instance)
    async fn add_reply(&self, user: &User, url: &PostUri, reply: &Reply) -> Result<(), Error>;
    /// Add a Share for an existing post-- the backend is responsibile for verifying that [User]
    /// `user` actually made a post with [PostId] `postid`! This is to preserve the possibility for
    /// the implementation to optimize that implementation (by doing it a single request with a
    /// "WHERE" clause, for instance)
    async fn add_share(&self, user: &User, url: &PostUri, share: &Share) -> Result<(), Error>;
    /// Add a new user
    async fn add_user(&self, user: &User) -> Result<(), Error>;
    /// Confirm a follow for a user
    async fn confirm_following(&self, user: &User, following: &UserUrl) -> Result<(), Error>;
    /// Remove a post-- return true if a [Post] was actually removed, false else
    async fn delete_post(&self, user: &User, url: &PostUri) -> Result<bool, Error>;
    /// Delete a tag for a user; since we've denormalized the tags (i.e. we store them along with the
    /// posts to avoid a join), this is likely to be an inefficient operation. I might want to consider
    /// special logic here for rate-limiting.
    async fn delete_tag(&self, user: &User, tag: &Tagname) -> Result<(), Error>;
    async fn get_post_by_id(&self, id: &PostId) -> Result<Option<Post>, Error>;
    /// Retrieve full posts with various filtering options
    async fn get_posts(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
        day: &PostDay,
        uri: &Option<PostUri>,
    ) -> Result<Vec<Post>, Error>;
    async fn get_posts_by_day(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
    ) -> Result<Vec<(PostDay, usize)>, Error>;
    /// Retrieve all of a user's posts, optionally filtering by time & tags. The implementation shall
    /// return the tags in reverse chronological order.
    async fn get_all_posts(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
        dates: &DateRange,
    ) -> Result<Vec<Post>, Error>;
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
    /// Update the `first_update` and `last_update` for the given user
    async fn update_user_post_times(&self, user: &User, dt: &DateTime<Utc>) -> Result<(), Error>;
    /// Retrieve a [User] instance given a textual username. None means there is no user by that
    /// name.
    async fn user_for_name(&self, name: &str) -> Result<Option<User>, Error>;
}
