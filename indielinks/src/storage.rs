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
    entities::{Post, PostDay, PostUri, Tagname, User},
    util::UpToThree,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use snafu::Backtrace;

use std::collections::{HashMap, HashSet};

#[derive(Debug)]
#[allow(dead_code)] // `backtrace` is never read (?)
pub struct Error {
    source: Box<dyn std::error::Error + Send + Sync + 'static>,
    backtrace: Backtrace,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.source)
    }
}

impl std::error::Error for Error {}

impl Error {
    pub fn new(err: impl std::error::Error + Send + Sync + 'static) -> Error {
        Error {
            source: Box::new(err),
            backtrace: Backtrace::capture(),
        }
    }
}

#[async_trait]
pub trait Backend {
    /// Add a Post for `user`; return true if a new post was actually created, false if the post
    /// already existed and `replace` was set to false.
    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<bool, Error>;
    /// Remove a post-- return true if a [Post] was actually removed, false else
    async fn delete_post(&self, user: &User, url: &PostUri) -> Result<bool, Error>;
    async fn get_posts_by_day(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
    ) -> Result<Vec<(PostDay, usize)>, Error>;
    /// Retrieve full posts with various filtering options
    async fn get_posts(
        &self,
        user: &User,
        tags: &UpToThree<Tagname>,
        day: &PostDay,
        uri: &Option<PostUri>,
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
    /// Update the `first_update` and `last_update` for the given user
    async fn update_user_post_times(&self, user: &User, dt: &DateTime<Utc>) -> Result<(), Error>;
    /// Retrieve a [User] instance given a textual username. None means there is no user by that
    /// name.
    async fn user_for_name(&self, name: &str) -> Result<Option<User>, Error>;
}
