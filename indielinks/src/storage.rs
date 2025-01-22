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

use crate::entities::{Post, PostDay, PostUri, TagId, Tagname, User, UserId};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct Error {
    source: Box<dyn std::error::Error + Send + Sync + 'static>,
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
        tags: &HashSet<TagId>,
    ) -> Result<bool, Error>;
    /// Remove posts in batch
    ///
    /// It may seem appealing to design this as taking an iterator yielding [PostId]s, but just as
    /// C++ can't have template virtual methods, Rust can't handle generic methods in (object-safe)
    /// traits. However, it seems reasonable to require callers to prove that there are no
    /// duplicates among the collection (trying to delete a given post twice would lead to errors);
    /// the closest I know how to come to that in Rust is to demand a HashSet. Unfortunately, I
    /// can't hash [Post]s easily because they themselves contain a [HashSet] which ironically can't
    /// be hashed.
    ///
    /// [PostId]: crate::entities::PostId
    async fn delete_posts(&self, posts: &[Post]) -> Result<(), Error>;
    /// Retrieve full posts by day
    async fn get_posts_by_day(&self, userid: &UserId, day: &PostDay) -> Result<Vec<Post>, Error>;
    /// Retrieve a users's posts given a URI.
    async fn get_posts_by_uri(&self, userid: &UserId, uri: &PostUri) -> Result<Vec<Post>, Error>;
    /// Retrieve the user's tag cloud
    async fn get_tag_cloud(&self, user: &User) -> Result<HashMap<Tagname, usize>, Error>;
    /// Given a userid & URI, find the tags used on all posts for that user & that URI. Return a
    /// mapping of [TagId] to use count.
    async fn get_tag_cloud_for_uri(
        &self,
        userid: &UserId,
        uri: &PostUri,
    ) -> Result<HashMap<TagId, usize>, Error>;
    /// Update the user's tag cloud. Internally, create the new tags if they're not already there,
    /// with a count of one. If they're there, increment their counts. Return their TagIds.
    ///
    /// Ideally, this function would take an iterator over [String] along with a proof that the
    /// yielded [String]s are unique. The closest approximation of which I'm aware in Rust is to
    /// just take a [HashSet]. Returning a [HashSet] expresses this method's commitment that the
    /// resulting collection of [TagId]s is also unique.
    ///
    /// This is the one place (so far) I couldn't wriggle out of performing a join in application
    /// logic. We need to ensure that each tag exists in the database (creating them if need be) &
    /// increment their counts,
    async fn update_tag_cloud_on_add(
        &self,
        userid: &UserId,
        tags: &HashSet<Tagname>,
    ) -> Result<HashSet<TagId>, Error>;
    /// Update the user's tag cloud. Decrease the use count of each tag by the given amount.
    async fn update_tag_cloud_on_delete(
        &self,
        userid: &UserId,
        counts: &HashMap<TagId, usize>,
    ) -> Result<(), Error>;
    /// Update the `first_update` and `last_update` for the given user
    async fn update_user_post_times(&self, userid: &User, dt: &DateTime<Utc>) -> Result<(), Error>;
    /// Retrieve a [User] instance given a textual username. None means there is no user by that
    /// name.
    async fn user_for_name(&self, name: &str) -> Result<Option<User>, Error>;
}
