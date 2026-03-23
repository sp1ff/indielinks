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

use std::{collections::HashMap, num::NonZero};

use chrono::{DateTime, NaiveDate, Utc};
use nonempty_collections::NEVec;
use secrecy::{CloneableSecret, SecretBox, SerializableSecret, zeroize::Zeroize};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    entities::{Post, PostDay, StorUrl, Tagname, UserEmail, Username},
    nonempty_string::NonEmptyString,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                          Requests & Response for the del.icio.us API                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateRsp {
    pub update_time: DateTime<Utc>,
}

/// A deserializable struct representing the query parameters for `/posts/add`
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostAddReq {
    pub url: Url,
    #[serde(rename = "description")]
    pub title: NonEmptyString,
    #[serde(rename = "extended")]
    pub notes: Option<NonEmptyString>,
    pub tags: Option<String>,
    pub dt: Option<DateTime<Utc>>,
    pub replace: Option<bool>,
    pub shared: Option<bool>,
    #[serde(rename = "toread")]
    pub to_read: Option<bool>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostsDeleteReq {
    pub url: StorUrl,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostsGetReq {
    pub dt: Option<NaiveDate>,
    #[serde(rename = "url")]
    pub uri: Option<StorUrl>,
    #[serde(default, rename = "tag")]
    pub tag: Option<String>,
    #[serde(rename = "meta")]
    _meta: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostsGetRsp {
    pub date: DateTime<Utc>,
    pub user: Username,
    pub posts: Vec<Post>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostsRecentReq {
    pub tag: Option<String>,
    pub count: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostsRecentRsp {
    pub date: DateTime<Utc>,
    pub user: Username,
    pub posts: Vec<Post>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostsDatesReq {
    pub tag: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostsDate {
    pub count: usize,
    pub date: PostDay,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostsDatesRsp {
    pub user: Username,
    pub tag: String,
    pub dates: Vec<PostsDate>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostsAllReq {
    pub tag: Option<String>,
    pub start: Option<usize>,
    pub results: Option<usize>,
    pub fromdt: Option<DateTime<Utc>>,
    pub todt: Option<DateTime<Utc>>,
    #[serde(rename = "meta")]
    pub _meta: Option<bool>,
    pub unread: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostsAllRsp {
    pub user: Username,
    pub tag: String,
    pub posts: Vec<Post>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(transparent, deny_unknown_fields)]
pub struct TagsGetRsp {
    pub map: HashMap<Tagname, usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TagsRenameReq {
    pub old: Tagname,
    pub new: Tagname,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TagsDeleteReq {
    pub tag: Tagname,
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                             Requests & Response for the users API                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

pub const REFRESH_COOKIE: &str = "indielinks-refresh";

pub const REFRESH_CSRF_COOKIE: &str = "indielinks-refresh-csrf";

pub const REFRESH_CSRF_HEADER_NAME: &str = "X-Indielinks-Refresh-Csrf";

pub const REFRESH_CSRF_HEADER_NAME_LC: &str = "x-indielinks-refresh-csrf";

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Password(pub String);

impl Zeroize for Password {
    fn zeroize(&mut self) {
        self.0.zeroize()
    }
}

impl CloneableSecret for Password {}

impl SerializableSecret for Password {}

pub type SecretPassword = SecretBox<Password>;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SignupReq {
    pub username: Username,
    pub password: SecretPassword,
    pub email: UserEmail,
    pub discoverable: Option<bool>,
    #[serde(rename = "display-name")]
    pub display_name: Option<String>,
    pub summary: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SignupRsp {
    pub greeting: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LoginReq {
    // I first thought to make this a `Username`, but in that case, should the caller fat-finger the
    // username to something illegal, axum will fail to deserialize the request, producing the
    // unhelpful status code 422 Unprocessable Entity
    pub username: Username,
    pub password: SecretPassword,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LoginRsp {
    pub token: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FollowReq {
    pub id: Url,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MintKeyReq {
    pub expiry: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MintKeyRsp {
    pub key_text: String,
}

/// Opaque type representing a pagination token
///
/// Callers cannot create instances of this type; they are returned in response to timeline requests
/// and represent a particular post in a user's timeline. They are intended to be specified as as
/// "before" or "since" parameters to subsequent timeline requests.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TimelineToken(String);

impl AsRef<[u8]> for TimelineToken {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl TimelineToken {
    // Super-lame: I want this type to be visible to everyone, but only constructable from the API's
    // implementation in a higher-level module, on the back-end only.
    #[cfg(feature = "__internal")]
    #[doc(hidden)]
    pub fn new_internal(token: String) -> TimelineToken {
        Self(token)
    }
}

/// A timeline request body
// External tagging (the default) should work, here. `Since { since: 123, max_posts: None }` should
// serialize, in JSON, to { "Since": { "before": 123 } }, which I think is what we want.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub enum TimelineReq {
    /// Request the initial chunk of a user's timeline
    Initial {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_posts: Option<NonZero<usize>>,
    },
    /// Request a page of posts since a particular point in a user's timeline
    // "Return to me up to n posts since the most recent one I've seen". This will return up to
    // `max_posts` feed items whose identifiers/tags are strictly later than `since`
    Since {
        since: TimelineToken,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_posts: Option<NonZero<usize>>,
    },
    /// Request a page of posts prior to a particular point in a user's timeline
    Before {
        before: TimelineToken,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_posts: Option<NonZero<usize>>,
    },
}

/// A timeline post.
///
/// This defines a type encapsulating a "post"; not in the del.icio.us sense, not in the ActivityPub
/// sense, but in the sense of the indielinks UX.
// This is early code; I just knocked this together for front end prototyping purposes.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FeedPost {
    pub id: Url,
    pub actor: Url,
    pub in_reply_to: Option<Url>,
    pub published: DateTime<Utc>,
    // I'm not sure how I want to represent this; when & how should this be sanitized?
    pub content: String,
}

// For each sort of timeline request, the response may contain no posts, even on
// success (there could simply *be* no posts of the requested sort).

/// An initial page of a non-empty timeline
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TimelineInitialPage {
    pub posts: NEVec<FeedPost>,
    pub since: TimelineToken,
    pub before: TimelineToken,
}

/// Response payload for an initial timeline request
pub type TimelineInitialRsp = Option<TimelineInitialPage>;

/// A page of more-recent posts from a timeline
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TimelineSincePage {
    pub posts: NEVec<FeedPost>,
    pub since: TimelineToken,
}

/// Response payload for a timeline request for posts more recent than a given point
pub type TimelineSinceRsp = Option<TimelineSincePage>;

/// A page of older posts from a timeline
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TimelineBeforePage {
    pub posts: NEVec<FeedPost>,
    pub before: TimelineToken,
}

/// Response payload for a timeline request for posts older than a given point
pub type TimelineBeforeRsp = Option<TimelineBeforePage>;

/// `/users/like` request payload
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LikeRequest {
    pub id: Url,
    pub actor: Url,
}

/// `/users/reply` request payload
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplyRequest {
    pub id: Url,
    pub actor: Url,
    pub text: String,
}
