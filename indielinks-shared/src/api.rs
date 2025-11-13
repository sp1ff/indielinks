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
use secrecy::{zeroize::Zeroize, CloneableSecret, SecretBox, SerializableSecret};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::entities::{Post, PostDay, StorUrl, Tagname, UserEmail, Username};

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
    pub url: StorUrl,
    #[serde(rename = "description")]
    pub title: String,
    #[serde(rename = "extended")]
    pub notes: Option<String>,
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

// This defines a type encapsulating a "post"; not in the del.icio.us sense, not in the ActivityPub
// sense, but in the sense of the indielinks UX.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FeedPost {
    pub id: Url,
    pub in_reply_to: Option<Url>,
    pub published: DateTime<Utc>,
    // Should this be sanitized, somehow?
    pub content: String,
}

// External tagging (the default) should work, here. `Since { since: 123, max_posts: None }` should
// serialize, in JSON, to { "Since": { "before": 123 } }, which I think is what we want.
//
// I *hate* exposing the pagination tokens as just `String`s, but using an opaque type is really
// difficult, because I want it to be constructable only from the API's implementation in a
// higher-level module, on the back-end only.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub enum TimelineReq {
    Initial {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_posts: Option<NonZero<usize>>,
    },
    // "Return to me up to n posts since the most recent one I've seen". This will return up to `max_posts`
    // feed items whose identifiers/tags are strictly later than `since`
    Since {
        since: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_posts: Option<usize>,
    },
    //
    Before {
        before: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_posts: Option<usize>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub enum TimelineRsp {
    Initial {
        posts: Option<(NEVec<FeedPost>, String, String)>,
    },
    Since {
        posts: Option<(NEVec<FeedPost>, String)>,
    },
    Before {
        posts: Option<(NEVec<FeedPost>, String)>,
    },
}
