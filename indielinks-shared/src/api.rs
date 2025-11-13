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

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
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

// TODO(sp1ff): Coding speculatively, here. The act of coding is forcing me to clarify my model of a
// "timeline". Conceptually, each user has a "home timeline": an ever-growing list of feed items
// posted by people that user follows, ordered by time. The list goes back in time to the earliest
// thing posted by anyone the user follows, and is continually growing into the future as follows
// post new items. It also grows into the past, as the user follows new people who have older
// content.

// Not only is the list growing on either end, its elements "in the middle" change as the user
// follows new people, or unfollows current follows, the list membership will change.

// If we consider each fixed set of follows to be a "generation", we can picture a user's timeline
// as going through generational changes. A given generation of the timeline, then, will only
// grow into the future.

// In general, it would be expensive (both in terms of network calls & memory) to compute the
// user's full timeline, even for a fixed generation. But that's OK, I can't really imagine a case,
// outside of testing & debugging, where we'd want to. The use case would generally be:

// 1. give me the most recent chunk of my timeline so I can read it
// 2. OK, I've read it. Give me the next chunk
// 3. I've been away for a bit. Give me any new items I've missed.

// In fact, all three operations are insensitive to the generation; items 2 & 3 could be fairly
// described in more detail as:

// 2. I've read a chunk that in generation `g` was up to & included post `i`. I've added & removed
// some follows in the meantime, bringing my timeline to generation `h`. Give me a chunk of my
// timeline at generation `h` consisting of posts *after* `i`.

// 3. I've been away for a bit. I may have added & removed follows. Give me any items in the
// now-current generation of my timeline after the most recent post I read.

// In other words, we have a well-defined datastructure (a generational list that grows forward in
// time for each generation), but it would be inconveient to actually calculate the list even
// for a given generation, let alone across generations (which would, for starters, require a
// linear traversal of the list inserting & removing items). But that's OK: the operations we
// care about don't require that.

// Consider a datastructure consisting of a deque of items. The front of the deque corresponds to
// now, the tail to the past. The first time a user's home timeline is requested, we walk their
// follows' outboxes and assemble a leading subsequence of the timeline. If they request more, and
// we can satisfy the request from the deque, do so. Else, walk their follows' outboxes further and
// build-out the deck towards the tail (i.e. the past). As notifications come in to the public
// inbox, push salient items on to the head of the deque. This is what we'll use to satisfy the
// "catch-up" request.

// When the user follows or unfollows someone, we have two choices:
//
// 1. update the deque, which could be inconvenient
//
// 2. drop the deque, but then how do we handle after/since requests? Well, we'd have to re-build
// the deque, regardless. If it's a "before" request, we could just require the process to pull a
// minimum number of items before the given timestamp
//
// I think I like option 2.
//
// Finally, the preceeding implicitly requires there to be a total ordering on follows' posts.
// ActivityPub timestamps only seem to go down to the second, so that won't be enough (given enough
// follows, eventually two will post something in the same second). For lack of anything better,
// I propose adding a second sort key, the URL (sorted lexicographically)-- that should be enough
// to impose a total order.

// This defines a type encapsulating a "post"; not in the del.icio.us sense, not in the ActivityPub
// sense, but in the sense of the indielinks UX. Leaving it blank, for now.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FeedPost;

// I'm also going to need a page token. I'd like this to be opaque to my callers. Just had a really
// good chat with OpenAI on this. For now, just make this a placeholder.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PageToken;

// TODO(sp1ff): I *think* external tagging (the default) should work, here. `Since { since: 123,
// max_posts: None }` should serialize, in JSON, to { "Since": { "before": 123 } }, which I think is
// what we want.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub enum TimelineReq {
    Initial {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_posts: Option<usize>,
    },
    // "Return to me up to n posts since the most recent one I've seen". This will return up to `max_posts`
    // feed items whose identifiers/tags are strictly later than `since`
    Since {
        since: PageToken,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_posts: Option<usize>,
    },
    //
    Before {
        before: PageToken,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_posts: Option<usize>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub enum TimelineRsp {
    // TODO(sp1ff): Response to the initial timeline request; an initial chunk of feed items, along
    // with a pair of cookies identifying the first & last item in this chunk. I haven't decided
    // exactly how to represent that, yet, but it need only be unique up to the user's `Timeline`
    // instance. I'll just say `usize` for now.
    Initial {
        posts: Vec<FeedPost>,
        // Let's say [first, last), so if a user _has_ no feed items, we can return [0, 0)
        first: PageToken,
        last: PageToken,
    },
    Since {
        posts: Vec<FeedPost>,
        last: PageToken,
    },
    Before {
        posts: Vec<FeedPost>,
        first: PageToken,
    },
}
