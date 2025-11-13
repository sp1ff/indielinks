// Copyright (C) 2025 Michael Herstine <sp1ff@pobox.com>
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

//! # The Home Timeline
//!
//! ## Introduction
//!
//! Conceptually, each user has a *home timeline*: an ever-growing list of items posted by people
//! that that user follows, ordered by time. The list goes back in time to the earliest thing posted
//! by anyone the user follows, and is continually growing into the future as follows post new
//! items. It also grows into the past, as the user follows new people who have older content.
//!
//! Not only is the list growing on either end, its elements "in the middle" change as the user
//! follows new people, or unfollows current follows.
//!
//! ## The Model
//!
//! If we consider each fixed set of follows to be a "generation", we can picture a user's timeline
//! as going through generational changes. A given generation of the timeline, then, will only grow
//! into the future.
//!
//! In general, it would be expensive (both in terms of network calls & memory) to compute the
//! user's full timeline, even for a fixed generation. But that's OK, I can't really imagine a case,
//! outside of testing & debugging, where we'd want to. The use case would generally be:
//!
//! 1. give me the most recent chunk of my timeline so I can read it
//! 2. OK, I've read it. Give me the next chunk
//! 3. I've been away for a bit. Give me any new items I've missed.
//!
//! In fact, all three operations are insensitive to the generation; items 2 & 3 could be fairly
//! described in more detail as:
//!
//! 2. I've read a chunk that in generation `g` was up to & included post `i`. I've added & removed
//!    some follows in the meantime, bringing my timeline to generation `h`. Give me a chunk of my
//!    timeline at generation `h` consisting of posts *after* `i`.
//!
//! 3. I've been away for a bit. I may have added & removed follows. Give me any items in the
//!    now-current generation of my timeline after the most recent post I read.
//!
//! In other words, we have a well-defined datastructure (a generational list that grows forward in
//! time for each generation), but it would be inconvenient to actually calculate the list even for
//! a given generation, let alone across generations (which would, for starters, require a linear
//! traversal of the list inserting & removing items). But that's OK: the operations we care about
//! don't require that.

use std::{
    cmp::{min, Ordering},
    collections::{BTreeMap, VecDeque},
    error::Error as StdError,
    fmt::Display,
    num::NonZero,
    pin::Pin,
    result::Result as StdResult,
};

use async_stream::try_stream;
use base64::prelude::*;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, TryStreamExt};
use hmac::Hmac;
use http::Method;
use indielinks_shared::{
    entities::{StorUrl, Username},
    origin::Origin,
};
use nonempty_collections::NEVec;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use snafu::{prelude::*, Backtrace};
use tracing::debug;
use url::Url;

use crate::{
    activity_pub::send_activity_pub,
    ap_entities::{Actor, Item, Note, OutboxPage},
    client::ClientType,
    entities::{Following, User},
    signing_keys::SigningKey,
    storage::Backend as StorageBackend,
    util::exactly_two,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("While deserializing an Actor, {source}"))]
    ActorDe {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("When base64-decoding the token's HMAC, {source}"))]
    Base64De {
        token: String,
        source: base64::DecodeError,
        backtrace: Backtrace,
    },
    #[snafu(display("While calling through the indielinks client, {source}"))]
    Call {
        source: Box<dyn StdError + Send + Sync>,
        backtrace: Backtrace,
    },
    #[snafu(display("While streaming follows, {source}"))]
    Follow { source: crate::storage::Error },
    #[snafu(display("Bad HMAC: {source}"))]
    Hmac {
        source: digest::MacError,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid key length for HMAC-SHA-256"))]
    KeyLength {
        source: crypto_common::InvalidLength,
        backtrace: Backtrace,
    },
    #[snafu(display("While deserializing a Note page, {source}"))]
    NoteDe {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While looking-up {user}'s outbox page, {source}"))]
    OutboxLookup {
        user: Username,
        source: crate::activity_pub::Error,
    },
    #[snafu(display("While fetching an Outbox page, {source}"))]
    OutboxPage { source: crate::activity_pub::Error },
    #[snafu(display("While forming a request for the next outbox page, {source}"))]
    PageRequest {
        source: http::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While deserializing a PostKey from JSON, {source}"))]
    PostKeyDe {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While serializing a PostKey to JSON, {source}"))]
    PostKeySer {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While waiting for the indielinks client to be ready, {source}"))]
    Ready {
        source: Box<dyn StdError + Send + Sync>,
        backtrace: Backtrace,
    },
    #[snafu(display("While building an http::Request, {source}"))]
    Request {
        source: http::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("{token} is not a valid pagination token"))]
    TokenFormat { token: String, backtrace: Backtrace },
}

type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sort key for items by people we follow
///
/// The [above](self) implicitly requires there to be a total ordering on follows' posts.
/// ActivityPub timestamps only seem to go down to the second, so that won't be enough (given enough
/// follows, eventually two will post something in the same second). For lack of anything better,
/// I'm adding a second sort key, the URL (sorted lexicographically)-- that should be enough to
/// impose a total order.
///
/// Deriving [Ord] would result in an ascending sort order, whereas we want *descending*.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct PostKey {
    timestamp: DateTime<Utc>,
    id: Url,
}

impl Ord for PostKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if (self.timestamp > other.timestamp)
            || (self.timestamp == other.timestamp && self.id > other.id)
        {
            Ordering::Less
        } else if self.timestamp == other.timestamp && self.id == other.id {
            Ordering::Equal
        } else {
            Ordering::Greater
        }
    }
}

impl PartialOrd for PostKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl TryFrom<(String, &SigningKey)> for PostKey {
    type Error = Error;

    fn try_from((value, key): (String, &SigningKey)) -> StdResult<Self, Self::Error> {
        use hmac::Mac;
        use secrecy::ExposeSecret;

        let (hmac, json) = exactly_two(value.split(".")).map_err(|_| {
            TokenFormatSnafu {
                token: value.clone(),
            }
            .build()
        })?;

        let hmac = BASE64_STANDARD.decode(hmac).context(Base64DeSnafu {
            token: value.clone(),
        })?;

        let mut mac = Hmac::<Sha256>::new_from_slice(key.as_ref().as_ref().expose_secret())
            .context(KeyLengthSnafu)?;
        mac.update(json.as_bytes());
        mac.verify_slice(&hmac).context(HmacSnafu)?;

        serde_json::from_str::<PostKey>(json).context(PostKeyDeSnafu)
    }
}

impl PostKey {
    pub fn new(item: &Item) -> PostKey {
        match item {
            Item::Note(note) => PostKey {
                timestamp: *note.published(),
                id: note.id().clone(),
            },
        }
    }
    // Should really version this structure, tho the tokens are expected to be so short-lived as to
    // probably not be worth it
    pub fn as_token(&self, key: &SigningKey) -> Result<String> {
        use hmac::Mac;
        use secrecy::ExposeSecret;

        let json = serde_json::to_string(self).context(PostKeySerSnafu)?;

        let mut mac = Hmac::<Sha256>::new_from_slice(key.as_ref().as_ref().expose_secret())
            .context(KeyLengthSnafu)?;
        mac.update(json.as_bytes());
        let mac = BASE64_STANDARD.encode(mac.finalize().into_bytes());

        Ok(format!("{mac}.{json}"))
    }
}

#[cfg(test)]
mod check_postkey_ord {
    use super::*;

    #[test]
    fn smoke() {
        assert!(
            PostKey {
                timestamp: DateTime::parse_from_rfc3339("2025-11-12T19:46:55Z")
                    .unwrap()
                    .with_timezone(&Utc),
                id: Url::parse("http://b.com").unwrap(),
            } > PostKey {
                timestamp: DateTime::parse_from_rfc3339("2025-11-12T19:47:45Z")
                    .unwrap()
                    .with_timezone(&Utc),
                id: Url::parse("http://a.com").unwrap(),
            }
        );
        assert!(
            PostKey {
                timestamp: DateTime::parse_from_rfc3339("2025-11-12T19:46:55Z")
                    .unwrap()
                    .with_timezone(&Utc),
                id: Url::parse("http://b.com").unwrap(),
            } < PostKey {
                timestamp: DateTime::parse_from_rfc3339("2025-11-12T19:46:55Z")
                    .unwrap()
                    .with_timezone(&Utc),
                id: Url::parse("http://a.com").unwrap(),
            }
        );
    }
}

/// A type representing a place in an ActivityPub outbox pagination
///
/// Specifically, this should hold the URL of the *next*, as yet unread, page.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct NextOutboxPage(Url);

impl Display for NextOutboxPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Url> for NextOutboxPage {
    fn from(value: Url) -> Self {
        Self(value)
    }
}

/// A stream yielding items from an ActivityPub outbox
///
/// ActivityPub [outboxes] are setup with pagination. [Outbox] presents an iterator-style facade
/// over that pagination for the caller's convenience. Since, internally, we'll be fetching new
/// pages asynchronously, we implement [Stream] instead of [Iterator].
///
/// [outboxes]: https://www.w3.org/TR/activitypub/#outbox
#[derive(Clone)]
pub struct Outbox {
    /// AcivityPub identifier naming the owner of the outbox over which we are iterating
    _id: Url,
    user: User,
    origin: Origin,
    client: ClientType,
    /// The total number of items, as reported by the remote server at the beginning of our
    /// pagination
    _num_items: usize,
    /// URL naming the next page of items
    next_page: Option<NextOutboxPage>,
}

impl Outbox {
    // One possible optimization: cache the id :=> // outbox mapping, so as to save a call in
    // this method.
    pub async fn new(
        follow: &StorUrl,
        user: User,
        origin: Origin,
        mut client: ClientType,
    ) -> Result<Outbox> {
        debug!("Building an Outbox for {follow}");

        let actor = send_activity_pub::<String, (), Actor>(
            &user,
            &origin,
            Method::GET,
            follow.to_string(),
            None,
            None,
            &mut client,
        )
        .await
        .context(OutboxLookupSnafu {
            user: user.username().clone(),
        })?;

        let outbox = send_activity_pub::<String, (), crate::ap_entities::Outbox>(
            &user,
            &origin,
            Method::GET,
            actor.outbox().to_string(),
            None,
            None,
            &mut client,
        )
        .await
        .context(OutboxPageSnafu)?;

        Ok(Outbox {
            _id: follow.clone().into(),
            user,
            origin,
            client,
            _num_items: outbox.total_items,
            next_page: Some(outbox.first.into()),
        })
    }
    /// Produce a [Stream] of [Item]s from this outbox
    pub fn stm(&self) -> impl Stream<Item = Result<crate::ap_entities::Item>> {
        let mut items: VecDeque<Item> = VecDeque::new();
        let mut next_page = self.next_page.clone().map(|x| x.to_string());
        let user = self.user.clone();
        let origin = self.origin.clone();
        let mut client = self.client.clone();
        try_stream! {
            loop {
                match (items.pop_front(), &next_page) {
                    (Some(item), _) => yield item,
                    (None, Some(page)) => {
                        let page = send_activity_pub::<&String, (), OutboxPage>(&user, &origin, Method::GET, page, None, None, &mut client).await.context(OutboxPageSnafu)?;
                        if ! page.ordered_items.is_empty() {
                            items = page
                                .ordered_items
                                .into_iter()
                                .map(|create| serde_json::from_value::<Note>(serde_json::Value::Object(create.object)))
                                .collect::<StdResult<VecDeque<Note>, _>>()
                                .context(NoteDeSnafu)?
                                .into_iter()
                                .map(Item::Note)
                                .collect();
                            next_page = page.next.map(|url| url.into());
                            yield items.pop_front().unwrap(/* known good */)
                        } else {
                            next_page = None;
                        }
                    },
                    (None, None) => return,
                }
            }
        }
    }
}

/// A user's home timeline
///
/// Consider a datastructure consisting of a deque of items. The front of the deque corresponds to
/// now, the tail to the past. The first time a user's home timeline is requested, we walk their
/// follows' outboxes and assemble a leading subsequence of the timeline. If they request more, and
/// we can satisfy the request from the deque, do so. Else, walk their follows' outboxes further and
/// build-out the deck towards the tail (i.e. the past). As notifications come in to the public
/// inbox, push salient items on to the head of the deque. This is what we'll use to satisfy the
/// "catch-up" request.
///
/// When the user follows or unfollows someone, we have two choices:
///
/// 1. update the deque, which could be inconvenient
///
/// 2. drop the deque, but then how do we handle after/since requests? Well, we'd have to re-build
///    the deque, regardless. If it's a "before" request, we could just require the process to pull a
///    minimum number of items before the given timestamp
///
/// I think I like option 2.
pub struct Timeline {
    // Not sure how I want to represent the posts, but let's start with this. Would probably save
    // space to just store the ID & cache the mapping
    items: BTreeMap<PostKey, Item>,
    streams: Vec<
        Pin<
            Box<
                // Nb. The streams produced by `Oubox::stm()` are Send but not Sync, which I gather
                // is expected (?)
                dyn futures::prelude::Stream<Item = Result<crate::ap_entities::Item>> + Send,
            >,
        >,
    >,
}

impl Timeline {
    pub async fn new(
        user: &User,
        origin: &Origin,
        storage: &(dyn StorageBackend + Send + Sync),
        client: &crate::client::ClientType,
    ) -> Result<Timeline> {
        debug!("Creating the timeline for {user:?}");

        let mut follows = futures::prelude::stream::iter(
            storage
                .get_following(user)
                .await
                .context(FollowSnafu)?
                .try_collect::<Vec<Following>>()
                .await
                .context(FollowSnafu)?
                .into_iter(),
        )
        .then(|following| async move {
            Outbox::new(
                following.actor_id(),
                user.clone(),
                origin.clone(),
                client.clone(),
            )
            .await
        })
        .collect::<Vec<StdResult<_, _>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

        let streams = follows
            .iter_mut()
            .map(|outbox| Box::pin(outbox.stm()) as _)
            .collect();

        Ok(Self {
            items: BTreeMap::new(),
            streams,
        })
    }
    /// Continue an extant pagination, requesting a page's worth of items before `before`.
    ///
    /// If there are none, return `None`. If there are some, return a non-empty vector of up to
    /// `page_size` [Item]s and a token representing the last [Item] in the factor (for the next
    /// [`before()`](Timeline::before) invocation)
    pub async fn before(
        &mut self,
        before: PostKey,
        page_size: Option<NonZero<usize>>,
    ) -> Result<Option<(NEVec<Item>, PostKey)>> {
        let num_items = page_size.unwrap_or(NonZero::new(16).unwrap(/* known good */));

        // It may seem tempting to serve a request from the items currently in memory if we have
        // enough laying around, but it may be that some of those items are older than as-yet-unseen
        // items in other, more frequently posting, outboxes. Just call `grow()` no matter what to
        // keep the timeline accurate.
        self.grow(num_items).await?;

        let items = self
            .items
            .range(before..)
            .take(num_items.get())
            .collect::<Vec<(&PostKey, &Item)>>();

        if items.is_empty() {
            return Ok(None);
        }

        let after = items.last().unwrap(/* known good */).0.clone();
        let head = items.first().unwrap(/* known good */).1.clone();
        let tail = items
            .into_iter()
            .skip(1)
            .map(|(_, value)| value.clone())
            .collect::<Vec<Item>>();
        Ok(Some(((head, tail).into(), after)))
    }
    /// Begin a pagination
    ///
    /// If the [Timeline] is empty, return `None`. If it is non-empty, it will return a non-empty
    /// vector of [Item] along with two [PostKey]s, the first naming the first element in the
    /// vector, and the second the last element in the factor. These can be passed as parameters to
    /// [`before()`](Timeline::before) and [`since()`](Timeline::since).
    // I considered a dedicated type for return value, but it's not *quite* complex enough IMO.
    pub async fn begin(
        &mut self,
        page_size: Option<NonZero<usize>>,
    ) -> Result<Option<(NEVec<Item>, PostKey, PostKey)>> {
        let page_size = page_size.unwrap_or(NonZero::new(16).unwrap(/* known good */));

        // We're beginning a pagination of this timeline, with a page size of `page_size`. If we
        // don't have enough elements in `self.items` to serve the first pge, grow it:
        if page_size.get() > self.items.len() {
            self.grow(page_size).await?;
        }

        // Now, it's possible that the timeline is empty (perhaps the user follows no-one, yet). If
        // that's the case, we'll know that now & can return early.
        if self.items.is_empty() {
            return Ok(None);
        }

        // If we're here, the timeline has at least one element in it-- note it's key:
        let (first, head) = self
            .items
            .first_key_value()
            .map(|(key, value)| (key.clone(), value.clone()))
            .unwrap(/* known good */);

        // Now, it's possible that while the timeline is non-empty, it has fewer than `page_size`
        // items in it.
        let num_items = min(page_size.get(), self.items.len());

        let tail = self
            .items
            .iter()
            .skip(1)
            .take(num_items - 1)
            .map(|(_, item)| item.clone())
            .collect::<Vec<Item>>();

        let last = self
            .items
            .iter()
            .skip(num_items - 1)
            .take(1)
            .map(|(key, _)| key.clone())
            .next()
            .unwrap(/* known good */);

        Ok(Some(((head, tail).into(), first, last)))
    }
    /// Grow the timeline into the past
    ///
    /// This isn't a great solution. The problem here is that we're pulling from multiple inboxes,
    /// some of which will, in general, be posting frequetnly and some of which will, again in
    /// general, will be posting rarely. So if outbox A is posting frequently, and outbox B rarely,
    /// and we pull n items from each, we may now have a long-ago item from outbox B, and have *not
    /// yet seen* more recent items from outbox A!
    ///
    /// We solve this by pulling a page's worth of posts from every single outbox, so that when
    /// our caller returns only a single page, that potentially old item from inbox B may be in
    /// `self.items`, but it won't go out to our caller. The next page request will cause another
    /// fetch of a page's worth of items from every outbox, pushing that errant, old item, ever
    /// further back.
    ///
    /// This comes, however, at the cost of growing `self.items` much more rapidly than is needed.
    /// If this becomes a problem, I'll have to revisit this algorithm.
    async fn grow(&mut self, num_elems: NonZero<usize>) -> Result<()> {
        futures::prelude::stream::iter(self.streams.iter_mut())
            .then(|stm| async move {
                stm.take(num_elems.get())
                    .collect::<Vec<StdResult<Item, _>>>()
                    .await
            })
            .collect::<Vec<Vec<StdResult<Item, _>>>>()
            .await
            .into_iter()
            .flatten()
            .collect::<StdResult<Vec<Item>, _>>()?
            .into_iter()
            .for_each(|item| {
                self.items.insert(PostKey::new(&item), item);
            });
        Ok(())
    }
    /// Grab some posts that have appeared since this pagination was undertaken
    pub async fn since(
        &mut self,
        after: PostKey,
        page_size: Option<NonZero<usize>>,
    ) -> Option<(NEVec<Item>, PostKey)> {
        let mut items = self
            .items
            .range(..after)
            .rev()
            .take(
                page_size
                    .unwrap_or(NonZero::new(16).unwrap(/* known good */))
                    .get(),
            )
            .collect::<Vec<(&PostKey, &Item)>>();
        items.reverse();
        if items.is_empty() {
            None
        } else {
            let first = items.first().unwrap(/* known good */).0.clone();
            let head = items.first().unwrap(/* known good */).1.clone();
            let tail = items
                .into_iter()
                .skip(1)
                .map(|(_, value)| value.clone())
                .collect::<Vec<Item>>();
            Some(((head, tail).into(), first))
        }
    }
}
