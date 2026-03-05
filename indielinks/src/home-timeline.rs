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
    fmt::{Display, Formatter},
    num::NonZero,
    ops::{Bound, Deref, RangeBounds},
    pin::Pin,
    result::Result as StdResult,
    sync::Arc,
};

use async_stream::try_stream;
use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};
use chacha20poly1305::{
    aead::{Aead, KeyInit},
    ChaCha20Poly1305, Nonce,
};
use chrono::{DateTime, Utc};
use either::Either::Left;
use futures::{stream::iter, Stream, StreamExt, TryStreamExt};
use hkdf::Hkdf;
use http::Method;
use nonempty_collections::NEVec;
use nonzero::nonzero;
use rand::{rngs::OsRng, RngCore};
use secrecy::ExposeSecret;
use serde::{ser::SerializeStruct, Deserialize, Serialize};
use sha2::Sha256;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use tokio::sync::Mutex;
use tracing::debug;
use url::Url;

use indielinks_shared::{
    api::TimelineToken,
    entities::{StorUrl, Username},
    origin::Origin,
};

use crate::{
    ap_entities::{ap_request, AnnounceOrCreate, Item, OutboxPage},
    ap_resolution::ApResolver,
    client_types::ClientType,
    entities::{Following, User},
    signing_keys::SigningKey,
    storage::Backend as StorageBackend,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("While resolving an ActivityPub entity, {source}"))]
    ApResolution { source: crate::ap_resolution::Error },
    #[snafu(display("Bad pagination token format"))]
    BadToken { backtrace: Backtrace },
    #[snafu(display("While streaming follows, {source}"))]
    Follow { source: crate::storage::Error },
    #[snafu(display("While deriving the encryption key, {source}"))]
    Hkdf {
        source: hkdf::InvalidLength,
        backtrace: Backtrace,
    },
    #[snafu(display("While looking-up {user}'s outbox page, {source}"))]
    OutboxLookup {
        user: Username,
        source: crate::ap_entities::Error,
    },
    #[snafu(display("While fetching an Outbox page, {source}"))]
    OutboxPage { source: crate::ap_entities::Error },
    #[snafu(display("While decrypting the pagination token, {source}"))]
    TokenDecrypt {
        source: chacha20poly1305::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While deserializing the pagination token, {source}"))]
    TokenDeser {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While encrypting the pagination token, {source}"))]
    TokenEncrypt {
        source: chacha20poly1305::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("The pagination token is not base64-encoded: {source}"))]
    TokenNotBase64 {
        source: base64::DecodeError,
        backtrace: Backtrace,
    },
    #[snafu(display("While serializing the pagination token, {source}"))]
    TokenSer {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            PostKey                                             //
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

impl PostKey {
    pub fn new(item: &Item) -> PostKey {
        match item {
            Item::Note(note) => PostKey {
                timestamp: *note.published(),
                id: note.id().clone(),
            },
            Item::Share(share) => PostKey {
                timestamp: *share.published(),
                id: share.id().clone(),
            },
        }
    }
    pub fn from_pagination_token(token: &TimelineToken, signing_key: &SigningKey) -> Result<Self> {
        // We have a base64-encoded bytestring "12-octet-nonce | encrypted JSON"
        let buf = BASE64_URL_SAFE_NO_PAD
            .decode(token)
            .context(TokenNotBase64Snafu)?;
        // Split the buffer into nonce & ciphertext
        let (nonce, cipher_text) = buf.split_at_checked(12).context(BadTokenSnafu)?;
        // indielinks signing keys, at the time of this writing, are 64 octets in length (don't
        // remember why I picked that ATM). Use a simple KDF to derive a 32 octet long key, which is
        // what's used by ChaCha20Poly1305.
        let key = PostKey::derive_key(signing_key)?;
        let cipher = ChaCha20Poly1305::new(&key);
        let nonce = Nonce::from_slice(nonce);

        serde_json::from_slice::<PostKey>(
            cipher
                .decrypt(nonce, cipher_text)
                .context(TokenDecryptSnafu)?
                .as_slice(),
        )
        .context(TokenDeserSnafu)?
        .pipe(Ok)
    }
    /// Turn this sort key into a pagination token
    // `PostKey`s are exposed at the API level as `TimelineToken`s-- an opaque represetnation of
    // `PostKey`. Per <https://google.aip.dev/158?utm_source=chatgpt.com> "Page tokens provided by
    // APIs must be opaque (but URL-safe) strings, and must not be user-parseable. This is because
    // if users are able to deconstruct these, they will do so. This effectively makes the
    // implementation details of your API's pagination become part of the API surface, and it
    // becomes impossible to update those details without breaking users."
    pub fn to_pagination_token(&self, signing_key: &SigningKey) -> Result<TimelineToken> {
        // indielinks signing keys, at the time of this writing, are 64 octets in length (don't
        // remember why I picked that ATM). Use a simple KDF to derive a 32 octet long key, which is
        // what's used by ChaCha20Poly1305.
        let key = PostKey::derive_key(signing_key)?;
        let cipher = ChaCha20Poly1305::new(&key);
        // Symmetric encryption generally requires a per-message nonce. We're not really encrypting
        // for security or message integrity here, just obfuscation. And I'm not interested in
        // managing per pagination token state! So, we'll just prepend the nonce to the cipher text
        // (it's only a 12-byte nonce).
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        // OK-- serialize ourselves to JSON & encrypt.
        let mut cipher_text = cipher
            .encrypt(
                nonce,
                serde_json::to_vec(&self).context(TokenSerSnafu)?.as_slice(),
            )
            .context(TokenEncryptSnafu)?;

        // Finally, append the two (nonce + ciphertext) and base64-encode. I couldn't come-up with a
        // simpler way. Since `encode()` requires something that is `Deref<[u8]>`, I need a
        // contiguous block, meaning one copy, regardless:
        let mut out = Vec::with_capacity(12 + cipher_text.len());
        out.extend_from_slice(&nonce_bytes); // Copy here
        out.append(&mut cipher_text); // move here

        Ok(TimelineToken::new_internal(
            BASE64_URL_SAFE_NO_PAD.encode(out),
        ))
    }

    /// Use an HMAC key derivation function to derive a 32-byte key from a 64-byte [SigningKey]
    fn derive_key(signing_key: &SigningKey) -> Result<chacha20poly1305::Key> {
        let hk = Hkdf::<Sha256>::new(None, signing_key.as_ref().expose_secret());
        let mut key = [0u8; 32];
        hk.expand(b"TimelineToken xchacha20poly1305", &mut key)
            .context(HkdfSnafu)?;
        Ok(key.into())
    }
}

#[cfg(test)]
mod test_postkey {
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

    #[test]
    fn serde() {
        let key = SigningKey::new([0; 64].to_vec()).unwrap(/* known good */);
        let x = PostKey {
            timestamp: chrono::DateTime::UNIX_EPOCH,
            id: Url::parse("https://example.com").unwrap(/* known good */),
        };
        let token = x.to_pagination_token(&key).unwrap(/* known good */);
        eprintln!("token: {token:#?}");

        let post_key = PostKey::from_pagination_token(&token, &key).unwrap();
        eprintln!("{post_key:#?}");

        assert_eq!(post_key, x);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             Outbox                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

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

impl Deref for NextOutboxPage {
    type Target = Url;

    fn deref(&self) -> &Self::Target {
        &self.0
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
    /// The user requesting this outbox
    user: User,
    origin: Origin,
    client: ClientType,
    /// The total number of items, as reported by the remote server at the beginning of our
    /// pagination
    _num_items: usize,
    /// URL naming the next page of items
    next_page: Option<NextOutboxPage>,
    /// Shared cache for looking up ActivityPub entities
    ap_resolver: Arc<Mutex<ApResolver>>,
}

impl Outbox {
    pub async fn new(
        follow: &StorUrl,
        user: User,
        origin: Origin,
        mut client: ClientType,
        ap_resolver: Arc<Mutex<ApResolver>>,
    ) -> Result<Outbox> {
        let outbox = ap_resolver
            .lock()
            .await
            .actor_id_to_outbox(Left(&user), follow.as_ref())
            .await
            .context(ApResolutionSnafu)?;

        let outbox: crate::ap_entities::Outbox = ap_request(
            &mut client,
            &origin,
            Left(&user),
            &outbox,
            Method::GET,
            None,
            &(),
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
            ap_resolver,
        })
    }
    /// Produce a [Stream] of [Item]s from this outbox
    // This function returns a stream of `Result<Item>`s. We don't name the return type, because
    // it's produced by the `try_stream!` macro (from the async-stream crate).
    pub fn stm(&self) -> impl Stream<Item = Result<Item>> {
        // My scheme is to mantain a deque of `Item` corresponding to the members of the current
        // page. When it's non-empty, we'll yield the elements one-by-one. When it becomes empty,
        // we'll request the next page. If it's empty and there *is* no next page, then we're done.
        let mut items: VecDeque<Item> = VecDeque::new();
        // I'm a bit hazy on the rules for working with `try_stream!`, but it seems to capture
        // locals just like an `async move {...}` block would. I could probably move `items` inside
        // the macro body, but if I want to refer to any member variables therein, I have two
        // choices:
        //
        // 1. borrow and return an additional lifetime bound on the return value (IOW, assert that
        // the returned stream may not outlive this `Outbox` instance)
        // 2. clone the member variables I need & move 'em into the macro body
        //
        // Since everything I need is cheaply clonable, I'll go with option 2.
        let mut next_page = self.next_page.clone();
        let user = self.user.clone();
        let origin = self.origin.clone();
        let mut client = self.client.clone();
        let ap_resolver = self.ap_resolver.clone();

        async fn to_note(
            aoc: AnnounceOrCreate,
            user: &User,
            ap_resolver: Arc<Mutex<ApResolver>>,
        ) -> Result<Item> {
            match aoc {
                AnnounceOrCreate::Announce(announce) => ap_resolver
                    .lock()
                    .await
                    .note_id_to_note(Left(user), announce.object())
                    .await
                    .context(ApResolutionSnafu)?,
                AnnounceOrCreate::Create(create) => match create.object() {
                    crate::ap_entities::CreateObject::Note(note) => note.clone(),
                },
            }
            .pipe(Box::new)
            .pipe(Item::Note)
            .pipe(Ok)
        }

        try_stream! {
            loop {
                match (items.pop_front(), &next_page) {
                    // In this case, `items` is non-empty-- just yield the next element:
                    (Some(item), _) => yield item,
                    // Here, `items` is empty, but there's another page available-- grab the next
                    // page, re-charge `items` and return the (new) first element.
                    (None, Some(page)) => {
                        // We expect the the page to contain either `Announce` or `Create` items, both of
                        // which have an `object` field. That field, however, may contain an ID, or a full
                        // object. For instance, here are the first two items in my outbox at
                        // indieweb.social:

                        // {
                        //     "id":"https://indieweb.social/users/sp1ff/statuses/115897254140784892/activity",
                        //     "type":"Announce",
                        //     ...
                        //     "object":"https://calckey.world/notes/ahihtbvduv"
                        // },
                        // {
                        //     "id":"https://indieweb.social/users/sp1ff/statuses/115707352593100014/activity",
                        //     "type":"Create",
                        //     ...
                        //     "object":{
                        //         "id":"https://indieweb.social/users/sp1ff/statuses/115707352593100014",
                        //         "type":"Note",
                        //         "summary":null,
                        //         "inReplyTo":null,
                        //         ...

                        let page: OutboxPage = ap_request(
                            &mut client,
                            &origin,
                            Left(&user),
                            page.deref(),
                            Method::GET,
                            None,
                            &(),
                        ).await.context(OutboxPageSnafu)?;
                        if ! page.ordered_items.is_empty() {
                            items = iter(page.ordered_items)
                                .then(|aoc| to_note(aoc, &user, ap_resolver.clone()))
                                .collect::<VecDeque<StdResult<Item, _>>>()
                                .await
                                .into_iter()
                                .collect::<StdResult<VecDeque<Item>, _>>()?;
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
pub struct Timeline {
    // Not sure how I want to represent the posts, but let's start with this. Would probably save
    // space to just store the ID & cache the mapping.
    // `items` will be stored in *descending* order (i.e. most-recent first)
    items: BTreeMap<PostKey, Item>,
    streams: Vec<
        Pin<
            Box<
                // Nb. The streams produced by `Oubox::stm()` are Send but not Sync, which I gather
                // is expected (?)
                dyn Stream<Item = Result<Item>> + Send,
            >,
        >,
    >,
}

impl std::fmt::Debug for Timeline {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> StdResult<(), std::fmt::Error> {
        fmt.write_str("Timeline { items: ")?;
        fmt.debug_map()
            .entries(self.items.iter().map(|(k, v)| (k.clone(), v.clone())));
        write!(fmt, ", {} outboxes }}", self.streams.len())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FirstPage {
    pub items: NEVec<Item>,
    pub since: PostKey,
    pub before: PostKey,
}

// Avaialbe on nightly, but I don't feel like changing my entire toolchain just to save a few lines
// of code.
struct RangeAbove<'a> {
    pub start: &'a PostKey,
}

impl<'a> RangeBounds<PostKey> for RangeAbove<'a> {
    fn start_bound(&self) -> Bound<&PostKey> {
        Bound::Excluded(self.start)
    }

    fn end_bound(&self) -> Bound<&PostKey> {
        Bound::Unbounded
    }
}

struct RangeBelow<'a> {
    pub start: &'a PostKey,
}

impl<'a> RangeBounds<PostKey> for RangeBelow<'a> {
    fn start_bound(&self) -> Bound<&PostKey> {
        Bound::Unbounded
    }

    fn end_bound(&self) -> Bound<&PostKey> {
        Bound::Excluded(self.start)
    }
}

// Helper newtype for producing a JSON representation of a `Timeline` for logging/debugging purposes
pub(crate) struct JsonRepr<'a>(pub &'a Timeline);

impl<'a> serde::ser::Serialize for JsonRepr<'a> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let t = self.0;
        let mut st = serializer.serialize_struct("Timeline", 2)?;
        let pairs = t
            .items
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<(PostKey, Item)>>();
        st.serialize_field("items", &pairs)?;
        st.serialize_field("num_streams", &t.streams.len())?;
        st.end()
    }
}

impl Timeline {
    pub async fn new(
        user: &User,
        origin: &Origin,
        storage: &(dyn StorageBackend + Send + Sync),
        client: &ClientType,
        ap_resolver: Arc<Mutex<ApResolver>>,
    ) -> Result<Timeline> {
        let mut follows = iter(
            storage
                .get_following(user)
                .await
                .context(FollowSnafu)?
                .try_collect::<Vec<Following>>()
                .await
                .context(FollowSnafu)?,
        )
        .then(|following| {
            let ap_resolver = ap_resolver.clone();
            async move {
                Outbox::new(
                    following.actor_id(),
                    user.clone(),
                    origin.clone(),
                    client.clone(),
                    ap_resolver,
                )
                .await
            }
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

    /// Add a new `Item` to the timeline
    pub fn add(&mut self, item: Item) {
        self.items.insert(PostKey::new(&item), item);
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
        let num_items = page_size.unwrap_or(nonzero!(16usize));

        // It may seem tempting to serve a request from the items currently in memory if we have
        // enough laying around, but it may be that some of those items are older than as-yet-unseen
        // items in other, more frequently posting, outboxes. Just call `grow()` no matter what to
        // keep the timeline accurate.
        self.grow(num_items).await?;

        let range = RangeAbove { start: &before };

        let items = self
            .items
            .range(range)
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
    /// vector, and the second the last element in the vector. These can be passed as parameters to
    /// [`before()`](Timeline::before) and [`since()`](Timeline::since).
    pub async fn begin(&mut self, page_size: Option<NonZero<usize>>) -> Result<Option<FirstPage>> {
        let page_size = page_size.unwrap_or(nonzero!(16usize));

        // We're beginning a pagination of this timeline, with a page size of `page_size`. If we
        // don't have enough elements in `self.items` to serve the first page, grow it:
        if page_size.get() > self.items.len() {
            self.grow(page_size).await?;
        }

        // Now, it's possible that the timeline is empty (perhaps the user follows no one, yet). If
        // that's the case, we'll know that now & can return early.
        if self.items.is_empty() {
            debug!("Timeline is empty.");
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

        Ok(Some(FirstPage {
            items: (head, tail).into(),
            since: first,
            before: last,
        }))
    }

    /// Grab some posts that have appeared since this pagination was undertaken
    pub async fn since(
        &mut self,
        after: PostKey,
        page_size: Option<NonZero<usize>>,
    ) -> Option<(NEVec<Item>, PostKey)> {
        let mut items = self
            .items
            .range(RangeBelow { start: &after })
            // Reverse since we only want one page's worth since `after`
            .rev()
            .take(
                page_size
                    .unwrap_or(NonZero::new(16).unwrap(/* known good */))
                    .get(),
            )
            .collect::<Vec<(&PostKey, &Item)>>();
        // reverse again to undo the first reverse, above
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

    /// Grow the timeline into the past
    ///
    /// This isn't a great solution. The problem here is that we're pulling from multiple inboxes,
    /// some of which will, in general, be posting frequently and some of which will, again in
    /// general, be posting rarely. So if outbox A is posting frequently, and outbox B rarely, and
    /// we pull n items from each, we may now have a long-ago item from outbox B, and have *not yet
    /// seen* more recent items from outbox A!
    ///
    /// We solve this by pulling a page's worth of posts from every single outbox, so that when our
    /// caller returns only a single page, that potentially old item from inbox B may be in
    /// `self.items`, but it won't go out to our caller. The next page request will cause another
    /// fetch of a page's worth of items from every outbox, pushing that errant, old item, ever
    /// further back.
    ///
    /// This comes, however, at the cost of growing `self.items` much more rapidly than is needed.
    /// If this becomes a problem, I'll have to revisit this algorithm.
    async fn grow(&mut self, num_elems: NonZero<usize>) -> Result<()> {
        iter(self.streams.iter_mut())
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
}
