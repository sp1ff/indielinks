// Copyright (C) 2026 Michael Herstine <sp1ff@pobox.com>
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

//! # [indielinks] user [ActivityPub] [outbox]es
//!
//! [indielinks]: crate
//! [ActivityPub]: https://www.w3.org/TR/activitypub/
//! [outbox]: https://www.w3.org/TR/activitypub/#outbox
//!
//! ## Introduction
//!
//! This module contains the implementation of the user "[outbox]" abstraction; a list of their
//! (public) posts, replies & shares sorted in reverse time order, available for pagination by
//! interested callers.
//!
//! ## Design
//!
//! My approach here is similar to that taken (at the time of this writing, at least) for the [home
//! timeline](crate::home_timeline): for each user, we build an in-memory datastructure representing
//! the outbox which is used to serve requests. Once created, the datastructure is kept up-to-date
//! as new outbox items are created. Since the in-memory datastructure is effectively a materialized
//! view of the backing datastore, constituted in a format convenient for pagination, it can be
//! discarded at any time to free-up memory. The work (and memory load) is distributed around the
//! cluster by making each cluster node responsible for a subset of the instance's users.
//!
//! The situation is complicated slightly by the fact that we need to pull from two sources: the
//! `posts` table and the `likes_replies_shares` table. When the first pagination is begun for a
//! user, we'll pull a page's worth of items from both sources and interleave them by sort key (on
//! which more below). We'll return a single page to our caller, along with a pagination token
//! containing the oldest sort key they've seen so far.
//!
//! When each new page is requested, we'll need to pull a page's worth of items from each table and
//! add them to the in-memory outbox (just "the outbox" hereafter), so that the outbox will grow
//! roughly twice as fast as it needs to, while avoiding hammering the datastore with many small
//! requests.
//!
//! This approach, while conceptually simple, carries the cost of holding potentially many,
//! potentially large outboxes in memory at the same time. However, before optimizing, I'd like to
//! focus on good observability and see some real-world usage data.
//!
//! I considered instead holding, for each pagination, a pair of cursors, and serving each page
//! using the cursors. This seems likely to be less memory-intensive, but has a number of
//! disadvantages:
//!
//! - each pagination is "one shot"; the caller cannot send the same pagination token more than once
//! - every pagination requires its own state to be maintained
//!
//! ## API
//!
//! I'm largely modelling this on the Mastodon outbox pagination API. When the outbox is requested
//! with no query parameters, the response will be a summary, something like:
//!
//! ```ignore
//! {
//!   "@context": "https://www.w3.org/ns/activitystreams",
//!   "id": "https://indiemark.net/users/sp1ff/outbox",
//!   "type": "OrderedCollection",
//!   "totalItems": 115,
//!   "first": "https://indiemark.net/users/sp1ff/outbox?page=true",
//! }
//! ```
//!
//! One concern I have is that my scheme gives me no way to pre-compute the "last" element, which _is_
//! supplied by Mastodon.
//!
//! The "page" query parameter indicates that we're beginning a pagination; we'll assemble a page
//! and an obfuscated & signed pagination token.

use std::{
    collections::BTreeMap,
    num::NonZero,
    ops::{Bound, RangeBounds},
    pin::Pin,
    result::Result as StdResult,
};

use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};
use chacha20poly1305::{aead::Aead, ChaCha20Poly1305, Nonce};
use chrono::{DateTime, Utc};
use crypto_common::KeyInit;
use futures::{future, Stream, StreamExt, TryStreamExt};
use hkdf::Hkdf;
use indielinks_shared::{
    api::OutboxToken,
    entities::{Post, UserId, Username},
    origin::Origin,
};
use lazy_static::lazy_static;
use lru::LruCache;
use nonempty_collections::NEVec;
use nonzero::nonzero;
use rand::{rngs::OsRng, RngCore};
use secrecy::ExposeSecret;
use serde::{ser::SerializeStruct, Deserialize, Serialize};
use sha2::Sha256;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use tracing::debug;
use uuid::Uuid;

use crate::{
    ap_entities::{AnnounceOrCreate, Create, Note},
    entities::{LikeReplyShare, User},
    signing_keys::SigningKey,
    storage::{Backend as StorageBackend, DateRange},
    util::UpToThree,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("While fetching all posts for a user outbox, {source}"))]
    AllPosts { source: crate::storage::Error },
    #[snafu(display("Bad pagination token format"))]
    BadToken { backtrace: Backtrace },
    #[snafu(display("While fetching likes, replies & shares for a user outbox, {source}"))]
    FetchLikesRepliesShares { source: crate::storage::Error },
    #[snafu(display("While fetching posts for a user outbox, {source}"))]
    FetchPosts { source: crate::storage::Error },
    #[snafu(display("While deriving the encryption key, {source}"))]
    Hkdf {
        source: hkdf::InvalidLength,
        backtrace: Backtrace,
    },
    #[snafu(display("While fetching all likes, replies & shares for a user outbox, {source}"))]
    LikesRepliesShares { source: crate::storage::Error },
    #[snafu(display("While parsing a like, reply or share, {source}"))]
    ParseLikeReplyShare { source: crate::ap_entities::Error },
    #[snafu(display("While parsing a Post, {source}"))]
    ParsePost { source: crate::ap_entities::Error },
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

pub type Result<T> = StdResult<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          ActivityKey                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sort key for outbox activities
///
/// The [above](self) requires a total ordering on the activities listed in the outbox. ActivityPub
/// timestamps only seem to go down to the second, which isn't fine-grained enough for me (someone,
/// somewhere, will, perhaps through scripting/automation, generate two activities in the same
/// second). For lack of anything better, I'm adding a second sort key, the UUID we associate with
/// each sort of activity (sorted lexicographically)-- that should be enough to impose a total
/// order.
///
/// Deriving [Ord] would result in an ascending sort order, whereas we want *descending*.
///
/// See also type [PostKey][crate::home_timeline::PostKey].
// Conversion to & from pagination tokens is pretty-much the same as with `PostKey`; I can't tell if
// it's worth it yet to refactor this.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ActivityKey {
    timestamp: DateTime<Utc>,
    id: Uuid,
}

// We have a minor problem, here: how do we represent the beginning of a pagination? We need a key
// that represents a value *later*, or more recent, than any possible value already in the outbox
// (recall we're sorting in descending time order). `chrono` offers just the thing: `MAX_UTC`.
// `uuid` doesn't offer a constant, but it does offer an associated function: `max()`.
lazy_static! {
    pub static ref FIRST_ACTIVITY_KEY: ActivityKey = ActivityKey {
        timestamp: chrono::DateTime::<Utc>::MAX_UTC,
        id: Uuid::max(),
    };
}

impl ActivityKey {
    pub fn new(timestamp: DateTime<Utc>, id: Uuid) -> Self {
        Self { timestamp, id }
    }
    pub fn from_pagination_token(token: &OutboxToken, signing_key: &SigningKey) -> Result<Self> {
        // We have a base64-encoded bytestring "12-octet-nonce | encrypted JSON"
        let buf = BASE64_URL_SAFE_NO_PAD
            .decode(token)
            .context(TokenNotBase64Snafu)?;
        // Split the buffer into nonce & ciphertext
        let (nonce, cipher_text) = buf.split_at_checked(12).context(BadTokenSnafu)?;
        // indielinks signing keys, at the time of this writing, are 64 octets in length (don't
        // remember why I picked that ATM). Use a simple KDF to derive a 32 octet long key, which is
        // what's used by ChaCha20Poly1305.
        let key = ActivityKey::derive_key(signing_key)?;
        let cipher = ChaCha20Poly1305::new(&key);
        let nonce = Nonce::from_slice(nonce);

        serde_json::from_slice::<ActivityKey>(
            cipher
                .decrypt(nonce, cipher_text)
                .context(TokenDecryptSnafu)?
                .as_slice(),
        )
        .context(TokenDeserSnafu)?
        .pipe(Ok)
    }
    /// Turn this sort key into a pagination token
    // `ActivityKey`s are exposed at the API level as `OutboxToken`s-- an opaque representation of
    // `ActivityKey`. Per <https://google.aip.dev/158?utm_source=chatgpt.com> "Page tokens provided by
    // APIs must be opaque (but URL-safe) strings, and must not be user-parseable. This is because
    // if users are able to deconstruct these, they will do so. This effectively makes the
    // implementation details of your API's pagination become part of the API surface, and it
    // becomes impossible to update those details without breaking users."
    pub fn to_pagination_token(&self, signing_key: &SigningKey) -> Result<OutboxToken> {
        // indielinks signing keys, at the time of this writing, are 64 octets in length (don't
        // remember why I picked that ATM). Use a simple KDF to derive a 32 octet long key, which is
        // what's used by ChaCha20Poly1305.
        let key = ActivityKey::derive_key(signing_key)?;
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

        Ok(OutboxToken::new_internal(
            BASE64_URL_SAFE_NO_PAD.encode(out),
        ))
    }
    /// Use an HMAC key derivation function to derive a 32-byte key from a 64-byte [SigningKey]
    fn derive_key(signing_key: &SigningKey) -> Result<chacha20poly1305::Key> {
        let hk = Hkdf::<Sha256>::new(None, signing_key.as_ref().expose_secret());
        let mut key = [0u8; 32];
        hk.expand(b"OutboxToken xchacha20poly1305", &mut key)
            .context(HkdfSnafu)?;
        Ok(key.into())
    }
}

impl Ord for ActivityKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
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

impl PartialOrd for ActivityKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<&Post> for ActivityKey {
    fn from(post: &Post) -> ActivityKey {
        ActivityKey::new(post.posted(), post.id().into())
    }
}

impl From<&LikeReplyShare> for ActivityKey {
    fn from(lrs: &LikeReplyShare) -> ActivityKey {
        match lrs {
            LikeReplyShare::Like(like) => ActivityKey::new(like.posted(), like.id().into()),
            LikeReplyShare::Reply(reply) => ActivityKey::new(reply.posted(), reply.id().into()),
            LikeReplyShare::Share(share) => ActivityKey::new(share.posted(), share.id().into()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use uuid::uuid;

    #[test]
    fn test_activity_key_ord() {
        // Make sure `FIRST_ACTIVITY_KEY` is before any other key
        let some_date: DateTime<Utc> = "2026-05-29T05:46:00Z"
            .parse()
            .expect("Failed to parse DateTime");
        assert!(
            *FIRST_ACTIVITY_KEY
                < ActivityKey::new(some_date, uuid!("2a4bb9b8-51b7-4f94-ab41-9ec4d7057ca5"))
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             Outbox                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A materialization of an [indielinks] user's [ActivityPub] [outbox].
///
/// [indielinks]: crate
/// [ActivityPub]: https://www.w3.org/TR/activitypub/
/// [outbox]: https://www.w3.org/TR/activitypub/#outbox
pub struct Outbox {
    // This instance's origin (used to form `Announce`s or `Create`s from Replies/Shares and Posts,
    // resp.)
    origin: Origin,
    // The name of the User for this outbox (used to create `Notes`s from `Post`s)
    username: Username,
    /// The activities we've read into memory so far
    items: BTreeMap<ActivityKey, AnnounceOrCreate>,
    /// A [Stream] yielding [indielinks] [Post]s from our backing datastore
    // Not sure if I want to use the raw `Stream` returned from the storage backend, or to wrap it.
    // For now, just do the latter, but this means we need to resolve to a `Result` whose second
    // type parameter is the *storage* `Error`, not our own.
    posts: Pin<Box<dyn Stream<Item = StdResult<Post, crate::storage::Error>> + Send + 'static>>,
    /// A [Streaim] yielding [indielinks] [LikeReplyShare]s from our backing datastore
    // See above.
    lrs: Pin<
        Box<dyn Stream<Item = StdResult<LikeReplyShare, crate::storage::Error>> + Send + 'static>,
    >,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Page {
    pub items: NEVec<AnnounceOrCreate>,
    pub next: Option<ActivityKey>,
}

// Avaialbe on nightly, but I don't feel like changing my entire toolchain just to save a few lines
// of code.
struct RangeAbove<'a> {
    pub start: &'a ActivityKey,
}

impl<'a> RangeBounds<ActivityKey> for RangeAbove<'a> {
    fn start_bound(&self) -> Bound<&ActivityKey> {
        Bound::Excluded(self.start)
    }

    fn end_bound(&self) -> Bound<&ActivityKey> {
        Bound::Unbounded
    }
}

// Helper newtype for producing a JSON representation of a `Outbox` for logging/debugging purposes
pub(crate) struct JsonRepr<'a>(pub &'a Outbox);

impl<'a> serde::ser::Serialize for JsonRepr<'a> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let t = self.0;
        let mut st = serializer.serialize_struct("Outbox", 1)?;
        let pairs = t
            .items
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<(ActivityKey, AnnounceOrCreate)>>();
        st.serialize_field("items", &pairs)?;
        st.end()
    }
}

impl Outbox {
    /// Construct a new [Outbox] instance for a given [User]
    ///
    /// This constructs the materialized view of the [ActivityPub] [outbox]. This is separate
    /// and distinct from beginning a pagination. This instance can support arbitrarily many
    /// paginations. To do that, see [Outbox::next()].
    ///
    /// [User]: crate::entities::User
    /// [ActivityPub]: https://www.w3.org/TR/activitypub/
    /// [outbox]: https://www.w3.org/TR/activitypub/#outbox
    pub async fn new(
        origin: Origin,
        user: &User,
        storage: &(dyn StorageBackend + Send + Sync),
    ) -> Result<Self> {
        Ok(Self {
            origin,
            username: user.username().clone(),
            items: BTreeMap::new(),
            posts: Box::pin(
                storage
                    .get_all_posts(user, &UpToThree::None, &DateRange::None, false)
                    .await
                    .context(AllPostsSnafu)?
                    .try_filter(|post| future::ready(post.public())),
            ),
            lrs: Box::pin(
                storage
                    .get_all_likes_replies_and_shares(user)
                    .await
                    .context(LikesRepliesSharesSnafu)?
                    .try_filter(|lrs| {
                        future::ready(matches!(
                            lrs,
                            LikeReplyShare::Reply(..) | LikeReplyShare::Share(..)
                        ))
                    }),
            ),
        })
    }

    /// Continue a pagination
    pub async fn next(
        &mut self,
        before: &ActivityKey,
        num_elem: Option<NonZero<usize>>,
    ) -> Result<Option<Page>> {
        let num_elem = num_elem.unwrap_or(nonzero!(16usize));

        // It may seem tempting to serve a request from the items currently in memory if we have
        // enough laying around, but it may be that some of those items in one of the two sources
        // are older than as-yet-unseen items in the other. Just call `grow()` no matter what to
        // keep the timeline accurate.
        let exhausted = self.grow(num_elem).await?;

        let range = RangeAbove { start: before };

        let items = self
            .items
            .range(range)
            .take(num_elem.get())
            .collect::<Vec<(&ActivityKey, &AnnounceOrCreate)>>();

        if items.is_empty() {
            return Ok(None);
        }

        let after = items.last().unwrap(/* known good */).0.clone();
        debug!("after is {after:#?}");
        let head = items.first().unwrap(/* known good */).1.clone();
        let tail = items
            .into_iter()
            .skip(1)
            .map(|(_, value)| value.clone())
            .collect::<Vec<AnnounceOrCreate>>();

        Ok(Some(Page {
            items: (head, tail).into(),
            next: if exhausted { None } else { Some(after) },
        }))
    }

    /// Grow our list of items into the past
    async fn grow(&mut self, num_elem: NonZero<usize>) -> Result<bool> {
        // Grab `num_elem` `Post`s, convert them to (ActivityKey, Create) pairs, and insert 'em:
        let posts = self
            .posts
            .by_ref()
            .take(num_elem.get())
            .collect::<Vec<StdResult<Post, _>>>()
            .await
            .into_iter()
            .collect::<StdResult<Vec<Post>, _>>()
            .context(FetchPostsSnafu)?;

        let posts_exhausted = posts.len() < num_elem.get();

        posts
            .into_iter()
            // Creating a `Note` from a `Post` is fallible (since we sanitize the HTML), as
            // is going from a `Note` to a `Create`
            .map(|post| {
                Note::new(&post, &self.username, &self.origin)
                    .and_then(|note| note.try_into())
                    .map(|create| ((&post).into(), create))
                    .context(ParsePostSnafu)
            })
            .collect::<Result<Vec<(ActivityKey, Create)>>>()?
            .into_iter()
            .for_each(|(key, create)| {
                self.items.insert(key, AnnounceOrCreate::Create(create));
            });

        // Now, do the same for replies & shares.
        let lrs = self
            .lrs
            .by_ref()
            .take(num_elem.get())
            .collect::<Vec<StdResult<LikeReplyShare, _>>>()
            .await
            .into_iter()
            .collect::<StdResult<Vec<LikeReplyShare>, _>>()
            .context(FetchLikesRepliesSharesSnafu)?;

        let lrs_exhausted = lrs.len() < num_elem.get();

        lrs.into_iter()
            // I guess, for now, I'll render `Reply`s & `Share`s as `Create`s, though Mastodon seems
            // to render them as `Announce`s. This is a fallible operation, so we'll need another collect.
            .map(|lrs| {
                match lrs {
                    // This case should be ruled-out by our constructor
                    LikeReplyShare::Like(_) => unimplemented!(),
                    LikeReplyShare::Reply(ref reply) => {
                        Note::from_reply(reply, &self.username, &self.origin)
                    }
                    LikeReplyShare::Share(ref share) => {
                        Note::from_share(share, &self.username, &self.origin)
                    }
                }
                .and_then(|note| note.try_into())
                .map(|create| ((&lrs).into(), create))
                .context(ParseLikeReplyShareSnafu)
            })
            .collect::<Result<Vec<(ActivityKey, Create)>>>()?
            .into_iter()
            .for_each(|(key, create)| {
                self.items.insert(key, AnnounceOrCreate::Create(create));
            });

        Ok(posts_exhausted && lrs_exhausted)
    }
}

pub type UserOutboxes = LruCache<UserId, Outbox>;
