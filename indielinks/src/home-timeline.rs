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
//! Conceptually, each user has a "home timeline": an ever-growing list of feed items
//! posted by people that user follows, ordered by time. The list goes back in time to the earliest
//! thing posted by anyone the user follows, and is continually growing into the future as follows
//! post new items. It also grows into the past, as the user follows new people who have older
//! content.
//!
//! Not only is the list growing on either end, its elements "in the middle" change as the user
//! follows new people, or unfollows current follows, the list membership will change.
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
//! time for each generation), but it would be inconveient to actually calculate the list even for a
//! given generation, let alone across generations (which would, for starters, require a linear
//! traversal of the list inserting & removing items). But that's OK: the operations we care about
//! don't require that.

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sort key for posts by people we follow
///
/// The [above](home_timeline) implicitly requires there to be a total ordering on follows' posts.
/// ActivityPub timestamps only seem to go down to the second, so that won't be enough (given enough
/// follows, eventually two will post something in the same second). For lack of anything better, I
/// propose adding a second sort key, the URL (sorted lexicographically)-- that should be enough to
/// impose a total order.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PostKey {
    timestamp: DateTime<Utc>,
    id: Url,
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
            } < PostKey {
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
            } > PostKey {
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NextOutboxPage(Url);

/// A type representing the current state of a given outbox pagination
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Pagination {
    /// AcivityPub identifier naming the follow
    id: Url,
    num_items: usize,
    next_page: NextOutboxPage,
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Timeline {
    // Not sure how I want to represent the posts, but let's start with a simple `Url`
    items: VecDeque<Url>,
    follows: HashMap<Url, Pagination>,
}
