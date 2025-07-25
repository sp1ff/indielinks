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

//! # indielinks caching
//!
//! ## Background
//!
//! This module implements a distributed cache using consistent hashing. The idea is that the range
//! of the hash function is distributed around a ring (so if the hash function's output ranges from
//! 0 to `n-1`, 0 & `n-1` would be adjacent to one another in the circle.) We posit `n` physical
//! nodes & `m` virtual nodes per physical node, and we distribute the resulting `n*m` virtual nodes
//! around the ring. When access to a value that hashes to, say, `i` is required, we "walk" around
//! the ring clockwise from `i` until we hit a virtual node. The physical node corresponding to that
//! virtual node is responsible for storing the desired value.
//!
//! ### Example
//!
//! Let's work a simple example. Let's suppose we have three nodes, each with two virtual nodes,
//! so we can identify each node with two naturals n:m (physical node number: 0, 1, 2 and virtual
//! node number: 0, 1). Merely for illustration purposes, we'll use the following "rolling hash":
//! given `k` digits `c_0`, `c_1`, ..., `c_{k_1}`, we'll hash them like so:
//!
//! ```text
//! H = c_0 * 31^k + c_1 * 31^{k-1} + ... + c_{k-1}*31
//! ```
//!
//! Let's give our hash ring a size of 18, so all the arithmetic involved in the computation of
//! `H`, above, will be done modulo 18. Let's compute the hash values of our virtual nodes:
//!
//! ```text
//! 00 |=> 0 * 961 + 0 * 31 = 0    |=>  0
//! 01 |=> 0 * 961 + 1 * 31 = 31   |=> 13
//! 10 |=> 1 * 961 + 0 * 31 = 961  |=>  7
//! 11 |=> 1 * 961 + 1 * 31 = 992  |=>  2
//! 20 |=> 2 * 961 + 0 * 31 = 1922 |=> 14
//! 21 |=> 2 * 961 + 1 * 31 = 1953 |=>  9
//! ```
//!
//! Yielding a ring that looks like:
//!
//! ```text
//!                                           1
//!   0   1   2   3   4   5   6   7   8   9   0   1   2   3   4   5   6   7
//! +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
//! |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   | --+
//! +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+   |
//!  0:0     1:1                 1:0     2:1             0:1 2:0                |
//!   ^                                                                         |
//!   |                                                                         |
//!   +-------------------------------------------------------------------------+
//! ```
//!
//! Alright, not a great distribution, but then this is a highly-contrived example. Now, suppose we
//! hash a given key to, say, `4`. We would walk the ring to the right, until we hit `7` where we
//! find "1:0" (i.e. node 1, virtual node 0)-- physical node 1 is responsible for this value. If we
//! were to hash a key to `16`, we would again walk to the right, hit the end, wrap around and find
//! that physical node 0 is responsible for this value.
//!
//! ### Adding & Removing Nodes
//!
//! One of the advantages to constistent hashing is that nodes can be added & removed without
//! changing the responsibility for _every_ partition. Adding a node only affects the segment
//! between its predecessor on the ring and itself: the keys in that range remap to the new node.
//! Removing a node only causes its segment to "collapse" onto its successor.
//!
//! At least initially, I plan on an even simpler way to handle membership changes: do nothing. When
//! a node loses responsibility for any given partition, it will simply receive no more requests for
//! that partition, and the elements will slowly age-out. When a newly added node joins, it will
//! slowly build-out state as it serves requests for the partitions for which it is newly
//! responsible. I'll add this to the project backlog of features to be added 1) later and 2) if
//! there seems to be a need.

use std::{fmt::Debug, num::NonZeroUsize};

use lru::LruCache;
use serde::{Serialize, de::DeserializeOwned};
use snafu::{Backtrace, ResultExt, Snafu};
use tracing::debug;

use crate::{network::ClientFactory, raft::CacheNode, types::CacheId};

use std::error::Error as StdError;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Snafu)]
pub enum Error<C>
where
    C: crate::network::Client + 'static,
    C::ErrorType: StdError + std::fmt::Debug + 'static,
{
    #[snafu(display("Failed to insert a remote cache value: {source}"))]
    Insert { source: crate::raft::CacheError<C> },
    #[snafu(display("Failed to lookup a remote cache value: {source}"))]
    Lookup { source: crate::raft::CacheError<C> },
    #[snafu(display("This operation can't be invoked until the Raft is initialized: {source}"))]
    Uninit {
        #[snafu(source(from(crate::raft::Error, Box::new)))]
        source: Box<crate::raft::Error>,
        backtrace: Backtrace,
    },
}

// Deriving this trait would require `C` to implement `Debug`, which I'd prefer not to do
impl<C> std::fmt::Debug for Error<C>
where
    C: crate::network::Client,
    C::ErrorType: StdError + std::fmt::Debug + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Insert { source } => write!(f, "{source}"),
            Error::Lookup { source } => write!(f, "{source}"),
            Error::Uninit { source, .. } => write!(f, "{source}"),
        }
    }
}

pub type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////

/// A simple distributed cache
// Make sure to use a different hash algorithm on the hash ring that on the per-node hash map!
// We use xxhash to map the keys to nodes, then we use the default hasher for `LruCache`, which
// I believe to be foldhash.
pub struct Cache<F: ClientFactory, K, V> {
    id: CacheId,
    node: CacheNode<F>,  // Cheaply clonable
    map: LruCache<K, V>, // This node's partitions' storage
}

impl<F, K, V> Cache<F, K, V>
where
    F: ClientFactory + Send + Sync + Clone + 'static,
    F::CacheClient: Clone + Send + Sync + 'static,
    K: Eq + std::hash::Hash + Serialize + Send + Sync + Debug + for<'a> std::convert::From<&'a K>,
    V: Clone + DeserializeOwned + Serialize + Send + Sync,
{
    pub fn new(id: CacheId, node: impl Into<CacheNode<F>>) -> Cache<F, K, V> {
        Cache {
            id,
            node: node.into(),
            map: LruCache::new(NonZeroUsize::new(256).unwrap(/* known good */)),
        }
    }
    pub async fn get(&mut self, k: &K) -> StdResult<Option<V>, Error<F::CacheClient>> {
        // I need to hash `k`, get the responsible node from our Raft, and, if it's not this node,
        // send a message to that node.
        let nodeid = self.node.node_for_key(k).await.context(UninitSnafu)?;
        debug!("Cache::get(): nodeid {}", nodeid);
        if self.node.id().await == nodeid {
            Ok(self.map.get(k).cloned())
        } else {
            debug!("Querying node {nodeid} for the key");
            Ok(self
                .node
                .cache_lookup::<K, V>(nodeid, self.id, k)
                .await
                .context(LookupSnafu)?)
        }
    }
    pub async fn insert(&mut self, k: K, v: V) -> StdResult<(), Error<F::CacheClient>> {
        let nodeid = self.node.node_for_key(&k).await.context(UninitSnafu)?;
        if self.node.id().await == nodeid {
            self.map.put(k, v);
            Ok(())
        } else {
            self.node
                .cache_insert::<K, V>(nodeid, self.id, k, v)
                .await
                .context(InsertSnafu)?;
            Ok(())
        }
    }
}
