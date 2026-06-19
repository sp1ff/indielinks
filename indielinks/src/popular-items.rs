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

//! # PopularItems
//!
//! ## Introduction
//!
//! This module implements a simple, approximate, solution to the "streaming top-k" problem: given
//! an incoming stream of items, we wish to maintain a running list of the top k most frequently
//! seen item instances. It's generic over the sort of item being counted.
//!
//! ## Usage Note
//!
//! When run in an [indielinks] cluster, there's not a natural way to shard this algorithm. And
//! since the size of the datastructure is bounded, there's not really a compelling reason to do so.
//! Instead, [indielinks] designates a single node as the "owner" of any given top k list and other
//! nodes proxy observations to that owner via gRPC.
//!
//! [indielinks]: ../indielinksd/index.html
//!
//! ## The Algorithm
//!
//! This is a variant of the [space saving algorithm]: maintains a fixed-capacity table of `M`
//! elements and their counters. For each new item:
//!
//! - If the incoming item is already in the table, increment its counter
//! - If the table isn't full, the new item is added with a count of 1
//! - If the table is full and the item is new, it replaces the element with the lowest count. The
//!   new item inherits that minimum count value plus one
//!
//! [space saving algorithm]: https://www.cs.ucsb.edu/sites/default/files/documents/2005-23.pdf
//!
//! However, we'll apply an [exponential discount] to the item's score on update: instead of adding
//! 1 to a counter, we discount existing scores dynamically:
//!
//! [exponential discount]: https://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf
//!
//! - when item $X$ arrives at time $t_{now}$, look up its last update time $t_{last}$ and current score $S$
//! - discount the old score using a decay constant $\lambda$: $S_{decayed}=S\times e^{-\lambda (t_{now}-t_{last})}$
//! - update the item's score: $S_{new} = S_{decayed} + 1$
//! - update the item's timestamp record to $t_{now}$

use std::{
    collections::{hash_map::Entry as HmEntry, HashMap},
    error::Error as StdError,
    net::SocketAddr,
    num::NonZero,
    result::Result as StdResult,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use indielinks_cache::{
    network::ClientFactory as CacheClientFactory,
    raft::CacheNode,
    types::{NodeId, SlotIndex},
};
use indielinks_shared::known_good;
use keyed_priority_queue::{Entry as KpqEntry, KeyedPriorityQueue};
use ordered_float::NotNan;
use serde::{de::DeserializeOwned, Serialize};
use snafu::{prelude::*, Backtrace, IntoError};

use crate::{
    cache::{DecodeSnafu, EncodeSnafu, GrpcClient, GrpcClientFactory, TonicSnafu},
    protobuf_interop::protobuf::{AddSightingsRequest, GetTopKRequest},
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Client error: {source}"))]
    Client {
        // I *hate* erasing the type, but if I don't, the client implementation infects code
        // that has nothing to do with the client (through this type)
        source: Box<dyn StdError + Send + Sync + 'static>,
    },
    #[snafu(display("While encoding to msgpack: {source}"))]
    MsgPackEncoding { source: rmp_serde::encode::Error },
    #[snafu(display("While looking up a peer network location, {source}"))]
    NetLoc {
        source: indielinks_cache::raft::Error,
    },
    #[snafu(display("The Raft has not been initialized"))]
    Uninitialized { backtrace: Backtrace },
}

pub type Result<T> = StdResult<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         Popular Items                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

// Make the `KeyedPriorityQueue` a min-heap, rather than max, by changing the sort order
#[derive(Clone, Debug, Eq)]
struct Priority {
    // `NotNan` implements `Eq`, `Ord` & `Hash`, whereas `f64` does not
    s: NotNan<f64>,
    t: DateTime<Utc>,
}

impl Priority {
    pub fn new() -> Self {
        Self {
            s: known_good!(NotNan::new(1.0)),
            t: Utc::now(),
        }
    }
    pub fn from_last(last: &Priority, lambda: NotNan<f64>) -> Self {
        let now = Utc::now();
        let s = last.s
            * (-lambda * known_good!(NotNan::new((now - last.t).num_seconds() as f64))).exp()
            + 1.0;
        Self {
            s: known_good!(NotNan::new(s)),
            t: now,
        }
    }
}

impl Ord for Priority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.s < other.s {
            std::cmp::Ordering::Greater
        } else if self.s == other.s {
            self.t.cmp(&other.t).reverse()
        } else {
            std::cmp::Ordering::Less
        }
    }
}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Priority {
    fn eq(&self, other: &Self) -> bool {
        self.s == other.s
    }
}

/// Space-saving streaming top-k collection
// Aside from building my own (which it may come to), I had two choices in terms of extant crates:
// keyed-priority-queue & priority-queue. I've gone with the former since it offers an "entry"
// interface.
pub struct PopularItems<Item>
where
    Item: Eq + std::hash::Hash,
{
    queue: KeyedPriorityQueue<Item, Priority>,
    max_len: NonZero<usize>,
    lambda: NotNan<f64>,
}

impl<Item> PopularItems<Item>
where
    Item: Clone + Eq + std::hash::Hash,
{
    pub fn new(max_len: NonZero<usize>) -> Self {
        Self {
            queue: KeyedPriorityQueue::with_capacity(max_len.get() + 1),
            max_len,
            // $\lambda$ is chosen so as to degrade a contribution by one-half after a week.
            // If you want to re-calibrate, the formula is $- ln(0.5)/DURATION-IN-SECONDS$
            lambda: known_good!(NotNan::new(1.146_076_687_433_772e-6)),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
    pub fn len(&self) -> usize {
        self.queue.len()
    }
    // Record a sighting of one instance of the item we are tracking
    // - if the item is already in the table, just update its score; else:
    // - if the table isn't full, add the new item with a score of 1.0
    // - if the table is full, replace the lowest-ranked item in the table, updating the score
    pub fn record_sighting(&mut self, item: Item) {
        // `entry()` (below) takes a `&mut self`, so we need to collect any information we might
        // need that requires a `&self` here (cloning if need be).
        let len = self.queue.len();
        let least: Option<(Item, Priority)> = self
            .queue
            .peek()
            .map(|(item, priority)| (item.clone(), priority.clone()));
        match self.queue.entry(item) {
            KpqEntry::Occupied(occupied) => {
                let new_priority = Priority::from_last(occupied.get_priority(), self.lambda);
                // `set_priority()` consumes `self` so we need to release the immutable borrow
                // via `get_priority()` (above) before invoking it.
                occupied.set_priority(new_priority);
            }
            KpqEntry::Vacant(vacant) => {
                match least {
                    Some((least_item, least_priority)) => {
                        // The queue is *not* empty, so we may or may not need to drop an item.
                        if len == self.max_len.get() {
                            vacant.set_priority(Priority::from_last(&least_priority, self.lambda));
                            self.queue.remove_entry(&least_item);
                        } else {
                            vacant.set_priority(Priority::new());
                        }
                    }
                    None => {
                        // If the queue is empty, we can't possibly be at capacity-- just add the
                        // new/first item:
                        vacant.set_priority(Priority::new())
                    }
                }
            }
        }
    }
    // Retrieve the current "top k" items
    // Include the scores, I guess (?) they could be useful to the caller
    pub fn top_k(&self, num_items: NonZero<usize>) -> Vec<(Item, NotNan<f64>)> {
        let mut v: Vec<(Item, Priority)> = self
            .queue
            .iter()
            .map(|(item, priority)| (item.clone(), priority.clone()))
            .collect();
        v.sort_by(|left, right| left.1.cmp(&right.1));
        v.into_iter()
            .take(num_items.get())
            .map(|(item, Priority { s, t: _ })| (item, s))
            .collect()
    }
}

#[cfg(test)]
mod top_k_tests {

    use approx::assert_abs_diff_eq;
    use nonzero::nonzero;

    use super::*;

    #[test]
    fn validate_priority_queue() {
        // Create a priority queue, add a few items, ensure we've got a min heap:
        let mut queue: KeyedPriorityQueue<i32, Priority> = KeyedPriorityQueue::new();
        queue.push(
            1,
            Priority {
                s: known_good!(NotNan::new(1.0)),
                t: Utc::now(),
            },
        );
        queue.push(
            2,
            Priority {
                s: known_good!(NotNan::new(2.0)),
                t: Utc::now(),
            },
        );

        let least = queue.peek().expect("Minimum item");
        assert!(*least.0 == 1);
    }

    #[test]
    fn smoke() {
        let mut items = PopularItems::<i32>::new(nonzero!(2usize));
        items.record_sighting(1);
        items.record_sighting(1);
        items.record_sighting(2);
        items.record_sighting(3);

        assert_eq!(items.len(), 2);

        let v = items.top_k(nonzero!(2usize));
        // Should be:
        // [
        //     (
        //         3,
        //         2.0,
        //     ),
        //     (
        //         1,
        //         2.0,
        //     ),
        // ]
        assert_eq!(v[0].0, 3);
        assert_eq!(v[1].0, 1);
        // but we of course have to be careful when comparing floating-point numbers:
        assert_abs_diff_eq!(v[0].1.into_inner(), 2.0, epsilon = f64::EPSILON);
        assert_abs_diff_eq!(v[1].1.into_inner(), 2.0, epsilon = f64::EPSILON);

        let mut items = PopularItems::<i32>::new(nonzero!(16usize));
        items.record_sighting(1); // [(1, 1.0)]
        items.record_sighting(2); // [(2, 1.0), (1, 1.0)]
        items.record_sighting(3); // [(3, 1.0), (2, 1.0), (1, 1.0)]
        items.record_sighting(1); // [(1, 2.0), (3, 1.0), (2, 1.0)]
        items.record_sighting(2); // [(2, 2.0), (1, 2.0), (3, 1.0)]
        items.record_sighting(1); // [(1, 3.0), (2, 2.0), (3, 1.0)]

        assert_eq!(items.len(), 3);

        let v = items.top_k(nonzero!(16usize));
        // Should be:
        // [
        //     (
        //         1,
        //         3.0,
        //     ),
        //     (
        //         2,
        //         2.0,
        //     ),
        //     (
        //         3,
        //         1.0,
        //     ),
        // ]
        assert_eq!(v[0].0, 1);
        assert_abs_diff_eq!(v[0].1.into_inner(), 3.0, epsilon = f64::EPSILON);
        assert_eq!(v[1].0, 2);
        assert_abs_diff_eq!(v[1].1.into_inner(), 2.0, epsilon = f64::EPSILON);
        assert_eq!(v[2].0, 3);
        assert_abs_diff_eq!(v[2].1.into_inner(), 1.0, epsilon = f64::EPSILON);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                      A facade that makes a PopularItems type appear local                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait ClientFactory<Item> {
    type Client: Client<Item>;
    async fn new_client(&self, node: NodeId, addr: SocketAddr) -> Self::Client;
}

#[async_trait]
pub trait Client<Item> {
    type Error: StdError + Send + Sync + 'static;
    async fn add_sightings<I: Iterator<Item = Item> + Send>(
        &mut self,
        slot: SlotIndex,
        iter: I,
    ) -> StdResult<(), Self::Error>;
    async fn get_top_k(
        &mut self,
        slot: SlotIndex,
        k: NonZero<usize>,
    ) -> StdResult<Vec<(Item, NotNan<f64>)>, Self::Error>;
}

/// A facade for a streaming top-k collection that _looks_ local but in fact is maintained
/// completely on one node in the indielinks cache
pub struct CachedTopK<Item, CCF, F>
where
    Item: Eq + std::hash::Hash,
    CCF: CacheClientFactory,
    F: ClientFactory<Item>,
{
    slot: SlotIndex,
    factory: F,
    clients: HashMap<NodeId, F::Client>,
    cache_node: CacheNode<CCF>,
    items: PopularItems<Item>,
}

impl<Item, CCF, F> CachedTopK<Item, CCF, F>
where
    Item: Clone + Eq + std::hash::Hash,
    CCF: CacheClientFactory + Send + Sync + Clone + 'static,
    <CCF as CacheClientFactory>::CacheClient: Send + Sync + Clone,
    F: ClientFactory<Item>,
    <F as ClientFactory<Item>>::Client: Clone,
{
    pub fn new(
        slot: SlotIndex,
        factory: F,
        cache_node: CacheNode<CCF>,
        max_len: NonZero<usize>,
    ) -> Self {
        Self {
            slot,
            factory,
            clients: HashMap::new(),
            cache_node,
            items: PopularItems::new(max_len),
        }
    }
    pub async fn add_sightings(&mut self, items: impl Iterator<Item = Item> + Send) -> Result<()> {
        match self.responsible().await? {
            Some((node_id, addr)) => {
                self.client_for_node_id(node_id, addr)
                    .await
                    .add_sightings(self.slot, items)
                    .await
                    .map_err(|e| ClientSnafu.into_error(Box::new(e)))?;
            }
            None => {
                items.for_each(|item| self.items.record_sighting(item));
            }
        }
        Ok(())
    }
    pub async fn get_top_k(&mut self, k: NonZero<usize>) -> Result<Vec<(Item, NotNan<f64>)>> {
        match self.responsible().await? {
            Some((node_id, addr)) => self
                .client_for_node_id(node_id, addr)
                .await
                .get_top_k(self.slot, k)
                .await
                .map_err(|e| ClientSnafu.into_error(Box::new(e))),
            None => Ok(self.items.top_k(k)),
        }
    }
    // Given a node ID, retrieve a client
    async fn client_for_node_id(&mut self, node_id: NodeId, addr: SocketAddr) -> F::Client {
        match self.clients.entry(node_id) {
            HmEntry::Occupied(entry) => entry.get().clone(),
            HmEntry::Vacant(entry) => {
                let client = self.factory.new_client(node_id, addr).await;
                entry.insert(client.clone());
                client
            }
        }
    }
    // Determine whether the indielinks node on which we're current executing is responsible for
    // this "top-k" list. If so, return `None`. If not, return a pair consisting of the Node ID and
    // network location of the responsible node.
    async fn responsible(&self) -> Result<Option<(NodeId, SocketAddr)>> {
        let owner = self
            .cache_node
            .node_for_slot(self.slot)
            .await
            .context(UninitializedSnafu)?;
        if owner == self.cache_node.id().await {
            Ok(None)
        } else {
            Ok(Some((
                owner,
                self.cache_node
                    .socket_addr_for_id(owner)
                    .await
                    .context(NetLocSnafu)?,
            )))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//              comms traits implementations for `GrpcClientFactory` & `GrpcClient`               //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<Item> ClientFactory<Item> for GrpcClientFactory
where
    Item: DeserializeOwned + Send + Sync + Serialize + 'static,
{
    type Client = GrpcClient;
    async fn new_client(&self, node: NodeId, addr: SocketAddr) -> Self::Client {
        GrpcClient::new(node, addr)
    }
}

#[async_trait]
impl<Item> Client<Item> for GrpcClient
where
    Item: DeserializeOwned + Send + Sync + Serialize + 'static,
{
    type Error = crate::cache::Error;
    async fn add_sightings<I: Iterator<Item = Item> + Send>(
        &mut self,
        slot: SlotIndex,
        items: I,
    ) -> StdResult<(), Self::Error> {
        self.ensure_connected()
            .await?
            .add_sightings(AddSightingsRequest {
                slot: slot.get() as u64,
                items: rmp_serde::to_vec(&items.collect::<Vec<Item>>()).context(EncodeSnafu)?,
            })
            .await
            .context(TonicSnafu)
            .map(|_| ())
    }
    async fn get_top_k(
        &mut self,
        slot: SlotIndex,
        k: NonZero<usize>,
    ) -> StdResult<Vec<(Item, NotNan<f64>)>, Self::Error> {
        self.ensure_connected()
            .await?
            .get_top_k(GetTopKRequest {
                slot: slot.get() as u64,
                num_items: k.get() as u64,
            })
            .await
            .context(TonicSnafu)
            .and_then(|response| {
                rmp_serde::from_slice::<Vec<(Item, NotNan<f64>)>>(&response.into_inner().items)
                    .context(DecodeSnafu)
            })
    }
}
