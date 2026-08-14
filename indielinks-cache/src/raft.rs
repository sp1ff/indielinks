// Copyright (C) 2025-2026 Michael Herstine <sp1ff@pobox.com>
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

//! # The [indielinks-cache] [Raft] implementation
//!
//! [indielinks-cache]: crate
//! [Raft]: https://raft.github.io/raft.pdf
//!
//! ## Introduction
//!
//! [indielinks-cache] is built on top of the [openraft] crate. [openraft] requires [Raft]
//! implementations built on top of it to implement a set of traits. THis module contains the
//! implementations of the traits concerning the [Raft] state machine & persistence: [LogReader],
//! [RaftLogStorage], and [RaftStateMachine].
//!
//! [LogReader]: openraft::storage::RaftLogStorage::LogReader
//!
//! ## Discussion
//!
//! The [openraft] abstractions are not... easily aprehended. Or maybe it's just me. Regardless,
//! I _think_ I've wrapped my head around them, and I want to commit my thoughts here for future
//! reference.
//!
//! As a brief overview, [Raft] is a consensus protocol for synchronizing state among the nodes of a
//! distributed cluster. It doesn't synchronize state directly; rather, it guarantees that each node
//! in the cluster will see a sequence of log messages, and that each node will see them in the same
//! order. The log messages notionally carry state changes. Any particular node's state at any given
//! moment is the result of applying each log message it has seen, in order. This leaves open the
//! possibility of some nodes lagging behind others, but, eventually, they are all guaranteed to see
//! each message.
//!
//! A natural way to model each node in the cluster is as a state machine that shifts in response to
//! both incoming log messages & other, protocol-defined events such as becoming a leader or a
//! follower, compacting logs, and so on.
//!
//! This crate, [indielinks-cache] is a library for building distributed key-value stores on top of
//! the [Raft] protocol. I've tried to make it generic, but, clearly, its intended audience is the
//! [indielinks] crate. [indielinks-cache], in turn, relies upon the [openraft] crate for the core
//! [Raft] implementation.
//!
//! [indielinks]: ../indielinks/index.html
//!
//! There are three primary traits we need to implement in order to work with [openraft]
//!
//! - [RaftLogReader] for allowing [openraft] to _read_ application-defined log messages from
//!   durable store
//! - [RaftLogStorage] to allow [openraft] to generally manage the node state: store & retrieve
//!   votes, truncate or purge the log, &c
//! - [RaftStateMachine] to allow [openraft] to manage the state machine
//!
//! An application developer seeking to implement them might be forgiven for approaching them
//! as three independent abstractions, but they're not... at all.
//!
//! Firstly, [RaftLogStorage] is constrained to implement [RaftLogReader] as well, leading the
//! sample implementations I perused to just implement the two traits on the same abstraction, an
//! approach I've taken here.
//!
//! Secondly, and this is a mistake I made in my first implementation, one might assume that, having
//! implement [RaftLogStorage], you're done with dealing with persistent, durable storage, and you
//! are free to focus on making your [RaftStateMachine] implementation strictly an in-memory affair.
//! And the [openraft] [docs] even encourage this! "The state machine in the Raft application is
//! typically an in-memory component."
//!
//! [docs]: https://docs.rs/openraft/latest/openraft/docs/components/state_machine/index.html
//!
//! But it's not so: when [RaftStateMachine::install_snapshot] is invoked, we need to store
//! _durably_. If we don't, and the node is restarted, it will need to receive all the log messages
//! since the beginning from the leader, who likely won't have them (since it just took a snapshot
//! of its state).
//!
//! In my opinion, the [openraft] docs could be much clearer on this, though I suppose the authors
//! could reasonably argue that, at the time of my first implementation, I didn't understand the
//! protocol well enough. Which is true enough, except that's why I was using a library in the first
//! place!
//!
//! Regardless: here's a diagram illustrating the current implementation:
//!
//! ```text
//!
//! +------------------+                    +--------------+
//! | StateMachineData |                    |   LogStore   |
//! +------------------+                    +--------------+
//! | ring, slots, &c  |                    | Arc<Backend> |
//! +------------------+  RaftStateMachine  +--------------+
//!   ^       ^             ^                |     ^
//!   |       |             |       <<implements>> |
//!   |      data    <<implements>>          |     |
//!   |       |             |                v     |
//!   |  +---------------------+    RaftLogReader  |
//!   |  | StateMachineStorage |    RaftLogStorage |
//!   |  +---------------------+                   |
//!   |  |     Arc<Backend>    |                   |
//!   |  +---------------------+                   |
//!   |           ^                                |
//!   |           |                                |
//!   |           +--------------------------------+
//!   |                             |
//!   |                       +----------+
//!   |                       |   Raft   |
//!   |                       +----------+
//!   |                             |
//!   +-----------------------------+
//!                 |
//!        +-----------------+
//!        |    CacheNode    |
//!        +-----------------+
//!        | id, clients, &c |
//!        +-----------------+
//!
//! ```

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, hash_map::Entry as HashEntry},
    convert::identity,
    error::Error as StdError,
    fmt::Debug,
    io::Cursor,
    net::SocketAddr,
    num::NonZero,
    ops::{Bound, RangeBounds},
    result::Result as StdResult,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{prelude::*, stream::iter};
use itertools::iproduct;
use non_zero::non_zero;
use nonempty_collections::IntoNonEmptyIterator;
use openraft::{
    Entry, ErrorSubject, ErrorVerb, LogId, LogState, Membership, OptionalSend, Raft, RaftLogId,
    RaftLogReader, RaftSnapshotBuilder, Snapshot, SnapshotMeta, SnapshotPolicy, StorageIOError,
    StoredMembership, Vote,
    error::{ClientWriteError, InstallSnapshotError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    storage::{LogFlushed, RaftLogStorage, RaftStateMachine},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use tokio::sync::RwLock;
use tracing::{debug, info};
use typenum::Unsigned;
use xxhash_rust::xxh64::Xxh64Builder;

use crate::{
    network::{Client, ClientFactory, Network},
    types::{
        CacheId, ClusterNode, NUMBER_OF_CACHE_SLOTS, NodeId, Request, Response, SlotIndex,
        TypeConfig,
    },
};

pub use openraft::{ChangeMembers, StorageError, raft::ClientWriteResponse};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module error types                                       //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to change cluster membership: {source}"))]
    ChangeMembership {
        #[snafu(source(from(RaftError<NodeId, ClientWriteError<NodeId, ClusterNode>>, Box::new)))]
        source: Box<RaftError<NodeId, ClientWriteError<NodeId, ClusterNode>>>,
    },
    #[snafu(display("Invalid configuration: {source}"))]
    Config {
        #[snafu(source(from(openraft::ConfigError, Box::new)))]
        source: Box<openraft::ConfigError>,
    },
    #[snafu(display("Can't hash a key with an empty hash ring"))]
    EmptyRing { backtrace: Backtrace },
    #[snafu(display("While checking for an initial snapshot, {source}"))]
    InitialSnapshot {
        #[snafu(source(from(StorageError<NodeId>, Box::new)))]
        source: Box<StorageError<NodeId>>,
        backtrace: Backtrace,
    },
    #[snafu(display("While installing the initial snapshot, {source}"))]
    InstallInitialSnapshot {
        #[snafu(source(from(StorageError<NodeId>, Box::new)))]
        source: Box<StorageError<NodeId>>,
        backtrace: Backtrace,
    },
    #[snafu(display("Unknown Node ID {node_id}"))]
    NoNode {
        node_id: NodeId,
        backtrace: Backtrace,
    },
    #[snafu(display("No nodes in membership change"))]
    NoNodes { backtrace: Backtrace },
    #[snafu(display("Failed to create the Raft: {source}"))]
    Raft {
        #[snafu(source(from(openraft::error::Fatal<NodeId>, Box::new)))]
        source: Box<openraft::error::Fatal<NodeId>>,
    },
    #[snafu(display("Failed to initialize the Raft: {source}"))]
    RaftInit {
        #[snafu(source(from(openraft::error::RaftError<
                NodeId,
                openraft::error::InitializeError<NodeId, ClusterNode>,
            >, Box::new)))]
        source: Box<
            openraft::error::RaftError<
                NodeId,
                openraft::error::InitializeError<NodeId, ClusterNode>,
            >,
        >,
    },
    #[snafu(display("Raft client write failed; {source}"))]
    RaftWrite {
        #[snafu(source(from(openraft::error::RaftError<
                NodeId,
                openraft::error::ClientWriteError<NodeId, ClusterNode>,
            >, Box::new)))]
        source: Box<
            openraft::error::RaftError<
                NodeId,
                openraft::error::ClientWriteError<NodeId, ClusterNode>,
            >,
        >,
        backtrace: Backtrace,
    },
}

pub type Result<T> = StdResult<T, Error>;

/// Separate error type for cache-related errors to avoid having to carry the client type
/// around as part of the module error type.
#[derive(Snafu)]
pub enum CacheError<C>
where
    C: crate::network::Client,
    C::ErrorType: StdError + Debug + 'static,
{
    #[snafu(display("While looking up a node: {source}"))]
    BadId {
        #[snafu(source(from(Error, Box::new)))]
        source: Box<Error>,
        backtrace: Backtrace,
    },
    #[snafu(display("Network error: {source}"))]
    Network {
        #[snafu(source(from(C::ErrorType, Box::new)))]
        source: Box<C::ErrorType>,
        backtrace: Backtrace,
    },
}

// Deriving this trait would require `C: Debug`, which I'd prefer not to do
impl<C> std::fmt::Debug for CacheError<C>
where
    C: crate::network::Client,
    C::ErrorType: StdError + std::fmt::Debug + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::BadId { source, .. } => write!(f, "{source}"),
            CacheError::Network { source, .. } => write!(f, "{source}"),
        }
    }
}

// Combinator trait to keep the `OptionalSend` trait bound on a dyn-compatible trait method, below
pub trait LogEntryIterator: Iterator<Item = Entry<TypeConfig>> + OptionalSend {}

impl<T> LogEntryIterator for T where T: Iterator<Item = Entry<TypeConfig>> + OptionalSend {}

/// Convenience type for serializing [Snapshot]s
#[derive(Clone, Debug, Serialize)]
pub struct StoredSnapshotRef<'a> {
    pub meta: &'a SnapshotMeta<NodeId, ClusterNode>,
    pub snapshot: &'a [u8],
}

/// Convenience type for deserializing [Snapshot]s
#[derive(Clone, Debug, Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, ClusterNode>,
    pub snapshot: Vec<u8>,
}

impl From<StoredSnapshot> for Snapshot<TypeConfig> {
    fn from(value: StoredSnapshot) -> Self {
        Snapshot {
            meta: value.meta,
            snapshot: Box::new(Cursor::new(value.snapshot)),
        }
    }
}

/// Object-safe trait abstracting over the storage backend for operations required by [Raft]
///
/// The [LogStore] will persist [Raft] log messages and the `StateMachineStorage` will persist
/// snapshots to the [indielinks](crate) storage backend, which in production may be ScyllaDB or
/// DynamoDB. This trait is intended to be implemented by either of the corresponding backends, and
/// an implementation given to this module's implementations.
#[async_trait]
pub trait Backend {
    /// Append log entries; they must be durably committed to backing store before returning.
    // The weird `dyn Iterator<...>` is to keep this trait dyn-compatible. Callers should use
    // the the `BackendExt` trait, below
    async fn append(
        &self,
        entries: &mut dyn LogEntryIterator,
    ) -> StdResult<(), StorageError<NodeId>>;
    /// Remove all state in the backing store; used only for testing, at the time of this writing
    async fn drop_all_rows(&self) -> StdResult<(), StorageError<NodeId>>;
    /// Returns the last deleted log id and the last log id.
    ///
    /// Per the [openraft] [docs], "The \[implementation\] should *not* consider the applied log id in
    /// \[the\] state machine. The returned last_log_id could be the log id of the last present log
    /// entry, or the last_purged_log_id if there is no entry at all."
    ///
    /// [docs]: https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html#tymethod.get_log_state
    async fn get_log_state(&self) -> StdResult<LogState<TypeConfig>, StorageError<NodeId>>;
    /// Remove all logs up to & including `log_id`. This must not leave a "hole" in the logs.
    async fn purge(&self, log_id: LogId<NodeId>) -> StdResult<(), StorageError<NodeId>>;
    /// Read a [StoredSnapshot] to storage, if it exists
    async fn read_snapshot(&self) -> StdResult<Option<Snapshot<TypeConfig>>, StorageError<NodeId>>;
    /// Return the most recently saved [Vote], if it exists.
    async fn read_vote(&self) -> StdResult<Option<Vote<NodeId>>, StorageError<NodeId>>;
    /// Save a [StoredSnapshot] to storage
    async fn save_snapshot(
        &self,
        meta: &SnapshotMeta<NodeId, ClusterNode>,
        snapshot_bytes: &[u8],
    ) -> StdResult<(), StorageError<NodeId>>;
    /// Save a [Vote] to storage; the [Vote] must be committed to backing storage on return.
    async fn save_vote(&self, vote: &Vote<NodeId>) -> StdResult<(), StorageError<NodeId>>;
    /// Remove all logs since `log_id`, inclusive. This must not leave a "hole" in the log.
    async fn truncate(&self, log_id: LogId<NodeId>) -> StdResult<(), StorageError<NodeId>>;
    /// Get a series of log entries from storage.
    ///
    /// The [openraft] [docs] promise "The start value is inclusive in the search and the stop value
    /// is non-inclusive: [start, stop)" but it would seem improdent to rely on that.
    ///
    /// [docs]: https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogReader.html#tymethod.try_get_log_entries
    async fn try_get_log_entries(
        &self,
        lower_bound: Bound<&u64>,
        upper_bound: Bound<&u64>,
    ) -> StdResult<Vec<Entry<TypeConfig>>, StorageError<NodeId>>;
}

#[async_trait]
trait BackendExt: Backend {
    async fn append_from<I>(&self, entries: I) -> StdResult<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut entries = entries.into_iter();
        self.append(&mut entries).await
    }
}

impl<T: Backend + ?Sized> BackendExt for T {}

pub fn to_storage_io_err(
    subject: ErrorSubject<NodeId>,
    verb: ErrorVerb,
    source: impl Into<openraft::AnyError>,
) -> StorageError<NodeId> {
    StorageError::<NodeId>::IO {
        source: StorageIOError::<NodeId>::new(subject, verb, source),
    }
}

/// The [indielinks-cache](crate) [Raft] log storage
///
/// [LogStore] holds a reference to the backend, which it uses to implement trait [RaftLogStorage]
/// and [RaftLogReader]. The reader may wonder: why a separate type? Why not simply implement
/// [RaftLogStorage] on struct `StateMachineStorage` (defined below)? Primarily because [openraft]'s
/// model encourages that: at the end of the day, we're going to be constructing a [Raft]
/// implementation, which demands [RaftLogStorage] and [RaftStateMachine] implementations as two
/// separate values. Yes, yes, we could, of course, implement them on
/// `Arc<MyBigStateMachineImplementation>` and hand two copies to [Raft::new], but for now I decided
/// to keep it simple. This is how the sample I read handled it, as well, FWIW.
// In the samples I read for the first draft, the log storage type was `Clone` via wrapping a
// non-clonable inner. That said, I couldn't see why it should be: once constructed we just move it
// into the `Raft`. This is unlike the state machine: there, we need to give a reference to the
// `Raft` while (likely) we keep a reference for the application so that it can read state.
//
// The answer is found in [RaftLogStorage::get_log_reader]; you wouldn't think so-- the name and
// signature suggests that we're returning a separate, new type. However, `RaftLogStorage` is
// contrained to itself implement `RaftLogReader`, which, I suppose, is why every sample I've seen
// just clones itself.
//
// This also complicated the approach of just implementing `RaftLogReader` and `RaftLogStorage`
// directly on my backend implementations; i.e. just dispensing with `LogStore` altogether.
// `scylla::Session` isn't `Clone` (at this time), and so implmeneting `get_log_reader()` would be
// tough. We could pretty easily make it clone (by wrapping the native `scylla`) session field in a
// reference & a guard, but I'd rather not take on that effort and performance hit if I don't have
// to.
#[derive(Clone)]
pub struct LogStore {
    backend: Arc<dyn Backend + Send + Sync>,
}

impl LogStore {
    pub fn new(backend: Arc<dyn Backend + Send + Sync>) -> LogStore {
        LogStore { backend }
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<R>(
        &mut self,
        range: R,
    ) -> StdResult<Vec<Entry<TypeConfig>>, StorageError<NodeId>>
    where
        R: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        self.backend
            .try_get_log_entries(range.start_bound(), range.end_bound())
            .await
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StdResult<LogState<TypeConfig>, StorageError<NodeId>> {
        self.backend.get_log_state().await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> StdResult<(), StorageError<NodeId>> {
        self.backend.save_vote(vote).await
    }

    async fn read_vote(&mut self) -> StdResult<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.backend.read_vote().await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> StdResult<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // The openraft docs hint at copying `entries` into memory, returning immedately thereafter,
        // and only invoking `callback` once some asynchronous process has committed them to backing
        // store. However: "when this method returns, the entries must be readable, i.e., a
        // LogReader can read these entries" We keep no in-memory copy in this implementation, so
        // our only feasible approach is to persist them in-line, then invoke `callback`.
        self.backend.append_from(entries).await?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StdResult<(), StorageError<NodeId>> {
        self.backend.truncate(log_id).await
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> StdResult<(), StorageError<NodeId>> {
        self.backend.purge(log_id).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             hashing                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A thing that can hash virtual nodes and hash keys via the xxhash64 algorithm.
#[derive(Clone, Default)] // `Xxh64Builder` doesn't implement `Debug`
struct Hasher {
    hash: Xxh64Builder,
}

// No constructor; instances are generally created through [default]
impl Hasher {
    /// Hash a virtual node (in our hash ring) to a `u64` via the xxhash64 algorithm
    pub fn hash_node(&self, id: &NodeId, m: usize) -> u64 {
        let mut hash = self.hash.build();
        use std::hash::{Hash, Hasher};
        id.hash(&mut hash);
        hash.write_i8(58); // 58 is ASCII ':'
        hash.write_usize(m);
        hash.finish()
    }
    /// Hash an arbitrary key to a `u64` via the xxhash64 algorithm
    pub fn hash_key<K: std::hash::Hash>(&self, key: &K) -> u64 {
        use std::hash::BuildHasher;
        self.hash.hash_one(key)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       the state machine                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

// It seems handy to have the state machine shared by the `Raft` instance *and* the application
// state. That said, the `raft-kv-memstore` sample seems needlessly complex: they wrap their state
// machine in an inner, then wrap the "outer" in an `Arc` and implement the salient traits on the
// `Arc`.
//
// `Hasher` has no `Debug` implementation, meaning that if we find we need it, we'll have to author
// it by hand.
#[derive(Clone)]
struct StateMachineData {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, ClusterNode>,
    hasher: Hasher,
    num_virtual: NonZero<usize>,
    // The hash ring for our distributed KV store
    ring: Vec<(u64, (NodeId, usize))>,
    // An array of `NodeId`s; handy for designating a single node as being responsible for something
    slots: [Option<NodeId>; NUMBER_OF_CACHE_SLOTS::USIZE],
    initialized: Option<DateTime<Utc>>,
}

impl StateMachineData {
    /// Given an arbitrary hash key, return the node responsible for hosting it
    pub fn node_for_key<K: std::hash::Hash + std::fmt::Debug>(&self, key: &K) -> Result<NodeId> {
        if self.ring.is_empty() {
            return EmptyRingSnafu.fail();
        }

        let shard = self.hasher.hash_key(key);
        let idx = self
            .ring
            .binary_search_by(|(i, _)| i.cmp(&shard))
            .map_err(|i| if i >= self.ring.len() { 0 } else { i })
            .unwrap_or_else(identity);
        debug!(
            "Key {:?} hashed to shard {} which maps to NodeId: {}",
            key, shard, idx
        );
        Ok(self.ring[idx].1.0)
    }
    /// Given a slot index, retrieve the `NodeId` stored therein, if any
    pub fn node_for_slot(&self, fin: SlotIndex) -> Option<NodeId> {
        self.slots[fin.get()]
    }
    /// Retrieve the current hash ring & slot allocations
    // Mostly here for testing purposes, hence allowing the lint:
    #[allow(clippy::type_complexity)]
    pub fn current_state(
        &self,
    ) -> (
        Vec<(u64, (NodeId, usize))>,
        [Option<NodeId>; NUMBER_OF_CACHE_SLOTS::USIZE],
    ) {
        (self.ring.clone(), self.slots)
    }
}

impl Default for StateMachineData {
    fn default() -> Self {
        Self {
            last_applied_log: Default::default(),
            last_membership: Default::default(),
            hasher: Default::default(),
            num_virtual: non_zero!(5usize),
            ring: Default::default(),
            slots: Default::default(),
            initialized: Default::default(),
        }
    }
}

/// The [indielinks-cache] [RaftStateMachine] implementation.
///
/// [Raft] doesn't directly synchronize state: it rather synchronizes an append-only log that is
/// shared among the cluster. Each node's state is the result of applying all log messages received
/// up to the current moment.
///
/// As a result, it's fine to provide accessors on the state machine, but we _cannot_ provide
/// mutators: mutation must be through [Raft::client_write] which will distribute the given log
/// message across the cluster, and only invoke [RaftStateMachine::apply] when appropriate.
///
/// Per the [docs], "The state machine in the Raft application is typically an in-memory component."
///
/// [docs]: https://docs.rs/openraft/latest/openraft/docs/components/state_machine/index.html
#[derive(Clone)]
struct StateMachineStorage {
    // `Arc` because ownership will be shared with the `CacheNode`, below. I can't shake the feeling
    // that I need a lock of some kind, as well, but let's see what the borrow checker tells us.
    // And, indeed, I do: you can't mutate data through an `Arc`. I considered `RefCell`, but when
    // installing a `Snapshot`, I want to update `data` atomically.
    data: Arc<RwLock<StateMachineData>>,
    // We implement `raft::StateMachine` on this type, meaning that we're responsible
    // for loading & persisting snapshots.
    backend: Arc<dyn Backend + Send + Sync>,
}

impl StateMachineStorage {
    pub async fn new(
        data: Arc<RwLock<StateMachineData>>,
        backend: Arc<dyn Backend + Send + Sync>,
    ) -> Result<Self> {
        let maybe_snapshot = backend
            .read_snapshot()
            .await
            .context(InitialSnapshotSnafu)?;
        let mut this = Self { data, backend };
        if let Some(snapshot) = maybe_snapshot {
            debug!(
                "Installing extant snapshot {} into the new StateMachineStorage",
                snapshot.meta.snapshot_id
            );
            this.install_snapshot(&snapshot.meta, snapshot.snapshot)
                .await
                .context(InstallInitialSnapshotSnafu)?;
        }
        Ok(this)
    }

    async fn initialize<N, S>(&self, nodes: N, num_virtual: NonZero<usize>, slots: S)
    where
        N: IntoNonEmptyIterator<Item = NodeId>,
        S: IntoIterator<Item = (SlotIndex, NodeId)>,
    {
        debug!("StateMachineStorage::initialize");
        let mut data = self.data.write().await;
        data.ring = iproduct!(nodes.into_iter(), 0..num_virtual.get())
            .map(|(node_id, m)| (data.hasher.hash_node(&node_id, m), (node_id, m)))
            .collect();
        data.ring.sort();
        data.num_virtual = num_virtual;

        slots
            .into_iter()
            .for_each(|(idx, val)| data.slots[idx.get()] = Some(val));
    }

    async fn insert_nodes<N>(&self, nodes: N)
    where
        N: IntoIterator<Item = NodeId>,
    {
        debug!("StateMachineStorage::insert_nodes");
        let data = self.data.read().await;
        let new_nodes = iproduct!(nodes.into_iter(), 0..data.num_virtual.get())
            .map(|(node_id, m)| (data.hasher.hash_node(&node_id, m), (node_id, m)));

        let mut data = self.data.write().await;
        data.ring.extend(new_nodes);
        data.ring.sort();
    }

    async fn process_entry(
        &self,
        entry: Entry<TypeConfig>,
    ) -> StdResult<Response, StorageError<NodeId>> {
        info!(
            "Replicating {} to this State Machine: {:?}",
            entry.log_id, entry
        );

        let result = match entry.payload {
            openraft::EntryPayload::Blank => Ok(Response(())),
            openraft::EntryPayload::Normal(request) => {
                self.process_request(request).await;
                Ok(Response(()))
            }
            openraft::EntryPayload::Membership(membership) => {
                self.update_membership(entry.log_id, membership).await
            }
        };
        if result.is_ok() {
            self.data.write().await.last_applied_log = Some(entry.log_id);
        }

        result
    }

    async fn process_request(&self, request: Request) {
        debug!("I am applying the following log message to my local state machine: {request:#?}");
        match request {
            Request::Init {
                nodes,
                num_virtual,
                slots,
            } => self.initialize(nodes, num_virtual, slots).await,
            Request::InsertNodes { nodes } => self.insert_nodes(nodes).await,
            Request::RemoveNodes { nodes } => self.remove_nodes(nodes).await,
            Request::SetSlots { slots } => self.set_slots(slots).await,
        }
    }

    async fn remove_nodes<N>(&self, nodes: N)
    where
        N: IntoIterator<Item = NodeId>,
    {
        debug!("StateMachineStorage::remove_nodes");
        let mut data = self.data.write().await;
        let remove = BTreeSet::from_iter(
            iproduct!(nodes.into_iter(), 0..data.num_virtual.get())
                .map(|(node_id, m)| (data.hasher.hash_node(&node_id, m), (node_id, m))),
        );
        data.ring.retain(|x| !remove.contains(x));
    }

    async fn set_slots<S>(&self, slots: S)
    where
        S: IntoIterator<Item = (SlotIndex, Option<NodeId>)>,
    {
        debug!("StateMachineStorage::set_slots");
        let mut data = self.data.write().await;
        slots
            .into_iter()
            .for_each(|(slot, value)| data.slots[slot.get()] = value);
        debug!("My local slots are now: {:#?}", data.slots);
    }

    async fn update_membership(
        &self,
        log_id: LogId<NodeId>,
        membership: Membership<NodeId, ClusterNode>,
    ) -> StdResult<Response, StorageError<NodeId>> {
        debug!("StateMachineStorage::update_membership: {log_id}, {membership:?}");
        // Cluster membership has changed-- the first time this happens is when the
        // cluster is initialized-- record this:
        let mut data = self.data.write().await;
        if data.initialized.is_none() {
            data.initialized = Some(Utc::now());
        }
        data.last_membership = StoredMembership::new(Some(log_id), membership);

        Ok(Response(()))
    }
}

// Now, what the heck *is* a `Snapshot`, anyway? Well, we have:
//
//     pub struct Snapshot<C: RaftTypeConfig>
//     {
//         pub meta: SnapshotMeta<C::NodeId, C::Node>,
//         pub snapshot: Box<C::SnapshotData>,
//     }
//
// so it's "metadata + data". Fine. Let's look at `SnapshotMeta`:
//
//     pub struct SnapshotMeta<NID: NodeId, N: Node>
//     {
//         pub last_log_id: Option<LogId<NID>>,
//         pub last_membership: StoredMembership<NID, N>,
//         pub snapshot_id: SnapshotId,
//     }
//
// so the metadata consists of the last log ID to be incorporated into this snapshot, the last
// membership status known to this snapshot, and an identifier-- not sure what the requirements on
// that last one are. Otherwise, again fine. Ah:
// <https://docs.rs/openraft/latest/openraft/type.SnapshotId.html> it just needs to be "globally
// unique".

// Newtype for serializing application-specific state machine state to the `snapshot` field of a
// `Snapshot`
#[derive(Clone, Debug, Deserialize)]
struct SnapshotState {
    num_virtual: NonZero<usize>,
    ring: Vec<(u64, (NodeId, usize))>,
    slots: [Option<NodeId>; NUMBER_OF_CACHE_SLOTS::USIZE],
}

// Newtype for deserializing application-specific state machine state from the `snapshot` field of a
// `Snapshot`
#[derive(Clone, Debug, Serialize)]
struct SnapshotStateRef<'a> {
    num_virtual: NonZero<usize>,
    ring: &'a Vec<(u64, (NodeId, usize))>,
    slots: &'a [Option<NodeId>],
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStorage {
    /// Per the
    /// [docs](https://docs.rs/openraft/latest/openraft/storage/trait.RaftSnapshotBuilder.html#tymethod.build_snapshot):
    /// "A snapshot has to contain state of all applied log, including membership. Usually it is
    /// just a serialized state machine.
    ///
    /// Building snapshot can be done by:
    ///
    /// - Performing log compaction, e.g. merge log entries that operates on the same key, like
    ///   a LSM-tree does,
    /// - or by fetching a snapshot from the state machine.
    ///
    /// The sample code again seems overly complex to me. AFAICT, we need to:
    ///
    /// 1. build a snapshot of our current state
    /// 2. save a copy into `current_snapshot`
    /// 3. return the snapshot
    async fn build_snapshot(&mut self) -> StdResult<Snapshot<TypeConfig>, StorageError<NodeId>> {
        debug!("RaftSnapshotBuilder::build_snapshot");

        let data = self.data.read().await;

        let snapshot_bytes = serde_json::to_vec(&SnapshotStateRef {
            num_virtual: data.num_virtual,
            ring: &data.ring,
            slots: &data.slots,
        })
        .map_err(|e| StorageIOError::read_state_machine(&e))?;

        let meta = SnapshotMeta::<NodeId, ClusterNode> {
            last_log_id: data.last_applied_log,
            last_membership: data.last_membership.clone(),
            snapshot_id: uuid::Uuid::new_v4().to_string(),
        };

        self.backend
            .save_snapshot(&meta, snapshot_bytes.as_slice())
            .await?;

        Ok(Snapshot::<TypeConfig> {
            meta,
            snapshot: Box::new(Cursor::new(snapshot_bytes)),
        })
    }
}

/// The Big Tuna-- [RaftStateMachine]
impl RaftStateMachine<TypeConfig> for StateMachineStorage {
    type SnapshotBuilder = Self;

    /// "[Return] the last applied log id which is recorded in state machine, and the last applied
    /// membership [configuration]" --
    /// [docs](https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html#tymethod.applied_state)
    ///
    /// Nb. "It is all right to return a membership with greater log id than the
    /// last-applied-log-id. Because upon startup, the last membership will be loaded by scanning
    /// logs from the last-applied-log-id."-- ibid.
    async fn applied_state(
        &mut self,
    ) -> StdResult<
        (Option<LogId<NodeId>>, StoredMembership<NodeId, ClusterNode>),
        StorageError<NodeId>,
    > {
        debug!("RaftStateMachine::applied_state");
        let data = self.data.read().await;
        debug!(
            "Applied state: {:?}, {:?}/{:p}. Self is {:p}",
            data.last_applied_log, data.last_membership, &data.last_membership, &self
        );
        Ok((data.last_applied_log, data.last_membership.clone()))
    }

    /// Apply the given payload of entries to the state machine
    ///
    /// This is where we update our state according to newly-arrived log messages. Per the [docs],
    /// for each entry we shall:
    ///
    /// [docs]: https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html#tymethod.apply
    ///
    /// - Store the log id as last applied log id.
    /// - Deal with the business logic log.
    /// - Store membership config if RaftEntry::get_membership() returns Some.
    ///
    /// And: "An implementation may choose to persist either the state machine or the snapshot:
    ///
    /// - An implementation with persistent state machine: persists the state on disk before
    ///   returning from apply(). So that a snapshot does not need to be persistent.
    /// - An implementation with persistent snapshot: apply() does not have to persist state on
    ///   disk. But every snapshot has to be persistent. And when starting up the application, the
    ///   state machine should be rebuilt from the last snapshot.""
    async fn apply<I>(&mut self, entries: I) -> StdResult<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        debug!("RaftStateMachine::apply");
        iter(entries)
            .then(|entry| self.process_entry(entry))
            .try_collect::<Vec<Response>>()
            .await
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        debug!("RaftStateMachine::get_snapshot_builder");
        self.clone()
    }

    /// I'm still confused as to this method. The [docs] merely say "Create a new blank snapshot,
    /// returning a writable handle to the snapshot object."
    ///
    /// [docs]: https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html#tymethod.begin_receiving_snapshot
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> StdResult<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        debug!("RaftStateMachine::begin_receiving_snapshot");
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    /// Install a snapshot which has finished streaming from the leader.
    ///
    /// This method shall, before returning:
    ///
    /// - replace the state machine with the new contents of the snapshot,
    /// - save the input snapshot (i.e. `Self::get_current_snapshot()` should return it)
    /// - delete all other snapshots
    ///
    /// Here again, the sample seems needlessly complex. All we need to do here is update our
    /// `StateMachine` from the given snapshot, and store the snapshot as the "current" snapshot.
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, ClusterNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> StdResult<(), StorageError<NodeId>> {
        info!("Installing snapshot: {meta:?}");
        // I guess we'll deserialize, first, since that's fallible.
        let SnapshotState {
            num_virtual,
            ring,
            slots,
        } = serde_json::from_slice(snapshot.get_ref())
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        // Store the snapshot durably.
        self.backend.save_snapshot(meta, snapshot.get_ref()).await?;

        // Finally, copy all the data over to our current state.
        let mut data = self.data.write().await;
        data.last_applied_log = meta.last_log_id;
        data.last_membership = meta.last_membership.clone(); // Arrrghhh
        data.num_virtual = num_virtual;
        data.ring = ring;
        data.slots = slots;

        Ok(())
    }

    /// Per the
    /// [docs](https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html#tymethod.get_current_snapshot):
    /// "Implementing this method should be straightforward. Check the configured snapshot directory
    /// for any snapshot files. A proper implementation will only ever have one active snapshot,
    /// though another may exist while it is being created. As such, it is recommended to use a file
    /// naming pattern which will allow for easily distinguishing between the current live snapshot,
    /// and any new snapshot which is being created.
    ///
    /// A proper snapshot implementation will store last-applied-log-id and the
    /// last-applied-membership config as part of the snapshot, which should be decoded for creating
    /// this method’s response data."
    ///
    /// So it _seems_ that we're expected to generate these periodically & keep them, or at least
    /// the most recent one, laying around on disk. It also seems possible that we have yet to
    /// create one, which is why we return an `Option`. That said, the raft-kv-memstore sample just
    /// keeps one in-memory and returns a copy on demand (?)
    async fn get_current_snapshot(
        &mut self,
    ) -> StdResult<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        debug!("RaftStateMachine::get_current_snapshot");
        self.backend.read_snapshot().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        InMemoryBackend                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

// I've gone with the "wrap an inner in an Arc<...>" idiom here so I can preserve the fact that
// each method on `Backend` takes a `&self`, and none take a `&mut self`.
#[derive(Debug, Default)]
struct InMemoryRaftStorage {
    /// The Raft log
    log: BTreeMap<u64, Entry<TypeConfig>>,
    /// The current granted vote.
    vote: Option<Vote<NodeId>>,
    last_purged_log_id: Option<LogId<NodeId>>,
    snapshot: Option<Snapshot<TypeConfig>>,
}

// An in-memory [Backend] implementation; primarily intended for testing. Construct via `Default`.
#[derive(Clone, Debug, Default)]
pub struct InMemoryBackend {
    inner: Arc<RwLock<InMemoryRaftStorage>>,
}

#[async_trait]
impl Backend for InMemoryBackend {
    /// Append log entries (presumably from the cluster leader)
    ///
    /// The contract is that this method shall return immediately after saving the input log entries
    /// in memory, and arrange to have the provided callback invoked once the entries are persisted
    /// on disk. That said, the intent is to avoid blocking in this method; the callback can be
    /// called either before or after this method returns.
    ///
    /// Per the [docs](https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html#tymethod.append):
    ///
    /// - When this method returns, the entries must be readable, i.e., a LogReader can read these entries
    /// - When the callback is called, the entries must be persisted on disk
    /// - There must not be a hole in logs. Because Raft only examine the last log id to ensure correctness
    ///
    /// This implementation is broken in that it doesn't write anything to disk (for now). I'm not
    /// entirely clear on what is meant by a "hole"-- I can only surmise that the log entries are
    /// numbered, and that, at the end of this method, the entries in our log must be sequential (?)
    async fn append(
        &self,
        entries: &mut dyn LogEntryIterator,
    ) -> StdResult<(), StorageError<NodeId>> {
        self.inner
            .write()
            .await
            .log
            .extend(entries.map(|entry| (entry.get_log_id().index, entry)));
        Ok(())
    }
    async fn drop_all_rows(&self) -> StdResult<(), StorageError<NodeId>> {
        let mut this = self.inner.write().await;
        this.log.clear();
        this.vote = None;
        this.last_purged_log_id = None;
        this.snapshot = None;
        Ok(())
    }
    async fn get_log_state(&self) -> StdResult<LogState<TypeConfig>, StorageError<NodeId>> {
        let this = self.inner.read().await;
        Ok(LogState {
            last_purged_log_id: this.last_purged_log_id,
            last_log_id: this
                .log
                .iter()
                .next_back()
                .map(|(_, ent)| *ent.get_log_id())
                .or(this.last_purged_log_id),
        })
    }
    /// Remove all logs up to & including `log_id`. This must not leave a "hole" in the logs.
    async fn purge(&self, log_id: LogId<NodeId>) -> StdResult<(), StorageError<NodeId>> {
        let mut this = self.inner.write().await;

        assert!(this.last_purged_log_id.as_ref() <= Some(&log_id));

        this.last_purged_log_id = Some(log_id);

        let to_be_removed = this
            .log
            .range(..=log_id.index)
            .map(|(k, _)| k)
            .cloned()
            .collect::<Vec<_>>();
        to_be_removed.into_iter().for_each(|k| {
            this.log.remove(&k);
        });

        Ok(())
    }
    /// Read a [StoredSnapshot] to storage, if it exists
    async fn read_snapshot(&self) -> StdResult<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(self.inner.read().await.snapshot.clone())
    }
    /// Return the most recently saved [Vote], if it exists.
    async fn read_vote(&self) -> StdResult<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.read().await.vote)
    }
    /// Save a [StoredSnapshot] to storage
    async fn save_snapshot(
        &self,
        meta: &SnapshotMeta<NodeId, ClusterNode>,
        snapshot_bytes: &[u8],
    ) -> StdResult<(), StorageError<NodeId>> {
        self.inner.write().await.snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(snapshot_bytes.to_vec())),
        });
        Ok(())
    }
    /// Save a [Vote] to storage; the [Vote] must be committed to backing storage on return.
    async fn save_vote(&self, vote: &Vote<NodeId>) -> StdResult<(), StorageError<NodeId>> {
        // In this implementation, we of course don't write to disk:
        self.inner.write().await.vote = Some(*vote);
        Ok(())
    }
    /// Remove all logs since `log_id`, inclusive. This must not leave a "hole" in the log.
    async fn truncate(&self, log_id: LogId<NodeId>) -> StdResult<(), StorageError<NodeId>> {
        let mut this = self.inner.write().await;
        let to_be_removed = this
            .log
            // Weirdly (to me), the openraft examples use the log *index* as a unique identifier
            // when serializing entries. For instance, the in-memory implementatino indexes its map
            // using the index, not the entire `LogId` (despite the fact that `LogId` implements
            // `Ord`).
            .range(log_id.index..)
            .map(|(k, _)| k)
            .cloned()
            .collect::<Vec<_>>();
        to_be_removed.into_iter().for_each(|k| {
            this.log.remove(&k);
        });
        Ok(())
    }
    /// Get a series of log entries from storage.
    ///
    /// The [openraft] [docs] promise "The start value is inclusive in the search and the stop value
    /// is non-inclusive: [start, stop)" but it would seem improdent to rely on that.
    ///
    /// [docs]: https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogReader.html#tymethod.try_get_log_entries
    async fn try_get_log_entries(
        &self,
        lower_bound: Bound<&u64>,
        upper_bound: Bound<&u64>,
    ) -> StdResult<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        self.inner
            .read()
            .await
            .log
            .range((lower_bound, upper_bound))
            .map(|(_, entry)| entry)
            .cloned()
            .collect::<Vec<_>>()
            .pipe(Ok)
    }
}

// Allow callers to test the `RaftStateMachine` & `RaftLogStorage` implementations resulting from
// their `Backend` implementation.
pub mod test_backend_implementations {

    use openraft::testing::{StoreBuilder, Suite};

    use super::*;

    pub struct Builder {
        pub backend: Arc<dyn Backend + Send + Sync>,
    }

    pub struct Dropper {
        pub backend: Arc<dyn Backend + Send + Sync>,
    }

    impl Drop for Dropper {
        fn drop(&mut self) {
            let backend = self.backend.clone();
            let result = tokio::task::block_in_place(move || {
                tokio::runtime::Handle::current()
                    .block_on(async move { backend.drop_all_rows().await })
                    .map_err(Box::new)
            });
            if result.is_err() {
                panic!("Failed to cleanup Raft storage: {result:#?}");
            }
        }
    }

    impl StoreBuilder<TypeConfig, LogStore, StateMachineStorage, Dropper> for Builder {
        async fn build(
            &self,
        ) -> StdResult<(Dropper, LogStore, StateMachineStorage), StorageError<NodeId>> {
            let log_storage = LogStore::new(self.backend.clone());

            let state_machine_data = Arc::new(RwLock::new(StateMachineData::default()));

            let mut state_machine_storage =
                StateMachineStorage::new(state_machine_data.clone(), self.backend.clone())
                    .await
                    .expect("Failed to build StateMachineStorage");

            let snapshot_result = self.backend.read_snapshot().await;
            assert!(snapshot_result.is_ok());

            if let Some(snapshot) = snapshot_result.unwrap() {
                assert!(
                    state_machine_storage
                        .install_snapshot(&snapshot.meta, snapshot.snapshot)
                        .await
                        .is_ok()
                )
            }

            Ok((
                Dropper {
                    backend: self.backend.clone(),
                },
                log_storage,
                state_machine_storage,
            ))
        }
    }

    /// [indielinks-cache] callers can test their backends here
    // Boxed result due to clippy::result-large-err
    pub fn test_backend(
        backend: Arc<dyn Backend + Send + Sync>,
    ) -> StdResult<(), Box<StorageError<NodeId>>> {
        Suite::test_all(Builder { backend }).map_err(Box::new)
    }
}

#[cfg(test)]
pub mod test {

    use crate::raft::test_backend_implementations::test_backend;

    use super::*;

    #[test_log::test]
    #[test]
    fn test_in_memory_backend() {
        assert!(test_backend(Arc::new(InMemoryBackend::default())).is_ok());
    }
}

// ////////////////////////////////////////////////////////////////////////////////////////////////////
// //                             The indielinks-cache public interface                              //
// ////////////////////////////////////////////////////////////////////////////////////////////////////

/// [indielinks-cache] cluster configuration
///
/// Outside of the ID for this node, the configuration should be uniform across the cluster.
///
/// I've exposed a few of the [openraft::Config] fields here.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Configuration {
    #[serde(rename = "cluster-name")]
    pub cluster_name: String,
    /// The numeric ID for *this* node
    #[serde(rename = "this-node")]
    pub this_node: NodeId,
    /// The interval at which leaders will send heartbeats to followers; this defaults to 50ms, which
    /// can make the cluster awfully chatty
    #[serde(rename = "heartbeat-interval")]
    pub heartbeat_interval: Duration,
    /// The lower bound on the election timeout (defaults to 150ms)
    #[serde(rename = "election-timeout-min")]
    pub election_timeout_min: Duration,
    /// The upper bound on the election timeout (defaults to 300ms)
    #[serde(rename = "election-timeout-max")]
    pub election_timeout_max: Duration,
    /// The policy governing log compaction/snapshot formation
    #[serde(rename = "snapshot-policy")]
    pub snapshot_policy: SnapshotPolicy,
}

/// Suitable for a single-node cluster only
impl Default for Configuration {
    fn default() -> Self {
        Self {
            cluster_name: "indielinks".to_owned(),
            this_node: 0,
            heartbeat_interval: Duration::from_millis(500),
            election_timeout_min: Duration::from_millis(1500),
            election_timeout_max: Duration::from_millis(3000),
            snapshot_policy: SnapshotPolicy::LogsSinceLast(5000),
        }
    }
}

impl Configuration {
    pub fn builder(cluster_name: impl Into<String>, this_node: NodeId) -> ConfigurationBuilder {
        ConfigurationBuilder::new(cluster_name, this_node)
    }
}

pub struct ConfigurationBuilder {
    cluster_name: String,
    this_node: NodeId,
    heartbeat_interval: Duration,
    election_timeout_min: Duration,
    election_timeout_max: Duration,
    snapshot_policy: SnapshotPolicy,
}

impl ConfigurationBuilder {
    pub fn new(cluster_name: impl Into<String>, this_node: NodeId) -> ConfigurationBuilder {
        ConfigurationBuilder {
            cluster_name: cluster_name.into(),
            this_node,
            heartbeat_interval: Duration::from_millis(50),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            snapshot_policy: SnapshotPolicy::LogsSinceLast(5000),
        }
    }
    pub fn build(self) -> Configuration {
        Configuration {
            cluster_name: self.cluster_name,
            this_node: self.this_node,
            heartbeat_interval: self.heartbeat_interval,
            election_timeout_min: self.election_timeout_min,
            election_timeout_max: self.election_timeout_max,
            snapshot_policy: self.snapshot_policy,
        }
    }
    pub fn heartbeat_interval(mut self, heartbeat_interval: Duration) -> Self {
        self.heartbeat_interval = heartbeat_interval;
        self
    }
    pub fn election_timeout_min(mut self, election_timeout_min: Duration) -> Self {
        self.election_timeout_min = election_timeout_min;
        self
    }
    pub fn election_timeout_max(mut self, election_timeout_max: Duration) -> Self {
        self.election_timeout_max = election_timeout_max;
        self
    }
    pub fn snapshot_policy(mut self, snapshot_policy: SnapshotPolicy) -> Self {
        self.snapshot_policy = snapshot_policy;
        self
    }
}

// This needs to be built-out: would be interesting to expose memory footprint, e.g.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Metrics {
    pub id: NodeId,
    pub raft: openraft::metrics::RaftMetrics<NodeId, ClusterNode>,
}

// I originally couldn't see a case where you'd want more than one "cluster node" in a process, and
// setup an `AtomicBool` that would record the first instance creation, and had the `CacheNode` ctor
// check it & fail on the second invocation. Since then, however, I realized there _is_ a quite
// important use case for this: testing! I've removed that logic & now let the application author do
// what they want in this regard.

/// A non-[Clone] type representing a node in a [Raft] cluster implementing a distributed cache
///
/// Generally speaking, an application using this crate is going to require shared, mutable access
/// to "the" cache node. To take just one example, each [indielinks] cache is going to need to hold
/// a reference to the process cache node as an integral part of their implementation.
///
/// [indielinks]: ../indielinks/index.html
///
/// My first implementation of this module went with the "non-clone inner wrapped in a clonable
/// outer holding an `Arc`" approach, and I became increasingly dissatisfied with it. Not only did
/// it _force_ users to pay for a lock (an [RwLock], IIRC) whether they wanted it or not, I wound-up
/// having to write a growing number of forwarding functions. This time, I'm going to make the
/// "inner" struct public, and just require callers to use whatever mechanism they want to ensure
/// shared, mutable ownership.
pub struct CacheNode<F: ClientFactory> {
    id: NodeId,
    raft: Raft<TypeConfig>,
    state: Arc<RwLock<StateMachineData>>,
    factory: F,
    clients: HashMap<NodeId, F::CacheClient>,
}

impl<F> CacheNode<F>
where
    F: ClientFactory + Send + Sync + Clone + 'static,
    F::CacheClient: Clone + Send + Sync + 'static,
{
    /// Initialize the current host as a member of an [indielinks-cache] cluster
    pub async fn new(
        backend: Arc<dyn Backend + Send + Sync>,
        config: &Configuration,
        factory: F,
    ) -> Result<CacheNode<F>> {
        let raft_config = Arc::new(
            openraft::Config {
                cluster_name: config.cluster_name.clone(),
                heartbeat_interval: config.heartbeat_interval.as_millis() as u64,
                election_timeout_min: config.election_timeout_min.as_millis() as u64,
                election_timeout_max: config.election_timeout_max.as_millis() as u64,
                ..Default::default()
            }
            .validate()
            .context(ConfigSnafu)?,
        );

        let log_storage = LogStore::new(backend.clone());

        let state_machine_data = Arc::new(RwLock::new(StateMachineData::default()));

        let state_machine_storage =
            StateMachineStorage::new(state_machine_data.clone(), backend).await?;

        // On return from `Raft::new()`, this Raft node will be in the "learner" state. The cluster
        // won't be established until `Raft::initialize()` is invoked on some member of the cluster.
        let raft = openraft::Raft::new(
            config.this_node,              // Node ID
            raft_config,                   // Config
            Network::new(factory.clone()), // Network
            log_storage,                   // Log Store
            state_machine_storage,         // State Machine
        )
        .await
        .context(RaftSnafu)?;

        Ok(CacheNode {
            id: config.this_node,
            raft,
            state: state_machine_data,
            factory,
            clients: HashMap::new(),
        })
    }

    pub async fn add_learner(
        &self,
        id: NodeId,
        node: ClusterNode,
        blocking: bool,
    ) -> StdResult<
        ClientWriteResponse<TypeConfig>,
        Box<RaftError<NodeId, ClientWriteError<NodeId, ClusterNode>>>,
    > {
        self.raft
            .add_learner(id, node, blocking)
            .await
            .map_err(Box::new)
    }

    pub async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> StdResult<AppendEntriesResponse<NodeId>, Box<RaftError<NodeId>>> {
        self.raft.append_entries(rpc).await.map_err(Box::new)
    }

    /// Insert a value into the distributed cache
    ///
    /// This method is intended to _send_ a key/value pair to another node in the cluster; it will
    /// panic if `node_id` names _this_ node.
    pub async fn cache_insert<K: Serialize + Send + Sync, V: Serialize + Send + Sync>(
        &mut self,
        node_id: NodeId,
        cache_id: CacheId,
        k: impl Into<K> + Send,
        v: impl Into<V> + Send,
    ) -> StdResult<(), CacheError<F::CacheClient>> {
        assert!(node_id != self.id); // I think this is panic-worthy
        self.client_for_id(node_id)
            .await
            .context(BadIdSnafu)?
            .cache_insert(cache_id, k, v)
            .await
            .context(NetworkSnafu)
    }

    /// Lookup a value in the distributed cache
    ///
    /// This method is intended to _pull_ a key/value pair to another node in the cluster; it will
    /// panic if `node_id` names _this_ node.
    pub async fn cache_lookup<K: Serialize + Send, V: DeserializeOwned>(
        &mut self,
        node_id: NodeId,
        cache_id: CacheId,
        k: impl Into<K> + Send,
    ) -> StdResult<Option<V>, CacheError<F::CacheClient>> {
        debug!("Inner CacheNode assert: {node_id} != {}", self.id);
        assert!(node_id != self.id); // I think this is panic-worthy
        debug!("cache_lookup: Querying node {node_id} for cache {cache_id}");
        self.client_for_id(node_id)
            .await
            .context(BadIdSnafu)?
            .cache_lookup(cache_id, k)
            .await
            .context(NetworkSnafu)
    }

    pub async fn change_membership(
        &self,
        members: impl Into<ChangeMembers<NodeId, ClusterNode>>,
        retain: bool,
    ) -> Result<ClientWriteResponse<TypeConfig>> {
        let mut rx = self.raft.server_metrics();
        let membership_response = self
            .raft
            .change_membership(members, retain)
            .await
            .context(ChangeMembershipSnafu)?;
        // Won't resolve until there's been a change *since channel creation*
        rx.changed().await.unwrap();

        let node_ids = rx
            .borrow()
            .membership_config
            .nodes()
            .map(|(nid, _)| *nid)
            .collect::<Vec<NodeId>>()
            .try_into()
            .map_err(|_| NoNodesSnafu.build())?;

        self.raft
            .client_write(Request::Init {
                nodes: node_ids,
                num_virtual: non_zero!(1usize),
                slots: Default::default(),
            })
            .await
            .context(RaftWriteSnafu)?;

        Ok(membership_response)
    }

    async fn client_for_id(&mut self, node_id: NodeId) -> Result<F::CacheClient> {
        let node = self.node_for_id(node_id)?;

        match self.clients.entry(node_id) {
            HashEntry::Occupied(occupied_entry) => occupied_entry.get().clone(),
            HashEntry::Vacant(vacant_entry) => {
                let client = self.factory.new_client(node_id, &node).await;
                debug!("Creating a gRPC client for node {node_id}");
                vacant_entry.insert(client.clone());
                client
            }
        }
        .pipe(Ok)
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    fn node_for_id(&self, node_id: NodeId) -> Result<ClusterNode> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .find(|(id, _node)| **id == node_id)
            .context(NoNodeSnafu { node_id })?
            .1
            .clone()
            .pipe(Ok)
    }

    pub fn metrics(&self) -> Metrics {
        Metrics {
            id: self.id,
            raft: self.raft.metrics().borrow().clone(),
        }
    }

    pub async fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> StdResult<InstallSnapshotResponse<NodeId>, Box<RaftError<NodeId, InstallSnapshotError>>>
    {
        self.raft.install_snapshot(req).await.map_err(Box::new)
    }

    pub async fn vote(
        &self,
        rpc: VoteRequest<NodeId>,
    ) -> StdResult<VoteResponse<NodeId>, Box<RaftError<NodeId>>> {
        self.raft.vote(rpc).await.map_err(Box::new)
    }

    /// Initialize the Raft cluster
    ///
    /// This should only be invoked once the entire cluster healthchecks
    pub async fn initialize<T, U>(&self, nodes: T, slots: U) -> Result<()>
    where
        T: IntoIterator<Item = (NodeId, ClusterNode)>,
        U: IntoIterator<Item = (SlotIndex, CacheId)>,
    {
        let nodes = BTreeMap::from_iter(nodes);
        let node_ids = nodes.keys().cloned().collect::<Vec<NodeId>>();

        let mut rx = self.raft.server_metrics();
        debug!(
            "Raft uninitialized: leader is {:?}",
            rx.borrow().current_leader
        );

        self.raft.initialize(nodes).await.context(RaftInitSnafu)?;

        debug!("Waiting on a server metrics change notification.");
        loop {
            rx.changed().await.unwrap();
            if rx.borrow_and_update().current_leader.is_some() {
                break;
            }
        }

        // Now that the underlying Raft has been initialized, we initialize the shared state,
        // itself:
        let rsp = self
            .raft
            .client_write(Request::Init {
                nodes: node_ids.try_into().map_err(|_| NoNodesSnafu.build())?,
                num_virtual: non_zero!(1usize),
                slots: slots.into_iter().collect(),
            })
            .await
            .context(RaftWriteSnafu)?;

        debug!("Initial write :=> {rsp:?}");

        Ok(())
    }

    pub async fn initialized(&self) -> Option<DateTime<Utc>> {
        self.state.read().await.initialized
    }

    pub async fn node_for_key<K: std::hash::Hash + std::fmt::Debug>(
        &self,
        key: &K,
    ) -> Result<NodeId> {
        self.state.read().await.node_for_key(key)
    }

    pub async fn node_for_slot(&self, fin: SlotIndex) -> Option<NodeId> {
        self.state.read().await.node_for_slot(fin)
    }

    pub async fn set_slots<T>(&self, slots: T) -> Result<()>
    where
        T: IntoIterator<Item = (SlotIndex, Option<CacheId>)>,
    {
        self.raft
            .client_write(Request::SetSlots {
                slots: slots.into_iter().collect(),
            })
            .await
            .context(RaftWriteSnafu)
            .map(|_| ())
    }

    pub fn socket_addr_for_id(&self, id: NodeId) -> Result<SocketAddr> {
        self.node_for_id(id).map(|node| node.addr)
    }

    pub async fn current_state(
        &self,
    ) -> (
        Vec<(u64, (NodeId, usize))>,
        [Option<NodeId>; NUMBER_OF_CACHE_SLOTS::USIZE],
    ) {
        self.state.read().await.current_state()
    }
}

/// Convenience typedef for shared, mutable ownership of a [CacheNode]
///
/// As noted [above](CacheNode), the caller is free to use any (or no) synchronization mechanism
/// they want. But this is probably what they want.
pub type SharedCacheNode<F> = Arc<RwLock<CacheNode<F>>>;

/// Convenience function for making new [SharedCacheNode] instances
pub async fn make_shared_cache_node<F>(
    backend: Arc<dyn Backend + Send + Sync>,
    config: &Configuration,
    factory: F,
) -> Result<SharedCacheNode<F>>
where
    F: ClientFactory + Send + Sync + Clone + 'static,
    F::CacheClient: Clone + Send + Sync + 'static,
{
    Ok(Arc::new(RwLock::new(
        CacheNode::new(backend, config, factory).await?,
    )))
}

// Let's assert that `CacheNode` is `Send` & `Sync`
#[cfg(test)]
mod cache_node_tests {

    use crate::network::null_client::NullClientFactory;

    use super::*;

    fn argument_is_send_and_sync<T: Send + Sync>(_: T) {}

    #[tokio::test]
    async fn cache_node_is_send_and_sync() {
        let cache_node_result = CacheNode::new(
            Arc::new(InMemoryBackend::default()),
            &Configuration::default(),
            NullClientFactory,
        )
        .await;
        assert!(cache_node_result.is_ok());
        argument_is_send_and_sync(cache_node_result.unwrap())
    }
}
