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

//! # indielinks-cache Raft implementation
//!
//! This module contains the core [Raft] implementation for [indielinks-cache].

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    convert::identity,
    io::Cursor,
    num::NonZero,
    ops::RangeBounds,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use openraft::{
    Entry, LogId, LogState, OptionalSend, Raft, RaftLogId, RaftLogReader, RaftSnapshotBuilder,
    RaftTypeConfig, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
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
use tracing::{debug, info, instrument};
use xxhash_rust::xxh64::Xxh64Builder;

use crate::{
    network::{Client, ClientFactory, Network},
    types::{CacheId, ClusterNode, NodeId, Request, Response, TypeConfig},
};

use std::error::Error as StdError;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The CacheNode representing this process has already been constructed"))]
    CacheNodeAlready,
    #[snafu(display("Invalid configuration: {source}"))]
    Config {
        #[snafu(source(from(openraft::ConfigError, Box::new)))]
        source: Box<openraft::ConfigError>,
    },
    #[snafu(display("Can't hash a key with an empty hash ring"))]
    EmptyRing,
    #[snafu(display(
        "Failed to initialize the hash ring after successfully initializing the Raft: {source}"
    ))]
    InitHashRing {
        #[snafu(source(from(RaftError<NodeId, ClientWriteError<NodeId, ClusterNode>>, Box::new)))]
        source: Box<RaftError<NodeId, ClientWriteError<NodeId, ClusterNode>>>,
    },
    #[snafu(display("Unknown Node ID {node_id}"))]
    NoNode {
        node_id: NodeId,
        backtrace: Backtrace,
    },
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

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
pub enum CacheError<C>
where
    C: crate::network::Client,
    C::ErrorType: StdError + std::fmt::Debug + 'static,
{
    #[snafu(display("While looking up a node: {source}"))]
    BadId {
        #[snafu(source(from(Error, Box::new)))]
        source: Box<Error>,
        backtrace: Backtrace,
    },
    #[snafu(display("Network error: {source}"))]
    Network {
        source: C::ErrorType,
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

pub type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          The Raft Log                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// The [indielinks-cache] log store.
///
/// [Raft] works by synchronizing log messages across a cluster of nodes (each of which modifies
/// a state machine maintained on each node). We'll hang the [RaftLogStorage] implementation
/// here.
///
/// [RaftLogStorage]: https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html
//  I'm really confused by this: I can't find any requirements on this type to be `Clone`, but
//  that's how the `raft-kv-memstore` sample sets it up. It *does* have to be Send, OptionalSync,
//  OptionalSend, Send & 'static. Oddly, if I implement `RaftLogStorage` on a type, I also have to
//  implement `RaftLogReader`
#[derive(Clone, Debug, Default)]
struct LogStore<C: RaftTypeConfig> {
    inner: Arc<RwLock<LogStoreInner<C>>>,
}

#[derive(Debug)]
struct LogStoreInner<C: RaftTypeConfig> {
    /// The Raft log
    log: BTreeMap<u64, C::Entry>,
    /// The current granted vote.
    vote: Option<Vote<C::NodeId>>,
    last_purged_log_id: Option<LogId<C::NodeId>>,
}

impl<C: RaftTypeConfig> Default for LogStoreInner<C> {
    fn default() -> Self {
        LogStoreInner {
            log: BTreeMap::new(),
            vote: None,
            last_purged_log_id: None,
        }
    }
}

/// From the [docs] "Typically, the log reader implementation as such will be hidden behind an
/// Arc<T> and this interface implemented on the Arc<T>. It can be co-implemented with RaftStorage
/// interface on the same cloneable object, if the underlying state machine is anyway synchronized."
///
/// [docs]: https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogReader.html
impl<C: RaftTypeConfig> RaftLogReader<C> for LogStore<C>
where
    C::Entry: Clone,
{
    async fn try_get_log_entries<R>(
        &mut self,
        range: R,
    ) -> StdResult<Vec<C::Entry>, StorageError<C::NodeId>>
    where
        R: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        self.inner
            .read()
            .await
            .log
            .range(range)
            .map(|(_, entry)| entry)
            .cloned()
            .collect::<Vec<_>>()
            .pipe(Ok)
    }
}

impl<C: RaftTypeConfig> RaftLogStorage<C> for LogStore<C>
where
    C::Entry: Clone,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StdResult<LogState<C>, StorageError<C::NodeId>> {
        let this = self.inner.read().await;
        Ok(LogState {
            last_purged_log_id: this.last_purged_log_id.clone(),
            last_log_id: this
                .log
                .iter()
                .next_back()
                .map(|(_, ent)| ent.get_log_id().clone())
                .or(this.last_purged_log_id.clone()),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    /// Write a [Vote] to storage
    ///
    /// Per the
    /// [docs](https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html#tymethod.save_vote),
    /// "The vote must be persisted on disk before returning."
    async fn save_vote(
        &mut self,
        vote: &Vote<C::NodeId>,
    ) -> StdResult<(), StorageError<C::NodeId>> {
        // In this implementation, we of course don't write to disk:
        self.inner.write().await.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> StdResult<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.inner.read().await.vote.clone())
    }

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
    #[instrument(level = "debug", skip(entries, callback))]
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> StdResult<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.inner.write().await.log.extend(
            entries
                .into_iter()
                .map(|entry| (entry.get_log_id().index, entry)),
        );
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    /// Remove the logs from `log_id` and later
    async fn truncate(
        &mut self,
        log_id: LogId<C::NodeId>,
    ) -> StdResult<(), StorageError<C::NodeId>> {
        let mut this = self.inner.write().await;
        let to_be_removed = this
            .log
            .range(log_id.index..)
            .map(|(k, _)| k)
            .cloned()
            .collect::<Vec<_>>();
        to_be_removed.into_iter().for_each(|k| {
            this.log.remove(&k);
        });
        Ok(())
    }

    /// Remove logs up to `log_id`, inclusive
    // Seems reasonable-- but we need to note this `LogId` for future reference
    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> StdResult<(), StorageError<C::NodeId>> {
        assert!(self.inner.read().await.last_purged_log_id.as_ref() <= Some(&log_id));

        let mut this = self.inner.write().await;

        this.last_purged_log_id = Some(log_id.clone());

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// OK: that's log storage done. Next up: the shared state machine.
////////////////////////////////////////////////////////////////////////////////////////////////////

/// The [indielinks-cache] shared ([Raft]) state machine
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
// This is my state machine. Again not sure about the ownership semantics, but it seems handy to
// have the state machine shared by the `Raft` instance *and* the application state. That said, the
// `raft-kv-memstore` sample seems needlessly complex: they wrap their state machine in an inner,
// then wrap the "outer" in an `Arc` and implement the salient traits on the `Arc`. I'm going to try
// replicating the idiom I used for the Raft storage.
//
// I'm struggling to get my head around the required behavior, here, but so far it seems we
// need to maintain:
//
//     - the state machine itself (in our case, the hash ring)
//     - the most recently installed "snapshot" (if any; we may not have seen one)
//     - I gather we also need to *build* snapshots off of our current state
#[derive(Clone, Default)]
struct StateMachine {
    inner: Arc<RwLock<StateMachineInner>>,
}

#[derive(Clone, Default)]
struct Hasher {
    hash: Xxh64Builder,
}

// No constructor; instances are generally created through [default]
impl Hasher {
    pub fn hash_node(&self, id: &NodeId, m: usize) -> u64 {
        let mut hash = self.hash.build();
        use std::hash::{Hash, Hasher};
        id.hash(&mut hash);
        hash.write_i8(58); // 58 is ASCII ':'
        hash.write_usize(m);
        hash.finish()
    }
    pub fn hash_key<K: std::hash::Hash>(&self, key: &K) -> u64 {
        use std::hash::BuildHasher;
        self.hash.hash_one(key)
    }
}

// Here again, the `raft-kv-memstore` sample introduces multiple layers of composition here, which
// seems needlessly complex to me.
//
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
//
// `SnapshotData` is just a `Box<C::SnapshotData>`, which by default & in our case, is a
// `Cursor<Vec<u8>>`. What sort of `Cursor`, I can't tell. I guess `std::io::Cursor`.
#[derive(Clone, Default)]
struct StateMachineInner {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, ClusterNode>,
    current_snapshot: Option<Snapshot<TypeConfig>>,
    hasher: Hasher,
    // The hash ring for our distributed KV store
    ring: Vec<(u64, (NodeId, usize))>,
}

// Again, no constructor: just construct via [Default]
impl StateMachineInner {
    pub fn node_for_key<K: std::hash::Hash + std::fmt::Debug>(&self, key: &K) -> Result<NodeId> {
        if self.ring.is_empty() {
            return Err(Error::EmptyRing);
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
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    /// Per the
    /// [docs](https://docs.rs/openraft/latest/openraft/storage/trait.RaftSnapshotBuilder.html#tymethod.build_snapshot):
    /// "A snapshot has to contain state of all applied log, including membership. Usually it is
    /// just a serialized state machine.
    ///
    /// Building snapshot can be done by:
    ///
    /// - Performing log compaction, e.g. merge log entries that operates on the same key, like
    ///   a LSM-tree does,
    /// - or by fetching a snapshot from the state machine.""
    ///
    /// The sample code again seems overly complex to me. AFAICT, we need to:
    ///
    /// 1. build a snapshot of our current state
    /// 2. save a copy into `current_snapshot`
    /// 3. return the snapshot
    async fn build_snapshot(&mut self) -> StdResult<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let sm = self.inner.read().await;
        let data =
            serde_json::to_vec(&sm.ring).map_err(|e| StorageIOError::read_state_machine(&e))?;

        let snap = Snapshot::<TypeConfig> {
            meta: SnapshotMeta::<NodeId, ClusterNode> {
                last_log_id: sm.last_applied_log,
                last_membership: sm.last_membership.clone(),
                snapshot_id: uuid::Uuid::new_v4().to_string(),
            },
            snapshot: Box::new(Cursor::new(data)),
        };

        drop(sm);

        let mut sm = self.inner.write().await;
        sm.current_snapshot = Some(snap.clone());

        Ok(snap)
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = Self;

    /// Nb. "It is all right to return a membership with greater log id than the
    /// last-applied-log-id. Because upon startup, the last membership will be loaded by scanning
    /// logs from the last-applied-log-id."--
    /// [docs](https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html#tymethod.applied_state)
    async fn applied_state(
        &mut self,
    ) -> StdResult<
        (Option<LogId<NodeId>>, StoredMembership<NodeId, ClusterNode>),
        StorageError<NodeId>,
    > {
        let sm = self.inner.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    /// Apply the given payload of entries to the state machine
    ///
    /// This is where we apply log entries to our state machine. Per the
    /// [docs](https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html#tymethod.apply),
    /// for each entry we shall:
    ///
    /// - Store the log id as last applied log id.
    /// - Deal with the business logic log.
    /// - Store membership config if RaftEntry::get_membership() returns Some.
    ///
    /// And: An implementation may choose to persist either the state machine or the snapshot:
    ///
    /// - An implementation with persistent state machine: persists the state on disk before
    ///   returning from apply(). So that a snapshot does not need to be persistent.
    /// - An implementation with persistent snapshot: apply() does not have to persist state on
    ///   disk. But every snapshot has to be persistent. And when starting up the application, the
    ///   state machine should be rebuilt from the last snapshot.
    #[instrument(level = "debug", skip(self, entries))]
    async fn apply<I>(&mut self, entries: I) -> StdResult<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut res = Vec::new();
        let mut sm = self.inner.write().await;
        for entry in entries {
            info!(
                "Replicating {} to this State Machine: {:?}",
                entry.log_id, entry
            );
            sm.last_applied_log = Some(entry.log_id);
            match entry.payload {
                openraft::EntryPayload::Blank => res.push(Response(())),
                openraft::EntryPayload::Normal(request) => {
                    let mut ring = BTreeSet::new();
                    match request {
                        Request::Init { nodes, num_virtual } => {
                            nodes.iter().for_each(|node_id| {
                                (0..num_virtual.get()).for_each(|m| {
                                    let shard = sm.hasher.hash_node(node_id, m);
                                    ring.insert((shard, (*node_id, m)));
                                })
                            });
                            sm.ring = Vec::from_iter(ring.into_iter());
                            debug!("I now have a hash ring of {:#?}", sm.ring);
                        }
                        Request::InsertNode { .. } => {
                            unimplemented!()
                        }
                        Request::RemoveNode { .. } => {
                            unimplemented!()
                        }
                    };
                    res.push(Response(()));
                }
                openraft::EntryPayload::Membership(membership) => {
                    // Cluster membership has changed-- I should probably record this
                    sm.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    res.push(Response(()));
                }
            };
        }
        info!("Returning {:?} from `apply()`", res);
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    /// I'm still confused as to this method. The
    /// [docs](https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html#tymethod.begin_receiving_snapshot)
    /// merely say "Create a new blank snapshot, returning a writable handle to the snapshot
    /// object."
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> StdResult<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
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
    #[instrument(level = "debug", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, ClusterNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> StdResult<(), StorageError<NodeId>> {
        let ring: Vec<(u64, (NodeId, usize))> = serde_json::from_slice(snapshot.get_ref())
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        let mut sm = self.inner.write().await;
        sm.last_applied_log = meta.last_log_id;
        sm.last_membership = meta.last_membership.clone();
        sm.current_snapshot = Some(Snapshot::<TypeConfig> {
            meta: meta.clone(),
            snapshot,
        });
        sm.ring = ring;

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
        Ok(self.inner.read().await.current_snapshot.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// The [indielinks-cache] public interface
////////////////////////////////////////////////////////////////////////////////////////////////////

/// [indielinks-cache] cluster configuration
///
/// Outside of the ID for this node, the configuration should be uniform across the cluster.
///
/// I've exposed a few of the [openraft::Config] fields here.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Configuration {
    cluster_name: String,
    // The ID for *this* node
    this_node: NodeId,
    // The interval at which leaders will send heartbeats to followers; this defaults to 50ms, which
    // can make the cluster awfully chatty
    heartbeat_interval: Duration,
    // The lower bound on the election timeout (defaults to 150ms)
    election_timeout_min: Duration,
    // The upper bound on the election timeout (defaults to 300ms)
    election_timeout_max: Duration,
}

pub struct ConfigurationBuilder {
    cluster_name: String,
    this_node: NodeId,
    heartbeat_interval: Duration,
    election_timeout_min: Duration,
    election_timeout_max: Duration,
}

impl ConfigurationBuilder {
    pub fn new(cluster_name: impl Into<String>, this_node: NodeId) -> ConfigurationBuilder {
        ConfigurationBuilder {
            cluster_name: cluster_name.into(),
            this_node,
            heartbeat_interval: Duration::from_millis(50),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
        }
    }
    pub fn build(self) -> Configuration {
        Configuration {
            cluster_name: self.cluster_name,
            this_node: self.this_node,
            heartbeat_interval: self.heartbeat_interval,
            election_timeout_min: self.election_timeout_min,
            election_timeout_max: self.election_timeout_max,
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
}

impl Configuration {
    pub fn builder(cluster_name: impl Into<String>, this_node: NodeId) -> ConfigurationBuilder {
        ConfigurationBuilder::new(cluster_name, this_node)
    }
}

// I really can't see a case where you'd want more than one "cluster node" in a process. I thought
// to is to hold an `RwLock` of that node inside a `OnceLock`; the first caller would call
// `get_or_init()` and the closure would capture the configuration from the enclosing ctor. The
// problem with this is, a second caller with a different configuration wouldn't fail-- they would
// receive the previously-constructed instance, which seems bad.
//
// For now, I'm just going to go with the simple approach:
static CACHE_NODE_CREATED: AtomicBool = AtomicBool::new(false);

struct CacheNodeInner<F>
where
    F: ClientFactory,
{
    id: NodeId,
    raft: Raft<TypeConfig>,
    state: StateMachine,
    factory: F,
    clients: HashMap<NodeId, F::CacheClient>,
}

impl<F> CacheNodeInner<F>
where
    F: ClientFactory + Send + Sync + Clone + 'static,
    F::CacheClient: Clone + Send + Sync + 'static,
{
    /// Initialize the current host as a member of an [indielinks-cache] cluster
    pub async fn new(config: &Configuration, factory: F) -> Result<CacheNodeInner<F>> {
        let already = CACHE_NODE_CREATED
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err();

        if already {
            return Err(Error::CacheNodeAlready);
        }

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

        let state_machine = StateMachine::default();
        let raft = openraft::Raft::new(
            config.this_node,              // Node ID
            raft_config,                   // Config
            Network::new(factory.clone()), // Network
            LogStore::default(),           // Log Store
            state_machine.clone(),         // State Machine
        )
        .await
        .context(RaftSnafu)?;

        Ok(CacheNodeInner {
            id: config.this_node,
            raft,
            state: state_machine,
            factory,
            clients: HashMap::new(),
        })
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

    async fn client_for_id(&mut self, node_id: NodeId) -> Result<F::CacheClient> {
        use std::collections::hash_map::Entry;

        let node = self.node_for_id(node_id)?;

        match self.clients.entry(node_id) {
            Entry::Occupied(occupied_entry) => occupied_entry.get().clone(),
            Entry::Vacant(vacant_entry) => {
                let client = self.factory.new_client(node_id, &node).await;
                vacant_entry.insert(client.clone());
                client
            }
        }
        .pipe(Ok)
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
    pub async fn cache_lookup<K: Serialize + Send, V: DeserializeOwned>(
        &mut self,
        node_id: NodeId,
        cache_id: CacheId,
        k: impl Into<K> + Send,
    ) -> StdResult<Option<V>, CacheError<F::CacheClient>> {
        assert!(node_id != self.id); // I think this is panic-worthy
        self.client_for_id(node_id)
            .await
            .context(BadIdSnafu)?
            .cache_lookup(cache_id, k)
            .await
            .context(NetworkSnafu)
    }
    pub async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> StdResult<AppendEntriesResponse<NodeId>, RaftError<NodeId>> {
        self.raft.append_entries(rpc).await
    }
    pub async fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> StdResult<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>> {
        self.raft.install_snapshot(req).await
    }
    pub async fn vote(
        &self,
        rpc: VoteRequest<NodeId>,
    ) -> StdResult<VoteResponse<NodeId>, RaftError<NodeId>> {
        self.raft.vote(rpc).await
    }
    /// Initialize the Raft cluster
    ///
    /// This should only be invoked once the entire cluster healthchecks
    pub async fn initialize<T>(&self, nodes: T) -> Result<()>
    where
        T: IntoIterator<Item = (NodeId, ClusterNode)>,
    {
        let nodes = BTreeMap::from_iter(nodes);
        let node_ids = nodes.keys().cloned().collect::<Vec<NodeId>>();

        self.raft.initialize(nodes).await.context(RaftInitSnafu)?;

        let mut leader = self.raft.current_leader().await;
        info!("Raft initialized: leader is {:?}", leader);

        while leader.is_none() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            leader = self.raft.current_leader().await;
            info!("The Raft leader is now {:?}", leader);
        }

        // Now that the underlying Raft has been initialized, we initialize the shared state,
        // itself:
        let rsp = self
            .raft
            .client_write(Request::Init {
                nodes: node_ids,
                num_virtual: NonZero::new(1).unwrap(/* known good */),
            })
            .await
            .context(RaftWriteSnafu)?;

        debug!("Initial write :=> {rsp:?}");

        Ok(())
    }
    pub async fn node_for_key<K: std::hash::Hash + std::fmt::Debug>(
        &self,
        key: &K,
    ) -> Result<NodeId> {
        self.state.inner.read().await.node_for_key(key)
    }
}

/// Represents this process' node in the [indielinks-cache] cluster
#[derive(Clone)]
pub struct CacheNode<F: ClientFactory> {
    inner: Arc<RwLock<CacheNodeInner<F>>>,
}

impl<F> CacheNode<F>
where
    F: ClientFactory + Send + Sync + Clone + 'static,
    F::CacheClient: Clone + Send + Sync + 'static,
{
    /// Initialize the current host as a member of an [indielinks-cache] cluster
    pub async fn new(config: &Configuration, factory: F) -> Result<CacheNode<F>> {
        Ok(CacheNode {
            inner: Arc::new(RwLock::new(CacheNodeInner::new(config, factory).await?)),
        })
    }
    pub async fn id(&self) -> NodeId {
        self.inner.read().await.id
    }
    pub async fn node_for_key<K: std::hash::Hash + std::fmt::Debug>(
        &self,
        key: &K,
    ) -> Result<NodeId> {
        self.inner.read().await.node_for_key(key).await
    }
    pub async fn cache_insert<K: Serialize + Send + Sync, V: Serialize + Send + Sync>(
        &mut self,
        node_id: NodeId,
        cache_id: CacheId,
        k: impl Into<K> + Send,
        v: impl Into<V> + Send,
    ) -> StdResult<(), CacheError<F::CacheClient>> {
        self.inner
            .write()
            .await
            .cache_insert(node_id, cache_id, k, v)
            .await
    }
    pub async fn cache_lookup<K: Serialize + Send + Sync, V: DeserializeOwned>(
        &mut self,
        node_id: NodeId,
        cache_id: CacheId,
        k: impl Into<K> + Send,
    ) -> StdResult<Option<V>, CacheError<F::CacheClient>> {
        self.inner
            .write()
            .await
            .cache_lookup(node_id, cache_id, k)
            .await
    }
    pub async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> StdResult<AppendEntriesResponse<NodeId>, RaftError<NodeId>> {
        self.inner.read().await.append_entries(rpc).await
    }
    pub async fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> StdResult<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>> {
        self.inner.read().await.install_snapshot(req).await
    }
    pub async fn vote(
        &self,
        rpc: VoteRequest<NodeId>,
    ) -> StdResult<VoteResponse<NodeId>, RaftError<NodeId>> {
        self.inner.read().await.vote(rpc).await
    }
    pub async fn initialize(&self, nodes: BTreeMap<NodeId, ClusterNode>) -> Result<()> {
        self.inner.write().await.initialize(nodes).await
    }
}
