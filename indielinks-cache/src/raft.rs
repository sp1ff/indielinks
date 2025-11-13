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
    fmt::Debug,
    io::Cursor,
    num::NonZero,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use openraft::{
    error::{ClientWriteError, InstallSnapshotError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    storage::{RaftLogStorage, RaftStateMachine},
    Entry, LogId, OptionalSend, Raft, RaftMetrics, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
    StorageIOError, StoredMembership,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use tokio::sync::RwLock;
use tracing::{debug, info, instrument};
use xxhash_rust::xxh64::Xxh64Builder;

use crate::{
    network::{Client, ClientFactory, Network},
    types::{CacheId, ClusterNode, NodeId, Request, Response, TypeConfig},
};

pub use openraft::{raft::ClientWriteResponse, ChangeMembers, StorageError};

use std::error::Error as StdError;

pub type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The CacheNode representing this process has already been constructed"))]
    CacheNodeAlready,
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

/// A thing that can hash virtual nodes and hash keys via the xxhash64 algorithm.
#[derive(Clone, Default)]
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
// It seems handy to have the state machine shared by the `Raft` instance *and* the application
// state. That said, the `raft-kv-memstore` sample seems needlessly complex: they wrap their state
// machine in an inner, then wrap the "outer" in an `Arc` and implement the salient traits on the
// `Arc`.
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

/// Allow [indielinks-cache](crate) clients to test their log storage implementations
pub mod test {
    use std::marker::PhantomData;

    use openraft::{
        storage::RaftLogStorage,
        testing::{StoreBuilder as RaftStoreBuilder, Suite},
        StorageError,
    };

    use crate::types::{NodeId, TypeConfig};

    /// Small trait to be implement by the [indielinks-cache](crate) caller allowing us to
    /// instantiate their [RaftLogStorage] implementation
    pub trait StoreBuilder<LS, G = ()>
    where
        LS: RaftLogStorage<TypeConfig>,
    {
        fn build(
            &self,
        ) -> impl Future<Output = std::result::Result<(G, LS), StorageError<NodeId>>> + Send;
    }
    /// Implementation of [RaftStoreBuilder] that _we_ will provide to openraft
    struct OurBuilder<F, LS, G>
    where
        LS: RaftLogStorage<TypeConfig>,
        F: StoreBuilder<LS, G>,
    {
        inner: F,
        _phantom: PhantomData<(LS, G)>,
    }

    impl<F, LS, G> RaftStoreBuilder<TypeConfig, LS, super::StateMachine, G> for OurBuilder<F, LS, G>
    where
        LS: RaftLogStorage<TypeConfig>,
        F: StoreBuilder<LS, G> + Send + Sync,
        G: Send + Sync,
    {
        async fn build(
            &self,
        ) -> std::result::Result<(G, LS, super::StateMachine), StorageError<NodeId>> {
            let (g, ls) = self.inner.build().await?;
            Ok((g, ls, super::StateMachine::default()))
        }
    }

    /// Run the openraft test
    /// [suite](https://docs.rs/openraft/latest/openraft/testing/struct.Suite.html) against our
    /// caller's [RaftLogStorage] implementation and our state machine.
    // `StorageError` is outside my control, and NodeId is just a u64... 🤷
    #[allow(clippy::result_large_err)]
    pub fn test_storage<LS, G, F>(f: F) -> std::result::Result<(), StorageError<NodeId>>
    where
        LS: RaftLogStorage<TypeConfig>,
        F: StoreBuilder<LS, G> + Send + Sync,
        G: Send + Sync,
    {
        Suite::test_all(OurBuilder {
            inner: f,
            _phantom: PhantomData,
        })
        // let builder = OurBuilder {
        //     inner: f,
        //     _phantom: PhantomData,
        // };
        // let (_g, store, sm) = builder.build().await?;
        // Suite::<TypeConfig, LS, StateMachine, OurBuilder<F, LS, G>, G>::last_membership_in_log_multi_step(store, sm).await
    }
}

/// The core state machine implementation
///
/// Each node will have precisely one instance of this type, but can hand-out many copies of the
/// [stateMachine] wrapper.
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
    /// Given an arbitrary hash key, return the node responsible for hosting it
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
        Ok(self.ring[idx].1 .0)
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
                        Request::InsertNodes { .. } => {
                            unimplemented!()
                        }
                        Request::RemoveNodes { .. } => {
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
//                             The indielinks-cache public interface                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// [indielinks-cache] cluster configuration
///
/// Outside of the ID for this node, the configuration should be uniform across the cluster.
///
/// I've exposed a few of the [openraft::Config] fields here.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Configuration {
    #[serde(rename = "cluster-name")]
    pub cluster_name: String,
    // The ID for *this* node
    #[serde(rename = "this-node")]
    pub this_node: NodeId,
    // The interval at which leaders will send heartbeats to followers; this defaults to 50ms, which
    // can make the cluster awfully chatty
    #[serde(rename = "heartbeat-interval")]
    pub heartbeat_interval: Duration,
    // The lower bound on the election timeout (defaults to 150ms)
    #[serde(rename = "election-timeout-min")]
    pub election_timeout_min: Duration,
    // The upper bound on the election timeout (defaults to 300ms)
    #[serde(rename = "election-timeout-max")]
    pub election_timeout_max: Duration,
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

// This needs to be built-out: would be interesting to expose memory footprint, e.g.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Metrics {
    pub id: NodeId,
    pub raft: RaftMetrics<NodeId, ClusterNode>,
}

// I really can't see a case where you'd want more than one "cluster node" in a process. I thought
// to is to hold an `RwLock` of that node inside a `OnceLock`; the first caller would call
// `get_or_init()` and the closure would capture the configuration from the enclosing ctor. The
// problem with this is, a second caller with a different configuration wouldn't fail-- they would
// receive the previously-constructed instance, which seems bad.
//
// For now, I'm just going to go with the simple approach:
static CACHE_NODE_CREATED: AtomicBool = AtomicBool::new(false);

/// Singleton type representing the current [indielinks-cache] node.
struct CacheNodeInner<F: ClientFactory> {
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
    pub async fn new<T: RaftLogStorage<TypeConfig>>(
        config: &Configuration,
        factory: F,
        storage: T,
    ) -> Result<CacheNodeInner<F>> {
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
        // On return from `Raft::new()`, this Raft node will be in the "learner" state. The cluster
        // won't be established until `Raft::initialize()` is invoked on some member.
        let raft = openraft::Raft::new(
            config.this_node,              // Node ID
            raft_config,                   // Config
            Network::new(factory.clone()), // Network
            storage,                       // Log Store
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

    async fn add_learner(
        &self,
        id: NodeId,
        node: ClusterNode,
        blocking: bool,
    ) -> StdResult<
        ClientWriteResponse<TypeConfig>,
        RaftError<NodeId, ClientWriteError<NodeId, ClusterNode>>,
    > {
        self.raft.add_learner(id, node, blocking).await
    }

    pub async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> StdResult<AppendEntriesResponse<NodeId>, RaftError<NodeId>> {
        self.raft.append_entries(rpc).await
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
        generation: Option<u64>,
        v: impl Into<V> + Send,
    ) -> StdResult<(), CacheError<F::CacheClient>> {
        assert!(node_id != self.id); // I think this is panic-worthy
        self.client_for_id(node_id)
            .await
            .context(BadIdSnafu)?
            .cache_insert(cache_id, k, generation, v)
            .await
            .context(NetworkSnafu)
    }
    /// Lookup a value in the distributed cache
    pub async fn cache_lookup<K: Serialize + Send, V: DeserializeOwned>(
        &mut self,
        node_id: NodeId,
        cache_id: CacheId,
        k: impl Into<K> + Send,
    ) -> StdResult<Option<(u64, V)>, CacheError<F::CacheClient>> {
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

    async fn change_membership(
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
            .collect::<Vec<NodeId>>();
        self.raft
            .client_write(Request::Init {
                nodes: node_ids,
                num_virtual: NonZero::new(1).unwrap(/* known good */),
            })
            .await
            .context(RaftWriteSnafu)?;

        Ok(membership_response)
    }

    async fn client_for_id(&mut self, node_id: NodeId) -> Result<F::CacheClient> {
        use std::collections::hash_map::Entry;

        let node = self.node_for_id(node_id)?;

        match self.clients.entry(node_id) {
            Entry::Occupied(occupied_entry) => occupied_entry.get().clone(),
            Entry::Vacant(vacant_entry) => {
                let client = self.factory.new_client(node_id, &node).await;
                debug!("Creating a gRPC client for node {node_id}");
                vacant_entry.insert(client.clone());
                client
            }
        }
        .pipe(Ok)
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

    fn metrics(&self) -> Metrics {
        Metrics {
            id: self.id,
            raft: self.raft.metrics().borrow().clone(),
        }
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

/// A Clonable representation of this process' node in the [indielinks-cache] cluster
// I have reservations about this approach. For one, it requires writing a bunch of forwarding
// functions. For another, it makes access to the state machine tedious: go through the `Arc`, get a
// lock on the inner, go through the `Arc` wrapping the state machine, get a lock that _that_ inner,
// then finally do the thing. That said, we need to keep multiple references to the `CacheNode`
// around (one per `Cache`, e.g.), hence the `Arc`. And we sometimes need mutable access to it,
// hence the `RwLock`.
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
    pub async fn new<T: RaftLogStorage<TypeConfig>>(
        config: &Configuration,
        factory: F,
        storage: T,
    ) -> Result<CacheNode<F>> {
        Ok(CacheNode {
            inner: Arc::new(RwLock::new(
                CacheNodeInner::new(config, factory, storage).await?,
            )),
        })
    }
    pub async fn add_learner(
        &self,
        id: NodeId,
        node: ClusterNode,
        blocking: bool,
    ) -> StdResult<
        ClientWriteResponse<TypeConfig>,
        RaftError<NodeId, ClientWriteError<NodeId, ClusterNode>>,
    > {
        self.inner
            .read()
            .await
            .add_learner(id, node, blocking)
            .await
    }

    pub async fn change_membership(
        &self,
        members: impl Into<ChangeMembers<NodeId, ClusterNode>>,
        retain: bool,
    ) -> Result<ClientWriteResponse<TypeConfig>> {
        self.inner
            .read()
            .await
            .change_membership(members, retain)
            .await
    }

    pub async fn id(&self) -> NodeId {
        self.inner.read().await.id
    }

    pub async fn metrics(&self) -> Metrics {
        self.inner.read().await.metrics()
    }

    pub async fn node_for_key<K: std::hash::Hash + std::fmt::Debug>(
        &self,
        key: &K,
    ) -> Result<NodeId> {
        self.inner.read().await.node_for_key(key).await
    }
    pub async fn cache_insert<K: Serialize + Send + Sync, V: Serialize + Send + Sync>(
        &self,
        node_id: NodeId,
        cache_id: CacheId,
        k: impl Into<K> + Send,
        generation: Option<u64>,
        v: impl Into<V> + Send,
    ) -> StdResult<(), CacheError<F::CacheClient>> {
        self.inner
            .write()
            .await
            .cache_insert(node_id, cache_id, k, generation, v)
            .await
    }
    pub async fn cache_lookup<K: Serialize + Send + Sync, V: DeserializeOwned>(
        &self,
        node_id: NodeId,
        cache_id: CacheId,
        k: impl Into<K> + Send,
    ) -> StdResult<Option<(u64, V)>, CacheError<F::CacheClient>> {
        let mut x = self.inner.write().await;
        x.cache_lookup(node_id, cache_id, k).await
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
    /// Return a [SocketAddr](std::net::SocketAddr) given a [NodeId]
    pub async fn socket_addr_for_id(&self, id: NodeId) -> Result<std::net::SocketAddr> {
        self.inner
            .read()
            .await
            .node_for_id(id)
            .map(|node| node.addr)
    }
}
