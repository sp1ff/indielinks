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

//! # indielinks-cache
//!
//! Trivial first implementation

use std::{
    collections::BTreeMap,
    io::Cursor,
    ops::RangeBounds,
    sync::{Arc, Mutex, RwLock},
};

use openraft::{
    BasicNode, Entry, LogId, LogState, OptionalSend, RaftLogId, RaftLogReader, RaftNetwork,
    RaftNetworkFactory, RaftSnapshotBuilder, RaftTypeConfig, Snapshot, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, Vote,
    error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable},
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    storage::{LogFlushed, RaftLogStorage, RaftStateMachine},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use snafu::Snafu;
use tap::Pipe;
use tracing::{info, instrument};

pub type NodeId = u64;

#[derive(Debug, Snafu)]
pub enum Error {}

pub type Result<T> = std::result::Result<T, Error>;

pub type StdResult<T, E> = std::result::Result<T, E>;

use std::error::Error as StdError;

// I've decided that my Raft state machine will manage the hash ring. Further, I've decided to model
// the hash ring as an array of `(shard, NodeId)` pairs, sorted by `shard`. Per the openraft
// "Getting Started" guide
// <https://docs.rs/openraft/latest/openraft/docs/getting_started/index.html>, this gives us
// something like this for our (Raft) client messages:

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Request {
    InsertNode { shard: u64, node: NodeId },
    RemoveNode { shard: u64 },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Response(());

openraft::declare_raft_types! (
   pub TypeConfig:
       D            = Request,
       R            = Response,
);

// Next-up: implement `RaftLogStorage`
// (<https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html>). I'm really
// confused by this: I can't find any requirements on this type to be `Clone`, but that's how the
// `raft-kv-memstore` sample sets it up. It *does* have to be Send, OptionalSync, OptionalSend, Send
// & 'static. Oddly, if I implement `RaftLogStorage` on a type, I also have to implement `RaftLogReader`
#[derive(Clone, Debug, Default)]
pub struct LogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<LogStoreInner<C>>>,
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

// From the docs (<https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogReader.html):
// "Typically, the log reader implementation as such will be hidden behind an Arc<T> and this
// interface implemented on the Arc<T>. It can be co-implemented with RaftStorage interface on the
// same cloneable object, if the underlying state machine is anyway synchronized."
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
        // let response = self.log.range(range.clone()).map(|(_, val)| val.clone()).collect::<Vec<_>>();
        self.inner
            .lock()
            .expect("Poisoned mutex!")
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
        let this = self.inner.lock().expect("Poisoned mutex!");
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
        self.inner.lock().expect("Poisoned mutex!").vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> StdResult<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.inner.lock().expect("Poisoned mutex!").vote.clone())
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
        self.inner.lock().expect("Poisoned mutex!").log.extend(
            entries
                .into_iter()
                .map(|entry| (entry.get_log_id().index, entry)),
        );
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    /// Remove the logs from `log_id` and later
    // Not sure why this would happen?
    #[instrument(level = "debug")]
    async fn truncate(
        &mut self,
        log_id: LogId<C::NodeId>,
    ) -> StdResult<(), StorageError<C::NodeId>> {
        let mut this = self.inner.lock().expect("Poisoned mutex!");
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
        assert!(
            self.inner
                .lock()
                .expect("Poisoned mutex!")
                .last_purged_log_id
                .as_ref()
                <= Some(&log_id)
        );

        let mut this = self.inner.lock().expect("Poisoned mutex!");

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

// This is my state machine. Again not sure about the semantics, but it seems handy to have the
// state machine shared by the `Raft` instance *and* the application state. That said, the
// `raft-kv-memstore` sample seems needlessly complex: they wrap their state machine in an inner,
// then wrap the "outer" in an `Arc` and implement the salient traits on the `Arc`. I'm going
// to try replicating the idiom I used for the Raft storage.

// Per the docs
// (<https://docs.rs/openraft/latest/openraft/docs/components/state_machine/index.html>), "The state
// machine in the Raft application is typically an in-memory component."

// I'm struggling to get my head around the required behavior, here, but so far it seems we
// need to maintain:
//
//     - the state machine itself (in our case, the hash ring)
//     - the most recently installed "snapshot" (if any; we may not have seen one)
//     - I gather we also need to *build* snapshots off of our current state
#[derive(Clone, Debug, Default)]
pub struct StateMachine {
    inner: Arc<RwLock<StateMachineInner>>,
}

// Here again, the `raft-kv-memstore` sample introduces multiple layers of composition here, which
// seems needlessly complex to me.

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
#[derive(Clone, Debug, Default)]
struct StateMachineInner {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
    pub current_snapshot: Option<Snapshot<TypeConfig>>,
    /// The hash ring for our distributed KV store
    pub ring: BTreeMap<u64, NodeId>,
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
        let sm = self.inner.read().expect("Poisoned R/W lock!");
        let data =
            serde_json::to_vec(&sm.ring).map_err(|e| StorageIOError::read_state_machine(&e))?;

        let snap = Snapshot::<TypeConfig> {
            meta: SnapshotMeta::<NodeId, BasicNode> {
                last_log_id: sm.last_applied_log,
                last_membership: sm.last_membership.clone(),
                snapshot_id: uuid::Uuid::new_v4().to_string(),
            },
            snapshot: Box::new(Cursor::new(data)),
        };

        drop(sm);

        let mut sm = self.inner.write().expect("Poisoned R/W lock!");
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
    ) -> StdResult<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let sm = self.inner.read().expect("Poisoned R/W lock!");
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
        let mut sm = self.inner.write().expect("Poisoned R/W lock!");
        for entry in entries {
            info!("Replicating {} to this State Machine", entry.log_id);
            sm.last_applied_log = Some(entry.log_id);
            match entry.payload {
                openraft::EntryPayload::Blank => res.push(Response(())),
                openraft::EntryPayload::Normal(request) => {
                    match request {
                        Request::InsertNode { shard, node } => sm.ring.insert(shard, node),
                        Request::RemoveNode { shard } => sm.ring.remove(&shard),
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
    #[instrument(level = "debug", skip(self))]
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
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> StdResult<(), StorageError<NodeId>> {
        let ring: BTreeMap<u64, NodeId> = serde_json::from_slice(snapshot.get_ref())
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        let mut sm = self.inner.write().expect("Poisoned R/W lock!");
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
        Ok(self
            .inner
            .read()
            .expect("Poisoned R/W lock!")
            .current_snapshot
            .clone())
    }
}

// I still need to test these implementations: <https://docs.rs/openraft/latest/openraft/testing/struct.Suite.html>

// Wheh! Finally, we're on to the "network" implementation. I'm again struggling to get my head
// around the abstractions. I apparently need a type on which to implement `RaftNetworkFactory`:

pub struct Network;

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            node: node.clone(),
            id: target,
            client: reqwest::Client::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NetworkConnection {
    node: BasicNode,
    id: NodeId, // may want this for logging purposes
    client: reqwest::Client,
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> StdResult<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    {
        send_raft_rpc(self.id, &self.node, &req, "raft/append", &self.client).await
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> StdResult<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        send_raft_rpc(self.id, &self.node, &req, "raft/install", &self.client).await
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> StdResult<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        send_raft_rpc(self.id, &self.node, &req, "raft/vote", &self.client).await
    }
}

async fn send_raft_rpc<Req, Resp, Err>(
    target: NodeId,
    target_node: &BasicNode,
    req: &Req,
    uri: &str,
    client: &reqwest::Client,
) -> StdResult<Resp, RPCError<NodeId, BasicNode, Err>>
where
    Req: Serialize,
    Resp: DeserializeOwned,
    Err: StdError + DeserializeOwned,
{
    let url = format!("http://{}/{}", target_node.addr, uri);

    let resp = client.post(url).json(&req).send().await.map_err(|e| {
        // If the error is a connection error, we return `Unreachable` so that connection isn't
        // retried immediately:
        if e.is_connect() {
            return openraft::error::RPCError::Unreachable(Unreachable::new(&e));
        }
        openraft::error::RPCError::Network(NetworkError::new(&e))
    })?;

    resp.json::<StdResult<Resp, Err>>()
        .await
        .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?
        .map_err(|e| openraft::error::RPCError::RemoteError(RemoteError::new(target, e)))
}
