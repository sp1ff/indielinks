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

//! # Basic types used throughout [indielinks-cache]
//!
//! While I generally loathe these sorts of "types" or "entities" modules, the [openraft] crate is
//! parameterized by types throughout; I thought it might make sense to collect them in one place
//! where we could define our [TypeConfig].

use std::{
    collections::BTreeMap,
    io::Cursor,
    net::{SocketAddr, SocketAddrV4},
    num::NonZero,
    ops::RangeBounds,
    result::Result as StdResult,
    str::FromStr,
    sync::Arc,
};

use openraft::{
    Entry, LogId, LogState, OptionalSend, RaftLogId, RaftLogReader, StorageError, Vote,
    storage::{LogFlushed, RaftLogStorage},
};
use serde::{Deserialize, Serialize};
use tap::Pipe;
use tokio::sync::RwLock;
use tracing::instrument;

/// Type for naming nodes in the [raft] cluster
///
/// [raft]: https://raft.github.io/raft.pdf
pub type NodeId = u64;

/// Type for naming individual caches in each node
///
/// Each node in the cluster will host the same set of caches, albeit each node housing different
/// shards. [CacheId] serves as a way of naming caches.
pub type CacheId = u64;

/// [Raft] log messages for [indielinks-cache](crate)
///
/// [Raft]: https://raft.github.io/raft.pdf
///
/// The [indielinks-cache] [raft] cluster shared state consists of the hash ring. Per the openraft
/// "Getting Started" [guide], this gives us something like this for our (Raft) client messages.
///
/// [guide]: https://docs.rs/openraft/latest/openraft/docs/getting_started/index.html
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Request {
    Init {
        nodes: Vec<NodeId>,
        num_virtual: NonZero<usize>,
    },
    InsertNodes {
        nodes: Vec<NodeId>,
    },
    RemoveNodes {
        nodes: Vec<NodeId>,
    },
}

/// Response type for the application of [Request]s
///
/// It might *seem* reasonable to return a [Result] here, but 1) most error types are not
/// serializable (as this type must be) and 2) any errors raised while applying a log message will
/// be handled by our [RaftStateMachine::apply] implementation, anyway.
///
/// [RaftStateMachine::apply]: openraft::storage::RaftStateMachine::apply
///
/// A reasonable value would be the new hash ring (i.e. the hash ring after application of this
/// message), but I don't want to add the overhead until I know I'm going to actually use that
/// information, somewhere.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Response(pub ());

/// Represents an arbitrary node in our Raft cluster
// I suspect that it will be handy to tuck away additional bits of information here, but for now,
// we'll just use the network location.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize)]
pub struct ClusterNode {
    pub addr: SocketAddr,
}

impl FromStr for ClusterNode {
    type Err = std::net::AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ClusterNode {
            addr: s.parse::<SocketAddr>()?,
        })
    }
}

// Not sure about this... `openraft` demands that our `Node` implementation implement `Default`,
// which doesn't make a lot of sense to me, if we're going to include the network location of the
// node in tye type-- their implemetnation of `Default` just provisions the instance with the empty
// string (!?)
impl Default for ClusterNode {
    fn default() -> Self {
        Self {
            addr: SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:18000").unwrap(/* known good*/)),
        }
    }
}

openraft::declare_raft_types! (
   pub TypeConfig:
    D    = Request,
    R    = Response,
    Node = ClusterNode,
);

////////////////////////////////////////////////////////////////////////////////////////////////////

/// An in-memory log store; primarily intended for testing.
#[derive(Clone, Debug, Default)]
pub struct InMemoryLogStore {
    inner: Arc<RwLock<LogStoreInner>>,
}

#[derive(Debug, Default)]
struct LogStoreInner {
    /// The Raft log
    log: BTreeMap<u64, Entry<TypeConfig>>,
    /// The current granted vote.
    vote: Option<Vote<NodeId>>,
    last_purged_log_id: Option<LogId<NodeId>>,
}

/// From the [docs] "Typically, the log reader implementation as such will be hidden behind an
/// `Arc<T>` and this interface implemented on the `Arc<T>`. It can be co-implemented with RaftStorage
/// interface on the same cloneable object, if the underlying state machine is anyway synchronized."
///
/// [docs]: https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogReader.html
impl RaftLogReader<TypeConfig> for InMemoryLogStore {
    async fn try_get_log_entries<R>(
        &mut self,
        range: R,
    ) -> StdResult<Vec<Entry<TypeConfig>>, StorageError<NodeId>>
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

impl RaftLogStorage<TypeConfig> for InMemoryLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StdResult<LogState<TypeConfig>, StorageError<NodeId>> {
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

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    /// Write a [Vote] to storage
    ///
    /// Per the
    /// [docs](https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html#tymethod.save_vote),
    /// "The vote must be persisted on disk before returning."
    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> StdResult<(), StorageError<NodeId>> {
        // In this implementation, we of course don't write to disk:
        self.inner.write().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> StdResult<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.read().await.vote)
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
        callback: LogFlushed<TypeConfig>,
    ) -> StdResult<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
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
    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StdResult<(), StorageError<NodeId>> {
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

    /// Remove logs up to `log_id`, inclusive
    // Seems reasonable-- but we need to note this `LogId` for future reference
    async fn purge(&mut self, log_id: LogId<NodeId>) -> StdResult<(), StorageError<NodeId>> {
        assert!(self.inner.read().await.last_purged_log_id.as_ref() <= Some(&log_id));

        let mut this = self.inner.write().await;

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
}
