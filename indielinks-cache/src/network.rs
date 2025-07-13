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

//! # [indielinks-cache] client-side networking
//!
//! Raft cluster nodes need to exchange messages to establish consensus. [indielinks-cache] node
//! members need to exchange messages to manage shared state. This module defines the *messages*
//! cluster nodes need to exchange, but the *application* will need to provide an implementation
//! of the network transport.

use async_trait::async_trait;
use openraft::{RaftNetwork, RaftNetworkFactory};
use serde::{Serialize, de::DeserializeOwned};

use crate::types::{CacheId, ClusterNode, NodeId, TypeConfig};

pub use openraft::{
    error::{InstallSnapshotError, RPCError, RaftError},
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};

use std::error::Error as StdError;

pub type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                              indielinks-cache client abstraction                               //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A trait defining the client-side behavior for [indielinks-cache](crate) cluster members
///
/// The [indielinks-cache] application is expected to provide an implementation for this trait,
/// along with [ClientFactory], and to provide the latter implementation to [indielinks-cache]
/// through the [CacheNode] constructor.
///
/// [CacheNode]: crate::raft::CacheNode
///
/// The first three methods pertain to the [Raft] protocol and are copied verbatim from the
/// [corresponding] [openraft] trait. The second two are specific to [indielinks-cache].
///
/// [Raft]: https://raft.github.io/raft.pdf
/// [corresponding]: openraft::network::RaftNetwork
///
/// Nb that the target of these requests is assumed to be encoded into the implementor.
#[async_trait]
pub trait Client {
    type ErrorType: StdError + std::fmt::Debug;
    /// Append Raft log entries to the target node's log store
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> StdResult<AppendEntriesResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>>;
    /// Install a state snapshot on the target node
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> StdResult<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, ClusterNode, RaftError<NodeId, InstallSnapshotError>>,
    >;
    /// Request a leadership vote from the target node
    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> StdResult<VoteResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>>;
    /// Ask the target node to insert a key/value pair into it's LRU cache
    async fn cache_insert<K: Serialize + Sync, V: Serialize + Sync>(
        &mut self,
        cache: CacheId,
        key: impl Into<K> + Send,
        value: impl Into<V> + Send,
    ) -> StdResult<(), Self::ErrorType>;
    /// Request a value for a given key from the target node
    async fn cache_lookup<K: Serialize, V: DeserializeOwned>(
        &mut self,
        cache: CacheId,
        key: impl Into<K> + Send,
    ) -> StdResult<Option<V>, Self::ErrorType>;
}

/// A trait defining the interface for a factory creating connections between [indielinks-cache]
/// cluster members
///
/// This is organized in largely the same way as [RaftNetworkFactory]; the implementor is
/// responsible for producing [Client] implementations connected to cluster members on demand.
///
/// [RaftNetworkFactory]: openraft::network::RaftNetworkFactory
#[async_trait]
pub trait ClientFactory {
    type CacheClient: Client;
    async fn new_client(&mut self, target: NodeId, node: &ClusterNode) -> Self::CacheClient;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                     openraft trait implementations derived from our traits                     //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Implementation of the [openraft] [RaftNetwork] trait that we, in turn, can provide to
/// [openraft], that works by simply forwarding requests to the underlying [Client] implementation.
pub(crate) struct Connection<C> {
    inner: C,
}

impl<C: Client + Send + Sync + 'static> RaftNetwork<TypeConfig> for Connection<C> {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> StdResult<AppendEntriesResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>>
    {
        self.inner.append_entries(rpc, option).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> StdResult<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, ClusterNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.inner.install_snapshot(rpc, option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> StdResult<VoteResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>> {
        self.inner.vote(rpc, option).await
    }
}

/// Implementation of the [openraft] [RaftNetworkFactory] trait that we, in turn, can provide to
/// [openraft].
pub(crate) struct Network<F> {
    inner: F,
}

impl<F> Network<F> {
    pub fn new(inner: F) -> Network<F> {
        Network { inner }
    }
}

impl<F> RaftNetworkFactory<TypeConfig> for Network<F>
where
    F: ClientFactory + Send + Sync + 'static,
    F::CacheClient: Send + Sync,
{
    type Network = Connection<F::CacheClient>;

    async fn new_client(&mut self, target: NodeId, node: &ClusterNode) -> Self::Network {
        Connection {
            inner: self.inner.new_client(target, node).await,
        }
    }
}
