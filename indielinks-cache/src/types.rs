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
    io::Cursor,
    net::{SocketAddr, SocketAddrV4},
    num::NonZero,
    str::FromStr,
};

use serde::{Deserialize, Serialize};

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
