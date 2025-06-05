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
//! parameterized by types throughout.

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
// This seems like a C typedef-- should I go with the newtype idiom instead?
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
    InsertNode {
        node: NodeId,
    },
    RemoveNode {
        node: NodeId,
    },
}

// Seems like I should be returning something that can express failure?
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Response(pub ());

/// Represents an arbitrary node in our Raft cluster
// This is in-progress
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize)]
pub struct ClusterNode {
    pub addr: SocketAddr,
}

// Not sure about this... `openraft` demands that our `Node` implementation implement `Default`,
// which doesn't make a lot of sense to me, if we're going to include the network location of the
// node in tye type-- their implemetnation of `Default` just provisions the instance with the empty
// string (!?)
impl Default for ClusterNode {
    fn default() -> Self {
        Self {
            addr: SocketAddr::V4(SocketAddrV4::from_str("127.0.0.1:80").unwrap(/* known good*/)),
        }
    }
}

openraft::declare_raft_types! (
   pub TypeConfig:
    D    = Request,
    R    = Response,
    Node = ClusterNode,
);
