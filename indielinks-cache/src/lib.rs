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
//! ## Some First Thoughts
//!
//! The Raft consensus protocol (see [In Search of an Understandable Consensus Algorithm])
//! synchronises a (typically small) state across a cluster of nodes. It doesn't guarantee that the
//! state is exactly the same across all nodes at all times; rather it guarantees that a message log
//! is *seen* by all the nodes in the same order. A given node may be behind it's peers at any given
//! time, in terms of the log messages it's committed, but given enough time, it will eventually
//! catch-up, and it will catch up by committing the exact messages committed by its peers, in the
//! same order.
//!
//! [In Search of an Understandable Consensus Algorithm]: https://raft.github.io/raft.pdf
//!
//! The state on any given node then, is the cumulative application of all the committed log
//! messages. In order to use the [openraft] crate, we need to customize its generic implementation
//! in several ways, beginning with the _log messages_. [openraft] defines a trait consisting
//! solely of associated types, [RaftTypeConfig]. The first associated type, [D], is the
//! application-specific log message. This is typically a sum type containing all the differnet
//! ways in which the state can be mutated.
//!
//! [openraft]: https://docs.rs/openraft/latest/openraft/docs/getting_started/index.html
//! [RaftTypeConfig]: https://docs.rs/openraft/latest/openraft/type_config/trait.RaftTypeConfig.html
//! [D]: https://docs.rs/openraft/latest/openraft/type_config/trait.RaftTypeConfig.html#associatedtype.D
//!
//! In our case, this means:
//!
//! - variants for manipulating the hash ring (setting it to its initial state, inserting nodes, &
//!   removing nodes)
//! - variants for designating "owners" for the assorted top-k counts
//!
//! Raft models the process of applying one log message after another as a state machine, and makes
//! certain demands on the implementation as to their persistence and management (compaction e.g.).
//! [openraft] models these demands as [RaftLogStorage] and [RaftStateMachine], respectively.
//!
//! [RaftLogStorage]: https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html
//! [RaftStateMachine]: https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html
//!
//! As the name implies, [RaftLogStorage] is mostly concerned with persistence. In our case, we'll
//! leverage the backend for this, defining our own trait listing the methods we'll need, and
//! demanding an implementation from our caller. [RaftStateMachine] is concerned with updating the
//! state in response to received log messages, as well snapshot handling.
//!
//! Finally, [openraft] is in the same boat we're in: we need to exchange messages with other
//! members of the cluster, but we'd prefer not to open sockets & choose transport mechanisms in the
//! library. [openraft] handles this by defining the [RaftNetwork] trait with a method per message
//! to be exchanged. On the _receiving_ side, [openraft] says... very little. You pretty-much have
//! to look at the samples to realize that you need to setup a server that is compatible with your
//! [RaftNetwork] implementation, and invoke certain methods on the [Raft] struct you're
//! maintaining.
//!
//! [RaftNetwork]: https://docs.rs/openraft/latest/openraft/network/trait.RaftNetwork.html
//! [Raft]: https://docs.rs/openraft/latest/openraft/raft/struct.Raft.html
//!
//! I'd hoped to present a nicer abstraction to this crate's consumers, but I have to admit:
//! it's a challenging design problem. I'm working off of two use cases:
//!
//! 1. the application starts by wanting to use JSON over HTTP (simple, easy to trouble-shoot)
//! 2. once things are working the application wants to mvoe to gRPC for performance reasons
//!
//! I'd _like_ the application to be able to shift from 1. to 2. with no change to the library code.
//! I believe this implies that the library is going to have to:
//!
//! 1. leak its messages to the app on the send and receive side
//! 2. just document the fact that the application, on receipt of thus-and-such a message, should
//!    call this-or-that method in the library
//!
//! I thought about doing something clever on the receive side, like having the application setup a
//! =TcpStream= and handing it off to the library, but that would mean the library would need to
//! know all about the transport mechanism the app has chosen in order to deserialize the incoming
//! messages! I just don't see a way out of having serde handled by the app, not the library.
//!
//! There is one other possible thing that occurs to me: have the library provide indirect support
//! for JSON/HTTP & gRPC. The library could provide, say an =axum::Route= instance and a
//! =tonic::Grpc= instance; the dispatch piece of a server without the routing piece. The
//! application could assemble the provided routing piece with their own transport choices. The
//! downside here is that if the app wants _another_ RPC mechanism, the library would have to
//! implement it.
//!
//! ## Why Am I Not Using Redis?
//!
//! The reader may well ask: why do all this? Why just not use Redis (or something similar)? Perhaps
//! I will, yet, but I don't think that should be the default answer. Taking a dependency is not a
//! cost-free, unalloyed good. You're now dependent on someone else's code, and in the case of a
//! completely separate distributed cache, you've added a network hop & concomitant
//! point-of-failure. I'd _still_ need to write code to specialize the cache to indielinks'
//! particular use case. Finally, I've tried to get any number of Fediverse apps up & running
//! locally (to test interoperability); in every case, I've only succeeded when some dev
//! took the time to write a `docker-compose` file with the correct incantations, since they
//! _all have so many moving parts_
//!
//! Rather, the choice to take a dependency should be viewed as a trade-off: is the work savings
//! worth the cost? Without that limiting prinicple, you wind-up in the state node finds itself in,
//! with packages devoted to [trimming whitespace] (there are several; the package at the link was
//! just the first search hit). Now, fifteen or twenty years ago, the answer would have been clear:
//! consensus protocols were bleeding edge technology; available only in the academic literature or
//! in closed-source code bases at places like Google or Twitter. Today? I'm not so sure. [openraft]
//! has been [downloaded] hundreds of thousands of times, and I had it up & running after two
//! afternoons of hacking. For now, that along with the operational simplicity I can gain by not
//! adding another component makes me land on the side of "just coding it up".
//!
//! [trimming whitespace]: https://www.npmjs.com/package/trim-whitespace
//! [downloaded]: https://crates.io/crates/openraft

// I'm thinking the initialization process will work like this:
// 1. the application stands-up it's server implementation
// 2. the app then initializes indielinks-cache, passing it:
//    - the Raft cluster configuration
//    - any Raft configuration I choose to expose
//    - a "network" implementation
// 3. we'll create the Raft instance here, and return... something (cookie, token, type whatever)
// 4. indielinks then creates its caches (and top-k lists), _passing in_ the token it got
//    in step 3

pub mod cache;
pub mod network;
pub mod raft;
pub mod types;
