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

//! # [RecentPostsList]: A cluster-wide list of the most recently created (public) posts
//!
//! ## Introduction
//!
//! Every time a public [Post] is created anywhere in the cluster a copy is pushed onto the front of
//! a single, fixed-length, in-memory list; the oldest entry is dropped once the list is full. The
//! list is *not* sharded: it lives on exactly one node, the *owner*, designated by the value of a
//! single Raft "slot" ([SLOT_RECENT_POSTS]). An operator sets the owner at Raft-initialization time
//! and may change it at runtime; until an owner exists, the add & query operations will fail.
//!
//! [RecentPostsList] is a facade that makes the (possibly remote) list look local to its callers.
//! On every operation it consults the owning slot and either acts locally (this node is the owner)
//! or proxies the operation, over gRPC, to the owner.

use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, BTreeMap, HashMap},
    error::Error as StdError,
    fmt::{Debug, Display},
    net::SocketAddr,
    num::NonZero,
    ops::Bound,
    result::Result as StdResult,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use nonempty_collections::NEVec;
use serde::{Deserialize, Serialize};
use snafu::{prelude::*, Backtrace, IntoError, ResultExt};

use indielinks_cache::{
    network::ClientFactory as CacheClientFactory, raft::CacheNode, types::NodeId,
};
use indielinks_shared::{entities::Post, known_good};

use crate::{
    cache::{
        DecodeSnafu, EncodeSnafu, GrpcClient, GrpcClientFactory, InvalidGrpcSnafu, TonicSnafu,
        SLOT_RECENT_POSTS,
    },
    protobuf_interop::protobuf::{AddRecentPostRequest, GetRecentPostsRequest},
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Bad pagination token format"))]
    BadToken { backtrace: Backtrace },
    #[snafu(display("Cache/gRPC error: {source}"))]
    Cache { source: crate::cache::Error },
    #[snafu(display("Client error: {source}"))]
    Client {
        // I *hate* erasing the type, but if I don't, the client implementation infects code
        // that has nothing to do with the client (through this type)
        source: Box<dyn StdError + Send + Sync + 'static>,
    },
    #[snafu(display("gRPC error {source}"))]
    Grpc {
        #[snafu(source(from(tonic::Status, Box::new)))]
        source: Box<tonic::Status>,
    },
    #[snafu(display("While deriving the encryption key, {source}"))]
    Hkdf {
        source: hkdf::InvalidLength,
        backtrace: Backtrace,
    },
    #[snafu(display("While decoding to msgpack: {source}"))]
    MessagePackDecode { source: rmp_serde::decode::Error },
    #[snafu(display("While encoding to msgpack: {source}"))]
    MessagePackEncode { source: rmp_serde::encode::Error },
    #[snafu(display("While looking up a peer network location, {source}"))]
    NetLoc {
        source: indielinks_cache::raft::Error,
    },
    #[snafu(display("While decrypting the pagination token, {source}"))]
    TokenDecrypt {
        source: chacha20poly1305::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While deserializing the pagination token, {source}"))]
    TokenDeser {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While encrypting the pagination token, {source}"))]
    TokenEncrypt {
        source: chacha20poly1305::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("The pagination token is not base64-encoded: {source}"))]
    TokenNotBase64 {
        source: base64::DecodeError,
        backtrace: Backtrace,
    },
    #[snafu(display("While serializing the pagination token, {source}"))]
    TokenSer {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("The Raft has not been initialized"))]
    Uninitialized { backtrace: Backtrace },
}

pub type Result<T> = StdResult<T, Error>;

/// Pagination token
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PostKey(DateTime<Utc>);

// It is more convenient to store the `Post`s in reverse chronological order (i.e. the order in
// which we'll be serving them). That means we need to sort our keys in *reverse* chronological
// order.
impl Ord for PostKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.0 > other.0 {
            Ordering::Less
        } else if self.0 == other.0 {
            Ordering::Equal
        } else {
            Ordering::Greater
        }
    }
}

impl PartialOrd for PostKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                traits abstracting network comms                                //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait ClientFactory {
    type Client: Client + Debug;
    async fn new_client(&self, node: NodeId, addr: SocketAddr) -> Self::Client;
}

#[async_trait]
pub trait Client {
    type Error: Debug + Display + StdError + Send + Sync + 'static;
    /// Send a [Post] to the node that owns the recent posts list
    async fn add_post(&mut self, post: &Post) -> StdResult<(), Self::Error>;
    /// Retrieve the recent posts list/begin a pagination
    async fn get_posts(
        &mut self,
        key: Option<&PostKey>,
        page_size: Option<NonZero<usize>>,
    ) -> StdResult<Option<(NEVec<Post>, PostKey)>, Self::Error>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                     the recent posts list                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

const DEFAULT_PAGE_SIZE: NonZero<usize> = known_good!(NonZero::new(40));

/// A facade making the cluster's recent posts list appear local
pub struct RecentPostsList<CCF: CacheClientFactory, F: ClientFactory> {
    factory: F,
    clients: HashMap<NodeId, F::Client>,
    cache_node: CacheNode<CCF>,
    // Entries are stored from newest to oldest (i.e. we'll be popping from the back to maintain
    // list size)
    post_list: BTreeMap<PostKey, Post>,
    max_len: NonZero<usize>,
}

impl<CCF, F> RecentPostsList<CCF, F>
where
    CCF: CacheClientFactory + Send + Sync + Clone + 'static,
    <CCF as CacheClientFactory>::CacheClient: Send + Sync + Clone,
    F: ClientFactory,
    <F as ClientFactory>::Client: Clone,
{
    pub fn new(cache_node: CacheNode<CCF>, max_len: NonZero<usize>, factory: F) -> Self {
        Self {
            factory,
            clients: HashMap::new(),
            cache_node,
            post_list: BTreeMap::new(),
            max_len,
        }
    }
    /// Invoke this function for each new public [Post] made in the cluster
    pub async fn add_post(&mut self, post: Post) -> Result<()> {
        match self.responsible().await? {
            Some((node, addr)) => {
                self.post_list.clear();
                self.client_for_node(node, addr)
                    .await
                    .add_post(&post)
                    .await
                    .map_err(|e| ClientSnafu.into_error(Box::new(e)))
            }
            None => {
                self.post_list.insert(PostKey(Utc::now()), post);
                while self.post_list.len() > self.max_len.get() {
                    self.post_list.pop_last();
                }
                Ok(())
            }
        }
    }
    /// Retrieve the current list; begin/continue a pagination
    pub async fn get_recent_posts(
        &mut self,
        key: Option<&PostKey>,
        page_size: Option<NonZero<usize>>,
    ) -> Result<Option<(NEVec<Post>, PostKey)>> {
        match self.responsible().await? {
            Some((node, addr)) => self
                .client_for_node(node, addr)
                .await
                .get_posts(key, page_size)
                .await
                .map_err(|e| ClientSnafu.into_error(Box::new(e))),
            None => Ok(match key {
                Some(key) => self.make_page(
                    self.post_list
                        .range((Bound::Excluded(key), Bound::Unbounded)),
                    page_size,
                ),
                None => self.make_page(self.post_list.iter(), page_size),
            }),
        }
    }

    // Infallibly take an iterator pointing to the beginning of what will be a page and convert it
    // to that page
    fn make_page<'a>(
        &self,
        mut i: impl Iterator<Item = (&'a PostKey, &'a Post)>,
        page_size: Option<NonZero<usize>>,
    ) -> Option<(NEVec<Post>, PostKey)> {
        // `i` points to the first element we'll be returning. It may be past the end of our
        // collection; that's fine, we'll just return `None` in that case to signal the end of the
        // pagination.
        match i.next() {
            Some((key, post)) => {
                // Our page has at least one `Post`. Collect the rest as a vector of references into
                // our page list (we don't need to make copies quite yet-- we need to discover the
                // next pagination token).
                let rest = i
                    .take(page_size.unwrap_or(DEFAULT_PAGE_SIZE).get() - 1)
                    .collect::<Vec<(&PostKey, &Post)>>();
                // If `res` is empty, our pagination token is just the key for the sole `Post` in
                // our page; else it's the key of the last element of `rest`.
                let token = match rest.last() {
                    Some((key, _)) => key,
                    None => key,
                }
                .clone();
                Some((
                    // That's it-- form a non-empty vector of `Post`:
                    (
                        post.clone(),
                        rest.into_iter()
                            .map(|(_, v)| v.clone())
                            .collect::<Vec<Post>>(),
                    )
                        .into(),
                    token,
                ))
            }
            None => None,
        }
    }

    async fn client_for_node(&mut self, node: NodeId, addr: SocketAddr) -> F::Client {
        match self.clients.entry(node) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let client = self.factory.new_client(node, addr).await;
                entry.insert(client.clone());
                client
            }
        }
    }
    // Detrmine whether the node on which we're currently running is responsible for the "recent
    // posts" list; if not, return a pair consisting of the Node ID & `SocketAddr` of the
    // responsible node.
    async fn responsible(&self) -> Result<Option<(NodeId, SocketAddr)>> {
        let owner = self
            .cache_node
            .node_for_slot(*SLOT_RECENT_POSTS)
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
impl ClientFactory for GrpcClientFactory {
    type Client = GrpcClient;
    async fn new_client(&self, node: NodeId, addr: SocketAddr) -> Self::Client {
        GrpcClient::new(node, addr)
    }
}

#[async_trait]
impl Client for GrpcClient {
    type Error = crate::cache::Error;
    async fn add_post(&mut self, post: &Post) -> StdResult<(), Self::Error> {
        self.ensure_connected()
            .await?
            .add_recent_post(AddRecentPostRequest {
                post: rmp_serde::to_vec_named(post).context(EncodeSnafu)?,
            })
            .await
            .context(TonicSnafu)
            .map(|_| ())
    }
    async fn get_posts(
        &mut self,
        key: Option<&PostKey>,
        page_size: Option<NonZero<usize>>,
    ) -> StdResult<Option<(NEVec<Post>, PostKey)>, Self::Error> {
        let response = self
            .ensure_connected()
            .await?
            .get_recent_posts(GetRecentPostsRequest {
                key: key
                    .map(|key| rmp_serde::to_vec_named(&key).context(EncodeSnafu))
                    .transpose()?,
                page_size: page_size.map(|x| x.get() as u64),
            })
            .await
            .context(TonicSnafu)?
            .into_inner();
        tracing::debug!("get_posts() response is {response:?}");
        // Arghhh... the impedence mismatch between protobufs & Rust is bad here. Regrettably,
        // an empty `Vec<u8>` will *not* deserialize to an empty `Vec<Post>`.
        let posts = if response.posts.is_empty() {
            Vec::new()
        } else {
            rmp_serde::from_slice::<Vec<Post>>(response.posts.as_slice()).context(DecodeSnafu)?
        };
        let mut posts = posts.into_iter();
        match (posts.next(), response.key) {
            (None, None) => Ok(None), // Pagination done
            (Some(first), Some(key)) => Ok(Some((
                (first, posts.collect::<Vec<Post>>()).into(),
                rmp_serde::from_slice(key.as_slice()).context(DecodeSnafu)?,
            ))),
            (_, _) => InvalidGrpcSnafu.fail(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           unit tests                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use std::{convert::Infallible, net::SocketAddr};

    use chrono::Utc;
    use nonzero::nonzero;

    use indielinks_cache::{
        network::null_client::NullClientFactory as CCFNull,
        types::{ClusterNode, InMemoryLogStore, SlotIndex},
    };
    use indielinks_shared::known_good;

    use super::*;

    #[derive(Clone, Debug)]
    struct NullClient;

    #[async_trait]
    impl Client for NullClient {
        type Error = Infallible;

        async fn add_post(&mut self, _: &Post) -> StdResult<(), Self::Error> {
            unimplemented!()
        }
        async fn get_posts(
            &mut self,
            _: Option<&PostKey>,
            _: Option<NonZero<usize>>,
        ) -> StdResult<Option<(NEVec<Post>, PostKey)>, Self::Error> {
            unimplemented!()
        }
    }

    #[derive(Clone)]
    struct NullClientFactory;

    #[async_trait]
    impl ClientFactory for NullClientFactory {
        type Client = NullClient;
        async fn new_client(&self, _: NodeId, _: SocketAddr) -> Self::Client {
            NullClient
        }
    }

    #[tokio::test]
    async fn smoke() {
        let cache_node = known_good!(
            CacheNode::new(&Default::default(), CCFNull, InMemoryLogStore::default(),).await
        );
        let mut posts =
            RecentPostsList::new(cache_node.clone(), nonzero!(2usize), NullClientFactory);

        // Raft not initialized-- we're done.
        assert!(posts.get_recent_posts(None, None).await.is_err());

        cache_node
            .initialize(
                vec![(
                    0,
                    ClusterNode {
                        addr: known_good!("127.0.0.1:8080".parse::<SocketAddr>()),
                    },
                )],
                vec![(known_good!(SlotIndex::new(0)), 0)],
            )
            .await
            .expect("Raft initialization failed");

        let result = posts
            .add_post(Post::new(
                &known_good!("https://example.com".to_owned().try_into()),
                &Default::default(),
                &Default::default(),
                &Utc::now(),
                "Example 1",
                None,
                &Default::default(),
                true,
                false,
            ))
            .await;
        assert!(result.is_ok());
        let result = posts
            .add_post(Post::new(
                &known_good!("https://example.com".to_owned().try_into()),
                &Default::default(),
                &Default::default(),
                &Utc::now(),
                "Example 2",
                None,
                &Default::default(),
                true,
                false,
            ))
            .await;
        assert!(result.is_ok());
        let result = posts
            .add_post(Post::new(
                &known_good!("https://example.com".to_owned().try_into()),
                &Default::default(),
                &Default::default(),
                &Utc::now(),
                "Example 3",
                None,
                &Default::default(),
                true,
                false,
            ))
            .await;
        assert!(result.is_ok());

        // Retrieve 'em a page at a time; make sure the first was dropped & the order is
        // reverse-chronological.
        let (page, token) = posts
            .get_recent_posts(None, Some(nonzero!(1usize)))
            .await
            .expect("get_recent_posts (page 1) failed")
            .expect("expected a first page");
        assert_eq!(page.len().get(), 1);
        assert_eq!(page.first().title(), "Example 3");

        let (page, _token) = posts
            .get_recent_posts(Some(&token), Some(nonzero!(1usize)))
            .await
            .expect("get_recent_posts (page 2) failed")
            .expect("expected a second page");
        assert_eq!(page.len().get(), 1);
        assert_eq!(page.first().title(), "Example 2");
    }
}
