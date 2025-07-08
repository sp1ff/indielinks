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

//! Trivial implementation of main to exercise [indielinks-cache](crate).
//!
//! # Introduction
//!
//! This binary is a trivial implementation of an [indielinks-cache] node for a cluster implementing
//! distributed key-value store.

use std::{
    collections::BTreeMap,
    io,
    net::{SocketAddr, SocketAddrV4},
    ops::RangeBounds,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::State,
    response::IntoResponse,
    routing::{get, post},
};
use bpaf::{Parser, construct};
use http::StatusCode;
use openraft::{
    Entry, LogId, LogState, OptionalSend, RaftLogId, RaftLogReader, StorageError, Vote,
    error::{NetworkError, RemoteError, Unreachable},
    storage::{LogFlushed, RaftLogStorage},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{from_value, to_value};
use snafu::{Backtrace, IntoError, ResultExt, Snafu};
use tap::{Pipe, Tap};
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
    sync::{Notify, RwLock},
};
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::{Level, debug, error, info, instrument, subscriber::set_global_default};
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};

use indielinks_cache::{
    cache::Cache,
    network::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotError, InstallSnapshotRequest,
        InstallSnapshotResponse, RPCError, RPCOption, RaftError, VoteRequest, VoteResponse,
    },
    raft::{CacheNode, Configuration},
    types::{CacheId, ClusterNode, NodeId, TypeConfig},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Cache error {source}"))]
    Cache {
        #[snafu(source(from(indielinks_cache::cache::Error<Client>, Box::new)))]
        source: Box<indielinks_cache::cache::Error<Client>>,
        backtrace: Backtrace,
    },
    #[snafu(display("Unknown cache ID {id}"))]
    CacheId { id: CacheId },
    #[snafu(display("Cache RPC failure: {source}"))]
    CacheRpc {
        source: reqwest::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("When deserializing a cache RPC response: {source}"))]
    CacheRpcDe {
        source: reqwest::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to form a cache RPC request: {source}"))]
    CacheRpcRequest {
        source: reqwest::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Cache RPC response with an error status code: {source}"))]
    CacheRpcStatus {
        source: reqwest::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While deserializing from a JSON value: {source}"))]
    JsonDe {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While serializing to a JSON value: {source}"))]
    JsonSer {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Raft error {source}"))]
    Raft {
        source: indielinks_cache::raft::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Bad socket address: {source}"))]
    SocketAddr {
        addr: String,
        source: std::net::AddrParseError,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

use std::error::Error as StdError;

#[derive(Clone)]
struct ClientFactory;

#[async_trait]
impl indielinks_cache::network::ClientFactory for ClientFactory {
    type CacheClient = Client;
    async fn new_client(&mut self, target: NodeId, node: &ClusterNode) -> Self::CacheClient {
        Client::new(target, node.addr)
    }
}

#[derive(Clone)]
pub struct Client {
    id: NodeId,
    addr: SocketAddr,
    client: reqwest::Client,
}

#[derive(Debug, Deserialize, Serialize)]
struct CacheInsertRequest {
    cache: CacheId,
    key: serde_json::Value,
    value: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize)]
struct CacheInsertResponse {
    cache: CacheId,
    key: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize)]
struct CacheLookupRequest {
    cache: CacheId,
    key: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize)]
struct CacheLookupResponse {
    cache: CacheId,
    key: serde_json::Value,
    value: Option<serde_json::Value>,
}

impl Client {
    pub fn new(id: NodeId, addr: SocketAddr) -> Client {
        Client {
            id,
            addr,
            client: reqwest::Client::new(),
        }
    }
    pub async fn send_raft_rpc<Req, Rsp, Err>(
        &self,
        req: &Req,
        path: impl AsRef<str>,
    ) -> StdResult<Rsp, RPCError<NodeId, ClusterNode, Err>>
    where
        Req: Serialize,
        Rsp: DeserializeOwned,
        Err: StdError + DeserializeOwned,
    {
        let url = format!("http://{}/{}", self.addr, path.as_ref());

        let resp = self.client.post(url).json(&req).send().await.map_err(|e| {
            // If the error is a connection error, we return `Unreachable` so that connection isn't
            // retried immediately:
            if e.is_connect() {
                return indielinks_cache::network::RPCError::Unreachable(Unreachable::new(&e));
            }
            indielinks_cache::network::RPCError::Network(NetworkError::new(&e))
        })?;

        resp.json::<StdResult<Rsp, Err>>()
            .await
            .map_err(|e| indielinks_cache::network::RPCError::Network(NetworkError::new(&e)))?
            .map_err(|e| {
                indielinks_cache::network::RPCError::RemoteError(RemoteError::new(self.id, e))
            })
    }
    pub async fn send_cache_rpc<Req, Rsp>(
        &self,
        method: reqwest::Method,
        body: &Req,
        path: impl AsRef<str>,
    ) -> Result<Rsp>
    where
        Req: Serialize,
        Rsp: DeserializeOwned,
    {
        self.client
            .execute(
                self.client
                    .request(method, format!("http://{}/{}", self.addr, path.as_ref()))
                    .json(&body)
                    .build()
                    .context(CacheRpcRequestSnafu)?,
            )
            .await
            .context(CacheRpcSnafu)?
            .tap(|rsp| debug!("Client::send_cache_rpc: resp is {:?}", rsp))
            .pipe(|rsp| rsp.error_for_status())
            .context(CacheRpcStatusSnafu)?
            .json::<Rsp>()
            .await
            .context(CacheRpcDeSnafu)
    }
}

#[async_trait]
impl indielinks_cache::network::Client for Client {
    type ErrorType = Error;
    /// Append Raft log entries to the target node's log store
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> StdResult<AppendEntriesResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>>
    {
        self.send_raft_rpc(&req, "raft/append").await
    }
    /// Install a state snapshot on the target node
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> StdResult<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, ClusterNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.send_raft_rpc(&req, "raft/install").await
    }
    /// Request a leadership vote from the target node
    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> StdResult<VoteResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>> {
        self.send_raft_rpc(&req, "raft/vote").await
    }
    /// Ask the target node to insert a key/value pair into it's LRU cache
    async fn cache_insert<K: Serialize + Sync, V: Serialize + Sync>(
        &mut self,
        cache: CacheId,
        key: impl Into<K> + Send,
        value: impl Into<V> + Send,
    ) -> Result<()> {
        let rsp: CacheInsertResponse = self
            .send_cache_rpc(
                reqwest::Method::POST,
                &CacheInsertRequest {
                    cache,
                    key: to_value::<K>(key.into()).context(JsonSerSnafu)?,
                    value: to_value::<V>(value.into()).context(JsonSerSnafu)?,
                },
                "cache/insert",
            )
            .await?;
        debug!("Cache insert :=> {rsp:?}");
        Ok(())
    }
    /// Request a value for a given key from the target node
    async fn cache_lookup<K: Serialize, V: DeserializeOwned>(
        &mut self,
        cache: CacheId,
        key: impl Into<K> + Send,
    ) -> Result<Option<V>> {
        let rsp: CacheLookupResponse = self
            .send_cache_rpc(
                reqwest::Method::GET,
                &CacheLookupRequest {
                    cache,
                    key: to_value::<K>(key.into()).context(JsonSerSnafu)?,
                },
                "cache/lookup",
            )
            .await?;
        match rsp.value {
            Some(v) => Ok(Some(from_value::<V>(v).context(JsonDeSnafu)?)),
            None => Ok(None),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          Log Storage                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Default)]
struct LogStore {
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
impl RaftLogReader<TypeConfig> for LogStore {
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

impl RaftLogStorage<TypeConfig> for LogStore {
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

#[cfg(test)]
mod test {
    use openraft::StorageError;

    use super::*;

    struct Builder;

    impl indielinks_cache::raft::test::StoreBuilder<LogStore> for Builder {
        async fn build(&self) -> StdResult<((), LogStore), StorageError<NodeId>> {
            Ok(((), LogStore::default()))
        }
    }

    #[test_log::test]
    fn test_log_store() {
        let res = indielinks_cache::raft::test::test_storage(Builder);
        debug!("openraft :=> {res:#?}");
        assert!(res.is_ok());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                   Raft API -- this is the Raft nodes talking to one another                    //
////////////////////////////////////////////////////////////////////////////////////////////////////

async fn raft_append(
    State(state): State<AppState>,
    Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> axum::response::Response {
    // Serialize the entire `Result`
    Json(state.node.append_entries(req).await).into_response()
}

async fn raft_install(
    State(state): State<AppState>,
    Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> axum::response::Response {
    // Serialize the entire `Result`
    Json(state.node.install_snapshot(req).await).into_response()
}

async fn raft_vote(
    State(state): State<AppState>,
    Json(req): Json<VoteRequest<NodeId>>,
) -> axum::response::Response {
    // Serialize the entire `Result`
    Json(state.node.vote(req).await).into_response()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                  Cache API -- this is the cache nodes talking to one another                   //
////////////////////////////////////////////////////////////////////////////////////////////////////

async fn cache_insert(
    State(state): State<AppState>,
    Json(req): Json<CacheInsertRequest>,
) -> axum::response::Response {
    async fn cache_insert1(
        state: AppState,
        req: CacheInsertRequest,
    ) -> Result<CacheInsertResponse> {
        if req.cache != 1 {
            return CacheIdSnafu { id: req.cache }.fail();
        }

        // Here, I "just know" that our cache maps String -> usize, so...
        state
            .cache
            .write()
            .await
            .insert(
                from_value::<String>(req.key.clone()).context(JsonDeSnafu)?,
                from_value::<usize>(req.value).context(JsonDeSnafu)?,
            )
            .await
            .context(CacheSnafu)?;

        Ok(CacheInsertResponse {
            cache: req.cache,
            key: req.key,
        })
    }

    // It's kind of cool that the raft handlers (above) just return the entire `StdResult`, but they
    // can only get away with that because `openraft::error::RPCError` implements `Serialize`, and I
    // don't care to do that for this module's `Error` type.
    match cache_insert1(state, req).await {
        Ok(rsp) => (StatusCode::OK, Json(rsp)).into_response(),
        Err(err) => {
            error!("{err:?}");
            StatusCode::BAD_REQUEST.into_response()
        }
    }
}

async fn cache_lookup(
    State(state): State<AppState>,
    Json(req): Json<CacheLookupRequest>,
) -> axum::response::Response {
    async fn cache_lookup1(
        state: AppState,
        req: CacheLookupRequest,
    ) -> Result<CacheLookupResponse> {
        if req.cache != 1 {
            return CacheIdSnafu { id: req.cache }.fail();
        }
        // Here, I "just know" that our cache maps String -> usize, so...
        let value = state
            .cache
            .write()
            .await
            .get(&from_value::<String>(req.key.clone()).context(JsonDeSnafu)?)
            .await
            .context(CacheSnafu)?
            .map(|n| to_value(n).context(JsonSerSnafu))
            .transpose()?;

        Ok(CacheLookupResponse {
            cache: req.cache,
            key: req.key,
            value,
        })
    }

    // It's kind of cool that the raft handlers (above) just return the entire `StdResult`, but they
    // can only get away with that because `openraft::error::RPCError` implements `Serialize`, and I
    // don't care to do that for this module's `Error` type.
    match cache_lookup1(state, req).await {
        Ok(rsp) => (StatusCode::OK, Json(rsp)).into_response(),
        Err(err) => {
            error!("{err:?}");
            StatusCode::BAD_REQUEST.into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                      Admin API -- here's where you configure the cluster                       //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Initialize this node as part of a Raft cluster with the given members
///
/// This is an awful API, but expedient-- if `req` is the empty vector, intiialize this node as a
/// single-node cluster using the ID & address provided on the command line. Else, initialize it as
/// a multi-node cluster.
async fn admin_init(
    State(state): State<AppState>,
    // Refine this request type-- this is awful
    Json(req): Json<Vec<(NodeId, String /* SockAddrV4 */)>>,
) -> impl axum::response::IntoResponse {
    async fn admin_init1(state: AppState, mut req: Vec<(u64, String)>) -> Result<()> {
        if req.is_empty() {
            req.push((state.id, state.addr.to_string()));
        }

        let req = req
            .into_iter()
            .map(|(id, addr)| match SocketAddr::from_str(&addr) {
                Ok(addr) => Ok((id, ClusterNode { addr })),
                Err(err) => Err(SocketAddrSnafu { addr }.into_error(err)),
            })
            .collect::<Result<Vec<(NodeId, ClusterNode)>>>()?;
        state
            .node
            .initialize(BTreeMap::from_iter(req.into_iter()))
            .await
            .tap(|result| info!("Initialization of the Raft yielded: {:?}", result))
            .context(RaftSnafu)?;

        Ok(())
    }

    // Unlike the raft & cache handlers above, this endpoint is user-facing, so let's try to return
    // something a bit more ergonomic for a human
    match admin_init1(state, req).await {
        Ok(_) => StatusCode::CREATED,
        err @ Err(Error::SocketAddr { .. }) => {
            error!("{err:?}");
            StatusCode::BAD_REQUEST
        }
        Err(err) => {
            error!("{err:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn admin_metrics(State(state): State<AppState>) -> axum::response::Response {
    Json(state.node.metrics().await).into_response()
}

/// Add a node to the cluster
///
/// New nodes must be added as learners.
async fn admin_add_learner(
    State(state): State<AppState>,
    Json((node_id, addr)): Json<(NodeId, String /* SocketAddrV4 */)>,
) -> axum::response::Response {
    match addr.parse::<SocketAddr>() {
        Ok(addr) => Json(
            state
                .node
                // Imma try setting `blocking` to true for now
                .add_learner(node_id, ClusterNode { addr }, true)
                .await,
        )
        .into_response(),
        Err(err) => {
            error!("{err:?}");
            (StatusCode::BAD_REQUEST, format!("{err:#?}")).into_response()
        }
    }
}

async fn admin_change_membership(
    State(state): State<AppState>,
    Json(req): Json<Vec<NodeId>>,
) -> axum::response::Response {
    Json(state.node.change_membership(req, false).await).into_response()

    //     // This is kinda lame-- we shouldn't re-init the hash ring, just update it.
    //     if result.is_ok() {
    //         let nodes = state
    //             .raft
    //             .metrics()
    //             .borrow()
    //             .membership_config
    //             .nodes()
    //             .map(|(id, _)| *id)
    //             .collect();
    //         let _ = state
    //             .raft
    //             .client_write(Request::Init {
    //                 nodes,
    //                 num_virtual: 0,
    //             })
    //             .await;
    //     }

    //     Json(result).into_response()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        application API                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

// async fn get_hash_ring(State(state): State<NodeState>) -> axum::response::Response {
//     Json(state.state_machine.get_hash_ring()).into_response()
// }

// async fn update_hash_ring(
//     State(state): State<NodeState>,
//     Json((_shard, node)): Json<(u64, NodeId)>,
// ) -> axum::response::Response {
//     Json(state.raft.client_write(Request::InsertNode { node }).await).into_response()
// }

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             main()                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

// Giving `bpaf` a try
#[derive(Debug)]
struct Options {
    no_color: bool,
    verbose: bool,
    id: NodeId,
    addr: SocketAddrV4,
}

fn options() -> impl Parser<Options> {
    construct!(Options {
        no_color(bpaf::short('c').long("no-color").help("Disable logging in color").switch()),
        verbose(bpaf::short('v').long("verbose").help("Increase the verbosity").switch()),
        id(bpaf::positional::<NodeId>("ID").help("Node ID, expressed as an unsigned integer")),
        addr(bpaf::positional::<String>("SOCKADDR").parse(|s| SocketAddrV4::from_str(&s))),
    })
}

#[derive(Clone)]
struct AppState {
    id: NodeId,
    addr: SocketAddrV4,
    node: CacheNode<ClientFactory>,
    // I don't think `Cache` needs to be wrapped this way, since the inner is already
    cache: Arc<RwLock<Cache<ClientFactory, String, usize>>>,
}

#[tokio::main]
async fn main() {
    let opts = options().to_options().run();
    set_global_default(
        Registry::default()
            .with(
                fmt::Layer::default()
                    .compact()
                    .with_ansi(!opts.no_color)
                    .with_writer(io::stdout),
            )
            .with(
                EnvFilter::builder()
                    .with_default_directive(if opts.verbose {
                        Level::DEBUG.into()
                    } else {
                        Level::INFO.into()
                    })
                    .from_env()
                    .expect("Failed to retrieve RUST_LOG"),
            ),
    )
    .expect("Failed to set the global default tracing subscriber");

    info!("Logging initialized");

    let config = Configuration::builder("in-memory-test", opts.id)
        .heartbeat_interval(Duration::from_millis(500))
        .election_timeout_min(Duration::from_millis(1500))
        .election_timeout_max(Duration::from_millis(3000))
        .build();
    let this_node = CacheNode::new(&config, ClientFactory, LogStore::default())
        .await
        .expect("Failed to create this process' indielinks-cache node");

    let state = AppState {
        id: opts.id,
        addr: opts.addr,
        node: this_node.clone(),
        cache: Arc::new(RwLock::new(Cache::new(1, this_node))),
    };

    let mut sigkill = signal(SignalKind::terminate()).expect("Failed to subscribe to SIGKILLs");
    let nfy = Arc::new(Notify::new());

    let mut server = axum::serve(
        TcpListener::bind(opts.addr)
            .await
            .expect("Failed to bind the given address-- is someone already listening?"),
        Router::new()
            .route("/healthcheck", get(|| async move { "GOOD" }))
            .route("/raft/append", post(raft_append))
            .route("/raft/install", post(raft_install))
            .route("/raft/vote", post(raft_vote))
            .route("/cache/lookup", get(cache_lookup))
            .route("/cache/insert", post(cache_insert))
            .route("/admin/init", post(admin_init))
            .route("/admin/metrics", get(admin_metrics))
            .route("/admin/add-learner", post(admin_add_learner))
            .route("/admin/membership", post(admin_change_membership))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(
                        DefaultMakeSpan::new()
                            .include_headers(true)
                            .level(Level::INFO),
                    )
                    .on_response(
                        DefaultOnResponse::new()
                            .include_headers(true)
                            .level(Level::INFO),
                    ),
            )
            .with_state(state),
    )
    .with_graceful_shutdown({
        let nfy = nfy.clone();
        || async move { nfy.notified().await }
    }())
    .into_future();

    info!(
        "Serving requests at http://{}; healtcheck endpoint at http://{}/healthcheck",
        opts.addr, opts.addr
    );

    tokio::select! {
        _ = &mut server => unimplemented!(),
        _ = sigkill.recv() => {
            info!("Received SIGKILL; shutting down...");
            nfy.notify_one();
            if let Err(err) = server.await {
                error!("On server shutdown: {err:#?}");
            }
            else {
                info!("Received SIGKILL; shutting down...done.");
            }
        }
    }

    info!("Good-bye.");
}
