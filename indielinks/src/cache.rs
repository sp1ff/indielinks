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

//! # cache
//!
//! The [indielinks](crate) interface to [indielinks-cache].

use std::{
    collections::BTreeMap,
    fmt::Debug,
    net::SocketAddr,
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use async_trait::async_trait;
use axum::{
    Router,
    extract::{Json, State},
    response::IntoResponse,
    routing::{get, post},
};
use http::{HeaderValue, StatusCode, header::CONTENT_TYPE};
use num_bigint::BigInt;
use openraft::{
    Entry, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend, RaftLogReader, StorageError,
    StorageIOError, Vote,
    error::NetworkError,
    storage::{LogFlushed, RaftLogStorage},
};
use scylla::{
    DeserializeRow, SerializeRow,
    cluster::metadata::ColumnType,
    deserialize::{FrameSlice, value::DeserializeValue},
    errors::{DeserializationError, SerializationError, TypeCheckError},
    serialize::{
        value::SerializeValue,
        writers::{CellWriter, WrittenCellProof},
    },
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{from_value, to_value};
use snafu::{Backtrace, IntoError, ResultExt, Snafu};
use tap::{Conv, Pipe, TryConv};
use tonic::Code;

use indielinks_cache::{
    network::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotError, InstallSnapshotRequest,
        InstallSnapshotResponse, RPCError, RPCOption, RaftError, VoteRequest, VoteResponse,
    },
    raft::CacheNode,
    types::{CacheId, ClusterNode, NodeId, TypeConfig},
};
use tower_http::{cors::CorsLayer, set_header::SetResponseHeaderLayer};
use tracing::{error, info};

use crate::{
    entities::{FollowerId, StorUrl},
    http::Indielinks,
    protobuf_interop::*,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Cache error: {source}"))]
    Cache {
        // The obvious approach, 👇, creates a cycle!
        // source: indielinks_cache::cache::Error<GrpcClient>,
        #[snafu(source(from(indielinks_cache::cache::Error::<GrpcClient>, Box::new)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to connect to gRPC endpoint: {source}"))]
    Conn {
        source: tonic::transport::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Deserialization error: {source}"))]
    De {
        source: rmp_serde::decode::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Attempted to deserialize an invalid value for Flavor: {n}"))]
    FlavorDe { n: i8, backtrace: Backtrace },
    #[snafu(display("Failed to deserialize {key} as a FollowerId: {source}"))]
    FollowerId {
        key: serde_json::Value,
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("openraft/protbuf interoperability error: {source}"))]
    Interop {
        source: crate::protobuf_interop::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("gRPC error: {source}"))]
    Tonic {
        source: tonic::Status,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to serialize an URL to JSON: {source}"))]
    Url {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize an URL to JSON: {source}"))]
    UrlDe {
        value: serde_json::Value,
        source: serde_json::Error,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[repr(i8)]
pub enum Flavor {
    Vote = 0,
    LastPurged = 1,
}

impl std::fmt::Display for Flavor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Flavor::Vote => "Vote",
                Flavor::LastPurged => "LastPurged",
            }
        )
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for Flavor {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        i8::type_check(typ)
    }
    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        match <i8 as DeserializeValue>::deserialize(typ, v)? {
            0 => Ok(Flavor::Vote),
            1 => Ok(Flavor::LastPurged),
            n => Err(DeserializationError::new(FlavorDeSnafu { n }.build())),
        }
    }
}

impl SerializeValue for Flavor {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&(*self as i8), typ, writer)
    }
}

/// Object-safe trait abstracting over the storage backend for operations required by [LogStore]
///
/// The [LogStore] will write [Raft] log messages and so on to the [indielinks](crate) storage
/// backend, which in production may be ScyllaDB or DynamoDB. This trait is intended to be
/// implemented by either of the corresponding backends, and an implementation given to the log
/// store.
///
/// [Raft]: https://raft.github.io/raft.pdf
#[async_trait]
pub trait Backend {
    async fn append(&self, entries: Vec<Entry<TypeConfig>>) -> StdResult<(), StorageError<NodeId>>;
    async fn drop_all_rows(&self) -> StdResult<(), StorageError<NodeId>>;
    async fn get_log_state(&self) -> StdResult<LogState<TypeConfig>, StorageError<NodeId>>;
    async fn purge(&self, log_id: LogId<NodeId>) -> StdResult<(), StorageError<NodeId>>;
    async fn read_vote(&self) -> StdResult<Option<Vote<NodeId>>, StorageError<NodeId>>;
    async fn save_vote(&self, vote: &Vote<NodeId>) -> StdResult<(), StorageError<NodeId>>;
    async fn truncate(&self, log_id: LogId<NodeId>) -> StdResult<(), StorageError<NodeId>>;
    async fn try_get_log_entries(
        &self,
        lower_bound: Bound<&u64>,
        upper_bound: Bound<&u64>,
    ) -> StdResult<Vec<Entry<TypeConfig>>, StorageError<NodeId>>;
}

pub fn to_storage_io_err(
    subject: ErrorSubject<NodeId>,
    verb: ErrorVerb,
    source: impl Into<openraft::AnyError>,
) -> StorageError<NodeId> {
    StorageError::<NodeId>::IO {
        source: StorageIOError::<NodeId>::new(subject, verb, source),
    }
}

/// The [indielinks](crate) [Raft] log storage
///
/// [Raft]: https://raft.github.io/raft.pdf
// In my samples, the log storage is `Clone` via wrapping a non-clonable inner, as are the
// [openraft] samples I've perused. That said, I couldn't see why it should be: once constructed we
// just move it into the `Raft`. This is unlike the state machine: there, we need to give a
// reference to the `Raft` while (likely) we keep a reference for the application so that it can
// read state.
//
// The answer is found in [RaftLogStorage::get_log_reader]; you wouldn't think so-- the name and
// signature suggests that we're returning a separate, new type. However, `RaftLogStorage` is
// contrained to itself implement `RaftLogReader`, which, I suppose, is why every sample I've seen
// just clones itself.
//
// This also complicates the approach of just implementing `RaftLogReader` and `RaftLogStorage`
// directly on my backend implementations; i.e. just dispensing with `LogStore` altogether.
// `scylla::Session` isn't `Clone` (at this time), and so implmeneting `get_log_reader()` would be
// tough. We could pretty easily make it clone (by wrapping the native `scylla`) session field in a
// reference & a guard, but I'd rather not take on that effort and performane hit if I don't have
// to.
//
// This has gotten really irritating: this implementation really does nothing except instantiate a
// few generic parameters & then forward every call to the `Backend` implementation. Once I have
// this working (and tested!), I'd like to experiment with making `dynamodb::Client` and
// `scylla::Session` both `Clone`, and then just implement `RaftLogStorage` directly on each of
// them.
#[derive(Clone)]
pub struct LogStore {
    // storage: Arc<RwLock<dyn Backend + Send + Sync>>,
    storage: Arc<dyn Backend + Send + Sync>,
}

impl LogStore {
    // pub fn new(storage: Arc<RwLock<dyn Backend + Send + Sync>>) -> LogStore {
    pub fn new(storage: Arc<dyn Backend + Send + Sync>) -> LogStore {
        LogStore { storage }
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<R>(
        &mut self,
        range: R,
    ) -> StdResult<Vec<Entry<TypeConfig>>, StorageError<NodeId>>
    where
        R: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        self.storage
            // .read()
            // .await
            .try_get_log_entries(range.start_bound(), range.end_bound())
            .await
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StdResult<LogState<TypeConfig>, StorageError<NodeId>> {
        self.storage./*read().await.*/get_log_state().await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> StdResult<(), StorageError<NodeId>> {
        self.storage./*read().await.*/save_vote(vote).await
    }

    async fn read_vote(&mut self) -> StdResult<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.storage./*read().await.*/read_vote().await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> StdResult<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.storage
            // .read()
            // .await
            // This is particularly galling: since we can't use generic parameters in a dyn-safe
            // trait, we need to make this completely useless copy:
            .append(entries.into_iter().collect::<Vec<Entry<TypeConfig>>>())
            .await?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StdResult<(), StorageError<NodeId>> {
        self.storage./*read().await.*/truncate(log_id).await
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> StdResult<(), StorageError<NodeId>> {
        self.storage./*read().await.*/purge(log_id).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                     Raft Log-Related Types                                     //
////////////////////////////////////////////////////////////////////////////////////////////////////

// New types to work around the orphan trait rule

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct NID(pub NodeId);

impl std::fmt::Display for NID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct LogIndex(pub u64);

impl std::fmt::Display for LogIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, Deserialize, DeserializeRow, Serialize, SerializeRow)]
pub struct RaftLog {
    pub node_id: NID,
    pub log_id: LogIndex,
    pub entry: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, DeserializeRow, Serialize, SerializeRow)]
pub struct RaftMetadata {
    pub node_id: NID,
    pub flavor: Flavor,
    pub data: Vec<u8>,
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for NID {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        BigInt::type_check(typ)
    }
    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        u64::try_from(&<BigInt as DeserializeValue>::deserialize(typ, v)?)
            .map_err(DeserializationError::new)
            .map(NID)
    }
}

// Again, the derive macro doesn't work with newtype structs.
impl SerializeValue for NID {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&Into::<BigInt>::into(self.0), typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for LogIndex {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        BigInt::type_check(typ)
    }
    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        u64::try_from(&<BigInt as DeserializeValue>::deserialize(typ, v)?)
            .map_err(DeserializationError::new)
            .map(LogIndex)
    }
}

// Again, the derive macro doesn't work with newtype structs.
impl SerializeValue for LogIndex {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&Into::<BigInt>::into(self.0), typ, writer)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          gRPC Service                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct GrpcService {
    cache_node: CacheNode<GrpcClientFactory>,
}

impl GrpcService {
    pub fn new(cache_node: CacheNode<GrpcClientFactory>) -> GrpcService {
        GrpcService { cache_node }
    }
}

// It seems a shame to lose the original error information, but there isn't a great way to map
// internal errors to tonic errors.
fn to_tonic<E: Debug>(err: E) -> tonic::Status {
    tonic::Status::internal(format!("{err:?}"))
}

// Need to set this up more systematically, but for now:
pub static FOLLOWER_TO_PUBLIC_INBOX: u64 = 1000;

#[tonic::async_trait]
impl protobuf::grpc_service_server::GrpcService for GrpcService {
    async fn append_entries(
        &self,
        req: tonic::Request<protobuf::AppendEntriesRequest>,
    ) -> StdResult<tonic::Response<protobuf::AppendEntriesResponse>, tonic::Status> {
        self.cache_node
            .append_entries(
                AppendEntriesRequest::<TypeConfig>::try_from(req.into_inner()).map_err(to_tonic)?,
            )
            .await
            .map_err(to_tonic)?
            .try_conv::<protobuf::AppendEntriesResponse>()
            .map_err(to_tonic)?
            .conv::<tonic::Response<protobuf::AppendEntriesResponse>>()
            .pipe(Ok)
    }
    async fn install_snapshot(
        &self,
        req: tonic::Request<protobuf::InstallSnapshotRequest>,
    ) -> StdResult<tonic::Response<protobuf::InstallSnapshotResponse>, tonic::Status> {
        self.cache_node
            .install_snapshot(
                InstallSnapshotRequest::<TypeConfig>::try_from(req.into_inner())
                    .map_err(to_tonic)?,
            )
            .await
            .map_err(to_tonic)?
            .conv::<protobuf::InstallSnapshotResponse>()
            .conv::<tonic::Response<protobuf::InstallSnapshotResponse>>()
            .pipe(Ok)
    }
    /// Submit a vote during leader election
    async fn vote(
        &self,
        req: tonic::Request<protobuf::VoteRequest>,
    ) -> StdResult<tonic::Response<protobuf::VoteResponse>, tonic::Status> {
        self.cache_node
            .vote(VoteRequest::<NodeId>::try_from(req.into_inner()).map_err(to_tonic)?)
            .await
            .map_err(to_tonic)?
            .conv::<protobuf::VoteResponse>()
            .conv::<tonic::Response<protobuf::VoteResponse>>()
            .pipe(Ok)
    }
    /// Insert a key, value pair into a cache
    async fn cache_insert(
        &self,
        req: tonic::Request<protobuf::CacheInsertRequest>,
    ) -> StdResult<tonic::Response<protobuf::CacheInsertResponse>, tonic::Status> {
        let req = req.into_inner();

        // OK-- here is where we need to "just know" the actual types for `K` & `V`, based upon the
        // `cache_id`:
        if FOLLOWER_TO_PUBLIC_INBOX == req.cache_id {
            let key = rmp_serde::from_slice::<crate::entities::FollowerId>(req.key.as_slice())
                .map_err(to_tonic)?;
            let value = rmp_serde::from_slice::<crate::entities::StorUrl>(req.value.as_slice())
                .map_err(to_tonic)?;
            self.cache_node
                .cache_insert::<crate::entities::FollowerId, crate::entities::StorUrl>(
                    self.cache_node.id().await,
                    100,
                    key,
                    value,
                )
                .await
                .map_err(to_tonic)?;

            Ok(protobuf::CacheInsertResponse {
                cache_id: req.cache_id,
                value: req.value,
            }
            .into())
        } else {
            Err(tonic::Status::invalid_argument(format!(
                "Unknown cache {}",
                req.cache_id
            )))
        }
    }
    /// Lookup a value given a key
    async fn cache_lookup(
        &self,
        req: tonic::Request<protobuf::CacheLookupRequest>,
    ) -> StdResult<tonic::Response<protobuf::CacheLookupResponse>, tonic::Status> {
        let req = req.into_inner();

        // OK-- here is where we need to "just know" the actual types for `K` & `V`, based upon the
        // `cache_id`:
        if 100 == req.cache_id {
            let key = rmp_serde::from_slice::<crate::entities::FollowerId>(req.key.as_slice())
                .map_err(to_tonic)?;
            let rsp = self
                .cache_node
                .cache_lookup::<crate::entities::FollowerId, crate::entities::StorUrl>(
                    self.cache_node.id().await,
                    req.cache_id,
                    key,
                )
                .await
                .map_err(to_tonic)?;

            Ok(protobuf::CacheLookupResponse {
                cache_id: req.cache_id,
                value: rsp
                    .map(|rsp| rmp_serde::to_vec(&rsp).map_err(to_tonic))
                    .transpose()?,
            }
            .into())
        } else {
            Err(tonic::Status::invalid_argument(format!(
                "Unknown cache {}",
                req.cache_id
            )))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          gRPC Client                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct GrpcClientFactory;

#[async_trait]
impl indielinks_cache::network::ClientFactory for GrpcClientFactory {
    type CacheClient = GrpcClient;
    // "This function should not create a connection but rather a client that will connect when
    // required. Therefore there is chance it will build a client that is unable to send out
    // anything, e.g., in case the Node network address is configured incorrectly. But this method
    // does not return an error because openraft can only ignore it." (openraft docs)
    async fn new_client(&mut self, target: NodeId, node: &ClusterNode) -> Self::CacheClient {
        GrpcClient {
            _id: target,
            endpoint: tonic::transport::Uri::builder().scheme("http").authority(node.addr.to_string()).build().unwrap(/* known good */),
            client: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct GrpcClient {
    _id: NodeId,
    endpoint: tonic::transport::Uri,
    client: Option<protobuf::grpc_service_client::GrpcServiceClient<tonic::transport::Channel>>,
}

impl GrpcClient {
    async fn ensure_connected(
        &mut self,
    ) -> Result<protobuf::grpc_service_client::GrpcServiceClient<tonic::transport::Channel>> {
        match &self.client {
            Some(client) => Ok(client.clone()),
            None => {
                match protobuf::grpc_service_client::GrpcServiceClient::<tonic::transport::Channel>::connect(self.endpoint.clone()).await {
                    Ok(client) => {
                        self.client = Some(client.clone());
                        Ok(client)
                    }
                    Err(err) => {
                        Err(ConnSnafu.into_error(err))
                    }
                }
            }
        }
    }
}

fn from_status<Err: std::error::Error>(err: tonic::Status) -> RPCError<NodeId, ClusterNode, Err> {
    match err.code() {
        Code::Unavailable => indielinks_cache::network::RPCError::Unreachable(
            openraft::error::Unreachable::new(&err),
        ),
        _ => indielinks_cache::network::RPCError::Network(NetworkError::new(&err)),
    }
}

fn from_interop<Err: std::error::Error + 'static>(
    err: crate::protobuf_interop::Error,
) -> RPCError<NodeId, ClusterNode, Err> {
    // This seems kinda lame, but then there really isn't a great match
    indielinks_cache::network::RPCError::Network(NetworkError::new(&err))
}

fn from_cache<Err: std::error::Error + 'static>(err: Error) -> RPCError<NodeId, ClusterNode, Err> {
    // This seems kinda lame, but then there really isn't a great match
    indielinks_cache::network::RPCError::Network(NetworkError::new(&err))
}

#[async_trait]
impl indielinks_cache::network::Client for GrpcClient {
    type ErrorType = Error;

    /// Append Raft log entries to the target node's log store
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> StdResult<AppendEntriesResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>>
    {
        self.ensure_connected()
            .await
            .map_err(from_cache)?
            .append_entries(protobuf::AppendEntriesRequest::from(req))
            .await
            .map_err(from_status)?
            .into_inner()
            .try_conv::<AppendEntriesResponse<NodeId>>()
            .map_err(from_interop)
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
        self.ensure_connected()
            .await
            .map_err(from_cache)?
            .install_snapshot(protobuf::InstallSnapshotRequest::from(req))
            .await
            .map_err(from_status)?
            .into_inner()
            .try_conv::<InstallSnapshotResponse<NodeId>>()
            .map_err(from_interop)
    }
    /// Request a leadership vote from the target node
    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> StdResult<VoteResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>> {
        self.ensure_connected()
            .await
            .map_err(from_cache)?
            .vote(protobuf::VoteRequest::from(req))
            .await
            .map_err(from_status)?
            .into_inner()
            .try_conv::<VoteResponse<NodeId>>()
            .map_err(from_interop)
    }
    /// Ask the target node to insert a key/value pair into it's LRU cache
    async fn cache_insert<K: Serialize + Sync, V: Serialize + Sync>(
        &mut self,
        cache: CacheId,
        key: impl Into<K> + Send,
        value: impl Into<V> + Send,
    ) -> Result<()> {
        self.ensure_connected()
            .await?
            .cache_insert(try_into_cache_insert_request(cache, key, value).context(InteropSnafu)?)
            .await
            .map(|_| ())
            .context(TonicSnafu)
    }
    /// Request a value for a given key from the target node
    async fn cache_lookup<K: Serialize, V: DeserializeOwned>(
        &mut self,
        cache: CacheId,
        key: impl Into<K> + Send,
    ) -> Result<Option<V>> {
        self.ensure_connected()
            .await?
            .cache_lookup(try_into_cache_lookup_request(cache, key).context(InteropSnafu)?)
            .await
            .context(TonicSnafu)?
            .into_inner()
            .pipe(|rsp| {
                rsp.value
                    .map(|val| rmp_serde::from_slice::<V>(val.as_slice()).context(DeSnafu))
            })
            .transpose()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                     Cluster Admin Service                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

async fn init_cluster(
    State(state): State<Arc<Indielinks>>,
    Json(req): Json<BTreeMap<NodeId, ClusterNode>>,
) -> axum::response::Response {
    info!(
        "Initializing a Raft cluster with members: {:?}",
        req.iter().collect::<Vec<(&NodeId, &ClusterNode)>>()
    );
    // This implementation seems awfully chatty, but I need to drill down into the `Err` variant in
    // case we just failed because the cluster is already initialized.
    match state.cache_node.initialize(req).await {
        Ok(_) => {
            info!("Successfully initialized your Raft cluster");
            (StatusCode::OK).into_response()
        }
        Err(indielinks_cache::raft::Error::RaftInit { source }) => match *source {
            RaftError::APIError(err) => match err {
                openraft::error::InitializeError::NotAllowed(_) => {
                    info!("Your Raft cluster is already initialized");
                    (StatusCode::OK).into_response()
                }
                openraft::error::InitializeError::NotInMembers(not_in_members) => {
                    error!("Initialization failed: {not_in_members:#?}");
                    (StatusCode::BAD_REQUEST, Json(not_in_members)).into_response()
                }
            },
            RaftError::Fatal(fatal) => {
                error!("While initializing the Raft cluster: {fatal:?}");
                (StatusCode::INTERNAL_SERVER_ERROR, Json(fatal)).into_response()
            }
        },
        Err(err) => {
            error!("While initializing the Raft cluster: {err:?}");
            // It would be nice if I could just make `indielinks_cache::raft::Error` serializable,
            // but various source error types themselves are not.
            (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#?}")).into_response()
        }
    }
}

async fn metrics(State(state): State<Arc<Indielinks>>) -> axum::response::Response {
    Json(state.cache_node.metrics().await).into_response()
}

async fn add_learner(
    State(state): State<Arc<Indielinks>>,
    Json((id, addr)): Json<(NodeId, SocketAddr)>,
) -> axum::response::Response {
    info!("Adding Node ({id}, {addr}) in state 'learning' to the cluster");
    match state
        .cache_node
        .add_learner(id, ClusterNode { addr }, true)
        .await
    {
        Ok(rsp) => {
            info!("Successfully added the new node as a learner.");
            (StatusCode::OK, Json(rsp)).into_response()
        }
        Err(err) => {
            error!("Failed to add the new node as a learner: {err:?}");
            // It might be nice to forward this request to the leader (if we're not the leader), but
            // for now just fail. Also, this can fail for a few reasons, not all of which are on the
            // user's side... I should make this more fine-grained.
            (StatusCode::BAD_REQUEST, Json(err)).into_response()
        }
    }
}

async fn change_membership(
    State(state): State<Arc<Indielinks>>,
    Json(req): Json<Vec<NodeId>>,
) -> axum::response::Response {
    info!("Changing Raft cluster membership to {:?}", req);
    match state.cache_node.change_membership(req, false).await {
        Ok(rsp) => {
            info!("Successfully changed membership");
            (StatusCode::OK, Json(rsp)).into_response()
        }
        Err(err) => {
            error!("Failed to change membership: {err:?}");
            // It might be nice to forward this request to the leader (if we're not the leader), but
            // for now just fail. Also, this can fail for a few reasons, not all of which are on the
            // user's side... I should make this more fine-grained.
            (StatusCode::BAD_REQUEST, format!("{err:#?}")).into_response()
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CacheInsertRequest {
    pub cache: CacheId,
    pub key: serde_json::Value,
    pub value: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CacheLookupRequest {
    pub cache: CacheId,
    pub key: serde_json::Value,
}

async fn query_cache(
    State(state): State<Arc<Indielinks>>,
    Json(req): Json<CacheLookupRequest>,
) -> axum::response::Response {
    // Here, we have to "just know" the concrete types for the key & value. We can do this via the
    // `cache` request parameter. At the time of this writing, there's only one cache.
    if FOLLOWER_TO_PUBLIC_INBOX != req.cache {
        return (StatusCode::BAD_REQUEST, format!("No cache {}", req.cache)).into_response();
    }

    async fn internal(
        state: Arc<Indielinks>,
        key: serde_json::Value,
    ) -> Result<Option<serde_json::Value>> {
        state
            .first_cache
            .write()
            .await
            .get(&from_value::<FollowerId>(key.clone()).context(FollowerIdSnafu { key })?)
            .await
            .context(CacheSnafu)?
            .map(to_value)
            .transpose()
            .context(UrlSnafu)
    }

    match internal(state, req.key).await {
        Ok(val) => (StatusCode::OK, Json(val)).into_response(),
        // Really not sure how to map `err` to status codes!
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#?}")).into_response(),
    }
}

async fn insert_into_cache(
    State(state): State<Arc<Indielinks>>,
    Json(req): Json<CacheInsertRequest>,
) -> axum::response::Response {
    // Here, we have to "just know" the concrete types for the key & value. We can do this via the
    // `cache` request parameter. At the time of this writing, there's only one cache.
    if FOLLOWER_TO_PUBLIC_INBOX != req.cache {
        return (StatusCode::BAD_REQUEST, format!("No cache {}", req.cache)).into_response();
    }

    async fn internal(
        state: Arc<Indielinks>,
        key: serde_json::Value,
        value: serde_json::Value,
    ) -> Result<()> {
        state
            .first_cache
            .write()
            .await
            .insert(
                from_value::<FollowerId>(key.clone()).context(FollowerIdSnafu { key })?,
                from_value::<StorUrl>(value.clone()).context(UrlDeSnafu { value })?,
            )
            .await
            .context(CacheSnafu)
    }

    match internal(state, req.key, req.value).await {
        Ok(_) => (StatusCode::CREATED).into_response(),
        // Really not sure how to map `err` to status codes!
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#?}")).into_response(),
    }
}

pub fn make_router(state: Arc<Indielinks>) -> Router<Arc<Indielinks>> {
    // Should probably add logging, maybe request ID?
    Router::new()
        .route("/init-cluster", post(init_cluster))
        .route("/metrics", get(metrics))
        .route("/add-learner", post(add_learner))
        .route("/membership", post(change_membership))
        .route("/cache-query", get(query_cache))
        .route("/cache-insert", post(insert_into_cache))
        .layer(SetResponseHeaderLayer::if_not_present(
            CONTENT_TYPE,
            HeaderValue::from_static("text/json; charset=utf-8"),
        ))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
