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

//! # grpc
//!
//! The [indielinks](crate) gRPC server. This module contains the top-level entities for _serving_
//! gRPC (i.e. it sits at the top of the module hierarchy, imported only by indielinksd).

use std::{collections::BTreeMap, fmt::Debug, net::SocketAddr, sync::Arc};

use axum::{
    extract::{Json, State},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use http::{header::CONTENT_TYPE, HeaderValue, StatusCode};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, Snafu};
use tap::{Conv, Pipe, TryConv};
use tower_http::{cors::CorsLayer, set_header::SetResponseHeaderLayer};
use tracing::{error, info};

use indielinks_cache::{
    cache::Cache,
    network::{AppendEntriesRequest, InstallSnapshotRequest, RaftError, VoteRequest},
    raft::CacheNode,
    types::{CacheId, ClusterNode, NodeId, TypeConfig},
};
use url::Url;

use crate::{
    ap_entities::{Actor, Note},
    cache::GrpcClientFactory,
    indielinks::Indielinks,
    protobuf_interop::*,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to deserialize {key} as a FollowerId: {source}"))]
    FollowerId {
        key: serde_json::Value,
        source: serde_json::Error,
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

type StdResult<T, E> = std::result::Result<T, E>;

pub struct GrpcService {
    cache_node: CacheNode<GrpcClientFactory>,
    actors: Arc<Cache<GrpcClientFactory, Url, Actor>>,
    notes: Arc<Cache<GrpcClientFactory, Url, Note>>,
}

impl GrpcService {
    pub fn new(
        cache_node: CacheNode<GrpcClientFactory>,
        actors: Arc<Cache<GrpcClientFactory, Url, Actor>>,
        notes: Arc<Cache<GrpcClientFactory, Url, Note>>,
    ) -> GrpcService {
        GrpcService {
            cache_node,
            actors,
            notes,
        }
    }
}

// It seems a shame to lose the original error information, but there isn't a great way to map
// internal errors to tonic errors.
fn to_tonic<E: Debug>(err: E) -> tonic::Status {
    tonic::Status::internal(format!("{err:?}"))
}

// Need to set this up more systematically, but for now:
pub const FOLLOWER_TO_PUBLIC_INBOX: u64 = 1000; // Unused, but will be needed, so keep around.
pub const ACTOR_ID_TO_ACTOR: u64 = 1001;
pub const NOTE_ID_TO_NOTE: u64 = 1002;

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
        match req.cache_id {
            ACTOR_ID_TO_ACTOR => {
                let key = rmp_serde::from_slice::<Url>(req.key.as_slice()).map_err(to_tonic)?;
                let value =
                    rmp_serde::from_slice::<Actor>(req.value.as_slice()).map_err(to_tonic)?;
                self.actors.insert(key, value).await.map_err(to_tonic)?;
            }
            NOTE_ID_TO_NOTE => {
                let key = rmp_serde::from_slice::<Url>(req.key.as_slice()).map_err(to_tonic)?;
                let value =
                    rmp_serde::from_slice::<Note>(req.value.as_slice()).map_err(to_tonic)?;
                self.notes.insert(key, value).await.map_err(to_tonic)?;
            }
            _ => {
                return Err(tonic::Status::invalid_argument(format!(
                    "Unknown cache {}",
                    req.cache_id
                )));
            }
        }

        Ok(protobuf::CacheInsertResponse {
            cache_id: req.cache_id,
            value: req.value,
        }
        .into())
    }
    /// Lookup a value given a key
    async fn cache_lookup(
        &self,
        req: tonic::Request<protobuf::CacheLookupRequest>,
    ) -> StdResult<tonic::Response<protobuf::CacheLookupResponse>, tonic::Status> {
        let req = req.into_inner();

        // OK-- here is where we need to "just know" the actual types for `K` & `V`, based upon the
        // `cache_id`:
        let rsp = match req.cache_id {
            ACTOR_ID_TO_ACTOR => {
                let key = rmp_serde::from_slice::<Url>(req.key.as_slice()).map_err(to_tonic)?;
                self.actors
                    .get(&key)
                    .await
                    .map_err(to_tonic)?
                    .map(|rsp| rmp_serde::to_vec(&rsp).map_err(to_tonic))
                    .transpose()?
            }
            NOTE_ID_TO_NOTE => {
                let key = rmp_serde::from_slice::<Url>(req.key.as_slice()).map_err(to_tonic)?;
                self.notes
                    .get(&key)
                    .await
                    .map_err(to_tonic)?
                    .map(|rsp| rmp_serde::to_vec(&rsp).map_err(to_tonic))
                    .transpose()?
            }
            _ => {
                return Err(tonic::Status::invalid_argument(format!(
                    "Unknown cache {}",
                    req.cache_id
                )));
            }
        };

        Ok(protobuf::CacheLookupResponse {
            cache_id: req.cache_id,
            value: rsp,
        }
        .into())
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
                error!("Fatal error while initializing the Raft cluster: {fatal:#?}");
                (StatusCode::INTERNAL_SERVER_ERROR, Json(fatal)).into_response()
            }
        },
        Err(err) => {
            error!("While initializing the Raft cluster: {err:#?}");
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

pub fn make_router(state: Arc<Indielinks>) -> Router<Arc<Indielinks>> {
    // Should probably add logging, maybe request ID?
    Router::new()
        .route("/init-cluster", post(init_cluster))
        .route("/metrics", get(metrics))
        .route("/add-learner", post(add_learner))
        .route("/membership", post(change_membership))
        .layer(SetResponseHeaderLayer::if_not_present(
            CONTENT_TYPE,
            HeaderValue::from_static("text/json; charset=utf-8"),
        ))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
