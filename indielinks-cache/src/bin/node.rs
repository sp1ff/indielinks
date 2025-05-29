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

//! # node
//!
//! Trivial implementation of main to exercise [indielinks-cache](crate).

use std::{collections::BTreeMap, io, net::SocketAddrV4, str::FromStr, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    response::IntoResponse,
    routing::{get, post},
};
use bpaf::{Parser, construct};
use openraft::{
    BasicNode,
    raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest},
};
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
    sync::Notify,
};
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::{Level, error, info, subscriber::set_global_default};
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};

use indielinks_cache::{LogStore, Network, NodeId, Request, StateMachine, TypeConfig};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                   Raft API -- this is the Raft nodes talking to one another                    //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Handler for appending Raft log entries-- required trait method
async fn raft_append(
    State(state): State<NodeState>,
    Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> axum::response::Response {
    // Serialize the entire `Result`
    Json(state.raft.append_entries(req).await).into_response()
}

async fn raft_install(
    State(state): State<NodeState>,
    Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> axum::response::Response {
    // Serialize the entire `Result`
    Json(state.raft.install_snapshot(req).await).into_response()
}

async fn raft_vote(
    State(state): State<NodeState>,
    Json(req): Json<VoteRequest<NodeId>>,
) -> axum::response::Response {
    // Serialize the entire `Result`
    Json(state.raft.vote(req).await).into_response()
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
    State(state): State<NodeState>,
    Json(mut req): Json<Vec<(NodeId, String /* SockAddrV4 */)>>,
) -> axum::response::Response {
    if req.is_empty() {
        req.push((state.id, state.addr.to_string()));
    }

    let result = state
        .raft
        .initialize(BTreeMap::from_iter(
            req.into_iter().map(|(id, addr)| (id, BasicNode { addr })),
        ))
        .await;

    if result.is_ok() {
        let nodes = state
            .raft
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .map(|(id, _)| *id)
            .collect();
        let _ = state.raft.client_write(Request::Init { nodes }).await;
    }

    Json(result).into_response()
}

async fn admin_metrics(State(state): State<NodeState>) -> axum::response::Response {
    Json(state.raft.metrics().borrow().clone()).into_response()
}

/// Add a node to the cluster
///
/// New nodes must be added as learners.
async fn admin_add_learner(
    State(state): State<NodeState>,
    Json((node_id, addr)): Json<(NodeId, String /* SocketAddrV4 */)>,
) -> axum::response::Response {
    Json(
        state
            .raft
            // Imma try setting `blocking` to true for now
            .add_learner(node_id, BasicNode { addr }, true)
            .await,
    )
    .into_response()
}

async fn admin_change_membership(
    State(state): State<NodeState>,
    Json(req): Json<Vec<NodeId>>,
) -> axum::response::Response {
    let result = state.raft.change_membership(req, false).await;

    // This is kinda lame-- we shouldn't re-init the hash ring, just update it.
    if result.is_ok() {
        let nodes = state
            .raft
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .map(|(id, _)| *id)
            .collect();
        let _ = state.raft.client_write(Request::Init { nodes }).await;
    }

    Json(result).into_response()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        application API                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

async fn get_hash_ring(State(state): State<NodeState>) -> axum::response::Response {
    Json(state.state_machine.get_hash_ring()).into_response()
}

async fn update_hash_ring(
    State(state): State<NodeState>,
    Json((shard, node)): Json<(u64, NodeId)>,
) -> axum::response::Response {
    Json(
        state
            .raft
            .client_write(Request::InsertNode { shard, node })
            .await,
    )
    .into_response()
}

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
        addr(bpaf::positional::<String>("SOCKADDR").parse(|s| SocketAddrV4::from_str(&s)))
    })
}

#[derive(Clone)]
struct NodeState {
    id: NodeId,
    addr: SocketAddrV4,
    raft: openraft::Raft<TypeConfig>,
    state_machine: StateMachine,
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

    let config = Arc::new(
        openraft::Config {
            cluster_name: "indielinks-cache".to_owned(),
            // These settings copied from the `raft-kv-memstore` sample
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    // Alright, at the time of this writing, we have a functioning web server. The next thing we
    // need is:
    let state_machine = StateMachine::default();
    let raft = openraft::Raft::new(
        opts.id,               // Node ID
        config.clone(),        // Config
        Network,               // Network
        LogStore::default(),   // Log Store
        state_machine.clone(), // State Machine
    )
    .await
    .expect("Failed to instantiate the Raft");

    let state = NodeState {
        id: opts.id,
        addr: opts.addr,
        raft,
        state_machine,
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
            .route("/admin/init", post(admin_init))
            .route("/admin/metrics", get(admin_metrics))
            .route("/admin/add-learner", post(admin_add_learner))
            .route("/admin/membership", post(admin_change_membership))
            .route("/app/hash-ring", get(get_hash_ring))
            .route("/app/hash-ring", post(update_hash_ring))
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
