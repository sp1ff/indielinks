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

//! # Application Logic
//!
//! ## Introduction
//!
//! I haven't been terribly careful about mixing application logic with HTTP handlers, but I've
//! finally gotten burned: I need to add posts from both the del.icio.us and the bookmarklets APIs.
//! I've refactored the common logic here, and I hope this module will grow into a general-purpose
//! "application logic" repository.

use std::{collections::HashSet, result::Result as StdResult, sync::Arc};

use chrono::{DateTime, Utc};
use nonempty_collections::{IntoNonEmptyIterator, NEVec, NonEmptyIterator};
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, ResultExt, Snafu};
use tap::Pipe;
use tracing::debug;
use url::Url;

use indielinks_cache::types::NodeId;
use indielinks_shared::{
    api::{
        TimelineBeforePage, TimelineBeforeRsp, TimelineInitialPage, TimelineInitialRsp,
        TimelineReq, TimelineSincePage, TimelineSinceRsp,
    },
    entities::{PostId, Tagname, UserId, Username},
    nonempty_string::NonEmptyString,
};

use crate::{
    activity_pub::SendCreate,
    ap_entities::Item,
    background_tasks::{BackgroundTasks, Sender},
    cache::GrpcClient,
    define_metric,
    entities::User,
    home_timeline::{FirstPage, PostKey, Timeline},
    indielinks::Indielinks,
    protobuf_interop::protobuf::{DropTimelineRequest, InsertTimelineItemRequest, TimelineRequest},
    signing_keys,
    storage::Backend as StorageBackend,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("While adding the post internally, {source}"))]
    AddPost { source: crate::storage::Error },
    #[snafu(display("While connecting a gRPC client, {source}"))]
    Connection { source: crate::cache::Error },
    #[snafu(display("While federating this post, {source}"))]
    Federation {
        source: crate::background_tasks::Error,
    },
    #[snafu(display("While computing the home timeline for {username}, {source}"))]
    HomeTimeline {
        username: Username,
        #[snafu(source(from(crate::home_timeline::Error, Box::new)))]
        source: Box<crate::home_timeline::Error>,
    },
    #[snafu(display("While deserialzing to messagepack, {source}"))]
    MessagePackDe {
        source: rmp_serde::decode::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While serialzing to messagepack, {source}"))]
    MessagePackSer {
        source: rmp_serde::encode::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to hash UserId {userid}: {source}"))]
    NodeHash {
        userid: UserId,
        source: indielinks_cache::raft::Error,
    },
    #[snafu(display("No signing keys available: {source}"))]
    NoSigningKeys { source: signing_keys::Error },
    #[snafu(display("While packing the pagination token, {source}"))]
    PackPaginationToken {
        #[snafu(source(from(crate::home_timeline::Error, Box::new)))]
        source: Box<crate::home_timeline::Error>,
    },
    #[snafu(display("While retrieving the address of node {node_id}, {source}"))]
    SocketAddr {
        node_id: NodeId,
        source: indielinks_cache::raft::Error,
    },
    #[snafu(display("While forwarding timeline request via gRPC: {source}"))]
    TimelineForward { source: crate::cache::Error },
    #[snafu(display("While forwarding a timeline insert request via gRPC: {source}"))]
    TimelineInsertForward { source: crate::cache::Error },
    #[snafu(display("gRPC error: {source}"))]
    Tonic {
        #[snafu(source(from(tonic::Status, Box::new)))]
        source: Box<tonic::Status>,
        backtrace: Backtrace,
    },
    #[snafu(display("While unpacking the pagination token, {source}"))]
    UnpackPaginationToken {
        #[snafu(source(from(crate::home_timeline::Error, Box::new)))]
        source: Box<crate::home_timeline::Error>,
    },
    #[snafu(display("While updating post times, {source}"))]
    UpdatePostTimes { source: crate::storage::Error },
}

pub type Result<T> = StdResult<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         home timeline                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Internal discriminant for routing timeline responses in the HTTP handler
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum TimelineRsp {
    Initial(TimelineInitialRsp),
    Since(TimelineSinceRsp),
    Before(TimelineBeforeRsp),
}

async fn this_node_is_responsible(state: Arc<Indielinks>, user: &User) -> Result<Option<NodeId>> {
    let responsible_node = state
        .cache_node
        .node_for_key(user.id())
        .await
        .context(NodeHashSnafu { userid: *user.id() })?;

    Ok((state.cache_node.id().await != responsible_node).then_some(responsible_node))
}

/// Execute a home timeline request on this node
pub async fn handle_timeline(
    state: Arc<Indielinks>,
    user: &User,
    request: TimelineReq,
) -> Result<TimelineRsp> {
    let (_, key) = state.signing_keys.current().context(NoSigningKeysSnafu)?;

    let mut timelines = state.home_timelines.lock().await;

    if !timelines.contains(user.id()) {
        let timeline = Timeline::new(
            user,
            &state.origin,
            state.storage.as_ref(),
            &state.ap_client,
            state.ap_resolver.clone(),
        )
        .await
        .context(HomeTimelineSnafu {
            username: user.username(),
        })?;
        timelines.put(*user.id(), timeline);
    }

    let timeline = timelines.get_mut(user.id()).unwrap(/* known good */);

    match request {
        TimelineReq::Initial { max_posts } => {
            match timeline.begin(max_posts).await.context(HomeTimelineSnafu {
                username: user.username().clone(),
            })? {
                Some(FirstPage {
                    items,
                    since,
                    before,
                }) => TimelineRsp::Initial(Some(TimelineInitialPage {
                    posts: items
                        .into_nonempty_iter()
                        .map(|item| item.into())
                        .collect::<NEVec<_>>(),
                    since: since
                        .to_pagination_token(&key)
                        .context(PackPaginationTokenSnafu)?,
                    before: before
                        .to_pagination_token(&key)
                        .context(PackPaginationTokenSnafu)?,
                })),
                None => TimelineRsp::Initial(None),
            }
        }
        TimelineReq::Since { since, max_posts } => {
            match timeline
                .since(
                    PostKey::from_pagination_token(&since, &key)
                        .context(UnpackPaginationTokenSnafu)?,
                    max_posts,
                )
                .await
            {
                Some((items, since)) => TimelineRsp::Since(Some(TimelineSincePage {
                    posts: items
                        .into_nonempty_iter()
                        .map(|item| item.into())
                        .collect::<NEVec<_>>(),
                    since: since
                        .to_pagination_token(&key)
                        .context(PackPaginationTokenSnafu)?,
                })),
                None => TimelineRsp::Since(None),
            }
        }
        TimelineReq::Before { before, max_posts } => {
            match timeline
                .before(
                    PostKey::from_pagination_token(&before, &key)
                        .context(UnpackPaginationTokenSnafu)?,
                    max_posts,
                )
                .await
                .context(HomeTimelineSnafu {
                    username: user.username().clone(),
                })? {
                Some((items, before)) => TimelineRsp::Before(Some(TimelineBeforePage {
                    posts: items
                        .into_nonempty_iter()
                        .map(|item| item.into())
                        .collect::<NEVec<_>>(),
                    before: before
                        .to_pagination_token(&key)
                        .context(PackPaginationTokenSnafu)?,
                })),
                None => TimelineRsp::Before(None),
            }
        }
    }
    .pipe(Ok)
}

/// Forward a timeline request to the node responsible for this user's timeline via gRPC
async fn redirect_timeline(
    state: Arc<Indielinks>,
    user: &User,
    request: TimelineReq,
    responsible_node: NodeId,
) -> Result<TimelineRsp> {
    let addr = state
        .cache_node
        .socket_addr_for_id(responsible_node)
        .await
        .context(SocketAddrSnafu {
            node_id: responsible_node,
        })?;
    let response = GrpcClient::new(responsible_node, addr)
        .ensure_connected()
        .await
        .context(ConnectionSnafu)?
        .timeline(TimelineRequest {
            user_id: rmp_serde::to_vec(user.id()).context(MessagePackSerSnafu)?,
            request: rmp_serde::to_vec(&request).context(MessagePackSerSnafu)?,
        })
        .await
        .context(TonicSnafu)?
        .into_inner();
    rmp_serde::from_slice(&response.response).context(MessagePackDeSnafu)
}

define_metric! { "user.timeline.redirects", user_timeline_redirects, Sort::IntegralCounter }

/// Dispatch a home timeline request — execute locally or forward to the responsible node
pub async fn handle_timeline_or_redirect(
    state: Arc<Indielinks>,
    user: &User,
    request: TimelineReq,
) -> Result<TimelineRsp> {
    match this_node_is_responsible(state.clone(), user).await? {
        None => handle_timeline(state, user, request).await,
        Some(responsible_node) => {
            user_timeline_redirects.add(1, &[KeyValue::new("username", user.username())]);
            redirect_timeline(state.clone(), user, request, responsible_node).await
        }
    }
}

pub async fn handle_timeline_insert(state: Arc<Indielinks>, user: &User, item: &Item) {
    // I suppose we *could* proactively start a timeline for `user`, even though they've never
    // asked for it, but better, I think, to just let it go.
    if let Some(timeline) = state.home_timelines.lock().await.get_mut(user.id()) {
        timeline.add(item.clone())
    }
}

async fn redirect_timeline_insert(
    state: Arc<Indielinks>,
    user: &User,
    item: &Item,
    responsible_node: NodeId,
) -> Result<()> {
    let addr = state
        .cache_node
        .socket_addr_for_id(responsible_node)
        .await
        .context(SocketAddrSnafu {
            node_id: responsible_node,
        })?;
    GrpcClient::new(responsible_node, addr)
        .ensure_connected()
        .await
        .context(ConnectionSnafu)?
        .insert_timeline_item(InsertTimelineItemRequest {
            user_id: rmp_serde::to_vec(user.id()).context(MessagePackSerSnafu)?,
            item: rmp_serde::to_vec(item).context(MessagePackSerSnafu)?,
        })
        .await
        .context(TonicSnafu)?;
    Ok(())
}

/// Insert a new item into a (possibly) extant timeline
pub async fn handle_timeline_insert_or_redirect(
    state: Arc<Indielinks>,
    user: &User,
    item: &Item,
) -> Result<()> {
    match this_node_is_responsible(state.clone(), user).await? {
        Some(responsible_node) => {
            redirect_timeline_insert(state.clone(), user, item, responsible_node).await
        }
        None => {
            handle_timeline_insert(state.clone(), user, item).await;
            Ok(())
        }
    }
}

pub async fn handle_timeline_drop(state: Arc<Indielinks>, user: &User) {
    let _ = state.home_timelines.lock().await.pop(user.id());
}

async fn redirect_timeline_drop(
    state: Arc<Indielinks>,
    user: &User,
    responsible_node: NodeId,
) -> Result<()> {
    let addr = state
        .cache_node
        .socket_addr_for_id(responsible_node)
        .await
        .context(SocketAddrSnafu {
            node_id: responsible_node,
        })?;
    GrpcClient::new(responsible_node, addr)
        .ensure_connected()
        .await
        .context(ConnectionSnafu)?
        .drop_timeline(DropTimelineRequest {
            user_id: rmp_serde::to_vec(user.id()).context(MessagePackSerSnafu)?,
        })
        .await
        .context(TonicSnafu)?;
    Ok(())
}

/// Drop a timeline
pub async fn handle_timeline_drop_or_redirect(state: Arc<Indielinks>, user: &User) -> Result<()> {
    match this_node_is_responsible(state.clone(), user).await? {
        Some(responsible_node) => {
            redirect_timeline_drop(state.clone(), user, responsible_node).await
        }
        None => {
            handle_timeline_drop(state.clone(), user).await;
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           bookmarks                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Add an indielinks [Post](indielinks_shared::entities::Post)
#[allow(clippy::too_many_arguments)]
pub async fn add_post(
    user: &User,
    url: &Url,
    title: &NonEmptyString,
    dt: Option<&DateTime<Utc>>,
    notes: Option<&NonEmptyString>,
    tags: &HashSet<Tagname>,
    replace: bool,
    shared: bool,
    to_read: bool,
    storage: &(dyn StorageBackend + Send + Sync),
    sender: Arc<BackgroundTasks>,
) -> Result<bool> {
    let postid = PostId::default();
    let now = Utc::now();
    let dt = dt.unwrap_or(&now);
    let added = storage
        .add_post(
            user,
            // At one time, I wondered whether we should resolve defaults here, or in the storage
            // backend. It turns out to be better to handle it at the API level.
            replace, url, &postid, title, dt, notes, shared, to_read, tags,
        )
        .await
        .context(AddPostSnafu)?;
    if added {
        storage
            .update_user_post_times(user, dt)
            .await
            .context(UpdatePostTimesSnafu)?;
        if shared {
            debug!(
                "Scheduling Post {} for communication to all federated servers",
                postid
            );
            sender
                .send(SendCreate::new(&postid, dt))
                .await
                .context(FederationSnafu)?;
        }
    }
    Ok(added)
}
