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

use std::{collections::HashSet, result::Result as StdResult, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use either::Either::Left;
use http::Method;
use nonempty_collections::{IntoNonEmptyIterator, NEVec, NonEmptyIterator};
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, ResultExt, Snafu};
use tap::Pipe;
use tokio::sync::Mutex;
use tracing::debug;
use url::Url;

use indielinks_cache::{raft::CacheNode, types::NodeId};
use indielinks_shared::{
    api::{
        OutboxToken, TimelineBeforePage, TimelineBeforeRsp, TimelineInitialPage,
        TimelineInitialRsp, TimelineReq, TimelineSincePage, TimelineSinceRsp, UserOutboxRequest,
    },
    entities::{PostId, StorUrl, Tagname, UserId, Username},
    nonempty_string::NonEmptyString,
    origin::Origin,
};
use uuid::Uuid;

use crate::{
    activity_pub::SendCreate,
    ap_entities::{
        ap_request, ap_request_no_response, make_follow_id, make_user_id, make_user_outbox, Actor,
        Follow, Item, Outbox, OutboxPage, ToJld,
    },
    background_tasks::{self, BackgroundTask, BackgroundTasks, Context, Sender, TaggedTask, Task},
    cache::{GrpcClient, GrpcClientFactory},
    define_metric,
    entities::{FollowId, User},
    home_timeline::{FirstPage, HomeTimelines, PostKey, Timeline},
    indielinks::Indielinks,
    outboxes::{ActivityKey, Outbox as UserOutbox, FIRST_ACTIVITY_KEY},
    protobuf_interop::protobuf::{DropTimelineRequest, InsertTimelineItemRequest, TimelineRequest},
    signing_keys::{self, SigningKey},
    storage::Backend as StorageBackend,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to write a follow for {username} of {actorid}: {source}"))]
    AddFollowing {
        username: Username,
        actorid: Box<Url>,
        source: crate::storage::Error,
    },
    #[snafu(display("While adding the post internally, {source}"))]
    AddPost { source: crate::storage::Error },
    #[snafu(display("ActivityPub entities error: {source}"))]
    Ap {
        #[snafu(source(from(crate::ap_entities::Error, Box::new)))]
        source: Box<crate::ap_entities::Error>,
    },
    #[snafu(display("ActivityPub entity resolver error: {source}"))]
    ApResolver {
        #[snafu(source(from(crate::ap_resolution::Error, Box::new)))]
        source: Box<crate::ap_resolution::Error>,
    },
    #[snafu(display("While connecting a gRPC client, {source}"))]
    Connection { source: crate::cache::Error },
    #[snafu(display("While federating this post, {source}"))]
    Federation {
        source: crate::background_tasks::Error,
    },
    #[snafu(display("Failed to form an URL for follow of {id} for {username}"))]
    FollowId {
        username: Username,
        id: FollowId,
        #[snafu(source(from(crate::ap_entities::Error, Box::new)))]
        source: Box<crate::ap_entities::Error>,
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
    #[snafu(display("Outbox error {source}"))]
    Outbox {
        #[snafu(source(from(crate::outboxes::Error, Box::new)))]
        source: Box<crate::outboxes::Error>,
    },
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
    #[snafu(display("Failed to form an URL for user {username}: {source}"))]
    UserId {
        username: Username,
        #[snafu(source(from(crate::ap_entities::Error, Box::new)))]
        source: Box<crate::ap_entities::Error>,
    },
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

async fn this_node_is_responsible(
    cache_node: CacheNode<GrpcClientFactory>,
    user: &User,
) -> Result<Option<NodeId>> {
    let responsible_node = cache_node
        .node_for_key(user.id())
        .await
        .context(NodeHashSnafu { userid: *user.id() })?;

    Ok((cache_node.id().await != responsible_node).then_some(responsible_node))
}

/// Execute a home timeline request on this node
pub async fn handle_timeline(
    state: Arc<Indielinks>,
    user: &User,
    request: TimelineReq,
) -> Result<TimelineRsp> {
    let (_, key) = state.signing_keys.current().context(NoSigningKeysSnafu)?;

    // `LruCache::try_get_or_insert_mut()` would be preferrable, here, but creating a new `Timeline`
    // is an asynchronous operation, and `LruCache` doesn't support that. I suppose I could add a
    // method, either privately or in a PR, but I'm not sure I'm going to be sticking with
    // `LruCache` long-term, yet.
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
    match this_node_is_responsible(state.cache_node.clone(), user).await? {
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
    match this_node_is_responsible(state.cache_node.clone(), user).await? {
        Some(responsible_node) => {
            redirect_timeline_insert(state.clone(), user, item, responsible_node).await
        }
        None => {
            handle_timeline_insert(state.clone(), user, item).await;
            Ok(())
        }
    }
}

pub async fn handle_timeline_drop(home_timelines: Arc<Mutex<HomeTimelines>>, user: &User) {
    home_timelines.lock().await.pop(user.id());
}

async fn redirect_timeline_drop(
    cache_node: CacheNode<GrpcClientFactory>,
    user: &User,
    responsible_node: NodeId,
) -> Result<()> {
    let addr = cache_node
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
pub async fn handle_timeline_drop_or_redirect(
    cache_node: CacheNode<GrpcClientFactory>,
    home_timelines: Arc<Mutex<HomeTimelines>>,
    user: &User,
) -> Result<()> {
    match this_node_is_responsible(cache_node.clone(), user).await? {
        Some(responsible_node) => redirect_timeline_drop(cache_node, user, responsible_node).await,
        None => {
            handle_timeline_drop(home_timelines, user).await;
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          user outbox                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum OutboxResponse {
    Outbox(Outbox),
    Paged(OutboxPage),
}

impl ToJld for OutboxResponse {
    fn get_type(&self) -> crate::ap_entities::Type {
        match self {
            OutboxResponse::Outbox(outbox) => outbox.get_type(),
            OutboxResponse::Paged(outbox_page) => outbox_page.get_type(),
        }
    }
}

fn make_outbox(user: &User, origin: &Origin, signing_key: &SigningKey) -> Result<Outbox> {
    let id = make_user_outbox(user.username(), origin).context(ApSnafu)?;
    let mut first = id.clone();

    let token = FIRST_ACTIVITY_KEY
        .to_pagination_token(signing_key)
        .context(OutboxSnafu)?;

    first.set_query(Some(&format!("page={token}")));

    Ok(Outbox {
        id,
        // I think this may bite me...
        total_items: None,
        first,
        last: None,
    })
}

async fn make_outbox_page(
    user: &User,
    outbox: &mut UserOutbox,
    origin: &Origin,
    page_token: &OutboxToken,
    signing_key: &SigningKey,
) -> Result<OutboxPage> {
    let id = make_user_outbox(user.username(), origin).context(ApSnafu)?;
    let mut prev = id.clone();
    prev.set_query(Some(&format!("page={page_token}")));

    let token = ActivityKey::from_pagination_token(page_token, signing_key).context(OutboxSnafu)?;

    match outbox.next(&token, None).await.context(OutboxSnafu)? {
        None => Ok(OutboxPage {
            part_of: id.clone(),
            id,
            next: None,
            prev: Some(prev),
            ordered_items: Vec::new(),
        }),
        Some(page) => {
            let next_token = token
                .to_pagination_token(signing_key)
                .context(OutboxSnafu)?;
            let mut next = id.clone();
            next.set_query(Some(&format!("page={next_token}")));

            Ok(OutboxPage {
                part_of: id.clone(),
                id,
                next: Some(next),
                prev: Some(prev),
                ordered_items: page.items.into(),
            })
        }
    }
}

async fn handle_outbox(
    state: Arc<Indielinks>,
    user: &User,
    request: UserOutboxRequest,
) -> Result<OutboxResponse> {
    let (_, key) = state.signing_keys.current().context(NoSigningKeysSnafu)?;

    // `LruCache::try_get_or_insert_mut()` would be preferrable, here, but creating a new `Outbox`
    // is an asynchronous operation, and `LruCache` doesn't support that. I suppose I could add a
    // method, either privately or in a PR, but I'm not sure I'm going to be sticking with
    // `LruCache` long-term, yet.

    let mut outboxes = state.user_outboxes.lock().await;

    if !outboxes.contains(user.id()) {
        let outbox = UserOutbox::new(state.origin.clone(), user, state.storage.as_ref())
            .await
            .context(OutboxSnafu)?;
        outboxes.put(*user.id(), outbox);
    }

    match request.pagination_token {
        Some(token) => {
            let outbox = outboxes.get_mut(user.id()).unwrap(/* known good */);
            Ok(OutboxResponse::Paged(
                make_outbox_page(user, outbox, &state.origin, &token, &key).await?,
            ))
        }
        None => Ok(OutboxResponse::Outbox(make_outbox(
            user,
            &state.origin,
            &key,
        )?)),
    }
}

define_metric! { "user.outbox.redirects", user_outbox_redirects, Sort::IntegralCounter }

/// Handle a user outbox request-- execute locally or forward to the responsible node in this cluster
pub async fn handle_outbox_or_redirect(
    state: Arc<Indielinks>,
    user: &User,
    request: UserOutboxRequest,
) -> Result<OutboxResponse> {
    match this_node_is_responsible(state.cache_node.clone(), user).await? {
        None => handle_outbox(state, user, request).await,
        Some(_) => unimplemented!(), // To be implemented
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

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           SendFollow                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A UUID identifying the background task [SendFollow]
// 256046f6-1afe-414d-a48a-fb19edd970e6
const SEND_FOLLOW: Uuid = Uuid::from_fields(
    0x256046f6,
    0x1afe,
    0x414d,
    &[0xa4, 0x8a, 0xfb, 0x19, 0xed, 0xd9, 0x70, 0xe6],
);

#[derive(Debug, Deserialize, Serialize)]
pub struct SendFollow {
    user: User,
    actorid: Url,
    id: FollowId,
}

impl SendFollow {
    pub fn new(user: User, actorid: Url, id: FollowId) -> SendFollow {
        SendFollow { user, actorid, id }
    }
}

#[async_trait]
impl Task<Context> for SendFollow {
    async fn exec(self: Box<Self>, context: Context) -> StdResult<(), background_tasks::Error> {
        debug!(
            "Sending a Follow request to {} for {}",
            AsRef::<str>::as_ref(&self.actorid), // Ugh
            self.user.username()
        );

        async fn exec1(this: Box<SendFollow>, context: Context) -> Result<()> {
            let mut client = context.ap_client.clone();

            // It's possible that `actorid` may not be the ActivityPub ID of the actor we wish to
            // follow. For instance, when testing against Mastodon locally,
            // `http://localhost:3001/users/admin` might have an ID of
            // `http://localhost:3001/ap/users/116133685929577914`. Since that's what will come back
            // in the Accept message, we need them to match.
            let actor: Actor = ap_request(
                &mut client,
                &context.origin,
                Left(&this.user),
                &this.actorid,
                Method::GET,
                None,
                &(),
            )
            .await
            .context(ApSnafu)?;
            let actorid = actor.id().clone();

            debug!("Resolved the ActorID to {actorid}");

            let inbox = context
                .ap_resolver
                .lock()
                .await
                .actor_id_to_inbox(Left(&this.user), &actorid)
                .await
                .context(ApResolverSnafu)?;

            debug!("Resolved the actor inbox to {inbox}");

            // Let's write the new follow to the database,
            let userurl = StorUrl::from(&actorid);
            context
                .storage
                .add_following(&this.user, &userurl, &this.id)
                .await
                .context(AddFollowingSnafu {
                    username: this.user.username().clone(),
                    actorid: Box::new(actorid.clone()),
                })?;

            debug!("Wrote {actorid} into the \"following\" table.\"");

            // then send the `Follow`
            let follow = Follow::new(
                actorid.clone(),
                make_follow_id(this.user.username(), &this.id, &context.origin).context(
                    FollowIdSnafu {
                        username: this.user.username().clone(),
                        id: this.id,
                    },
                )?,
                make_user_id(this.user.username(), &context.origin).context(UserIdSnafu {
                    username: this.user.username().clone(),
                })?,
            );

            debug!("Sending {follow:?}");

            let res = ap_request_no_response(
                &mut client,
                &context.origin,
                Left(&this.user),
                &inbox,
                Method::POST,
                None,
                &follow,
            )
            .await
            .context(ApSnafu);

            debug!("SendFollow :=> {res:?}");

            // Finally, and assuming this follow request is accepted, this user's home timline is
            // now incorrect. We *could* fetch enough of this new follow's outbox to repair it (we'd
            // need to fetch everything for the timespan for which we've materialized this user's
            // timeline in-memory) and insert the new items in the correct places, but for now we'll
            // take the more expedient course: drop the user's timeline & re-build it on demand.
            handle_timeline_drop_or_redirect(
                context.cache_node,
                context.home_timelines.clone(),
                &this.user,
            )
            .await
        }

        exec1(self, context)
            .await
            .map_err(background_tasks::Error::new)
    }
    fn timeout(&self) -> Option<Duration> {
        Some(Duration::from_secs(60))
    }
}

impl TaggedTask<Context> for SendFollow {
    type Tag = Uuid;
    fn get_tag() -> Self::Tag {
        SEND_FOLLOW
    }
}

inventory::submit! {
    BackgroundTask {
        id: SEND_FOLLOW,
        de: |buf| { Ok(Box::new(rmp_serde::from_slice::<SendFollow>(buf).unwrap())) }
    }
}
