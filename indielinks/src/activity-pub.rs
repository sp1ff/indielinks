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
// You should have received a copy of the GNU General Public License along with mpdpopm.  If not,
// see <http://www.gnu.org/licenses/>.

//! # ActivityPub Entities & Utilities
//!
//! ## Introduction
//!
//! I'm coding tentatively, here, but I'm feeling the need for a module that sits _above_
//! [ap_entities] and _below_ modules containing public endpoints. The most immediate need is for a
//! home for the logic for sending ActivityPub activities to federated servers, but I've been
//! feeling that [actor]'s due for a re-factor for some time & it's likely things from there will
//! end-up here, too.
//!
//! [actor]: crate::actor

use std::{collections::VecDeque, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use either::Either;
use futures::{stream, StreamExt};
use http::{header, Method};
use itertools::Itertools;
use reqwest::IntoUrl;
use serde::{Deserialize, Serialize};
use snafu::{prelude::*, Backtrace};
use tap::Pipe;
use tokio::time::Instant;
use tracing::{debug, warn};
use url::Url;
use uuid::Uuid;

use crate::{
    ap_entities::{
        self, make_follow_id, make_user_id, Actor, Create, Follow, Jld, Note, ToJld, Type,
    },
    background_tasks::{self, BackgroundTask, Context, TaggedTask, Task},
    client::request_builder,
    entities::{FollowId, PostId, User, UserId, UserUrl, Username},
    origin::Origin,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to write a follow for {username} of {actorid}: {source}"))]
    AddFollowing {
        username: Username,
        actorid: Url,
        source: crate::storage::Error,
    },
    #[snafu(display("Failed to set request body: {source}"))]
    Body {
        source: http::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to build a Create activity from a Note for Post {postid}: {source}"))]
    Create {
        postid: PostId,
        source: crate::ap_entities::Error,
    },
    #[snafu(display("ActivityPub request failed: {rsp:?}"))]
    FailedAp {
        rsp: reqwest::Response,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to form an URL for follow of {id} for {username}"))]
    FollowId {
        username: Username,
        id: FollowId,
        source: crate::ap_entities::Error,
    },
    #[snafu(display("Failed to serialize entity to JSON-LD: {source}"))]
    Jld { source: crate::ap_entities::Error },
    #[snafu(display("Failed to create a KeyId for user {username}: {source}"))]
    KeyId {
        username: Username,
        source: crate::ap_entities::Error,
    },
    #[snafu(display("No Post for Post ID {postid}"))]
    NoPost {
        postid: PostId,
        backtrace: Backtrace,
    },
    #[snafu(display("{username}'s server has no inbox"))]
    NoInbox {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("{username}'s server has no shared inbox"))]
    NoSharedInbox {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("No User with User ID {userid}"))]
    NoUser {
        userid: UserId,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to convert Post ID {postid} into a Note: {source}"))]
    Note {
        postid: PostId,
        source: crate::ap_entities::Error,
    },
    #[snafu(display("Failed to lookup Post ID {postid}: {source}"))]
    Post {
        postid: PostId,
        source: crate::storage::Error,
    },
    #[snafu(display("Failed to send a request: {source}"))]
    Request {
        source: reqwest_middleware::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize the request body to JSON: {source}"))]
    RspJson {
        source: reqwest::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to send an ActivityPub entity: {source}"))]
    SendAp {
        source: reqwest_middleware::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to sign a request for {keyid}: {source}"))]
    Signature {
        keyid: String,
        source: crate::authn::Error,
    },
    #[snafu(display("Conversion into an URL failed: {source}"))]
    Url {
        source: reqwest::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("{url} has no host component"))]
    UrlHost { url: Url, backtrace: Backtrace },
    #[snafu(display("Failed to lookup User ID {userid}: {source}"))]
    User {
        userid: UserId,
        source: crate::storage::Error,
    },
    #[snafu(display("Failed to form an URL for user {username}: {source}"))]
    UserId {
        username: Username,
        source: crate::ap_entities::Error,
    },
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Utilities                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

// This allows us to call `send_activity_pub*` for requests with no body by specifying `()` aas the
// body type. The return value doesn't matter-- the trait method will never be invoked in this case.
// Still not sure this is how I want to model it.
impl ToJld for () {
    fn get_type(&self) -> Type {
        Type::Accept // Just pick anything
    }
}

#[allow(clippy::too_many_arguments)]
async fn send_activity_pub_core<U: IntoUrl, B: ToJld + std::fmt::Debug>(
    user: &User,
    origin: &Origin,
    method: Method,
    url: U,
    body: Option<&B>,
    context: Option<ap_entities::Context>,
    client: &reqwest_middleware::ClientWithMiddleware,
) -> Result<reqwest::Response> {
    let url = url.into_url().context(UrlSnafu)?;
    request_builder(client, method, url.clone(), Some((user, origin)))
        .header(header::ACCEPT, "application/activity+json")
        .header(header::CONTENT_TYPE, "application/activity+json")
        .header(
            header::DATE,
            Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
        )
        .header(
            header::HOST,
            url.host_str().context(UrlHostSnafu { url: url.clone() })?,
        )
        .body::<reqwest::Body>(
            body.map(|b| Jld::new(b, context).context(JldSnafu))
                .transpose()?
                .map(|jld| jld.into())
                .unwrap_or_default(),
        )
        .send()
        .await
        .context(RequestSnafu)
}

// Consider a redesign of this API once ActivityPub support is fully implemented
#[allow(clippy::too_many_arguments)]
pub async fn send_activity_pub_no_response<U: IntoUrl, B: ToJld + std::fmt::Debug>(
    user: &User,
    origin: &Origin,
    method: Method,
    url: U,
    body: Option<&B>,
    context: Option<ap_entities::Context>,
    client: &reqwest_middleware::ClientWithMiddleware,
) -> Result<()> {
    let response = send_activity_pub_core(user, origin, method, url, body, context, client).await?;

    debug!("Got a response of {:?}", response);

    if !response.status().is_success() {
        return FailedApSnafu { rsp: response }.fail();
    }

    Ok(())
}

/// Send an ActivityPub entity, return the response as an ActivityPub entity
// Consider a redesign of this API once ActivityPub support is fully implemented
#[allow(clippy::too_many_arguments)]
pub async fn send_activity_pub<
    U: IntoUrl,
    B: ToJld + std::fmt::Debug,
    R: for<'de> Deserialize<'de>,
>(
    user: &User,
    origin: &Origin,
    method: Method,
    url: U,
    body: Option<&B>,
    context: Option<ap_entities::Context>,
    client: &reqwest_middleware::ClientWithMiddleware,
) -> Result<R> {
    let response = send_activity_pub_core(user, origin, method, url, body, context, client).await?;

    debug!("Got a response of {:?}", response);

    if !response.status().is_success() {
        return FailedApSnafu { rsp: response }.fail();
    }

    response.json::<R>().await.context(RspJsonSnafu)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           SendCreate                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A UUID identifying the background task [SendCreate]
// 847aff2f-a806-4f26-9374-aefa41101f98
const SEND_CREATE: Uuid = Uuid::from_fields(
    0x847aff2f,
    0xa806,
    0x4f26,
    &[0x93, 0x74, 0xae, 0xfa, 0x41, 0x10, 0x1f, 0x98],
);

#[derive(Debug, Deserialize, Serialize)]
pub struct SendCreate {
    postid: PostId,
    create_time: DateTime<Utc>,
}

impl SendCreate {
    pub fn new(postid: &PostId, create_time: &DateTime<Utc>) -> SendCreate {
        SendCreate {
            postid: *postid,
            create_time: *create_time,
        }
    }
    /// Resolve a follower (in the form of a [UserUrl]) to a public inbox
    // This should be factored-out
    async fn follower_to_public_inbox(
        user: &User,
        origin: &Origin,
        follower: &UserUrl,
        client: &reqwest_middleware::ClientWithMiddleware,
    ) -> Result<Url> {
        debug!("Resolving follower {:?} to a public inbox...", follower);
        send_activity_pub::<&'_ str, (), Actor>(
            user,
            origin,
            Method::GET,
            follower.as_ref(),
            None,
            None,
            client,
        )
        .await?
        .shared_inbox()
        .context(NoSharedInboxSnafu {
            username: user.username().clone(),
        })?
        .clone()
        .pipe(Ok)
    }
}

struct PendingCall {
    inbox: Url,
    failure_count: usize,
    next_delay: Duration,
    next_call: Instant,
}

impl From<Url> for PendingCall {
    fn from(url: Url) -> Self {
        PendingCall {
            inbox: url,
            failure_count: 0,
            next_delay: Duration::from_secs(1),
            next_call: Instant::now(),
        }
    }
}

#[async_trait]
impl Task<Context> for SendCreate {
    /// Send [Create] Activities regarding a user post.
    ///
    /// [Create]: https://www.w3.org/TR/activitystreams-vocabulary/#dfn-create
    async fn exec(self: Box<Self>, context: Context) -> StdResult<(), background_tasks::Error> {
        debug!(
            "Sending a Create for the Note corresponding to Post {}",
            self.postid
        );
        // Introduce a nested function that returns a `Result`; map that to a
        // `background_tasks::Error` below.
        async fn exec1(this: Box<SendCreate>, context: Context) -> Result<()> {
            let post = context
                .storage
                .get_post_by_id(&this.postid)
                .await
                .context(PostSnafu {
                    postid: this.postid,
                })?
                .ok_or(
                    NoPostSnafu {
                        postid: this.postid,
                    }
                    .build(),
                )?;

            let userid = post.user_id();

            let user = context
                .storage
                .get_user_by_id(&userid)
                .await
                .context(UserSnafu {
                    userid: post.user_id(),
                })?
                .ok_or(
                    NoUserSnafu {
                        userid: post.user_id(),
                    }
                    .build(),
                )?;

            let create: Create = Note::new(&post, user.username(), &context.origin)
                .context(NoteSnafu {
                    postid: this.postid,
                })?
                .try_into()
                .context(CreateSnafu {
                    postid: this.postid,
                })?;

            // `pending_calls` is a list of, well, pending calls. This is kinda lame: we're making a
            // network call to resolve each follower to a public inbox, when that's unlikely to
            // change (since the last such call). I'm going to need to build a cache.
            let (proto_calls, errs): (Vec<_>, Vec<_>) = stream::iter(user.followers())
                .then(|x| {
                    SendCreate::follower_to_public_inbox(&user, &context.origin, x, &context.client)
                })
                .collect::<Vec<Result<Url>>>()
                .await
                .into_iter()
                .partition_map(|res| res.map_or_else(Either::Right, Either::Left));

            // Let's handle the errors first-- log 'em, then drop 'em (for now):
            errs.into_iter().for_each(|err| {
                warn!("Failed to resolve a follower to a shared inbox: {:#?}", err)
            });

            let mut pending_calls = proto_calls
                .into_iter()
                .unique()
                .map(|inbox| inbox.into())
                .collect::<VecDeque<PendingCall>>();

            // My idea here is to walk the list of URLs in a loop. For each URL, if we succeed in
            // sending the `Create` activity, we remove that URL. If we fail, we note the next time at
            // which we can call, and put it on the end of the list.
            loop {
                if pending_calls.is_empty() {
                    break;
                }
                // Ho-kay: pop the first pending call off the list:
                let mut this_call = pending_calls.pop_front().unwrap(/* known good */);
                // and just hang-out until that call is "due":
                tokio::time::sleep_until(this_call.next_call).await;

                debug!("Sending a Create to {}", this_call.inbox);

                // If we're here, it's time-- make the call. Nb. that we're not taking advantage of
                // the retry facility offered by `send_activity_pub`-- we'll handle that here so as
                // to interleave the retries.
                match send_activity_pub_no_response::<&'_ str, Create>(
                    &user,
                    &context.origin,
                    Method::POST,
                    this_call.inbox.as_ref(),
                    Some(&create),
                    None,
                    &context.client,
                )
                .await
                {
                    Ok(_) => {
                        debug!("successfully sent a create to {}", this_call.inbox);
                    }
                    Err(err) => {
                        warn!("Failed to a create to {}: {:#?}", this_call.inbox, err);
                        this_call.failure_count += 1;
                        this_call.next_call = Instant::now() + this_call.next_delay;
                        this_call.next_delay *= 2;
                        pending_calls.push_back(this_call);
                    }
                }
            }

            debug!(
                "Sent a Create for the Note corresponding to Post {}",
                this.postid
            );

            Ok(())
        }

        exec1(self, context)
            .await
            .map_err(background_tasks::Error::new)
    }
    fn timeout(&self) -> Option<Duration> {
        // Not sure what I want to do, here... At a minimum, this should come from configuration
        // (and not be hard-coded).
        Some(Duration::from_secs(60))
    }
}

impl TaggedTask<Context> for SendCreate {
    type Tag = Uuid;
    fn get_tag() -> Self::Tag {
        SEND_CREATE
    }
}

inventory::submit! {
    BackgroundTask {
        id: SEND_CREATE,
        de: |buf| { Ok(Box::new(rmp_serde::from_slice::<SendCreate>(buf).unwrap())) }
    }
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
    async fn inbox_for_actor(
        user: &User,
        origin: &Origin,
        actorid: &Url,
        client: &reqwest_middleware::ClientWithMiddleware,
    ) -> Result<Url> {
        send_activity_pub::<&'_ str, (), Actor>(
            user,
            origin,
            Method::GET,
            actorid.as_str(),
            None,
            None,
            client,
        )
        .await?
        .inbox()
        .clone()
        .pipe(Ok)
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
            // Ugh-- this needs to be cleaned-up.
            let userurl = UserUrl::from(this.actorid.clone());

            let inbox = SendFollow::inbox_for_actor(
                &this.user,
                &context.origin,
                &this.actorid,
                &context.client,
            )
            .await?;

            // Let's write the new follow to the database,
            context
                .storage
                .add_following(&this.user, &userurl, &this.id)
                .await
                .context(AddFollowingSnafu {
                    username: this.user.username().clone(),
                    actorid: this.actorid.clone(),
                })?;

            // then send the `Follow`
            let follow = Follow::new(
                this.actorid.clone(),
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

            send_activity_pub_no_response::<Url, Follow>(
                &this.user,
                &context.origin,
                Method::POST,
                inbox,
                Some(&follow),
                None,
                &context.client,
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
