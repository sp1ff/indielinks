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
//! [ap_entities](crate::ap_entities) and _below_ modules containing public endpoints. The most
//! immediate need is for a home for the logic for sending ActivityPub activities to federated
//! servers, but I've been feeling that [actor]'s due for a re-factor for some time & it's likely
//! things from there will end-up here, too.
//!
//! [actor]: crate::actor

use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use either::Either::{self, Left};
use futures::{
    stream::{self, iter},
    StreamExt, TryStreamExt,
};
use http::Method;
use itertools::Itertools;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use snafu::{prelude::*, Backtrace, IntoError};
use tap::Pipe;
use tokio::time::Instant;
use tracing::{debug, error, warn};
use url::Url;
use uuid::Uuid;

use indielinks_shared::{
    entities::{PostId, StorUrl, UserId, Username},
    origin::Origin,
};

use crate::{
    ap_entities::{
        ap_request, ap_request_no_response, make_follow_id, make_like_id, make_user_followers,
        make_user_id, make_user_reply_id, Actor, ActorField, Create, CreateObject, Follow, Like,
        Note, Recipient, Replies, ToJld, Type,
    },
    background_tasks::{self, BackgroundTask, Context, TaggedTask, Task},
    entities::{FollowId, LikeId, OutgoingLike, OutgoingReply, ReplyId, User, Visibility},
    sanitized_html::{parse, ParseResult},
    storage::Backend as StorageBackend,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ActivityPub entities error: {source}"))]
    Ap { source: crate::ap_entities::Error },
    #[snafu(display("ActivityPub entity resolver error: {source}"))]
    ApResolver { source: crate::ap_resolution::Error },
    #[snafu(display("Failed to write a follow for {username} of {actorid}: {source}"))]
    AddFollowing {
        username: Username,
        actorid: Box<Url>,
        source: crate::storage::Error,
    },
    #[snafu(display("Failed to set request body: {source}"))]
    Body {
        source: http::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to build an HTTP request: {source}"))]
    BuildRequest {
        source: http::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to build a Create activity from a Note for Post {postid}: {source}"))]
    Create {
        postid: PostId,
        #[snafu(source(from(crate::ap_entities::Error, Box::new)))]
        source: Box<crate::ap_entities::Error>,
    },
    #[snafu(display("ActivityPub request failed: {rsp:?}"))]
    FailedAp {
        rsp: Box<http::Response<bytes::Bytes>>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to form an URL for follow of {id} for {username}"))]
    FollowId {
        username: Username,
        id: FollowId,
        #[snafu(source(from(crate::ap_entities::Error, Box::new)))]
        source: Box<crate::ap_entities::Error>,
    },
    #[snafu(display("Failed to obtain a follow: {source}"))]
    Following { source: crate::storage::Error },
    #[snafu(display("Failed to retrieve followers for {username}: {source}"))]
    GetFollowers {
        username: Username,
        source: crate::storage::Error,
    },
    #[snafu(display("Failed to serialize entity to JSON-LD: {source}"))]
    Jld {
        #[snafu(source(from(crate::ap_entities::Error, Box::new)))]
        source: Box<crate::ap_entities::Error>,
    },
    #[snafu(display("Failed to create a KeyId for user {username}: {source}"))]
    KeyId {
        username: Username,
        #[snafu(source(from(crate::ap_entities::Error, Box::new)))]
        source: Box<crate::ap_entities::Error>,
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
    #[snafu(display("No User with username {username}"))]
    NoUserName {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to convert Post ID {postid} into a Note: {source}"))]
    Note {
        postid: PostId,
        #[snafu(source(from(crate::ap_entities::Error, Box::new)))]
        source: Box<crate::ap_entities::Error>,
    },
    #[snafu(display("Failed to lookup Post ID {postid}: {source}"))]
    Post {
        postid: PostId,
        source: crate::storage::Error,
    },
    #[snafu(display("Failed to send a request: {source}"))]
    Request {
        source: either::Either<std::convert::Infallible, indielinks_shared::service::Error>,
        backtrace: Backtrace,
    },
    #[snafu(display("While waiting to send a request, {source}"))]
    RequestReady {
        source: either::Either<std::convert::Infallible, indielinks_shared::service::Error>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize the request body to JSON: {source}"))]
    RspJson {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While sanitizing the HTML {text}, {source}"))]
    Sanitize {
        text: String,
        source: std::fmt::Error,
    },
    #[snafu(display("Failed to sign a request for {keyid}: {source}"))]
    Signature {
        keyid: String,
        #[snafu(source(from(crate::authn::Error, Box::new)))]
        source: Box<crate::authn::Error>,
    },
    #[snafu(display("Datastore error: {source}"))]
    Storage { source: crate::storage::Error },
    #[snafu(display("Conversion into an URL failed: {source}"))]
    Url {
        source: reqwest::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("{url} has no host component"))]
    UrlHost { url: Box<Url>, backtrace: Backtrace },
    #[snafu(display("Failed to lookup User ID {userid}: {source}"))]
    User {
        userid: UserId,
        source: crate::storage::Error,
    },
    #[snafu(display("Failed to form an URL for user {username}: {source}"))]
    UserId {
        username: Username,
        #[snafu(source(from(crate::ap_entities::Error, Box::new)))]
        source: Box<crate::ap_entities::Error>,
    },
    #[snafu(display("Unable to derive visibility"))]
    Visibility { backtrace: Backtrace },
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Utilities                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

// This allows us to call `send_activity_pub*` for requests with no body by specifying `()` as the
// body type. The return value doesn't matter-- the trait method will never be invoked in this case.
// Still not sure this is how I want to model it.
impl ToJld for () {
    fn get_type(&self) -> Type {
        Type::Accept // Just pick anything
    }
}

/// Resolve a collection of [Recipient]s to a set of indielinks [User]s
pub async fn resolve_recipients<'a, I>(
    recipients: I,
    storage: &(dyn StorageBackend + Send + Sync),
) -> Result<HashSet<UserId>>
where
    I: Iterator<Item = &'a Recipient>,
{
    // This is a little irritating, but I'm going to do this in a few passes. The first pass will
    // resolve the `Recipient`s to `Result<Vec<UserId>>`s. If all are `Ok`, then the *second* pass
    // will flatten them into a `HashSet`.
    stream::iter(recipients)
        .then(|recipient| async move {
            match recipient {
                Recipient::Direct(username) => {
                    let id = *storage
                        .user_for_name(username.as_ref())
                        .await
                        .context(StorageSnafu)?
                        .context(NoUserNameSnafu { username })?
                        .id();
                    Ok(vec![id])
                }
                Recipient::Followers(actor_id) => {
                    // Now, getting ahold of the stream can fail...
                    debug!("I have a Followers for {actor_id}");
                    storage
                        .followers_for_actor(&actor_id.into())
                        .await
                        .context(StorageSnafu)?
                        // as can each invocation of `poll_next()`-- at this point, we have a `Stream`
                        // that yields `StdResult<Following, StorError>`, and we want one what yields
                        // `Result<UserId>`:
                        .map(|res| {
                            debug!("{res:#?}");
                            match res {
                                Ok(following) => Ok(*following.user_id()),
                                Err(err) => Err(FollowingSnafu.into_error(err)),
                            }
                        })
                        // collect that into a `Vec`...
                        .collect::<Vec<Result<UserId>>>()
                        // asynchronously...
                        .await
                        // and now `collect()` that into a `Result<Vec<...>>`:
                        .into_iter()
                        .collect::<Result<Vec<UserId>>>()
                }
            }
        })
        .collect::<Vec<Result<Vec<UserId>>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<Vec<UserId>>>>()?
        .into_iter()
        .flatten()
        .collect::<HashSet<UserId>>()
        .pipe(Ok)
}

lazy_static! {
    static ref PUBLIC: Url = Url::parse("https://www.w3.org/ns/activitystreams#Public").unwrap(/* known good */);
}

/// Derive a [Visibility] and a list of local recipients from a pair of collections of [Url]s
/// (presumably an ActivityPub message's "to" & "cc" fields)
pub fn derive_visibility<'a, S, T>(
    to: S,
    cc: T,
    origin: &Origin,
) -> Result<(Visibility, Vec<Recipient>)>
where
    S: Iterator<Item = &'a Url>,
    T: Iterator<Item = &'a Url>,
{
    // Honestly, this entire implementation feels awkward, but but we only get one pass on the
    // iterators unless we want to also demand that they be `Clone`. I'm also still
    // understanding how various AP platforms express visibility.

    // This should probably be public, but we'll see if it would come-in handy anywhere else.
    fn origin_is_eq(url: &Url, origin: &Origin) -> bool {
        matches!(url.try_into().map(|o: Origin| o == *origin), Ok(true))
    }

    let mut public_is_in_to = false;
    let to_local_recipients = to
        .filter_map(|url| {
            if *url == *PUBLIC {
                public_is_in_to = true;
            };
            if url.path().ends_with("/followers") || origin_is_eq(url, origin) {
                Some(url.clone())
            } else {
                None
            }
        })
        .collect::<HashSet<Url>>();

    debug!("to: {to_local_recipients:?}");

    let mut public_is_in_cc = false;
    let cc_local_recipients = cc
        .filter_map(|url| {
            if *url == *PUBLIC {
                public_is_in_cc = true;
            };
            if url.path().ends_with("/followers") || origin_is_eq(url, origin) {
                Some(url.clone())
            } else {
                None
            }
        })
        .collect::<HashSet<Url>>();

    debug!("cc: {cc_local_recipients:?}");

    let local_recipients = to_local_recipients
        .union(&cc_local_recipients)
        .map(|url| Recipient::new(origin, url).map_err(|err| ApSnafu.into_error(err)))
        .collect::<Result<Vec<Recipient>>>()?;

    debug!("local_recipients: {local_recipients:?}");

    // - public: to contains =https://www.w3.org/ns/activitystreams#Public=, cc contains ={actor ID}/followers=
    // - unlisted: to contains ={actor ID}/followers=, cc contains =https://www.w3.org/ns/activitystreams#Public=
    // - followers only: to contains ={actor ID}/followers=, cc is empty
    // - DM: to contains actor IDs, cc is empty
    if public_is_in_to {
        // We expect some local (follower) recipients in cc, but I'm not going to enforce that, yet.
        Ok((Visibility::Public, local_recipients))
    } else if public_is_in_cc {
        // We expect some local (follower) recipients in to, but I'm not going to enforce that, yet.
        Ok((Visibility::Unlisted, local_recipients))
    } else if !to_local_recipients.is_empty() {
        // We expect some local (follower) recipients in to, but I'm not going to enforce that, yet.
        Ok((Visibility::Followers, local_recipients))
    } else if cc_local_recipients.is_empty() && local_recipients.iter().all(Recipient::is_direct) {
        // We expect actors only, and an empty cc
        Ok((Visibility::DirectMessage, local_recipients))
    } else {
        Err(VisibilitySnafu.build())
    }
}

#[cfg(test)]
pub mod test_visibility {

    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_recipient() {
        let origin = Origin::from_str("http://indiemark.local").unwrap(/* known good */);
        let recip = Recipient::new(&origin, &Url::parse("http://indiemark.local/users/sp1ff")
                                   .unwrap(/* known good */))
            .unwrap(/* known good */);
        assert!(recip == Recipient::Direct(Username::new("sp1ff").unwrap(/* known good */)));

        let recip = Recipient::new(&origin, &Url::parse("http://indiemark.local/users/sp1ff/followers")
                                   .unwrap(/* known good */))
        .unwrap(/* known good */);
        assert!(
            recip
                == Recipient::Followers(
                    Url::parse("http://indiemark.local/users/sp1ff").unwrap(/* known good */)
                )
        );
    }
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

            debug!("Will send: {:?}", create);

            // `pending_calls` is a list of, well, pending calls.
            let (proto_calls, errs): (Vec<_>, Vec<_>) = context
                .storage
                .get_followers(&user)
                .await
                .context(GetFollowersSnafu {
                    username: user.username().clone(),
                })?
                .and_then(|follower| {
                    let user = user.clone();
                    let ap_resolver = context.ap_resolver.clone();
                    async move {
                        ap_resolver
                            .lock()
                            .await
                            .actor_id_to_shared_inbox(Left(&user), follower.actor_id().as_ref())
                            .await
                            .map_err(crate::storage::Error::new)?
                            .ok_or(crate::storage::Error::new(
                                NoSharedInboxSnafu {
                                    username: user.username().clone(),
                                }
                                .build(),
                            ))
                    }
                })
                .collect::<Vec<StdResult<Url, _>>>()
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

            debug!("I have {} pending calls.", pending_calls.len());

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
                let mut client3 = context.ap_client.clone();
                match ap_request_no_response(
                    &mut client3,
                    &context.origin,
                    Left(&user),
                    &this_call.inbox,
                    Method::POST,
                    None,
                    &create,
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

            res
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

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            SendLike                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A UUID identifying the background task [SendLike]
// 48434f6c-4d54-448d-82e0-7cfb67f7e030
const SEND_LIKE: Uuid = Uuid::from_fields(
    0x48434f6c,
    0x4d54,
    0x448d,
    &[0x82, 0xe0, 0x7c, 0xfb, 0x67, 0xf7, 0xe0, 0x30],
);

#[derive(Debug, Deserialize, Serialize)]
pub struct SendLike {
    origin: Origin,
    user: User,
    apid: Url,
    id: LikeId,
    actor: Url,
}

impl SendLike {
    pub fn new(origin: Origin, user: User, apid: Url, id: LikeId, actor: Url) -> SendLike {
        SendLike {
            origin,
            user,
            apid,
            id,
            actor,
        }
    }
}

#[async_trait]
impl Task<Context> for SendLike {
    async fn exec(self: Box<Self>, context: Context) -> StdResult<(), background_tasks::Error> {
        debug!(
            "Sending a like for {} on behalf of {}",
            self.apid,
            self.user.username()
        );

        async fn exec1(this: Box<SendLike>, context: Context) -> Result<()> {
            let outgoing = OutgoingLike::new(&this.user, &this.apid, Visibility::Public);
            context
                .storage
                .add_outgoing_like(&outgoing)
                .await
                .context(StorageSnafu)?;

            let mut client = context.ap_client.clone();

            let inbox = context
                .ap_resolver
                .lock()
                .await
                .actor_id_to_inbox(Left(&this.user), &this.actor)
                .await
                .context(ApResolverSnafu)?;

            debug!("Resolved the actor inbox to {inbox}");

            let like = Like::new(
                this.apid,
                make_like_id(this.user.username(), &this.id, &this.origin).context(ApSnafu)?,
                ActorField::Iri(make_user_id(this.user.username(), &this.origin).context(ApSnafu)?),
            );
            ap_request_no_response(
                &mut client,
                &this.origin,
                Left(&this.user),
                &inbox,
                Method::POST,
                None,
                &like,
            )
            .await
            .context(ApSnafu)
        }

        exec1(self, context)
            .await
            .map_err(background_tasks::Error::new)
    }
    fn timeout(&self) -> Option<Duration> {
        Some(Duration::from_secs(60))
    }
}

impl TaggedTask<Context> for SendLike {
    type Tag = Uuid;
    fn get_tag() -> Self::Tag {
        SEND_LIKE
    }
}

inventory::submit! {
    BackgroundTask {
        id: SEND_LIKE,
        de: |buf| { Ok(Box::new(rmp_serde::from_slice::<SendLike>(buf).unwrap())) }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           SendReply                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A UUID identifying the background task [SendReply]
// 0cef8325-c061-4e33-87df-68d82abc80da
const SEND_REPLY: Uuid = Uuid::from_fields(
    0x0cef8325,
    0xc061,
    0x4e33,
    &[0x87, 0xdf, 0x68, 0xd8, 0x2a, 0xbc, 0x80, 0xda],
);

#[derive(Debug, Deserialize, Serialize)]
pub struct SendReply {
    origin: Origin,
    user: User,
    /// ActivityPub ID of the post to which we are replying
    apid: Url,
    id: ReplyId,
    actor: Url,
    reply: String,
}

impl SendReply {
    pub fn new(
        origin: Origin,
        user: User,
        apid: Url,
        id: ReplyId,
        actor: Url,
        reply: String,
    ) -> Self {
        Self {
            origin,
            user,
            apid,
            id,
            actor,
            reply,
        }
    }
}

#[async_trait]
impl Task<Context> for SendReply {
    async fn exec(self: Box<Self>, context: Context) -> StdResult<(), background_tasks::Error> {
        debug!(
            "Sending a reply to {} on behalf of {}",
            self.apid,
            self.user.username()
        );

        async fn exec1(this: Box<SendReply>, context: Context) -> Result<()> {
            // Let's start by writing the reply to the database.
            context
                .storage
                .add_outgoing_reply(&OutgoingReply::new(
                    *this.user.id(),
                    this.id,
                    &this.apid,
                    Visibility::Public,
                    this.reply.clone(),
                ))
                .await
                .context(StorageSnafu)?;

            debug!("Wrote the outgoing reply to ScyllaDB. Sanitizing the reply HTML");

            let ParseResult {
                html,
                mentions,
                tags: _,
            } = parse(&this.reply).context(SanitizeSnafu {
                text: this.reply.clone(),
            })?;

            // Now, here's the story: we have the author of the post to which we are replying: he or
            // she is a recipient. We have zero or more mentions, in the form of "handles" (e.g.
            // @foo@bar.social). They need to be resolved to `Actor`s, and they are also recipients.
            // There may be duplicates (most obviously, the author may be mentioned, but it's always
            // possible someone was mentioned twice, too). Once de-duplicated, the ActivityPub IDs
            // of these actors, together with the followers URL for `this.user` will be listed in
            // the "cc:" field of both the `Create` and the `Note` that it will contain.
            let mut recipients: HashSet<Url> = HashSet::from_iter(
                iter(mentions.iter())
                    .then(|account| async {
                        context
                            .ap_resolver
                            .lock()
                            .await
                            .handle_to_actor(Left(&this.user), account)
                            .await
                            .map(|actor| actor.id().clone())
                    })
                    .collect::<Vec<StdResult<Url, _>>>()
                    .await
                    .into_iter()
                    .collect::<StdResult<Vec<Url>, _>>()
                    .context(ApResolverSnafu)?,
            );

            // We add the original author as a recipient:
            recipients.insert(this.actor.clone());

            // For all the recipients *including* the followers of `this.user`, we need to resolve
            // them to their shared inbox, de-duplicate again (there will, in general, be multiple
            // recipients on the same instance) and send the `Create` (with signatures, retries,
            // respecting our configured rate-limits &c) to all those inboxes.
            let mut shared_inboxes: HashSet<Url> = HashSet::from_iter(
                iter(recipients.iter())
                    .then(|url| async {
                        context
                            .ap_resolver
                            .lock()
                            .await
                            .actor_id_to_shared_inbox(Left(&this.user), url)
                            .await
                    })
                    .collect::<Vec<StdResult<Option<Url>, _>>>()
                    .await
                    .into_iter()
                    .collect::<StdResult<Vec<Option<Url>>, _>>()
                    .context(ApResolverSnafu)?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<Url>>(),
            );

            let followers_url =
                make_user_followers(this.user.username(), &this.origin).context(ApSnafu)?;
            recipients.insert(followers_url);

            shared_inboxes.extend(
                context
                    .storage
                    .get_followers(&this.user)
                    .await
                    .context(StorageSnafu)?
                    // Now: `get_followers()`, on success, resturns a stream of
                    // `Result<Follower, storage::Error>`. Let's map the Err variant to
                    // this module's `Error` type,
                    .map_err(|err| StorageSnafu.into_error(err))
                    // and then map each `Follower` to a shared inbox,
                    .and_then(|follower| {
                        let user = this.user.clone();
                        let ap_resolver = context.ap_resolver.clone();
                        async move {
                            ap_resolver
                                .lock()
                                .await
                                .actor_id_to_shared_inbox(Left(&user), follower.actor_id().as_ref())
                                .await
                                .context(ApResolverSnafu)
                        }
                    })
                    // resulting in a stream of `Result<Option<Url>>`. Collect that,
                    .collect::<Vec<Result<Option<Url>>>>()
                    .await
                    // and transform it into a `Result<Vec<Option<Url>>>`.
                    .into_iter()
                    .collect::<Result<Vec<Option<Url>>>>()?
                    // and finally, transform *that* into a `Vec<Url>`. It's probably not great that
                    // I'm modeling the shared inbox as an `Option<Url>` (instead of just an `Url`),
                    // but there's not much to be done about that, here.
                    .into_iter()
                    .flatten()
                    .collect::<Vec<Url>>(),
            );

            let user_id = make_user_id(this.user.username(), &this.origin).context(ApSnafu)?;
            let reply_id = make_user_reply_id(this.user.username(), &this.id, &this.origin)
                .context(ApSnafu)?;

            // Our next move is to create a Create activity referencing inline a Note.
            let note = Note::new_from_parts(
                reply_id.clone(),
                Some(this.apid),
                None,
                user_id.clone(),
                std::iter::once(PUBLIC.clone()),
                recipients.iter().cloned(),
                html,
                Replies::empty_for_reply(&context.origin, this.user.username(), &this.id)
                    .context(ApSnafu)?,
            )
            .context(ApSnafu)?;

            let create = Create::from_parts(
                reply_id,
                user_id,
                std::iter::once(PUBLIC.clone()),
                recipients.iter().cloned(),
                CreateObject::Note(note),
            )
            .context(ApSnafu)?;

            // Alright-- with that, we send `create` to everyone in `shared_inboxes` (with
            // signatures, respecting rate limits, retrying &c)
            let failures = iter(shared_inboxes)
                .then(|url| {
                    let mut client = context.ap_client.clone();
                    let origin = this.origin.clone();
                    let user = this.user.clone();
                    let create = create.clone();
                    async move {
                        ap_request_no_response(
                            &mut client,
                            &origin,
                            Left(&user),
                            &url,
                            Method::POST,
                            None,
                            &create,
                        )
                        .await
                    }
                })
                .collect::<Vec<StdResult<(), _>>>()
                .await
                .into_iter()
                .filter_map(|result| result.err())
                .collect::<Vec<crate::ap_entities::Error>>();

            // Not a lot we can do here; we've retried & still failed. I suppose we could add a
            // field to the database table, like, `sent` to set ourselves up for retry?
            failures
                .iter()
                .for_each(|err| error!("Failed to send a reply: {err:#?}"));

            match failures.into_iter().next() {
                Some(err) => Err(ApSnafu.into_error(err)),
                None => Ok(()),
            }
        }

        exec1(self, context)
            .await
            .map_err(background_tasks::Error::new)
    }
    fn timeout(&self) -> Option<Duration> {
        Some(Duration::from_secs(300))
    }
}

impl TaggedTask<Context> for SendReply {
    type Tag = Uuid;
    fn get_tag() -> Self::Tag {
        SEND_REPLY
    }
}

inventory::submit! {
    BackgroundTask {
        id: SEND_REPLY,
        de: |buf| { Ok(Box::new(rmp_serde::from_slice::<SendReply>(buf).unwrap())) }
    }
}
