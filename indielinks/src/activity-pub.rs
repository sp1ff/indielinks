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

use std::{
    collections::{HashSet, VecDeque},
    error::Error as StdError,
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use either::Either;
use futures::{StreamExt, TryStreamExt, stream};
use http::{Method, Request, header};
use itertools::Itertools;
use lazy_static::lazy_static;
use reqwest::IntoUrl;
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, IntoError, prelude::*};
use tap::Pipe;
use tokio::time::Instant;
use tower::{Service, ServiceExt};
use tracing::{debug, warn};
use url::Url;
use uuid::Uuid;

use indielinks_shared::{PostId, StorUrl, UserId, Username};

use crate::{
    ap_entities::{
        self, Actor, Create, Follow, Jld, Note, Recipient, ToJld, Type, make_follow_id,
        make_user_id,
    },
    background_tasks::{self, BackgroundTask, Context, TaggedTask, Task},
    client::ClientType,
    entities::{FollowId, User, Visibility},
    origin::Origin,
    storage::Backend as StorageBackend,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ActivityPub entities error: {source}"))]
    Ap { source: crate::ap_entities::Error },
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
        source: Box<dyn StdError + Send + Sync>,
        backtrace: Backtrace,
    },
    #[snafu(display("While waiting to send a request, {source}"))]
    RequestReady {
        source: Box<dyn StdError + Send + Sync>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize the request body to JSON: {source}"))]
    RspJson {
        source: serde_json::Error,
        backtrace: Backtrace,
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
    client: &mut ClientType,
) -> Result</*reqwest::Response*/ http::Response<Bytes>> {
    let url = url.into_url().context(UrlSnafu)?;

    let request = Request::builder()
        .method(method)
        .uri(url.as_str())
        .extension((user.clone(), origin.clone()))
        .header(header::ACCEPT, "application/activity+json")
        .header(header::CONTENT_TYPE, "application/activity+json")
        .header(
            header::DATE,
            Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
        )
        .header(
            header::HOST,
            url.host_str().context(UrlHostSnafu {
                url: Box::new(url.clone()),
            })?,
        )
        .body::<Bytes>(
            body.map(|b| Jld::new(b, context).context(JldSnafu))
                .transpose()?
                .map(|jld| jld.into())
                .unwrap_or_default(),
        )
        .context(BuildRequestSnafu)?;

    client
        .ready()
        .await
        .context(RequestReadySnafu)?
        .call(request)
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
    client: &mut ClientType,
) -> Result<()> {
    let response = send_activity_pub_core(user, origin, method, url, body, context, client).await?;

    debug!("Got a response of {:?}", response);

    if !response.status().is_success() {
        return FailedApSnafu {
            rsp: Box::new(response),
        }
        .fail();
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
    client: &mut ClientType,
) -> Result<R> {
    let response = send_activity_pub_core(user, origin, method, url, body, context, client).await?;

    debug!("Got a response of {:?}", response);

    if !response.status().is_success() {
        return FailedApSnafu { rsp: response }.fail();
    }

    let body = response.into_body();
    serde_json::from_slice::<R>(body.as_ref()).context(RspJsonSnafu)
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
                    storage
                        .followers_for_actor(&actor_id.into())
                        .await
                        .context(StorageSnafu)?
                        // as can each invocation of `poll_next()`-- at this point, we have a `Stream`
                        // that yields `StdResult<Following, StorError>`, and we want one what yields
                        // `Result<UserId>`:
                        .map(|res| match res {
                            Ok(following) => Ok(*following.user_id()),
                            Err(err) => Err(FollowingSnafu.into_error(err)),
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
            origin_is_eq(url, origin).then_some(url.clone())
        })
        .collect::<HashSet<Url>>();

    let mut public_is_in_cc = false;
    let cc_local_recipients = cc
        .filter_map(|url| {
            if *url == *PUBLIC {
                public_is_in_cc = true;
            };
            origin_is_eq(url, origin).then_some(url.clone())
        })
        .collect::<HashSet<Url>>();

    let local_recipients = to_local_recipients
        .union(&cc_local_recipients)
        .map(|url| Recipient::new(origin, url).map_err(|err| ApSnafu.into_error(err)))
        .collect::<Result<Vec<Recipient>>>()?;

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
    /// Resolve a follower (in the form of a [UserUrl]) to a public inbox
    // This should be factored-out
    async fn follower_to_public_inbox(
        user: &User,
        origin: &Origin,
        follower: &StorUrl,
        client: &mut ClientType,
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

            debug!("Will send: {:?}", create);

            // `pending_calls` is a list of, well, pending calls. This is kinda lame: we're making a
            // network call to resolve each follower to a public inbox, when that's unlikely to
            // change (since the last such call). I'm going to need to build a cache.
            let (proto_calls, errs): (Vec<_>, Vec<_>) = context
                .storage
                .get_followers(&user)
                .await
                .context(GetFollowersSnafu {
                    username: user.username().clone(),
                })?
                .and_then(|follower| {
                    let user = user.clone();
                    let origin = context.origin.clone();
                    let mut client = context.client.clone();
                    async move {
                        SendCreate::follower_to_public_inbox(
                            &user,
                            &origin,
                            follower.actor_id(),
                            &mut client,
                        )
                        .await
                        .map_err(crate::storage::Error::new)
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
                let mut client3 = context.client.clone();
                match send_activity_pub_no_response::<&'_ str, Create>(
                    &user,
                    &context.origin,
                    Method::POST,
                    this_call.inbox.as_ref(),
                    Some(&create),
                    None,
                    &mut client3,
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
        client: &mut ClientType,
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
            let userurl = StorUrl::from(this.actorid.clone());

            let mut client2 = context.client.clone();
            let inbox = SendFollow::inbox_for_actor(
                &this.user,
                &context.origin,
                &this.actorid,
                &mut client2,
            )
            .await?;

            // Let's write the new follow to the database,
            context
                .storage
                .add_following(&this.user, &userurl, &this.id)
                .await
                .context(AddFollowingSnafu {
                    username: this.user.username().clone(),
                    actorid: Box::new(this.actorid.clone()),
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

            let mut client3 = context.client.clone();
            send_activity_pub_no_response::<Url, Follow>(
                &this.user,
                &context.origin,
                Method::POST,
                inbox,
                Some(&follow),
                None,
                &mut client3,
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
