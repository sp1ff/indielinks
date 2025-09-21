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

//! # ActivityPub endpoints
//!
//! This module implements per-user actor endpoints, along with their inboxes, outboxes & so forth.
//! It also implements the instance shared inbox.

use std::{ops::Deref, sync::Arc, time::Duration};

use async_trait::async_trait;
use axum::{
    Extension, Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::IntoResponse,
    routing::{get, post},
};
use chrono::Utc;
use futures::{StreamExt, stream};
use http::Method;
use itertools::Itertools;
use picky::{hash::HashAlgorithm, http::HttpSignature, signature::SignatureAlgorithm};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, IntoError, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use tracing::{debug, error, info, warn};
use url::Url;
use uuid::Uuid;

use indielinks_shared::{PostId, StorUrl, Username};

use crate::{
    activity_pub::{derive_visibility, resolve_recipients, send_activity_pub_no_response},
    ap_entities::{
        self, Accept, Actor, Announce, AnnounceOrCreate, AsAccept, Create, Follow, FollowOrLike,
        Jld, Like, Note, Recipient, ToJld, Undo, make_user_followers, make_user_following,
        username_and_postid_from_url,
    },
    authn::{self, check_sha_256_content_digest},
    background_tasks::{self, BackgroundTask, BackgroundTasks, Context, Sender, TaggedTask, Task},
    client::ClientType,
    define_metric,
    entities::{
        self, ActivityPubPost, ActivityPubPostFlavor, Follower, Following, Reply, Share, User,
    },
    http::{ErrorResponseBody, Indielinks},
    origin::Origin,
    storage::{self, Backend as StorageBackend, Error as StorError},
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to send an Accept: {source}"))]
    Accept { source: reqwest::Error },
    #[snafu(display("Failed to form an Accept response: {source}"))]
    AcceptResponse { source: crate::ap_entities::Error },
    #[snafu(display("Failed to lookup the Accept header: {source}"))]
    AcceptLookup { source: crate::http::Error },
    #[snafu(display("Our Accept response was rejected by the server"))]
    AcceptRejected,
    #[snafu(display("Failed to produce an AP Actor: {source}"))]
    Actor { source: ap_entities::Error },
    #[snafu(display(
        "An AP request was signed by {bearer}, but the payload indicated an actor of {payload}"
    ))]
    ActorMismatch {
        bearer: Url,
        payload: Url,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to create an ActivityPub ID: {source}"))]
    ApId { source: crate::ap_entities::Error },
    #[snafu(display("{url} could not be parsed as an in-reply-to"))]
    BadInReplyTo {
        url: Url,
        source: crate::ap_entities::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to parse the key ID as an URL: {source}"))]
    BadKeyId {
        source: url::ParseError,
        backtrace: Backtrace,
    },
    #[snafu(display("{url} could not be parsed as an object"))]
    BadObject {
        url: Url,
        source: crate::ap_entities::Error,
    },
    #[snafu(display("Couldn't parse {url} as a Post ID: {source}"))]
    BadPost {
        url: Url,
        source: crate::ap_entities::Error,
    },
    #[snafu(display("Signature validation failure: {source}"))]
    BadSignature {
        source: picky::http::http_signature::HttpSignatureError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to enforce items 2 and/or 3 in Cavage et al section 2.3: {source}"))]
    Cavage2323 { source: crate::authn::Error },
    #[snafu(display("Mismatch in the content digest: {source}"))]
    ContentDigest { source: crate::authn::Error },
    #[snafu(display("While serving /following, failed to obtain a stream: {source}"))]
    FollowGetStream { source: storage::Error },
    #[snafu(display("While computing following, our stream yielded: {source}"))]
    FollowStream { source: storage::Error },
    #[snafu(display("While serving /followers, failed to obtain a stream: {source}"))]
    FollowersGetStream { source: storage::Error },
    #[snafu(display("While computing followers, our stream yielded: {source}"))]
    FollowersStream { source: storage::Error },
    #[snafu(display("Failed to append a query parameter to an URL: {source}"))]
    Join {
        source: url::ParseError,
        backtrace: Backtrace,
    },
    #[snafu(display("Error converting to JLD: {source}"))]
    Jrd { source: crate::ap_entities::Error },
    #[snafu(display("Failed to form Key ID: {source}"))]
    KeyId { source: crate::ap_entities::Error },
    #[snafu(display(
        "The Actor ID parsed from the request signature ({signature}) doesn't match that in the request body ({request})"
    ))]
    MismatchedActorId {
        signature: String,
        request: String,
        backtrace: Backtrace,
    },
    #[snafu(display("The signature does not cover the Digest with a non-trivial request body"))]
    MissingContentDigest,
    #[snafu(display("An in-reply-to field was expected, but not found"))]
    NoInReplyTo { backtrace: Backtrace },
    #[snafu(display("User {username} has no post with ID {postid}"))]
    NoPost {
        username: Username,
        postid: PostId,
        backtrace: Backtrace,
    },
    #[snafu(display("No user named {username}"))]
    NoUser {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("Signature header value not UTF-8: {source}"))]
    NonUtf8Signature {
        source: http::header::ToStrError,
        backtrace: Backtrace,
    },
    #[snafu(display("A Note was expected in the `object` field: {source}"))]
    NotNote {
        source: ap_entities::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to convert Post {postid} to a Note: {source}"))]
    Note {
        postid: PostId,
        source: crate::ap_entities::Error,
    },
    #[snafu(display("Exactly one Signature header expected"))]
    OneSignature { backtrace: Backtrace },
    #[snafu(display("No post {postid} for user {requested_username}"))]
    PostUserMismatch {
        postid: PostId,
        requested_username: Username,
        actual_username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to obtain an Actor public key: {source}"))]
    PublicKey { source: ap_entities::Error },
    #[snafu(display("Failed to resolve recipients {recipients:?}: {source}"))]
    Recipients {
        recipients: Vec<Recipient>,
        source: crate::activity_pub::Error,
    },
    #[snafu(display("Failed to buld an http request: {source}"))]
    Request { source: crate::activity_pub::Error },
    #[snafu(display("Failed to resolve a key ID to an Actor: {source}"))]
    ResolveKeyId { source: crate::ap_entities::Error },
    #[snafu(display("Couldn't parse the signature string: {source}"))]
    SignatureParse {
        source: picky::http::http_signature::HttpSignatureError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to sign an outgoing request: {source}"))]
    Signing { source: crate::authn::Error },
    #[snafu(display("Storage backend error: {source}"))]
    Storage { source: storage::Error },
    #[snafu(display("Failed to send a background task: {source}"))]
    TaskSend {
        source: crate::background_tasks::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to read the request body as bytes: {source}"))]
    ToBytes {
        source: axum::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to determine visibility: {source}"))]
    Visibility {
        source: crate::activity_pub::Error,
        backtrace: Backtrace,
    },
}

impl Error {
    /// Convert this error into an HTTP status code & message suitable for the response body
    pub fn as_status_and_msg(&self) -> (StatusCode, String) {
        match self {
            Error::Accept { .. } => (StatusCode::BAD_GATEWAY, format!("{}", self)),
            Error::AcceptLookup { source } => (
                StatusCode::BAD_REQUEST,
                format!("Unsupported Accept header value: {}", source),
            ),
            Error::AcceptResponse { .. } => (StatusCode::BAD_GATEWAY, format!("{}", self)),
            Error::Actor { .. } => (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", self)),
            Error::BadKeyId { .. } => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::BadSignature { .. } => (StatusCode::UNAUTHORIZED, format!("{}", self)),
            Error::Cavage2323 { .. } => (StatusCode::UNAUTHORIZED, format!("{}", self)),
            Error::ContentDigest { .. } => (StatusCode::UNAUTHORIZED, format!("{}", self)),
            Error::Jrd { .. } => (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", self)),
            Error::KeyId { .. } => (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", self)),
            Error::MismatchedActorId { .. } => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::MissingContentDigest => (StatusCode::UNAUTHORIZED, format!("{}", self)),
            Error::NoUser { username, .. } => {
                (StatusCode::NOT_FOUND, format!("Unknown user {}", username))
            }
            Error::NonUtf8Signature { .. } => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::Note { .. } => (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", self)),
            Error::OneSignature { .. } => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::PostUserMismatch { .. } => (StatusCode::NOT_FOUND, format!("{}", self)),
            Error::PublicKey { .. } => (StatusCode::UNAUTHORIZED, format!("{}", self)),
            Error::ResolveKeyId { .. } => (StatusCode::UNAUTHORIZED, format!("{}", self)),
            Error::SignatureParse { .. } => (StatusCode::BAD_REQUEST, format!("{}", self)),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", self)),
        }
    }
}

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let (code, msg) = self.as_status_and_msg();
        (code, Json(ErrorResponseBody { error: msg })).into_response()
    }
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                     assorted utilities                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "actor.verification.successes", actor_verification_successes, Sort::IntegralCounter }
define_metric! { "actor.verification.failures", actor_verification_failures, Sort::IntegralCounter }

/// Verify the signature on an incoming request
///
/// This function is intended to be used as tower middleware. It will look for a `Signature` header
/// containing a [draft-cavage-http-signatures-12] HTTP message signature and validate it. Per the
/// spec, "the `keyId` field is an opaque string that the server can use to look up the component
/// they need to validate the signature." In practice, however, this is an URL where the ActivityPub
/// `Actor` representing the sender can be fetched. On successful validation, this function will
/// clone the `Actor` struct into the request context for the convenience of downstream request
/// consumers.
///
/// [draft-cavage-http-signatures-12]: https://datatracker.ietf.org/doc/html/draft-cavage-http-signatures-12
// This is still evolving: this middleware requires a valid signature on each request it processes.
// Lacking that, the request will not be processed further. It occurs to me that we *may* want to
// accept *unsigned* requests in some circumstances (GET requests, e.g.), though of course invalid
// signatures will be rejected. I've begun to break it up into smaller pieces that can be
// independently tested, but I'm waiting on the opportunity to see more signatures in the wild
// before putting this in its final form. It should probably wind-up in `authn.rs`.
async fn verify_signature(
    State(state): State<Arc<Indielinks>>,
    headers: axum::http::HeaderMap,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    // I "backed-in" to the curious signature of this function while trying to maintain an
    // implementation of `tower_service::Service` on the outer function. I need access to the
    // request in order to validate the signature, but I also need to pass it on to `next`, below.
    // Passing an immutable borrow here leaves the resulting function failing to implement
    // `Service`, for some reason. So, I move the request into this method, use it, and then return
    // it to the caller.
    async fn verify_signature1(
        headers: axum::http::HeaderMap,  // := http::header::headerMap
        request: axum::extract::Request, // := http::request::Request
        mut client: ClientType,
    ) -> Result<(axum::extract::Request, ap_entities::Actor)> {
        // Huh. Per <https://datatracker.ietf.org/doc/html/draft-cavage-http-signatures-12>, the
        // signature should be transmitted in an Authorizatoin header, with a scheme of "Signature":
        // "The client is expected to send an Authorization header... where the 'auth-scheme' is
        // 'Signature' and the 'auth-param' parameters meet the requirements listed in ...The
        // Components of a Signature." Mastodon, however, sends a "signature" header 🤷 Not sure how
        // I want to handle this (like, should I accept both?); for now I just want to validate a
        // signature, so in the best traditions of ActivityPub, I'll just take my peer's
        // implementation as definitive.
        let signature_string = headers
            .get_all("signature")
            .into_iter()
            .exactly_one()
            .map_err(|_| OneSignatureSnafu.build())?
            .to_str()
            .context(NonUtf8SignatureSnafu)?;

        // Sample signature value: "keyId=\"http://localhost:3000/users/admin#main-key\",\
        // algorithm=\"rsa-sha256\",headers=\"host date digest content-type (request-target)\",\
        // signature=\"e6PjagEDONQIWVGvANYVBhW4ckZ6216+Z4XGJOUWsOvO++mYwKP1NtT1jyRUN1rkBx/hLahA\
        // 9B3GDj7JQgiDctcvPVwEtBPYLVuIxAM6zNXkHPugZu+e4NXLLpg9iCJWUenxhvYwT5/CRe9NhYYUQZ1ETL4p\
        // lULh9vBU2tvfNkGBM3gXgqM1yeMFn1HJeRLW0pm7cuRgxSXQHNXejM/iSU18IaaTiW4AvPRKognxRmsX16N1\
        // S2XcWY7wq/LYDG0GJQreL0U+f+5zEu5FhxLhltMb5aL4/bxHTJHfNw58xdi7G49kCb7T4BbxKIRSOJIXciRa\
        // +A+aEo9XNXcOKzDd1g==\""
        let parsed_http_signature = signature_string
            .parse::<HttpSignature>()
            .context(SignatureParseSnafu)?;

        // Alright, the next step is to resolve the "key ID" to an actual, you know, key:
        let key_id = &parsed_http_signature.key_id;
        debug!("Resolving key ID {}", key_id);

        let actor =
            ap_entities::resolve_key_id(&Url::parse(key_id).context(BadKeyIdSnafu)?, &mut client)
                .await
                .context(ResolveKeyIdSnafu)?;

        let public_key = actor.public_key().context(PublicKeySnafu)?;

        // Per the draft "Implementations MUST be able to discover metadata about the key from the
        // `keyId` such that they can determine the type of digital signature algorithm to employ
        // when creating or verifying signatures."

        // And "Implementers SHOULD derive the digital signature algorithm used by an implementation
        // from the key metadata identified by the `keyId` rather than from [the algorithm] field.
        // If `algorithm` is provided and differs from the key metadata identified by the `keyId`,
        // for example `rsa-sha256` but an EdDSA key is identified via `keyId`, then an
        // implementation MUST produce an error. Implementers should note that previous versions of
        // the `algorithm` parameter did not use the key information to derive the digital signature
        // type and thus could be utilized by attackers to expose security vulnerabilities."

        // Finally "The application verifying the signature MUST derive the digital signature
        // algorithm from the metadata associated with the `keyId` and MUST NOT use the value of
        // `algorithm` from the signed message."

        // TBH, the situation seems like a mess. For now, I'm just hard-coding this to "rsa-sha256":
        debug!(
            "Signature algorithm: {:?}, key kind: {:?}",
            parsed_http_signature.algorithm,
            public_key.kind()
        );

        authn::enforce_cavage_2_3_2_3(&parsed_http_signature).context(Cavage2323Snafu)?;

        // Ok-- let's actually validate the request; split the request...
        let (parts, body) = request.into_parts();

        // & verify the body digest.
        let bytes = axum::body::to_bytes(body, 16384)
            .await
            .context(ToBytesSnafu)?;

        // If it's not part of the signature, I'm going to deny the
        // request. To be fair, I haven't seen this mandated anywhere, but I'm not sure what the
        // point of signing a message and not covering the body (unless the body's empty):
        if !bytes.is_empty()
            && !parsed_http_signature
                .headers
                .iter()
                .any(|hdr| hdr == &picky::http::http_signature::Header::Name("digest".to_owned()))
        {
            return Err(Error::MissingContentDigest);
        }

        check_sha_256_content_digest(&parts, &bytes).context(ContentDigestSnafu)?;

        // With all that done, we can finally verify the signature:
        parsed_http_signature
            .verifier()
            .signature_method(
                &public_key,
                SignatureAlgorithm::RsaPkcs1v15(HashAlgorithm::SHA2_256),
            )
            .generate_signing_string_using_http_request(&parts)
            .now(Utc::now().timestamp() as u64)
            .verify()
            .context(BadSignatureSnafu)?;

        // If we're here, we verified the signature; re-assemble the request & pass it on:
        Ok((
            axum::extract::Request::from_parts(parts, bytes.into()),
            actor,
        ))
    }

    // If I borrow, this functions fails to implement `Service` (I suspect the future becomes no
    // longer `Send` or something like that).
    match verify_signature1(headers, request, state.client.clone()).await {
        Ok((mut request, actor)) => {
            // Place `actor` into the request context for the convenience of downstream consumers
            request.extensions_mut().insert(actor);
            actor_verification_successes.add(1, &[]);
            next.run(request).await
        }
        Err(err) => {
            error!("indielinks failed to verify an HTTP signature: {:?}", err);
            actor_verification_failures.add(1, &[]);
            err.into_response()
        }
    }
}

/// Utility function for handling an error in this module's handlers
///
/// Take an [Error], log it at level [ERROR], increment `metric`, and return a response with status
/// code [INTERNAL_SERVER_ERROR] and an [ErrorResponseBody] based on the error.
// This should probably be generalized further, moving the status code into the parameter list
fn handle_err<E: std::error::Error>(
    err: E,
    instrument: &opentelemetry::metrics::Counter<u64>,
) -> axum::response::Response {
    error!("{:#?}", err);
    instrument.add(1, &[]);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponseBody {
            error: format!("{}", err),
        }),
    )
        .into_response()
}

/// Set the ContentType header to "application/activity+json", overwriting the previous value, if
/// any
fn patch_content_type(mut rsp: axum::response::Response) -> axum::response::Response {
    rsp.headers_mut().remove(CONTENT_TYPE);
    rsp.headers_mut().insert(
        CONTENT_TYPE,
        "application/activity+json".parse().unwrap(/* known good */),
    );
    rsp
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        the shared inbox                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "shared_inbox.successes", shared_inbox_successes, Sort::IntegralCounter }
define_metric! { "shared_inbox.announcements", shared_inbox_announcements, Sort::IntegralCounter }
define_metric! { "shared_inbox.creates", shared_inbox_creates, Sort::IntegralCounter }
define_metric! { "shared_inbox.errors", shared_inbox_errors, Sort::IntegralCounter }

/// Handle `Note` creation
///
/// Any `Create` AP entity sent to the shared inbox will wind-up here. We can receive them for
/// a few reasons:
///
/// - a reply has been made to a Post on this instance: in this case, the `in_reply_to` field of the
///   enclosed `Note` will name an indielinks post residing on this instance
///
/// - an indielinks [User] on this instance has been mentioned: in this case, the `to` or `cc`
///   fields will include that [UserId]; if the actor mentioning that user has followers on this
///   instance, that actor's followers will also be listed as recipients
///
/// - else, this is an ActivityPub `Note` made by some actor with followers on this instance
async fn accept_create(
    actor: &ap_entities::Actor,
    create: &Create,
    storage: &(dyn StorageBackend + Send + Sync),
    origin: &Origin,
) -> Result<()> {
    // I expect to have a `Create` whose `object` field is a `Note` containing an `inReplyTo` field
    // naming a post on this instance. Ah... ActivityPub!
    if actor.id() != create.actor() {
        warn!(
            "I have a `Create` signed by {}, while the `Create` itself reports an actor of {}. \
             This seems weird and I'm not comfortable with it: rejecting this request.",
            actor.id(), // Url::url
            create.actor()
        );
        return ActorMismatchSnafu {
            bearer: actor.id().clone(),
            payload: create.actor().clone(),
        }
        .fail();
    }

    // I'm going to log aggressively here, as I explore the corners of the ActivityPub protocol:
    debug!("In receipt of a Create: {:?}", create);

    // At this time, we're expecting the `Create` entity's `object` attribute to be a `Note`-- the `Note`
    // corresponding to a Post made by some user on this instance.
    let note = create.de_object::<Note>().context(NotNoteSnafu)?;

    debug!("The Create denotes the creation of the note: {:?}", note);

    let (visibility, local_recipients) =
        derive_visibility(create.to(), create.cc(), origin).context(VisibilitySnafu)?;

    debug!("This Create has visibility {:?}", visibility);

    // Any local users mentioned?
    let mention = local_recipients
        .iter()
        .filter(|recipient| matches!(recipient, Recipient::Direct(_)))
        .collect::<Vec<_>>()
        .is_empty();

    let mut recipients = resolve_recipients(local_recipients.iter(), storage)
        .await
        .context(RecipientsSnafu {
            recipients: local_recipients.clone(),
        })?;

    let reply = match note
        .in_reply_to()
        .and_then(|reply| username_and_postid_from_url(origin, reply).ok())
    {
        Some((username, postid)) => {
            debug!(
                "The Note is (allegedly) in response to post {} by user {}.",
                postid, username
            );

            let user = storage
                .user_for_name(username.as_ref())
                .await
                .map_err(|err| StorageSnafu.into_error(err))?
                .ok_or(
                    NoUserSnafu {
                        username: username.clone(),
                    }
                    .build(),
                )?;

            let post = storage
                .get_post_by_id(&postid)
                .await
                .context(StorageSnafu)?
                .context(NoPostSnafu { username, postid })?;

            storage
                .add_reply(&Reply::new(user.id(), &post, note.id(), visibility))
                .await
                .context(StorageSnafu)?;

            recipients.remove(user.id());

            true
        }
        None => false,
    };

    let flavor = match (reply, mention) {
        (true, _) => ActivityPubPostFlavor::Reply,
        (false, true) => ActivityPubPostFlavor::Mention,
        (false, false) => ActivityPubPostFlavor::Post,
    };

    stream::iter(recipients.into_iter())
        .map(|recipient| ActivityPubPost::new(recipient, note.id(), flavor, visibility))
        .then(|post| async move { storage.add_activity_pub_post(&post).await })
        .collect::<Vec<StdResult<(), StorError>>>()
        .await
        .into_iter()
        .collect::<StdResult<Vec<()>, StorError>>()
        .context(StorageSnafu)?;

    Ok(())
}

/// Accept a share of an indielinks user's [Post]
// Any `Announce` AP entity sent to the shared inbox will end here. At the time of this writing, I
// only anticipate that happening for one reason: a a `Post` on this instance has been shared.
async fn accept_share(
    actor: &ap_entities::Actor,
    announce: &Announce,
    storage: &(dyn StorageBackend + Send + Sync),
    origin: &Origin,
) -> Result<()> {
    // I expect to have a `Create` whose `object` field references a `Note` containing the original
    // post on this instance. Ah... ActivityPub!
    if actor.id() != announce.actor() {
        warn!(
            "I have a `Create` signed by {}, while the `Create` itself reports an actor of {}. \
               This seems weird and I'm not comfortable with it: rejecting this request.",
            actor.id(),
            announce.actor()
        );
        return ActorMismatchSnafu {
            bearer: actor.id().clone(),
            payload: announce.actor().clone(),
        }
        .fail();
    }

    // I'm going to log aggressively here, as I explore the corners of the ActivityPub protocol:
    debug!("In receipt of an Announce: {:?}", announce);

    let object = announce.object();

    debug!("This Announce relates to the object {}", object);

    let (username, postid) =
        username_and_postid_from_url(origin, object).context(BadObjectSnafu {
            url: announce.object().clone(),
        })?;

    let (visibility, local_recipients) =
        derive_visibility(announce.to(), announce.cc(), origin).context(VisibilitySnafu)?;

    debug!("This Announce has visibility {:?}", visibility);
    debug!(
        "This Announce is addressed to the following local users: {:?}",
        local_recipients
    );

    let user = storage
        .user_for_name(username.as_ref())
        .await
        .map_err(|err| StorageSnafu.into_error(err))?
        .ok_or(
            NoUserSnafu {
                username: username.clone(),
            }
            .build(),
        )?;

    let post = storage
        .get_post_by_id(&postid)
        .await
        .context(StorageSnafu)?
        .context(NoPostSnafu { username, postid })?;

    storage
        .add_share(&Share::new(user.id(), &post, announce.id(), visibility))
        .await
        .context(StorageSnafu)?;

    // Alright-- at this point, we've stored the share, but there may be other recipients to whom
    // this message was addressed, including recpients in the form of "https://so-and-so/followers"
    let mut recipients = resolve_recipients(local_recipients.iter(), storage)
        .await
        .context(RecipientsSnafu {
            recipients: local_recipients.clone(),
        })?;
    recipients.remove(user.id());

    stream::iter(recipients.into_iter())
        .map(|recipient| {
            ActivityPubPost::new(
                recipient,
                // Dear God this is dumb...
                StorUrl::try_from(post.url().to_string()).unwrap(),
                ActivityPubPostFlavor::Share,
                visibility,
            )
        })
        .then(|post| async move { storage.add_activity_pub_post(&post).await })
        .collect::<Vec<StdResult<(), StorError>>>()
        .await
        .into_iter()
        .collect::<StdResult<Vec<()>, StorError>>()
        .context(StorageSnafu)?;

    Ok(())
}

/// `/inbox` handler
///
/// This is the indielinks instance shared inbox. Replies & boosts/shares of our posts come here, as
/// do mentions, as well as posts & replies by people _we_ follow. No idea what else might show-up
/// 🤷. I provide no response body at this time.
async fn shared_inbox(
    State(state): State<Arc<Indielinks>>,
    Extension(actor): Extension<ap_entities::Actor>,
    axum::extract::Json(body): axum::extract::Json<AnnounceOrCreate>,
) -> axum::response::Response {
    async fn shared_inbox1(
        actor: &ap_entities::Actor,
        body: &AnnounceOrCreate,
        storage: &(dyn StorageBackend + Send + Sync),
        origin: &Origin,
    ) -> Result<()> {
        match body {
            // AFAIK, an `Announce` is strictly used to notify us that someone has shared one of our
            // posts.
            AnnounceOrCreate::Announce(announce) => {
                shared_inbox_announcements.add(1, &[]);
                accept_share(actor, announce, storage, origin).await
            }
            AnnounceOrCreate::Create(create) => {
                shared_inbox_creates.add(1, &[]);
                accept_create(actor, create, storage, origin).await
            }
        }
    }

    match shared_inbox1(&actor, &body, state.storage.as_ref(), &state.origin).await {
        Ok(_) => {
            shared_inbox_successes.add(1, &[]);
            (StatusCode::ACCEPTED, ()).into_response()
        }
        Err(err) => {
            error!("{:#?}", err);
            shared_inbox_errors.add(1, &[]);
            err.as_status_and_msg().into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                  `/users/{username}` handler                                   //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "actor.retrieved", actor_retrieved, Sort::IntegralCounter }
define_metric! { "actor.errors", actor_errors, Sort::IntegralCounter }

/// `/users/{username}` handler
///
/// Return the ActivityPub [Person] corresponding to `username`. This can be as JSON-LD (if the
/// Accept header is set to "application/activity+json") or HTML (if it's not given, or if it's set
/// to "text/html").
///
/// [Person]: https://www.w3.org/TR/activitystreams-vocabulary/#dfn-person
async fn actor(
    State(state): State<Arc<Indielinks>>,
    axum::extract::Path(username): axum::extract::Path<Username>,
    headers: HeaderMap,
) -> axum::response::Response {
    async fn actor1(
        origin: &Origin,
        storage: &(dyn StorageBackend + Send + Sync),
        username: &Username,
        headers: &HeaderMap,
    ) -> Result<(Actor, crate::http::Accept)> {
        let accept =
            crate::http::Accept::lookup_from_header_map(headers).context(AcceptLookupSnafu)?;
        let user = storage
            .user_for_name(username.as_ref())
            .await
            .map_err(|err| StorageSnafu.into_error(err))?
            .ok_or(
                NoUserSnafu {
                    username: username.clone(),
                }
                .build(),
            )?;
        let actor = Actor::new(&user, origin).context(ActorSnafu)?;
        Ok((actor, accept))
    }

    match actor1(&state.origin, state.storage.as_ref(), &username, &headers).await {
        Ok((actor, crate::http::Accept::ActivityPub)) => match actor.as_jld(None) {
            Ok(jld) => {
                actor_retrieved.add(1, &[]);
                patch_content_type((StatusCode::OK, jld.to_string()).into_response())
            }
            Err(err) => handle_err(err, actor_errors.deref()),
        },
        Ok((actor, crate::http::Accept::Html)) => match actor.as_html() {
            Ok(html) => {
                actor_retrieved.add(1, &[]);
                (StatusCode::OK, html.to_string()).into_response()
            }
            Err(err) => handle_err(err, actor_errors.deref()),
        },
        Err(err @ Error::NoUser { .. }) => {
            error!("{}", err);
            posts_served.add(1, &[]);
            StatusCode::NOT_FOUND.into_response()
        }
        Err(err) => handle_err(err, actor_errors.deref()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                               `/users/{username}/inbox` handler                                //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "inbox.follows", inbox_follows, Sort::IntegralCounter }
define_metric! { "inbox.likes", inbox_likes, Sort::IntegralCounter }
define_metric! { "inbox.undos", inbox_undos, Sort::IntegralCounter }
define_metric! { "inbox.accepts", inbox_accepts, Sort::IntegralCounter }
define_metric! { "inbox.errors", inbox_errors, Sort::IntegralCounter }

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       Accepting a Follow                                       //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A UUID identifying the background task [AcceptFollow]
// 88aec562-6c5e-4819-8b1d-c05ad683b117
const ACCEPT_FOLLOW: Uuid = Uuid::from_fields(
    0x88aec562,
    0x6c5e,
    0x4819,
    &[0x8b, 0x1d, 0xc0, 0x5a, 0xd6, 0x83, 0xb1, 0x17],
);

/// A background task for sending an [Accept] in response to a [Follow] request.
#[derive(Debug, Deserialize, Serialize)]
struct AcceptFollow {
    user: User,
    origin: Origin,
    actor_inbox: Url,
    follow: Follow,
}

impl AcceptFollow {
    pub fn new(user: &User, origin: &Origin, actor_inbox: &Url, follow: &Follow) -> AcceptFollow {
        AcceptFollow {
            user: user.clone(),
            origin: origin.clone(),
            actor_inbox: actor_inbox.clone(),
            follow: follow.clone(),
        }
    }
}

use background_tasks::Error as BckError;

#[async_trait]
impl Task<Context> for AcceptFollow {
    async fn exec(self: Box<Self>, context: Context) -> StdResult<(), background_tasks::Error> {
        debug!(
            "Sending an Accept in the background for {} to {}",
            self.user.username(),
            self.actor_inbox
        );

        async fn exec1(this: Box<AcceptFollow>, context: Context) -> Result<()> {
            let mut client2 = context.client.clone();
            send_activity_pub_no_response::<&'_ str, Accept>(
                &this.user,
                &this.origin,
                Method::POST,
                this.actor_inbox.as_ref(),
                Some(
                    &Accept::for_follow(this.user.username(), &this.follow, &this.origin)
                        .context(AcceptResponseSnafu)?,
                ),
                None,
                &mut client2,
            )
            .await
            .context(RequestSnafu)
        }

        exec1(self, context).await.map_err(BckError::new)
    }
    fn timeout(&self) -> Option<Duration> {
        None
    }
}

impl TaggedTask<Context> for AcceptFollow {
    type Tag = Uuid;
    fn get_tag() -> Self::Tag {
        ACCEPT_FOLLOW
    }
}

inventory::submit! {
    BackgroundTask {
        id: ACCEPT_FOLLOW,
        de: |buf| { Ok(Box::new(rmp_serde::from_slice::<AcceptFollow>(buf).unwrap())) }
    }
}

/// Accept a follow request
///
/// `user` is the [User] being followed. `follow` is the [Follow] request as received on the wire,
/// `actor` is the follow request sender as authenticated from the request signature.
///
/// Post an [Accept] response to the `actor` inbox and, on success, update `user` to have the new
/// follower.
async fn accept_follow(
    user: &User,
    follow: &Follow,
    actor: &ap_entities::Actor,
    storage: &(dyn StorageBackend + Send + Sync),
    task_sender: Arc<BackgroundTasks>,
    origin: &Origin,
) -> Result<()> {
    debug!(
        "Accepting a Follow request for {}; request follows: {:?}",
        user.username(),
        follow
    );

    // `actor` is read off the Signature header to the request (i.e. this is authenticated); let's
    // check to be sure that the actor in the request matches-up-- it would be weird if Alice was
    // sending a follow request on behalf of Bob.
    if follow.actor_id() != actor.id() {
        return MismatchedActorIdSnafu {
            signature: actor.id().as_str().to_owned(),
            request: follow.actor_id().as_str().to_owned(),
        }
        .fail();
    }

    // We need to send an `Accept` in response-- do it in a background task:
    task_sender
        .as_ref()
        .send(AcceptFollow::new(user, origin, actor.inbox(), follow))
        .await
        .context(TaskSendSnafu)?;

    storage
        // Urp!?
        .add_follower(user, &actor.id().clone().into())
        .await
        .context(StorageSnafu)
}

/// Accept a [Like] for one of `user`'s posts.
async fn accept_like(
    user: &User,
    like: &Like,
    origin: &Origin,
    storage: &(dyn StorageBackend + Send + Sync),
) -> Result<()> {
    info!("Received a Like: {:?}", like);

    // We have, in `like`, an `Url` naming the post that was liked. We resolve that to a `Post` by
    // first extracting the post ID from that URL...
    let (username, postid) =
        username_and_postid_from_url(origin, like.object()).context(BadPostSnafu {
            url: like.object().clone(),
        })?;
    if username != *user.username() {
        error!(
            "The Like {:?} was posted to the inbox of a different user ({}) than that who made \
                the post that was liked ({}). This is weird and uncomfortable-- failing the request.",
            like,
            user.username(),
            username
        );
        return PostUserMismatchSnafu {
            postid,
            requested_username: user.username().clone(),
            actual_username: username,
        }
        .fail();
    }

    // Use the `PostId` to retrieve the `Post`...
    let post = storage
        .get_post_by_id(&postid)
        .await
        .context(StorageSnafu)?
        .context(NoPostSnafu { username, postid })?;

    let like = entities::Like::from_parts(user.id(), &post, like.id());

    storage.add_like(&like).await.context(StorageSnafu)?;

    Ok(())
}

// This is a stub.
async fn accept_undo(undo: &Undo) -> Result<()> {
    info!("Received an Undo: {:?}", undo);
    Ok(())
}

async fn accept_accept(
    user: &User,
    actor: &ap_entities::Actor,
    accept: &Accept,
    storage: &(dyn StorageBackend + Send + Sync),
) -> Result<()> {
    // Ahhh... ActivityPub: so many points of failure. I expect that the object of this `Accept` is
    // the same actor as the sender. Let's verify that, and reject the `Accept` if they don't
    // match-up.
    if actor.id() != accept.object_id() {
        warn!(
            "I have received an Accept from {}, but the request was signed by {}; this is weird \
               and I'm not comfortable with it-- rejecting the request.",
            accept.object_id(),
            actor.id()
        );
        return ActorMismatchSnafu {
            bearer: actor.id().clone(),
            payload: accept.object_id().clone(),
        }
        .fail();
    }
    storage
        .confirm_following(user, &actor.id().into())
        .await
        .context(StorageSnafu)
}

/// ActivityPub user inbox
///
/// This is still work in progress. In order to accept a `Follow`, I need to send an `Accept` back.
/// A `Like` or a `Boost` doesn't need any particular response (tho of course we'll send back an
/// HTTP response, it'll just have an empty body).
async fn inbox(
    State(state): State<Arc<Indielinks>>,
    axum::extract::Path(username): axum::extract::Path<Username>,
    Extension(actor): Extension<ap_entities::Actor>,
    axum::extract::Json(body): axum::extract::Json<FollowOrLike>,
) -> axum::response::Response {
    async fn inbox1(
        body: &FollowOrLike,
        username: &Username,
        actor: &ap_entities::Actor,
        origin: &Origin,
        storage: &(dyn StorageBackend + Send + Sync),
        task_sender: Arc<BackgroundTasks>,
    ) -> Result<()> {
        let user = storage
            .user_for_name(username.as_ref())
            .await
            .context(StorageSnafu)?
            .context(NoUserSnafu {
                username: username.clone(),
            })?;
        match body {
            FollowOrLike::Follow(follow) => {
                inbox_follows.add(1, &[]);
                accept_follow(&user, follow, actor, storage, task_sender, origin).await
            }
            FollowOrLike::Like(like) => {
                inbox_follows.add(1, &[]);
                accept_like(&user, like, origin, storage).await
            }
            FollowOrLike::Undo(undo) => {
                inbox_undos.add(1, &[]);
                accept_undo(undo).await
            }
            FollowOrLike::Accept(accept) => {
                inbox_accepts.add(1, &[]);
                accept_accept(&user, actor, accept, storage).await
            }
        }
    }

    fn handle_err(err: Error) -> axum::response::Response {
        error!("{:#?}", err);
        inbox_errors.add(1, &[]);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponseBody {
                error: format!("{}", err),
            }),
        )
            .into_response()
    }

    match inbox1(
        &body,
        &username,
        &actor,
        &state.origin,
        state.storage.as_ref(),
        state.task_sender.clone(),
    )
    .await
    {
        Ok(_) => (StatusCode::CREATED, ()).into_response(),
        Err(err) => handle_err(err),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Followers                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "followers.pages", followers_pages, Sort::IntegralCounter }
define_metric! { "followers.errors", followers_errors, Sort::IntegralCounter }

// No query params => None, "?page=1" => Some(1)
#[derive(Clone, Debug, Deserialize)]
struct CollectionPagination {
    page: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionPage {
    pub id: Url,
    pub total_items: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first: Option<Url>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<Url>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub part_of: Option<Url>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ordered_items: Option<Vec<StorUrl>>,
}

impl ToJld for CollectionPage {
    fn get_type(&self) -> ap_entities::Type {
        match self.part_of {
            Some(_) => ap_entities::Type::CollectionPage,
            None => ap_entities::Type::OrderedCollection,
        }
    }
}

/// Retrieve a user's followers collection
async fn followers(
    State(state): State<Arc<Indielinks>>,
    axum::extract::Path(username): axum::extract::Path<Username>,
    axum::extract::Query(pagination): axum::extract::Query<CollectionPagination>,
) -> axum::response::Response {
    async fn followers1(
        username: &Username,
        storage: &(dyn StorageBackend + Send + Sync),
        origin: &Origin,
        page: Option<usize>,
        page_size: usize,
    ) -> Result<CollectionPage> {
        // Lookup the User by username...
        let user = storage
            .user_for_name(username.as_ref())
            .await
            .map_err(|err| StorageSnafu.into_error(err))?
            .ok_or(
                NoUserSnafu {
                    username: username.clone(),
                }
                .build(),
            )?;
        // and extract their followers:
        let followers = storage
            .get_followers(&user)
            .await
            .context(FollowersGetStreamSnafu)?;
        let num_followers = followers.size_hint().0;
        let followers_id = make_user_followers(username, origin).context(ApIdSnafu)?;
        let first = followers_id.join("?page=0").context(JoinSnafu)?;
        // What we do now depends on `page`; if...
        match page {
            Some(page_num) => {
                // we have a page, we need to extract the corresponding chunk from `followers`...
                let items = match followers.chunks(page_size).skip(page_num).next().await {
                    Some(chunk) => chunk
                        .into_iter()
                        .collect::<StdResult<Vec<Follower>, _>>()
                        .context(FollowersStreamSnafu)?
                        .into_iter()
                        .map(|follower| follower.actor_id().clone())
                        .collect::<Vec<StorUrl>>(),
                    None => vec![],
                };
                // conditionally compute the `next` attribute...
                let next = ((page_num + 1) * page_size < num_followers).then_some(
                    followers_id
                        .join(&format!("?page={}", page_num + 1))
                        .context(JoinSnafu)?,
                );
                // and finally construct our page:
                CollectionPage {
                    id: followers_id.clone(),
                    total_items: num_followers,
                    first: Some(first),
                    next,
                    part_of: Some(followers_id),
                    ordered_items: Some(items),
                }
            }
            // Otherwise, we just return the "top" of the OrderedCollection, which will contain an
            // attribute (`first`) that tells our caller how to begin the pagination:
            None => CollectionPage {
                id: followers_id,
                total_items: num_followers,
                first: Some(first),
                next: None,
                part_of: None,
                ordered_items: None,
            },
        }
        .pipe(Ok)
    }

    match followers1(
        &username,
        state.storage.as_ref(),
        &state.origin,
        pagination.page,
        state.collection_page_size,
    )
    .await
    {
        Ok(ref page) => match Jld::new(page, None) {
            Ok(jrd) => {
                followers_pages.add(1, &[]);
                patch_content_type((StatusCode::OK, jrd.to_string()).into_response())
            }
            Err(err) => handle_err(err, followers_errors.deref()),
        },
        Err(err) => handle_err(err, followers_errors.deref()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Following                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "following.pages", following_pages, Sort::IntegralCounter }
define_metric! { "following.errors", following_errors, Sort::IntegralCounter }

/// Retrieve a user's following collection
async fn following(
    State(state): State<Arc<Indielinks>>,
    axum::extract::Path(username): axum::extract::Path<Username>,
    axum::extract::Query(pagination): axum::extract::Query<CollectionPagination>,
) -> axum::response::Response {
    async fn following1(
        username: &Username,
        storage: &(dyn StorageBackend + Send + Sync),
        origin: &Origin,
        page: Option<usize>,
        page_size: usize,
    ) -> Result<CollectionPage> {
        // Factor this out (shared by `followers()`, above, at the least):
        let user = storage
            .user_for_name(username.as_ref())
            .await
            .map_err(|err| StorageSnafu.into_error(err))?
            .ok_or(
                NoUserSnafu {
                    username: username.clone(),
                }
                .build(),
            )?;

        let following = storage
            .get_following(&user)
            .await
            .context(FollowGetStreamSnafu)?;
        let (num_following, _) = following.size_hint();
        let following_id = make_user_following(username, origin).context(ApIdSnafu)?;
        let first = following_id.join("?page=0").context(JoinSnafu)?;
        match page {
            Some(page_num) => {
                let items = match following.chunks(page_size).skip(page_num).next().await {
                    Some(chunk) => chunk.into_iter().collect::<StdResult<Vec<Following>, _>>(),
                    None => Ok(vec![]),
                }
                .context(FollowStreamSnafu)?
                .into_iter()
                .map(|f| f.actor_id().clone())
                .collect::<Vec<StorUrl>>();
                // conditionally compute the `next` attribute...
                let next = ((page_num + 1) * page_size < num_following).then_some(
                    following_id
                        .join(&format!("?page={}", page_num + 1))
                        .context(JoinSnafu)?,
                );
                // and finally construct our page:
                CollectionPage {
                    id: following_id.clone(),
                    total_items: num_following,
                    first: Some(first),
                    next,
                    part_of: Some(following_id),
                    ordered_items: Some(items),
                }
            }
            None => CollectionPage {
                id: following_id,
                total_items: num_following,
                first: Some(first),
                next: None,
                part_of: None,
                ordered_items: None,
            },
        }
        .pipe(Ok)
    }

    match following1(
        &username,
        state.storage.as_ref(),
        &state.origin,
        pagination.page,
        state.collection_page_size,
    )
    .await
    {
        Ok(ref page) => match Jld::new(page, None) {
            Ok(jrd) => {
                following_pages.add(1, &[]);
                patch_content_type((StatusCode::OK, jrd.to_string()).into_response())
            }
            Err(err) => handle_err(err, following_errors.deref()),
        },
        Err(err) => handle_err(err, following_errors.deref()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             Posts                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "posts.served", posts_served, Sort::IntegralCounter }
define_metric! { "posts.errors", posts_errors, Sort::IntegralCounter }

async fn get_post(
    State(state): State<Arc<Indielinks>>,
    axum::extract::Path((username, postid)): axum::extract::Path<(Username, PostId)>,
    headers: HeaderMap,
) -> axum::response::Response {
    async fn get_post1(
        origin: &Origin,
        storage: &(dyn StorageBackend + Send + Sync),
        username: &Username,
        postid: &PostId,
        headers: &HeaderMap,
    ) -> Result<(Note, crate::http::Accept)> {
        let accept =
            crate::http::Accept::lookup_from_header_map(headers).context(AcceptLookupSnafu)?;
        let user = storage
            .user_for_name(username.as_ref())
            .await
            .context(StorageSnafu)?
            .ok_or(
                NoUserSnafu {
                    username: username.clone(),
                }
                .build(),
            )?;
        let post = storage
            .get_post_by_id(postid)
            .await
            .context(StorageSnafu)?
            .ok_or(
                NoPostSnafu {
                    username: username.clone(),
                    postid: *postid,
                }
                .build(),
            )?;
        // Check-- username as expected?
        if user.username() != username {
            return PostUserMismatchSnafu {
                postid: *postid,
                requested_username: username.clone(),
                actual_username: user.username().clone(),
            }
            .fail();
        }
        let note = Note::new(&post, username, origin).context(NoteSnafu { postid: *postid })?;
        Ok((note, accept))
    }

    match get_post1(
        &state.origin,
        state.storage.as_ref(),
        &username,
        &postid,
        &headers,
    )
    .await
    {
        Ok((note, crate::http::Accept::ActivityPub)) => match note.as_jld(None) {
            Ok(jld) => {
                posts_served.add(1, &[]);
                patch_content_type((StatusCode::OK, jld.to_string()).into_response())
            }
            Err(err) => handle_err(err, posts_errors.deref()),
        },
        Ok((note, crate::http::Accept::Html)) => match note.as_html() {
            Ok(html) => {
                posts_served.add(1, &[]);
                (StatusCode::OK, html.to_string()).into_response()
            }
            Err(err) => handle_err(err, posts_errors.deref()),
        },
        Err(err @ Error::NoPost { .. }) => {
            error!("{}", err);
            posts_served.add(1, &[]);
            StatusCode::NOT_FOUND.into_response()
        }
        Err(err) => handle_err(err, posts_errors.deref()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Public API                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Return a [Router] handling all ActivityPub-related activity
pub fn make_router(state: Arc<Indielinks>) -> Router<Arc<Indielinks>> {
    Router::new()
        .route(
            "/inbox",
            post(shared_inbox).route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                verify_signature,
            )),
        )
        .route("/users/{username}", get(actor))
        .route(
            "/users/{username}/inbox",
            post(inbox).route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                verify_signature,
            )),
        )
        .route("/users/{username}/followers", get(followers))
        .route("/users/{username}/following", get(following))
        .route("/users/{username}/posts/{postid}", get(get_post))
        .with_state(state)
}
