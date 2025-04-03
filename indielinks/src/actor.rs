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

//! The ActivityPub Actor endpoint

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use axum::{
    extract::State,
    http::{header::CONTENT_TYPE, HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::{get, post},
    Extension, Json, Router,
};
use chrono::Utc;
use http::Method;
use itertools::Itertools;
use picky::{hash::HashAlgorithm, http::HttpSignature, signature::SignatureAlgorithm};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, IntoError, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use tracing::{debug, error, info};
use url::Url;
use uuid::Uuid;

use crate::{
    activity_pub::send_activity_pub_no_response,
    ap_entities::{
        self, make_user_followers, Accept, Announce, BoostFollowOrLike, Follow, Jld, Like, ToJld,
        Undo,
    },
    authn::{self, check_sha_256_content_digest},
    background_tasks::{self, BackgroundTask, BackgroundTasks, Context, Sender, TaggedTask, Task},
    counter_add,
    entities::{User, UserUrl, Username},
    http::{ErrorResponseBody, Indielinks},
    metrics::{self, Sort},
    origin::Origin,
    storage::{self, Backend as StorageBackend},
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
    #[snafu(display("Failed to create an ActivityPub ID: {source}"))]
    ApId { source: crate::ap_entities::Error },
    #[snafu(display("Failed to parse the key ID as an URL: {source}"))]
    BadKeyId {
        source: url::ParseError,
        backtrace: Backtrace,
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
    #[snafu(display("Failed to append a query parameter to an URL: {source}"))]
    Join {
        source: url::ParseError,
        backtrace: Backtrace,
    },
    #[snafu(display("Error converting to JLD: {source}"))]
    Jrd { source: crate::ap_entities::Error },
    #[snafu(display("Failed to form Key ID: {source}"))]
    KeyId { source: crate::ap_entities::Error },
    #[snafu(display("The Actor ID parsed from the request signature ({signature}) doesn't match that in the request body ({request})"))]
    MismatchedActorId {
        signature: String,
        request: String,
        backtrace: Backtrace,
    },
    #[snafu(display("The signature does not cover the Digest with a non-trivial request body"))]
    MissingContentDigest,
    NoUser {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("Signature header value not UTF-8: {source}"))]
    NonUtf8Signature {
        source: http::header::ToStrError,
        backtrace: Backtrace,
    },
    #[snafu(display("Exactly one Signature header expected"))]
    OneSignature { backtrace: Backtrace },
    #[snafu(display("Failed to obtain an Actor public key: {source}"))]
    PublicKey { source: ap_entities::Error },
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
            Error::OneSignature { .. } => (StatusCode::BAD_REQUEST, format!("{}", self)),
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
//                                        actor utilities                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("actor.verification.successes", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("actor.verification.failures", Sort::IntegralCounter) }

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
        client: &reqwest::Client,
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
            ap_entities::resolve_key_id(&Url::parse(key_id).context(BadKeyIdSnafu)?, client)
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
    match verify_signature1(headers, request, &state.client).await {
        Ok((mut request, actor)) => {
            // Place `actor` into the request context for the convenience of downstream consumers
            request.extensions_mut().insert(actor);
            counter_add!(state.instruments, "actor.verification.successes", 1, &[]);
            next.run(request).await
        }
        Err(err) => {
            error!("indielinks failed to verify an HTTP signature: {:?}", err);
            counter_add!(state.instruments, "actor.verification.failures", 1, &[]);
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
    instruments: &metrics::Instruments,
    metric: &str,
) -> axum::response::Response {
    error!("{:#?}", err);
    counter_add!(instruments, metric, 1, &[]);
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
//                                  `/users/{username}` handler                                   //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("actor.retrieved", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("actor.errors", Sort::IntegralCounter) }

// Not sure this is really how I want to handle this. Leaving it for now, to see if this is a
// repeating pattern.
trait Actor {
    fn as_jrd(&self, origin: &Origin) -> Result<Jld>;
    fn as_html(&self) -> String;
}

impl Actor for User {
    fn as_jrd(&self, origin: &Origin) -> Result<Jld> {
        ap_entities::Jld::new(
            &crate::ap_entities::Actor::new(self, origin).context(ActorSnafu)?,
            None,
        )
        .context(JrdSnafu)
    }
    fn as_html(&self) -> String {
        format!(
            "<html><body>{} ({}@indiemark.sh)</body></html>",
            self.display_name(),
            self.username()
        )
    }
}

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
        storage: &(dyn StorageBackend + Send + Sync),
        username: &Username,
        headers: &HeaderMap,
    ) -> Result<(User, crate::http::Accept)> {
        let accept =
            crate::http::Accept::lookup_from_header_map(headers).context(AcceptLookupSnafu)?;
        let user = storage
            .user_for_name(username)
            .await
            .map_err(|err| StorageSnafu.into_error(err))?
            .ok_or(
                NoUserSnafu {
                    username: username.clone(),
                }
                .build(),
            )?;
        Ok((user, accept))
    }

    match actor1(state.storage.as_ref(), &username, &headers).await {
        Ok((user, crate::http::Accept::ActivityPub)) => match user.as_jrd(&state.origin) {
            Ok(jrd) => {
                counter_add!(state.instruments, "actor.retrieved", 1, &[]);
                patch_content_type((StatusCode::OK, jrd.to_string()).into_response())
            }
            Err(err) => handle_err(err, &state.instruments, "actor.errors"),
        },
        Ok((user, crate::http::Accept::Html)) => {
            counter_add!(state.instruments, "actor.retrieved", 1, &[]);
            (StatusCode::OK, Html(user.as_html())).into_response()
        }
        Err(err) => handle_err(err, &state.instruments, "actor.errors"),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                               `/users/{username}/inbox` handler                                //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("inbox.boosts", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("inbox.follows", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("inbox.likes", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("inbox.undos", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("inbox.errors", Sort::IntegralCounter) }

// This is a stub.
async fn accept_boost(boost: &Announce) -> Result<()> {
    info!("Received an Announce: {:?}", boost);
    Ok(())
}

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
            send_activity_pub_no_response(
                &this.user,
                &this.origin,
                Method::POST,
                &this.actor_inbox,
                Some(
                    &Accept::for_follow(this.user.username(), &this.follow, &this.origin)
                        .context(AcceptResponseSnafu)?,
                ),
                None,
                &context.client,
                None,
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
    if follow.actor_id() != *actor.id() {
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
        .add_follower(user, actor.id())
        .await
        .context(StorageSnafu)
}

// This is a stub.
async fn accept_like(like: &Like) -> Result<()> {
    info!("Received a Like: {:?}", like);
    Ok(())
}

// This is a stub.
async fn accept_undo(undo: &Undo) -> Result<()> {
    info!("Received an Undo: {:?}", undo);
    Ok(())
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
    axum::extract::Json(body): axum::extract::Json<BoostFollowOrLike>,
) -> axum::response::Response {
    async fn inbox1(
        body: &BoostFollowOrLike,
        username: &Username,
        actor: &ap_entities::Actor,
        origin: &Origin,
        storage: &(dyn StorageBackend + Send + Sync),
        task_sender: Arc<BackgroundTasks>,
        instruments: &metrics::Instruments,
    ) -> Result<()> {
        let user = storage
            .user_for_name(username)
            .await
            .context(StorageSnafu)?
            .context(NoUserSnafu {
                username: username.clone(),
            })?;
        match body {
            BoostFollowOrLike::Announce(announce) => {
                counter_add!(instruments, "inbox.boosts", 1, &[]);
                accept_boost(announce).await
            }
            BoostFollowOrLike::Follow(follow) => {
                counter_add!(instruments, "inbox.follows", 1, &[]);
                accept_follow(&user, follow, actor, storage, task_sender, origin).await
            }
            BoostFollowOrLike::Like(like) => {
                counter_add!(instruments, "inbox.follows", 1, &[]);
                accept_like(like).await
            }
            BoostFollowOrLike::Undo(undo) => {
                counter_add!(instruments, "inbox.undos", 1, &[]);
                accept_undo(undo).await
            }
        }
    }

    fn handle_err(err: Error, instruments: &metrics::Instruments) -> axum::response::Response {
        error!("{:#?}", err);
        counter_add!(instruments, "inbox.errors", 1, &[]);
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
        &state.instruments,
    )
    .await
    {
        Ok(_) => (StatusCode::CREATED, ()).into_response(),
        Err(err) => handle_err(err, &state.instruments),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Followers                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("followers.pages", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("followers.errors", Sort::IntegralCounter) }

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
    pub ordered_items: Option<Vec<UserUrl>>,
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
            .user_for_name(username)
            .await
            .map_err(|err| StorageSnafu.into_error(err))?
            .ok_or(
                NoUserSnafu {
                    username: username.clone(),
                }
                .build(),
            )?;
        // and extract their followers:
        let followers = user.followers();
        let num_followers = user.num_followers();
        let followers_id = make_user_followers(username, origin).context(ApIdSnafu)?;
        let first = followers_id.join("?page=0").context(JoinSnafu)?;
        // What we do now depends on `page`; if...
        match page {
            Some(page_num) => {
                // we have a page, we need to extract the corresponding chunk from `followers`...
                let items = match followers.chunks(page_size).into_iter().nth(page_num) {
                    Some(chunk) => chunk.into_iter().cloned().collect::<Vec<UserUrl>>(),
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
                counter_add!(state.instruments, "followers.pages", 1, &[]);
                patch_content_type((StatusCode::OK, jrd.to_string()).into_response())
            }
            Err(err) => handle_err(err, &state.instruments, "followers.errors"),
        },
        Err(err) => handle_err(err, &state.instruments, "followers.errors"),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Public API                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn make_router(state: Arc<Indielinks>) -> Router<Arc<Indielinks>> {
    Router::new()
        .route("/users/{username}", get(actor))
        .route(
            "/users/{username}/inbox",
            post(inbox).route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                verify_signature,
            )),
        )
        .route("/users/{username}/followers", get(followers))
        .with_state(state)
}
