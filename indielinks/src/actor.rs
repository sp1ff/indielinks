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

use std::sync::Arc;

use axum::{
    extract::State,
    http::{header::CONTENT_TYPE, HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::{get, post},
    Extension, Json, Router,
};
use chrono::Utc;
use http::{header, Method};
use itertools::Itertools;
use picky::{hash::HashAlgorithm, http::HttpSignature, signature::SignatureAlgorithm};
use snafu::{Backtrace, IntoError, OptionExt, ResultExt, Snafu};
use tracing::{debug, error, info};
use url::Url;

use crate::{
    ap_entities::{self, Announce, BoostFollowOrLike, Follow, Like, Undo},
    authn::{self, check_sha_256_content_digest, sign_request},
    counter_add,
    entities::{User, Username},
    http::{ErrorResponseBody, Indielinks},
    metrics::{self, Sort},
    storage::{self, Backend as StorageBackend},
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to send an Accept: {source}"))]
    Accept { source: reqwest::Error },
    #[snafu(display("Failed to lookup the Accept header: {source}"))]
    AcceptLookup { source: crate::http::Error },
    #[snafu(display("Failed to produce an AP Actor: {source}"))]
    Actor { source: ap_entities::Error },
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
    #[snafu(display("Error converting to JLD: {source}"))]
    Jrd { source: crate::ap_entities::Error },
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
            Error::Accept { .. } => (StatusCode::SERVICE_UNAVAILABLE, format!("{}", self)),
            Error::AcceptLookup { source } => (
                StatusCode::BAD_REQUEST,
                format!("Unsupported Accept header value: {}", source),
            ),
            Error::Actor { .. } => (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", self)),
            Error::BadKeyId { .. } => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::BadSignature { .. } => (StatusCode::UNAUTHORIZED, format!("{}", self)),
            Error::Cavage2323 { .. } => (StatusCode::UNAUTHORIZED, format!("{}", self)),
            Error::ContentDigest { .. } => (StatusCode::UNAUTHORIZED, format!("{}", self)),
            Error::Jrd { .. } => (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", self)),
            Error::MissingContentDigest => (StatusCode::UNAUTHORIZED, format!("{}", self)),
            Error::NoUser { username, .. } => {
                (StatusCode::NOT_FOUND, format!("Unknown user {}", username))
            }
            Error::NonUtf8Signature { .. } => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::OneSignature { .. } => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::PublicKey { .. } => (StatusCode::UNAUTHORIZED, format!("{}", self)),
            Error::ResolveKeyId { .. } => (StatusCode::UNAUTHORIZED, format!("{}", self)),
            Error::SignatureParse { .. } => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::Signing { .. } => (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", self)),
            Error::Storage { .. } => (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", self)),
            Error::ToBytes { .. } => (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", self)),
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

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                  `/users/{username}` handler                                   //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("actor.retrieved", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("actor.errors", Sort::IntegralCounter) }

// Not sure this is really how I want to handle this. Leaving it for now, to see if this is a
// repeating pattern.
trait Actor {
    fn as_jrd(&self, domain: &str) -> Result<String>;
    fn as_html(&self) -> String;
}

impl Actor for User {
    fn as_jrd(&self, domain: &str) -> Result<String> {
        ap_entities::to_jrd(
            crate::ap_entities::Actor::new(self, domain).context(ActorSnafu)?,
            ap_entities::Type::Person,
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

    fn handle_err(err: Error, instruments: &metrics::Instruments) -> axum::response::Response {
        error!("{:#?}", err);
        counter_add!(instruments, "actor.errors", 1, &[]);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponseBody {
                error: format!("{}", err),
            }),
        )
            .into_response()
    }

    fn patch_content_type(mut rsp: axum::response::Response) -> axum::response::Response {
        rsp.headers_mut().remove(CONTENT_TYPE);
        rsp.headers_mut().insert(
            CONTENT_TYPE,
            "application/activity+json".parse().unwrap(/* known good */),
        );
        rsp
    }

    match actor1(state.storage.as_ref(), &username, &headers).await {
        Ok((user, crate::http::Accept::ActivityPub)) => match user.as_jrd(&state.domain) {
            Ok(jrd) => {
                counter_add!(state.instruments, "actor.retrieved", 1, &[]);
                patch_content_type((StatusCode::OK, jrd).into_response())
            }
            Err(err) => handle_err(err, &state.instruments),
        },
        Ok((user, crate::http::Accept::Html)) => {
            counter_add!(state.instruments, "actor.retrieved", 1, &[]);
            (StatusCode::OK, Html(user.as_html())).into_response()
        }
        Err(err) => handle_err(err, &state.instruments),
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

/// Accept a `Follow` request
// This is very much a work in progress; just trying to get the ActivityPub protocol up & running,
// right now. This implementation doesn't even persist the fact that the subject has a new follower.
async fn accept_follow(
    user: &User,
    follow: &Follow,
    actor: &ap_entities::Actor,
    domain: &str,
    client: &reqwest::Client,
) -> Result<()> {
    info!(
        "Accepting a Follow request for {}; request follows: {:?}",
        user.username(),
        follow
    );

    // We need to respond to Follows with an Accept; this is largely a stub implementation.
    let accept = ap_entities::Accept::for_follow(&user.username(), follow, domain);
    let accept = ap_entities::to_jrd(accept, ap_entities::Type::Accept, None).unwrap();
    debug!("Accept body: {:#?}", accept);
    let accept: axum::body::Body = accept.into();

    // It feels a little weird to me to send the Accept "inline" with handling the Follow; in
    // general, our peer will "see" the Accept before they've finished reading our response to their
    // Follow. It would feel more natural to me to respond to this Follow request and schedule the
    // Accept to be sent later, but axum doesn't provide any such facility (tho I suppose I could
    // just build it).
    //
    // Still, our peer should be prepared for this; even if I *did* send the accept after writing
    // our response to this process' socket buffer, there's really no guarantee the response will
    // arrive there ahead of the subsequent Accept request.
    let inbox = actor.inbox();
    let request = http::request::Request::builder()
        .method(Method::POST)
        .uri(inbox.as_str())
        .header(header::CONTENT_TYPE, "application/activity+json")
        .header(
            header::DATE,
            Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
        )
        .header(header::HOST, inbox.host_str().unwrap())
        .body(accept)
        .unwrap();

    let key_id = format!("https://{}/users/{}", domain, user.username());
    let request = sign_request(request, &key_id, user.priv_key().as_ref())
        .await
        .context(SigningSnafu)?;
    let rsp = client.execute(request).await.context(AcceptSnafu)?;
    debug!("Accept reponse: {:#?}", rsp);
    debug!("Accept reponse: {:#?}", &rsp.text().await);

    Ok(())
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
#[axum::debug_handler]
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
        domain: &str,
        storage: &(dyn StorageBackend + Send + Sync),
        client: &reqwest::Client,
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
                accept_follow(&user, follow, actor, domain, client).await
            }
            BoostFollowOrLike::Like(like) => {
                counter_add!(instruments, "inbox.likes", 1, &[]);
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
        &state.domain,
        state.storage.as_ref(),
        &state.client,
        &state.instruments,
    )
    .await
    {
        Ok(_) => (StatusCode::CREATED, ()).into_response(),
        Err(err) => handle_err(err, &state.instruments),
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
        .with_state(state)
}
