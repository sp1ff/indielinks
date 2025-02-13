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

//! # User API
//!
//! API for sign-up, minting API keys, and so forth.

use std::{collections::HashMap, str::FromStr, sync::Arc};

use axum::{
    extract::{Query, State},
    http::{header::CONTENT_TYPE, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::post,
    Json, Router,
};
use itertools::Itertools;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use snafu::{prelude::*, Backtrace, IntoError};
use tap::Pipe;
use tower_http::{cors::CorsLayer, set_header::SetResponseHeaderLayer};
use tracing::{debug, error, info};

use crate::{
    counter_add,
    entities::{self, User, UserApiKey, UserEmail, Username},
    http::{ErrorResponseBody, Indielinks},
    metrics::{self, Sort},
    peppers,
    storage::{self, Backend as StorageBackend},
    util::exactly_two,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to add user: {source}"))]
    AddUser { source: storage::Error },
    #[snafu(display("The supplied API key couldn't be parsed"))]
    BadApiKey {
        key: String,
        source: hex::FromHexError,
        backtrace: Backtrace,
    },
    #[snafu(display("An Authorization header had a value that couldn't be parsed."))]
    BadAuthHeaderParse {
        value: HeaderValue,
        backtrace: Backtrace,
    },
    #[snafu(display("{username} is not a valid username"))]
    BadUsername {
        username: String,
        source: crate::entities::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid API key"))]
    InvalidApiKey { key: UserApiKey },
    #[snafu(display("An Authorization header had a non-textual value: {source}"))]
    InvalidAuthHeaderValue {
        value: HeaderValue,
        source: axum::http::header::ToStrError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to find a colon in '{text}'"))]
    MissingColon { text: String, backtrace: Backtrace },
    #[snafu(display("Multiple Authorization headers were supplied; only one is accepted."))]
    MultipleAuthnHeaders,
    #[snafu(display("No authorization token found in the query string"))]
    NoAuthToken { backtrace: Backtrace },
    #[snafu(display("{source}"))]
    NoPepper { source: peppers::Error },
    #[snafu(display("Unknown username {username}"))]
    UnknownUser { username: Username },
    #[snafu(display("Authorization scheme {scheme} not supported"))]
    UnsupportedAuthScheme {
        scheme: String,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to lookup user {username}: {source}"))]
    User {
        username: Username,
        source: crate::storage::Error,
    },
    #[snafu(display("Failed to create user: {source}"))]
    UserSignup { source: entities::Error },
}

impl Error {
    pub fn as_status_and_msg(&self) -> (StatusCode, String) {
        match self {
            ////////////////////////////////////////////////////////////////////////////////////////
            // Broken requests-- tell the caller how to fix it
            ////////////////////////////////////////////////////////////////////////////////////////
            Error::BadAuthHeaderParse { value, .. } => (
                StatusCode::BAD_REQUEST,
                format!("Bad Authorization header: {:?}", value),
            ),
            Error::InvalidAuthHeaderValue { value, source, .. } => (
                StatusCode::BAD_REQUEST,
                format!("Bad Authorization header {:?}: {}", value, source),
            ),
            Error::MissingColon { text, .. } => (
                StatusCode::BAD_REQUEST,
                format!("Missing colon in {}", text),
            ),
            Error::MultipleAuthnHeaders => (
                StatusCode::BAD_REQUEST,
                "Multiple authorization headers".to_string(),
            ),
            ////////////////////////////////////////////////////////////////////////////////////////
            // Authorization failure-- don't tell a potential attacker the way in which they failed
            ////////////////////////////////////////////////////////////////////////////////////////
            Error::BadApiKey { .. } => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            Error::BadUsername { .. } => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            Error::InvalidApiKey { .. } => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            Error::NoAuthToken { .. } => (
                StatusCode::UNAUTHORIZED,
                "No Authorization header or auth_token".to_string(),
            ),
            Error::UnknownUser { .. } => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            Error::UnsupportedAuthScheme { .. } => {
                (StatusCode::UNAUTHORIZED, "Unauthorized".to_string())
            }
            ////////////////////////////////////////////////////////////////////////////////////////
            // Internal failure-- own up to it:
            ////////////////////////////////////////////////////////////////////////////////////////
            Error::AddUser { source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to add user: {source}"),
            ),
            Error::NoPepper { .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "No pepper available".to_string(),
            ),
            Error::User { username, source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "Internal server error looking-up user {}: {:?}",
                    username, source
                ),
            ),
            Error::UserSignup { source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create user: {source}"),
            ),
        }
    }
}

// Not sure about this approach-- the implementation of this trait is awfully prolix. OTOH, it does
// make the implementation of handlers much easier...
impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let (code, msg) = self.as_status_and_msg();
        (code, Json(ErrorResponseBody { error: msg })).into_response()
    }
}
type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         Authorization                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Authorization schemes
///
/// Unlike the [del.icio.us] API, I am here unburdened by legacy concerns; I can authenticate this
/// API any way I want. I loathe putting key material on the wire (let alone passwords), but at this
/// point in the development of indielinks I'm not quite ready to force clients to sign requests (I
/// still spend too much time using cUrl to test), so I'll continue to support "bearer" (i.e. API
/// key) tokens. I plan to *add* request signing should a particular caller not wish to put their
/// key on the wire, but I hope to move to JWTs as the primary authentication scheme.
///
/// [del.icio.us]: [delicious]
#[derive(Clone, Debug)]
enum AuthnScheme {
    BearerApiKey((Username, UserApiKey)),
}

impl AuthnScheme {
    /// Create an AuthnScheme instance from the the plain text "username:key-in-hex"
    fn from_api_key(payload: &str) -> Result<AuthnScheme> {
        // `payload` should be the plain text "username:key-in-hex"
        let (username, key) = payload.split_once(':').context(MissingColonSnafu {
            text: payload.to_string(),
        })?;
        Ok(AuthnScheme::BearerApiKey((
            Username::from_str(username).context(BadUsernameSnafu {
                username: username.to_owned(),
            })?,
            hex::decode(key.as_bytes())
                .context(BadApiKeySnafu {
                    key: key.to_owned(),
                })?
                .into(),
        )))
    }
}

impl TryFrom<&HeaderValue> for AuthnScheme {
    type Error = Error;

    fn try_from(value: &HeaderValue) -> StdResult<Self, Self::Error> {
        // This seems like a lot of code to parse a `HeaderValue` into a a pair of strings... should
        // I upgrade to a proper parsing library like `nom`? I guess a regex would do it, once we've
        // converted the header value to a string.
        let (scheme, payload) = value
            .to_str()
            .context(InvalidAuthHeaderValueSnafu {
                value: value.clone(),
            })?
            .split_ascii_whitespace()
            .collect::<Vec<&str>>()
            .into_iter()
            .pipe(exactly_two)
            .map_err(|_| {
                BadAuthHeaderParseSnafu {
                    value: value.clone(),
                }
                .build()
            })?;
        match scheme.to_ascii_lowercase().as_str() {
            "bearer" => AuthnScheme::from_api_key(payload),
            _ => UnsupportedAuthSchemeSnafu {
                scheme: scheme.to_owned(),
            }
            .fail(),
        }
    }
}

impl PartialEq for AuthnScheme {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                AuthnScheme::BearerApiKey((username1, key1)),
                AuthnScheme::BearerApiKey((username2, key2)),
            ) => username1 == username2 && key1 == key2,
            // Later: (_, _) => false,
        }
    }
}

inventory::submit! { metrics::Registration::new("user.auth.successes", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("user.auth.failures", Sort::IntegralCounter) }

/// Authenticate a request to the user API
///
/// # Introduction
///
/// Generally, I prefer the use of signed, limited duration bearer tokens or request signing, but
/// I'm going to retain the convenience of just putting the API key on the wire for now (in the
/// Authorization header (using the "Bearer" scheme).
///
/// Insert the user id (as a [UserId]) into the request's extensions on success.
///
/// # Middleware
///
/// This function leverages Axum's support for function-based [middleware]. The requirements on
/// our function are:
///
/// 1. Be an async fn.
/// 2. Take zero or more FromRequestParts extractors.
/// 3. Take exactly one FromRequest extractor as the second to last argument.
/// 4. Take Next as the last argument.
/// 5. Return something that implements IntoResponse
///
/// (see [here]).
///
/// [middleware]: https://docs.rs/axum/latest/axum/middleware/index.html
/// [here]: https://docs.rs/axum/latest/axum/middleware/fn.from_fn.html
async fn authenticate(
    State(state): State<Arc<Indielinks>>,
    Query(params): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    async fn authenticate1(
        headers: axum::http::HeaderMap,
        params: HashMap<String, String>,
        storage: &(dyn StorageBackend + Send + Sync),
    ) -> Result<User> {
        // Ahhhh... the joys of HTTP. Ostensibly, we expect authorization credentials in the
        // Authorization header. Of course, there's nothing stopping a client from including
        // *multiple* Authorization headers, so we have to handle that eventuality. I think, for
        // now, I'm going to just reject requests that carry more than one Authorization header
        // (smells too much like someone trying something fishy).
        let scheme = match headers
            .get_all("authorization")
            .into_iter()
            .at_most_one()
            .map_err(|_| Error::MultipleAuthnHeaders)?
        {
            Some(header_val) => AuthnScheme::try_from(header_val)?,
            None => AuthnScheme::from_api_key(
                params.get("auth_token").ok_or(NoAuthTokenSnafu.build())?,
            )?,
        };

        match scheme {
            AuthnScheme::BearerApiKey((username, key)) => {
                let user = storage
                    .user_for_name(username.as_ref())
                    .await
                    .context(UserSnafu {
                        username: username.clone(),
                    })?
                    .context(UnknownUserSnafu {
                        username: username.clone(),
                    })?;
                if user.check_key(&key) {
                    Ok(user)
                } else {
                    InvalidApiKeySnafu { key }.fail()
                }
            }
        }
    }

    match authenticate1(headers, params, state.storage.as_ref()).await {
        Ok(user) => {
            debug!("indielinks authorized user {}", user.id());
            request.extensions_mut().insert(user);
            counter_add!(state.instruments, "user.auth.successes", 1, &[]);
            next.run(request).await
        }
        Err(Error::NoAuthToken { .. }) => {
            info!("indielinks failed to authenticate this request");
            counter_add!(state.instruments, "user.auth.failures", 1, &[]);
            next.run(request).await
        }
        // I want to be careful about what sort of information we reveal to our caller...
        Err(err) => {
            error!("indielinks failed to authenticate this request");
            counter_add!(state.instruments, "user.auth.failures", 1, &[]);
            err.into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         `/user/signup`                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("user.signups.successful", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("user.signups.failures", Sort::IntegralCounter) }

#[derive(Clone, Debug, Deserialize)]
struct SignupReq {
    username: Username,
    password: SecretString,
    email: UserEmail,
    discoverable: Option<bool>,
    #[serde(rename = "display-name")]
    display_name: Option<String>,
    summary: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SignupRsp {
    pub greeting: String,
}

/// Signup as a new user
///
/// Parameters:
///
/// - username: indielinks usernames consist of alphanumeric characters and '-', '_' & '.'; the
///   username must be unique; if the request's `username` parameter is *not* unique, it will fail.
///
/// - password: indielinks passwords may be abitrary UTF-8 text; indielinks will not store passwords
///   (it stores an Argon2id hash of the salted & peppered password)
///
/// - email: a contact e-mail for this user
///
/// - discoverable: a boolean indicating whether this user wants to be discoverable via webfinter
///   (optional; defaults to true)
///
/// - display-name: the user's "display name" (generally intended to be used in user interfaces);
///   unlike usernames, this may be arbitrary UTF-8 encoded text (optional, defaults to the
///   username)
///
/// - summary: A short bio/blurb (optional; defaults to nothing); arbitrary UTF-8 text
///
/// Unlike other endpoints in this API, there is no authentication on this method.
async fn signup(
    State(state): State<Arc<Indielinks>>,
    Json(signup_req): Json<SignupReq>,
) -> axum::response::Response {
    async fn signup1(signup_req: &SignupReq, state: Arc<Indielinks>) -> Result<SignupRsp> {
        let (pepper_ver, pepper_key) = state.pepper.current_pepper().context(NoPepperSnafu)?;
        let user = User::new(
            &pepper_ver,
            &pepper_key,
            &signup_req.username,
            &signup_req.password,
            &signup_req.email,
            &signup_req.discoverable,
            &signup_req.display_name,
            &signup_req.summary,
        )
        .context(UserSignupSnafu)?;
        let storage: &(dyn StorageBackend + Send + Sync) = state.storage.as_ref();
        storage.add_user(&user).await.context(AddUserSnafu)?;
        Ok(SignupRsp {
            greeting: "Welcome to indielinks!".to_owned(),
        })
    }

    match signup1(&signup_req, state.clone()).await {
        Ok(rsp) => {
            info!("Created user {}", signup_req.username);
            counter_add!(state.instruments, "user.signups.successful", 1, &[]);
            (StatusCode::CREATED, Json(rsp)).into_response()
        }
        Err(Error::UserSignup { source }) => match source {
            entities::Error::PasswordEntropy { feedback, .. } => {
                info!(
                    "password rejected due to insufficient strength: {}",
                    feedback
                );
                counter_add!(state.instruments, "user.signups.failures", 1, &[]);
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponseBody {
                        error: format!("{}", feedback),
                    }),
                )
                    .into_response()
            }
            entities::Error::PasswordWhitespace { .. } => {
                info!("Password rejected due to leading and/or trailing whitespace");
                counter_add!(state.instruments, "user.signups.failures", 1, &[]);
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponseBody {
                        error: "Password rejected due to leading and/or trailing whitespace"
                            .to_owned(),
                    }),
                )
                    .into_response()
            }
            err => {
                error!("{:#?}", err);
                counter_add!(state.instruments, "user.signups.failures", 1, &[]);
                // Arghhhhh...
                let (status, msg) = UserSignupSnafu.into_error(err).as_status_and_msg();
                (status, Json(ErrorResponseBody { error: msg })).into_response()
            }
        },
        Err(Error::AddUser { source }) => match source {
            storage::Error::UsernameClaimed { username, .. } => {
                info!("Username {} already claimed", username);
                counter_add!(state.instruments, "user.signups.failures", 1, &[]);
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponseBody {
                        error: format!("Username {} is already claimed; sorry", username),
                    }),
                )
                    .into_response()
            }
            err => {
                error!("{:#?}", err);
                counter_add!(state.instruments, "user.signups.failures", 1, &[]);
                // Arghhhhh...
                let (status, msg) = AddUserSnafu.into_error(err).as_status_and_msg();
                (status, Json(ErrorResponseBody { error: msg })).into_response()
            }
        },
        Err(err) => {
            error!("{:#?}", err);
            counter_add!(state.instruments, "user.signups.failures", 1, &[]);
            let (status, msg) = err.as_status_and_msg();
            (status, Json(ErrorResponseBody { error: msg })).into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Public API                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Return a router for the User API
///
/// The returned [Router] will presumably be merged with other routres.
pub fn make_router(state: Arc<Indielinks>) -> Router<Arc<Indielinks>> {
    Router::new()
        // I suppose it would be more "RESTful" to have a `user/users` resource and have callers
        // perform this action by POSTing to it (they could retrieve a user via GETting
        // `user/users/:username`), but that model doesn't really map to the set of things one can
        // do via this API (how would we model minting a new API key, for instance? Or loggig-in?)
        .route("/users/signup", post(signup))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            authenticate,
        ))
        // All responses are JSON; add the appropriate Content-Type header (but leave the existing
        // Content-Type header should a handler set it specially).
        .layer(SetResponseHeaderLayer::if_not_present(
            CONTENT_TYPE,
            HeaderValue::from_static("text/json; charset=utf-8"),
        ))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
