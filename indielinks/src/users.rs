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

use std::sync::Arc;

use axum::{
    extract::{rejection::ExtensionRejection, State},
    http::{header::CONTENT_TYPE, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Extension, Json, Router,
};
use chrono::Duration;
use itertools::Itertools;
use opentelemetry::KeyValue;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use snafu::{prelude::*, Backtrace, IntoError};
use tower_http::{cors::CorsLayer, set_header::SetResponseHeaderLayer};
use tracing::{debug, error, info};
use url::Url;

use crate::{
    activity_pub::SendFollow,
    authn::{self, check_api_key, check_password, check_token, AuthnScheme},
    background_tasks::{self, BackgroundTasks, Sender},
    counter_add,
    entities::{self, FollowId, User, UserApiKey, UserEmail, Username},
    http::{ErrorResponseBody, Indielinks},
    metrics::{self, Sort},
    origin::Origin,
    peppers::{self, Peppers},
    signing_keys::{self, SigningKeys},
    storage::{self, Backend as StorageBackend},
    token::{self, mint_token},
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
    #[snafu(display("Incorrect password for {username}"))]
    BadPassword {
        username: Username,
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
        source: authn::Error,
    },
    #[snafu(display("Invalid credentials: {source}"))]
    InvalidCredentials { source: authn::Error },
    #[snafu(display("Failed to find a colon in '{text}'"))]
    MissingColon { text: String, backtrace: Backtrace },
    #[snafu(display("Multiple Authorization headers were supplied; only one is accepted."))]
    MultipleAuthnHeaders,
    #[snafu(display("No authorization token found in the query string"))]
    NoAuthToken { backtrace: Backtrace },
    #[snafu(display("No signing keys available: {source}"))]
    NoKeys {
        source: signing_keys::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("{source}"))]
    NoPepper { source: peppers::Error },
    #[snafu(display("Couldn't validate password for user {username}: {source}"))]
    Password {
        username: Username,
        source: entities::Error,
    },
    #[snafu(display(
        "Couldn't create background task for {username} following {actorid}: {source}"
    ))]
    SendFollow {
        username: Username,
        actorid: Url,
        source: background_tasks::Error,
    },
    #[snafu(display("Failed to mint a token for user {username}: {source}"))]
    Token {
        username: Username,
        #[snafu(source(from(token::Error, Box::new)))]
        source: Box<token::Error>,
    },
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
            Error::BadPassword { .. } => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            Error::InvalidApiKey { .. } => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            Error::InvalidCredentials { .. } => {
                (StatusCode::UNAUTHORIZED, "Unauthorized".to_string())
            }
            Error::NoAuthToken { .. } => (
                StatusCode::UNAUTHORIZED,
                "No Authorization header or auth_token".to_string(),
            ),
            Error::NoKeys { source, .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "No signing keys found ({}); did you configure the program?",
                    source
                ),
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
            Error::Password {
                username, source, ..
            } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Couldn't validate password for {}: {}", username, source),
            ),
            Error::SendFollow {
                username,
                actorid,
                source,
                ..
            } => (StatusCode::INTERNAL_SERVER_ERROR,
                  format!("Couldn't schedule a follow request for {actorid} on behalf of {username}: {source}"),),
            Error::Token {
                username, source, ..
            } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to mint a token for {}: {}", username, source),
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

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         Authorization                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

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
/// Insert the user id (as a [UserId]) into the request's extensions on success. On failure, we let
/// the request go through, so we can't use the [Extension] extractor, as we'll 500 if the handler
/// is invoked un-authorized.
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
    headers: axum::http::HeaderMap,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    async fn authenticate1(
        headers: axum::http::HeaderMap,
        storage: &(dyn StorageBackend + Send + Sync),
        peppers: &Peppers,
        keys: &SigningKeys,
        origin: &Origin,
    ) -> Result<User> {
        // This logic is esentially duplicated in `delicious`-- if this is a recurring pattern
        // across APIs, re-factor.
        let scheme = match headers
            .get_all("authorization")
            .into_iter()
            .at_most_one()
            .map_err(|_| Error::MultipleAuthnHeaders)?
        {
            Some(header_val) => AuthnScheme::try_from(header_val)
                .context(InvalidAuthHeaderValueSnafu { value: header_val })?,
            None => {
                return NoAuthTokenSnafu.fail();
            }
        };

        match scheme {
            AuthnScheme::BearerApiKey((username, key)) => check_api_key(storage, &username, &key)
                .await
                .context(InvalidCredentialsSnafu),
            AuthnScheme::BearerToken(token_string) => {
                check_token(storage, &token_string, keys, origin.host())
                    .await
                    .context(InvalidCredentialsSnafu)
            }
            AuthnScheme::Basic((username, password)) => {
                check_password(storage, peppers, &username, password)
                    .await
                    .context(InvalidCredentialsSnafu)
            }
        }
    }

    match authenticate1(
        headers,
        state.storage.as_ref(),
        &state.pepper,
        &state.signing_keys,
        &state.origin,
    )
    .await
    {
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
            None,
            signup_req.discoverable,
            signup_req.display_name.as_deref(),
            signup_req.summary.as_deref(),
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
//                                         `/user/login``                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("user.logins.successful", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("user.logins.failures", Sort::IntegralCounter) }

#[derive(Clone, Debug, Deserialize)]
struct LoginReq {
    username: Username,
    password: SecretString,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LoginRsp {
    token: String,
}

/// Login as an existing user
///
/// This endpoint will vend a time-limited JWT that can be supplied in the Authorization header
/// (with the bearer scheme) in subsequent requests.
async fn login(
    State(state): State<Arc<Indielinks>>,
    Json(login_req): Json<LoginReq>,
) -> axum::response::Response {
    async fn login1(
        storage: &(dyn StorageBackend + Send + Sync),
        peppers: &Peppers,
        token_lifetime: &Duration,
        signing_keys: &SigningKeys,
        origin: &Origin,
        username: &Username,
        password: SecretString,
    ) -> Result<LoginRsp> {
        let user = storage
            .user_for_name(username.as_ref())
            .await
            .context(UserSnafu {
                username: username.clone(),
            })?
            .context(UnknownUserSnafu {
                username: username.clone(),
            })?;
        user.check_password(peppers, password)
            .context(PasswordSnafu {
                username: username.clone(),
            })?;

        let (keyid, signing_key) = signing_keys.current().context(NoKeysSnafu)?;
        let token = mint_token(
            username,
            &keyid,
            &signing_key,
            origin.host(),
            token_lifetime,
        )
        .context(TokenSnafu {
            username: username.clone(),
        })?;
        Ok(LoginRsp { token })
    }

    match login1(
        state.storage.as_ref(),
        &state.pepper,
        &state.token_lifetime,
        &state.signing_keys,
        &state.origin,
        &login_req.username,
        login_req.password,
    )
    .await
    {
        Ok(rsp) => {
            info!("Logged-in user {}", login_req.username);
            counter_add!(
                state.instruments,
                "user.logins.successful",
                1,
                &[KeyValue::new("username", login_req.username.to_string())]
            );
            (StatusCode::OK, Json(rsp)).into_response()
        }
        Err(Error::BadPassword { username, .. }) => {
            error!("Bad password for user {}", username);
            counter_add!(
                state.instruments,
                "user.logins.failures",
                1,
                &[KeyValue::new("username", login_req.username.to_string())]
            );
            (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponseBody {
                    error: "Unauthorized".to_owned(),
                }),
            )
                .into_response()
        }
        Err(err) => {
            error!("{:#?}", err);
            counter_add!(
                state.instruments,
                "user.logins.failures",
                1,
                &[KeyValue::new("username", login_req.username.to_string())]
            );
            let (status, msg) = err.as_status_and_msg();
            (status, Json(ErrorResponseBody { error: msg })).into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        `/users/follow`                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("user.follows.successful", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("user.follows.failures", Sort::IntegralCounter) }

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FollowReq {
    pub id: Url,
}

type StdResult<T, E> = std::result::Result<T, E>;

/// Send an ActivityPub follow request
///
/// We need to send an ActivityPub [Follow] request to the target user. On receipt of an [Accept],
/// we'll record the new follow for this user.
async fn follow(
    State(state): State<Arc<Indielinks>>,
    user: StdResult<Extension<User>, ExtensionRejection>,
    Json(req): Json<FollowReq>,
) -> axum::response::Response {
    async fn follow1(user: &User, id: &Url, sender: &Arc<BackgroundTasks>) -> Result<()> {
        sender
            .send(SendFollow::new(
                user.clone(),
                id.clone(),
                FollowId::default(),
            ))
            .await
            .context(SendFollowSnafu {
                username: user.username().clone(),
                actorid: id.clone(),
            })?;
        Ok(())
    }

    match &user {
        Ok(user) => match follow1(user, &req.id, &state.task_sender).await {
            Ok(_) => {
                counter_add!(state.instruments, "user.follows.successful", 1, &[]);
                StatusCode::ACCEPTED.into_response()
            }
            Err(err) => {
                counter_add!(state.instruments, "user.follows.failures", 1, &[]);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorResponseBody {
                        error: format!("{}", err),
                    },
                )
                    .into_response()
            }
        },
        Err(_) => {
            counter_add!(state.instruments, "user.follows.failures", 1, &[]);
            StatusCode::UNAUTHORIZED.into_response()
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
        .route("/users/login", get(login))
        .route("/users/follow", post(follow))
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
