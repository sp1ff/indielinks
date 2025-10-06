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
//!
//! ## indielinks Front End Authentication
//!
//! indielinks uses what I believe to be a fairly standard approach to authenticating browser-based
//! web front ends: authentication is performed by having the client transmit the username &
//! password in the body of a request to `/usrs/login` (this is presumably done over TLS, so that's
//! secure). The password is then salted, peppered, hashed and compared against the stored hash (so
//! that the password need never be stored on the server). On success, the `/users/login` endpoint
//! vends a short-lived token in the response payload meant to be used by the front end
//! implementation (by including it in an Authorization header in subsequent requests).
//!
//! Keeping the token in memory leaves us vulnerable to XSS attacks, but since the token is short
//! lived the window of vulnerability is short. When the token expires, or when the user executes a
//! reload (say, by manually navigating to an URL), the front end will need to refresh the token.
//! This is done by means of a refresh token, also vended by `/users/login` but this time set as an
//! HttpOnly cookie. The cookie will be sent along with the request to `/users/refresh` and serve to
//! validate the request. Since it's HttpOnly, we're safe from XSS, but we're now vulnerable to
//! CSRF: if an attacker can induce us to click a link to `/users/refresh` in our browser, the
//! refresh cookie will be sent along with any other cookies on that domain, and the attacker could
//! harvest the access token.
//!
//! In general, the `/usrers/refresh` endpoint may be cross-origin from the front end, so SameSite
//! settings will be of no help-- we need to mitigate CSRF on this one endpoint. There are two
//! general approaches: the [Synchronizer Token Pattern] & the [Double-Submit Cookie Pattern]. The
//! former require us to store per-session state on the server, which I would prefer not to do. The
//! second comes in two flavors: naive & signed. Naive is worth considering, even if it's no longer
//! recommended. In the naive approach, a CSRF token is transmitted from the server to the client
//! in a Secure, but *not* Http-Only cookie. When the client makes a request, it must read the CSRF
//! token from the cookie, then add it to the request in a custom header. Since in a strictly CSRF
//! attack, the attacker can't read the cookie, all the server need do is verify the two values (the
//! custom header value and the cookie value) match.
//!
//! [Synchronizer Token Pattern]: https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#token-based-mitigation
//! [Double-Submit Cookie Pattern]: https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#alternative-using-a-double-submit-cookie-pattern
//!
//! However, there are attacks in which the attacker can *set* cookies (see [here]); in this case,
//! the attacker can pick any value for the CSRF token, set the cookie and set the custom header &
//! bypass the check on the server side.
//!
//! The signed variant blocks this by picking a *per-session* value, a nonce, and *signing* them.
//! The per-session value condition is important to prevent an attacker from logging in, collecting
//! the CSRF token he gets & reusing it in an attack on my account.
//!
//! [here]: https://owasp.org/www-chapter-london/assets/slides/David_Johansson-Double_Defeat_of_Double-Submit_Cookie.pdf
//!
//! So: login does the following:
//!
//!   - returns a short-lived JWT in the response body
//!   - returns a long-lived "refresh" token (another JWT) in a Secure, HttpOnly cookie; we'll
//!     include a UUID here to "name" the user's session (since when the refresh token expires, the
//!     user will be required to login again)
//!   - returns a CSRF token in a Secure cookie; this token is just an HMAC on the refresh token
//!     nonce using a secret key known only to the server
//!
//! When it's time to refresh the access token, the front end will send a request to the
//! `/users/refresh` endpoint, with the refresh token & CSRF token in the cookies, and copying the
//! CSRF token from the cookie into a custom header. The duplicated CSRF token proves the caller can
//! copy cookies into requests. The CSRF token contents proves that the caller obtained it (the CSRF
//! token) for the proferred refresh token, and that it obtained it from this server.
//!
//! Finally, note that this is designed to defend against CSRF. If an attacker has a successful
//! XSS exploit, they can refresh the token as long as they'd like!

use std::sync::Arc;

use axum::{
    extract::{rejection::ExtensionRejection, State},
    http::{header::CONTENT_TYPE, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Extension, Json, Router,
};
use axum_extra::extract::cookie::CookieJar;
use chrono::{DateTime, Duration, Utc};
use http::{header::SET_COOKIE, HeaderMap};
use itertools::Itertools;
use opentelemetry::KeyValue;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use snafu::{prelude::*, Backtrace, IntoError};
use tower_http::{
    cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer},
    set_header::SetResponseHeaderLayer,
};
use tracing::{debug, error, info};
use url::Url;

use indielinks_shared::{
    api::{
        FollowReq, LoginReq, LoginRsp, MintKeyReq, MintKeyRsp, SignupReq, SignupRsp,
        REFRESH_COOKIE, REFRESH_CSRF_COOKIE, REFRESH_CSRF_HEADER_NAME, REFRESH_CSRF_HEADER_NAME_LC,
    },
    entities::Username,
};

use crate::{
    activity_pub::SendFollow,
    authn::{self, check_api_key, check_password, check_token, AuthnScheme},
    background_tasks::{self, BackgroundTasks, Sender},
    define_metric,
    entities::{self, FollowId, User},
    http::{ErrorResponseBody, SameSite},
    indielinks::Indielinks,
    origin::Origin,
    peppers::{self, Peppers},
    signing_keys::{self, SigningKeys},
    storage::{self, Backend as StorageBackend},
    token::{self, mint_refresh_and_csrf_tokens, mint_token, refresh_token},
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to add a key to {user:?}: {source}"))]
    AddKey {
        user: User,
        source: entities::Error,
        backtrace: Backtrace,
    },
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
    #[snafu(display(
        "The CSRF token {token} doesn't match the CSRF header {header}; this is likely a bug, but could also result from a CSRF attack"
    ))]
    CsrfMismatch {
        token: String,
        header: String,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid API key: {source}"))]
    InvalidApiKey {
        source: authn::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("An Authorization header had a non-textual value: {source}"))]
    InvalidAuthHeaderValue {
        value: HeaderValue,
        source: authn::Error,
    },
    #[snafu(display("Invalid credentials: {source}"))]
    InvalidCredentials { source: authn::Error },
    #[snafu(display("Failed to find a colon in '{text}'"))]
    MissingColon { text: String, backtrace: Backtrace },
    #[snafu(display("A required authentication cookie was missing from the request"))]
    MissingCookie { backtrace: Backtrace },
    #[snafu(display("A required authentication header was missing from the request"))]
    MissingHeader { backtrace: Backtrace },
    #[snafu(display("Multiple Authorization headers were supplied; only one is accepted."))]
    MultipleAuthnHeaders,
    #[snafu(display("No authorization token found in the query string"))]
    NoAuthToken { backtrace: Backtrace },
    #[snafu(display("No signing keys available: {source}"))]
    NoKeys {
        source: signing_keys::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("The authorization header was not UTF-8: {source}"))]
    NonUtf8Header {
        source: http::header::ToStrError,
        backtrace: Backtrace,
    },
    #[snafu(display("{source}"))]
    NoPepper { source: peppers::Error },
    #[snafu(display("Couldn't validate password for user {username}: {source}"))]
    Password {
        username: String,
        source: entities::Error,
    },
    #[snafu(display("Failed to mint refresh and/or CSRF tokens: {source}"))]
    Refresh {
        source: crate::token::Error,
        backtrace: Backtrace,
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
    UnknownUser { username: String },
    #[snafu(display("Authorization scheme {scheme} not supported"))]
    UnsupportedAuthScheme {
        scheme: String,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to write a new key for {user:?}: {source}"))]
    UpdateKey {
        user: User,
        source: crate::storage::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to lookup user {username}: {source}"))]
    User {
        username: String,
        source: crate::storage::Error,
    },
    #[snafu(display("{username} is not a valid indielinks username"))]
    Username {
        username: String,
        source: indielinks_shared::entities::Error,
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
            Error::CsrfMismatch { .. } => (
                StatusCode::BAD_REQUEST,
                "The refresh CSRF token didn't match the CSRF header value".to_owned(),
            ),
            Error::InvalidAuthHeaderValue { value, source, .. } => (
                StatusCode::BAD_REQUEST,
                format!("Bad Authorization header {:?}: {}", value, source),
            ),
            Error::MissingColon { text, .. } => (
                StatusCode::BAD_REQUEST,
                format!("Missing colon in {}", text),
            ),
            Error::NonUtf8Header { source, .. } => (StatusCode::BAD_REQUEST, format!("{source:?}")),
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
            Error::InvalidCredentials { .. } => {
                (StatusCode::UNAUTHORIZED, "Unauthorized".to_string())
            }
            Error::MissingCookie { .. } => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            Error::MissingHeader { .. } => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
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
            Error::Username { .. } => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            ////////////////////////////////////////////////////////////////////////////////////////
            // Internal failure-- own up to it:
            ////////////////////////////////////////////////////////////////////////////////////////
            Error::AddKey {
                user: _, source, ..
            } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to add key: {source}"),
            ),
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
            Error::Refresh { .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Couldn't mint refresh and/or CSRF tokens".to_owned(),
            ),
            Error::SendFollow {
                username,
                actorid,
                source,
                ..
            } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "Couldn't schedule a follow request for {actorid} on behalf of {username}: {source}"
                ),
            ),
            Error::Token {
                username, source, ..
            } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to mint a token for {}: {}", username, source),
            ),
            Error::UpdateKey {
                user: _, source, ..
            } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to update key: {source}"),
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
//                                         Configuration                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

// I suppose I could pull-in the `cookie` crate... but c'mon: it's a few cookies.

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Configuration {
    #[serde(rename = "same-site")]
    pub same_site: SameSite,
    #[serde(rename = "secure-cookies")]
    pub secure_cookies: bool,
    #[serde(rename = "allowed-origins")]
    pub allowed_origins: Vec<Origin>,
}

/// Return a configuration suitable for non-same-origin, http
impl Default for Configuration {
    fn default() -> Self {
        Configuration {
            same_site: SameSite::None,
            secure_cookies: false,
            allowed_origins: vec![
                Origin::try_from("http://localhost:18080".to_owned()).unwrap(/* known good */),
                Origin::try_from("http://localhost:20676".to_owned()).unwrap(/* known good */),
                Origin::try_from("http://127.0.0.1:18080".to_owned()).unwrap(/* known good */),
                Origin::try_from("http://127.0.0.1:20676".to_owned()).unwrap(/* known good */),
                Origin::try_from("http://localhost:18443".to_owned()).unwrap(/* known good */),
                Origin::try_from("http://127.0.0.1:18443".to_owned()).unwrap(/* known good */),
            ],
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         Authorization                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "user.auth.successes", user_auth_successes, Sort::IntegralCounter }
define_metric! { "user.auth.failures", user_auth_failures, Sort::IntegralCounter }

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
                .context(InvalidApiKeySnafu),
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
            user_auth_successes.add(1, &[]);
            next.run(request).await
        }
        Err(Error::NoAuthToken { .. }) => {
            info!("indielinks failed to authenticate this request-- no auth token");
            user_auth_failures.add(1, &[]);
            next.run(request).await
        }
        // I want to be careful about what sort of information we reveal to our caller...
        Err(err) => {
            error!("indielinks failed to authenticate this request: {err:?}");
            user_auth_failures.add(1, &[]);
            err.into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        `/users/signup`                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "user.signups.successful", user_signups_successful, Sort::IntegralCounter }
define_metric! { "user.signups.failures", user_signups_failures, Sort::IntegralCounter }

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
            user_signups_successful.add(1, &[]);
            (StatusCode::CREATED, Json(rsp)).into_response()
        }
        Err(Error::UserSignup { source }) => match source {
            entities::Error::PasswordEntropy { feedback, .. } => {
                info!(
                    "password rejected due to insufficient strength: {}",
                    feedback
                );
                user_signups_failures.add(1, &[]);
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
                user_signups_failures.add(1, &[]);
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
                user_signups_failures.add(1, &[]);
                // Arghhhhh...
                let (status, msg) = UserSignupSnafu.into_error(err).as_status_and_msg();
                (status, Json(ErrorResponseBody { error: msg })).into_response()
            }
        },
        Err(Error::AddUser { source }) => match source {
            storage::Error::UsernameClaimed { username, .. } => {
                info!("Username {} already claimed", username);
                user_signups_failures.add(1, &[]);
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
                user_signups_failures.add(1, &[]);
                // Arghhhhh...
                let (status, msg) = AddUserSnafu.into_error(err).as_status_and_msg();
                (status, Json(ErrorResponseBody { error: msg })).into_response()
            }
        },
        Err(err) => {
            error!("{:#?}", err);
            user_signups_failures.add(1, &[]);
            let (status, msg) = err.as_status_and_msg();
            (status, Json(ErrorResponseBody { error: msg })).into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        `/users/login`                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "user.logins.successful", user_logins_successful, Sort::IntegralCounter }
define_metric! { "user.logins.failures", user_logins_failures, Sort::IntegralCounter }

/// Login as an existing user
///
/// This endpoint will vend a time-limited JWT that can be supplied in the Authorization header
/// (with the bearer scheme) in subsequent requests.
async fn login(
    State(state): State<Arc<Indielinks>>,
    Json(login_req): Json<LoginReq>,
) -> axum::response::Response {
    #[allow(clippy::too_many_arguments)]
    async fn login1(
        storage: &(dyn StorageBackend + Send + Sync),
        peppers: &Peppers,
        token_lifetime: &Duration,
        refresh_token_lifetime: &Duration,
        signing_keys: &SigningKeys,
        origin: &Origin,
        username: &str,
        password: SecretString,
    ) -> Result<(LoginRsp, String, String)> {
        let user = storage
            .user_for_name(username.as_ref())
            .await
            .context(UserSnafu {
                username: username.to_owned(),
            })?
            .context(UnknownUserSnafu {
                username: username.to_owned(),
            })?;
        user.check_password(peppers, password)
            .context(PasswordSnafu {
                username: username.to_owned(),
            })?;

        // It seems unlikely that a bad username could sneak in, but still.
        let username = Username::new(username).context(UsernameSnafu {
            username: username.to_owned(),
        })?;

        let (keyid, signing_key) = signing_keys.current().context(NoKeysSnafu)?;
        let token = mint_token(
            &username,
            &keyid,
            &signing_key,
            origin.host(),
            token_lifetime,
        )
        .context(TokenSnafu {
            username: username.clone(),
        })?;
        let (refresh_token, csrf_token) = mint_refresh_and_csrf_tokens(
            &username,
            &keyid,
            &signing_key,
            origin.host(),
            refresh_token_lifetime,
        )
        .context(RefreshSnafu)?;
        Ok((LoginRsp { token }, refresh_token, csrf_token))
    }

    match login1(
        state.storage.as_ref(),
        &state.pepper,
        &state.token_lifetime,
        &state.refresh_token_lifetime,
        &state.signing_keys,
        &state.origin,
        &login_req.username,
        login_req.password,
    )
    .await
    {
        Ok((rsp, refresh_token, csrf_token)) => {
            info!("Logged-in user {}", login_req.username);

            user_logins_successful.add(
                1,
                &[KeyValue::new("username", login_req.username.to_string())],
            );

            let mut refresh_cookie = format!(
                "{}={}; Max-Age={}; Path=/; HttpOnly; SameSite={}",
                REFRESH_COOKIE,
                refresh_token,
                state.refresh_token_lifetime.num_seconds(),
                state.users_same_site
            );
            let mut csrf_cookie = format!(
                "{}={}; Max-Age={}; Path=/; SameSite={}",
                REFRESH_CSRF_COOKIE,
                csrf_token,
                state.refresh_token_lifetime.num_seconds(),
                state.users_same_site
            );
            if state.users_secure_cookies {
                refresh_cookie += "; Partitioned; Secure";
                csrf_cookie += "; Partitioned; Secure";
            }

            axum::response::Response::builder()
                .status(StatusCode::OK)
                .header(SET_COOKIE, refresh_cookie)
                .header(SET_COOKIE, csrf_cookie)
                .body(axum::body::Body::from(serde_json::to_string(&rsp).unwrap()))
                .unwrap()
        }
        Err(Error::Password { username, .. }) => {
            error!("Bad password for user {}", username);
            user_logins_failures.add(
                1,
                &[KeyValue::new("username", login_req.username.to_string())],
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
            user_logins_failures.add(
                1,
                &[KeyValue::new("username", login_req.username.to_string())],
            );
            let (status, msg) = err.as_status_and_msg();
            (status, Json(ErrorResponseBody { error: msg })).into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       `/users/refresh`                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "user.refreshes.successful", user_refreshes_successful, Sort::IntegralCounter }
define_metric! { "user.refreshes.failures", user_refreshes_failures, Sort::IntegralCounter }

/// Refresh an expired or lost access token vended from `/user/login`
///
/// We expect the refresh token & corresponding CSRF token in the request cookies, and that the CSRF
/// token has been copied to the "x-indielinks-refresh-csrf" header.
async fn refresh(
    State(state): State<Arc<Indielinks>>,
    jar: CookieJar,
    headers: HeaderMap,
) -> axum::response::Response {
    fn refresh1(
        jar: &CookieJar,
        headers: &HeaderMap,
        token_lifetime: &Duration,
        signing_keys: &SigningKeys,
        origin: &Origin,
    ) -> Result<(LoginRsp, Username)> {
        let csrf_header_str = headers
            .get(REFRESH_CSRF_HEADER_NAME)
            .context(MissingHeaderSnafu)?
            .to_str()
            .context(NonUtf8HeaderSnafu)?;
        let refresh_token_str = jar
            .get(REFRESH_COOKIE)
            .context(MissingCookieSnafu)?
            .value_trimmed();
        let csrf_token_str = jar
            .get(REFRESH_CSRF_COOKIE)
            .context(MissingCookieSnafu)?
            .value_trimmed();
        if csrf_token_str != csrf_header_str {
            error!(
                "CSRF token {csrf_token_str} is not equal to the CSRF header {csrf_header_str}!"
            );
            return CsrfMismatchSnafu {
                token: csrf_token_str.to_owned(),
                header: csrf_header_str.to_owned(),
            }
            .fail();
        }
        let (token, username) = refresh_token(
            refresh_token_str,
            csrf_token_str,
            signing_keys,
            origin.host(),
            token_lifetime,
        )
        .context(RefreshSnafu)?;
        Ok((LoginRsp { token }, username))
    }

    match refresh1(
        &jar,
        &headers,
        &state.token_lifetime,
        &state.signing_keys,
        &state.origin,
    ) {
        Ok((rsp, username)) => {
            info!("Successfully refreshed an access token for {username}.");
            user_refreshes_successful.add(1, &[KeyValue::new("username", username.to_string())]);
            (StatusCode::OK, Json(rsp)).into_response()
        }
        Err(err) => {
            error!("{:#?}", err);
            user_refreshes_failures.add(1, &[]);
            let (status, msg) = err.as_status_and_msg();
            (status, Json(ErrorResponseBody { error: msg })).into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        `/users/logout`                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "user.logouts.successful", user_logouts_successful, Sort::IntegralCounter }
define_metric! { "user.logouts.failures", user_logouts_failures, Sort::IntegralCounter }

/// Logout an extant session
///
/// As we store no session state server-side, this isn't a true "logout"; the caller's access token
/// will remain valid until it expires (which shouldn't be long). All this call does is cancel the
/// refresh tokens so the caller won't be able to refresh their access token once it is, in fact,
/// expired.
async fn logout(
    State(state): State<Arc<Indielinks>>,
    user: StdResult<Extension<User>, ExtensionRejection>,
) -> axum::response::Response {
    match user {
        Ok(user) => {
            info!(
                "Logging-out user {}; note that any access tokens remain valid until they expire",
                user.username()
            );
            user_logouts_successful.add(1, &[]);
            let mut refresh_cookie = format!(
                "{}=; Max-Age=0; Path=/; HttpOnly; SameSite={}",
                REFRESH_COOKIE, state.users_same_site
            );
            let mut csrf_cookie = format!(
                "{}=; Max-Age=0; Path=/; SameSite={}",
                REFRESH_CSRF_COOKIE, state.users_same_site
            );
            if state.users_secure_cookies {
                refresh_cookie += "; Partitioned; Secure";
                csrf_cookie += "; Partitioned; Secure";
            }

            axum::response::Response::builder()
                .status(StatusCode::OK)
                .header(SET_COOKIE, refresh_cookie)
                .header(SET_COOKIE, csrf_cookie)
                .body(axum::body::Body::empty())
                .unwrap()
        }
        Err(err) => {
            error!("{err:?}");
            user_logouts_failures.add(1, &[]);
            StatusCode::UNAUTHORIZED.into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        `/users/follow`                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "user.follows.successful", user_follows_successful, Sort::IntegralCounter }
define_metric! { "user.follows.failures", user_follows_failures, Sort::IntegralCounter }

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
                user_follows_successful.add(1, &[]);
                StatusCode::ACCEPTED.into_response()
            }
            Err(err) => {
                user_follows_failures.add(1, &[]);
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
            user_follows_failures.add(1, &[]);
            StatusCode::UNAUTHORIZED.into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       `/users/mint-key`                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "user.mint-key.successful", user_mint_key_successful, Sort::IntegralCounter }
define_metric! { "user.mint-key.failures", user_mint_key_failures, Sort::IntegralCounter }

async fn mint_key(
    State(state): State<Arc<Indielinks>>,
    mut user: StdResult<Extension<User>, ExtensionRejection>,
    req: Option<Json<MintKeyReq>>,
) -> axum::response::Response {
    async fn mint_key1(
        storage: &(dyn StorageBackend + Send + Sync),
        user: &User,
        expiry: Option<DateTime<Utc>>,
    ) -> Result<MintKeyRsp> {
        let (keys, key_text) = user
            .add_key(expiry)
            .context(AddKeySnafu { user: user.clone() })?;
        storage
            .update_user_api_keys(user, &keys)
            .await
            .context(UpdateKeySnafu { user: user.clone() })?;
        Ok(MintKeyRsp { key_text })
    }

    let req = req.map(|req| req.expiry);
    match &mut user {
        Ok(Extension(user)) => match mint_key1(state.storage.as_ref(), user, req).await {
            Ok(rsp) => {
                info!(
                    "Minted an API key expiring {} for user {}",
                    req.map(|dt| format!("at {dt}"))
                        .unwrap_or("never".to_owned()),
                    user.username()
                );
                user_mint_key_successful.add(1, &[]);
                (StatusCode::CREATED, Json(rsp)).into_response()
            }
            Err(err) => {
                user_mint_key_failures.add(1, &[]);
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
            user_mint_key_failures.add(1, &[]);
            StatusCode::UNAUTHORIZED.into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Public API                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

fn mk_cors<H, M, O>(
    allow_credentials: bool,
    allow_headers: H,
    allow_methods: M,
    allow_origin: O,
) -> CorsLayer
where
    H: Into<AllowHeaders>,
    M: Into<AllowMethods>,
    O: Into<AllowOrigin>,
{
    CorsLayer::new()
        .allow_credentials(allow_credentials)
        .allow_private_network(true)
        .allow_headers(allow_headers)
        .allow_methods(allow_methods)
        .allow_origin(allow_origin)
}

/// Return a router for the User API
///
/// The returned [Router] will presumably be merged with other routres.
pub fn make_router(state: Arc<Indielinks>) -> Router<Arc<Indielinks>> {
    let allow_headers = [
        CONTENT_TYPE,
        http::header::USER_AGENT,
        http::header::REFERER,
    ];
    let allow_origin = state
        .allowed_origins
        .iter()
        .map(|o| o.to_string().parse().unwrap())
        .collect::<Vec<HeaderValue>>();
    Router::new()
        // I suppose it would be more "RESTful" to have a `user/users` resource and have callers
        // perform this action by POSTing to it (they could retrieve a user via GETting
        // `user/users/:username`), but that model doesn't really map to the set of things one can
        // do via this API (how would we model minting a new API key, for instance? Or loggig-in?)
        .route(
            "/users/signup",
            post(signup)
                // It might be nice to allow people to sign-up programmatically, but I think for now
                // I'm giong to restrict this to the front end
                .layer(mk_cors(
                    false,
                    allow_headers.clone(),
                    http::Method::POST,
                    allow_origin.clone(),
                )),
        )
        .route(
            "/users/login",
            get(login).merge(post(login)).layer(mk_cors(
                true,
                allow_headers.clone(),
                [http::Method::GET, http::Method::POST], // Do I really need to support `GET`?
                allow_origin.clone(),
            )),
        )
        .route(
            "/users/refresh",
            post(refresh).layer(mk_cors(
                true,
                [
                    CONTENT_TYPE,
                    http::header::USER_AGENT,
                    http::header::REFERER,
                    http::header::HeaderName::from_static(REFRESH_CSRF_HEADER_NAME_LC),
                ],
                http::Method::POST,
                allow_origin.clone(),
            )),
        )
        .route(
            "/users/logout",
            post(logout).layer(mk_cors(
                true,
                [
                    CONTENT_TYPE,
                    http::header::USER_AGENT,
                    http::header::REFERER,
                    http::header::AUTHORIZATION,
                    http::header::HeaderName::from_static(REFRESH_CSRF_HEADER_NAME_LC),
                ],
                http::Method::POST,
                allow_origin.clone(),
            )),
        )
        .route(
            "/users/follow",
            post(follow)
                // Likewise, for now I think I'll limit this to the front end
                .layer(mk_cors(
                    true,
                    allow_headers.clone(),
                    http::Method::POST,
                    allow_origin.clone(),
                )),
        )
        .route(
            "/users/mint-key",
            get(mint_key)
                // Likewise, for now I think I'll limit this to the front end
                .layer(mk_cors(
                    true,
                    allow_headers.clone(),
                    http::Method::GET,
                    allow_origin.clone(),
                )),
        )
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
        // .layer(CorsLayer::permissive())
        .with_state(state)
}
