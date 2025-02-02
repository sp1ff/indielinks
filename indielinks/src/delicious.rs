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

//! # del.icio.us
//!
//! Replicating the del.icio.us API
//!
//! # Introduction
//!
//! My aim here is to replicate, more or less, the del.icio.us and Pinboard APIs.
//!
//! At the moment, I am *not* supporting XML responses because 1) I'm trying to get a minimum viable
//! project put together and 2) I despise XML; it was a misconceived serialization format that
//! managed to combine the worst aspects of human- and machine-readable formats when it was
//! introduced back in the nineties & I'll be damned if I contribute to keeping it alive twenty-some
//! years later.
//!
//! I've chosen to have all the handlers just return an [axum::response::Response] so that I can use
//! different structures to represent responses. This has resulted in a little more boilerplate.

use crate::{
    counter_add,
    entities::{self, Post, PostDay, PostUri, TagId, Tagname, User, UserApiKey, Username},
    http::{ErrorResponseBody, Indielinks},
    metrics::{self, Sort},
    storage::{self, Backend as StorageBackend},
    util::exactly_two,
};

use axum::{
    extract::{Json, State},
    http::{header::CONTENT_TYPE, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use axum_extra::extract::Query;
use base64::prelude::*;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tap::Pipe;
use tower_http::{cors::CorsLayer, set_header::SetResponseHeaderLayer};
use tracing::{debug, error};

use std::{
    backtrace::Backtrace,
    cmp::Ordering,
    collections::{HashMap, HashSet},
    str::FromStr,
    string::FromUtf8Error,
    sync::Arc,
};

/// del.icio.us module error type
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("/posts/add failed: {source}"))]
    AddPost {
        source: crate::storage::Error,
        backtrace: Backtrace,
    },
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
    #[snafu(display("Failed to decode base64 field: {source}"))]
    BadBase64Encoding {
        text: String,
        source: base64::DecodeError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to fetch tag cloud for user {username}: {source}"))]
    BadTagCloud {
        username: Username,
        source: storage::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Bad tag name: {source}"))]
    BadTagName {
        source: entities::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("{username} is not a valid username"))]
    BadUsername {
        username: String,
        source: crate::entities::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to delete posts {posts:?}: {source}"))]
    DeletePosts {
        posts: Vec<Post>,
        source: crate::storage::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to get posts from backend: {source}"))]
    GetPosts {
        source: crate::storage::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("An Authorization header had a non-textual value: {source}"))]
    InvalidAuthHeaderValue {
        value: HeaderValue,
        source: axum::http::header::ToStrError,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid API key"))]
    InvalidApiKey { key: UserApiKey },
    #[snafu(display("Failed to find a colon in '{text}'"))]
    MissingColon { text: String, backtrace: Backtrace },
    #[snafu(display("No query parameters: this method has required query parameters"))]
    MissingQueryParams { backtrace: Backtrace },
    #[snafu(display("Multiple Authorization headers were supplied; only one is accepted."))]
    MultipleAuthnHeaders,
    #[snafu(display("No authorization token found in the query string"))]
    NoAuthToken { backtrace: Backtrace },
    #[snafu(display("User {username} has no posts, yet"))]
    NoPosts {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("The text was not valid UTF-8"))]
    NotUtf8 {
        source: FromUtf8Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to fetch posts by URI {uri}: {source}"))]
    PostByUri {
        uri: PostUri,
        source: storage::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to fetch the tag cloud for {uri}: {source}"))]
    TagCloudForUri {
        uri: PostUri,
        source: storage::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Request to {path} unauthorized"))]
    Unauthorized { path: String, backtrace: Backtrace },
    #[snafu(display("Unknown username {username}"))]
    UnknownUser { username: Username },
    #[snafu(display("Authorization scheme {scheme} not supported"))]
    UnsupportedAuthScheme {
        scheme: String,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to update tag cloud {tags:?}: {source}"))]
    UpdateTagCloudAdd {
        tags: HashSet<Tagname>,
        source: storage::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to update tag cloud {tags:?} on delete: {source}"))]
    UpdateTagCloudDel {
        tags: HashMap<TagId, usize>,
        source: storage::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to update the user's post times"))]
    UpdateUserPostTimes {
        source: storage::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to lookup user {username}: {source}"))]
    User {
        username: Username,
        source: crate::storage::Error,
    },
}

impl Error {
    pub fn as_status_and_msg(&self) -> (StatusCode, String) {
        match self {
            Error::NoPosts { username, .. } => {
                (StatusCode::OK, format!("{} has no posts, yet", username))
            }
            ////////////////////////////////////////////////////////////////////////////////////////
            // Broken requests-- tell the caller how to fix it
            ////////////////////////////////////////////////////////////////////////////////////////
            Error::BadAuthHeaderParse { value, .. } => (
                StatusCode::BAD_REQUEST,
                format!("Bad Authorization header: {:?}", value),
            ),
            Error::BadBase64Encoding { text, source, .. } => (
                StatusCode::BAD_REQUEST,
                format!("Bad base64 encoding {}: {}", text, source),
            ),
            Error::BadTagName { .. } => (
                StatusCode::BAD_REQUEST,
                "Tag names may be up to 255 characters, and may not contain whitespace or commas"
                    .to_string(),
            ),
            Error::InvalidAuthHeaderValue { value, source, .. } => (
                StatusCode::BAD_REQUEST,
                format!("Bad Authorization header {:?}: {}", value, source),
            ),
            Error::MissingColon { text, .. } => (
                StatusCode::BAD_REQUEST,
                format!("Missing colon in {}", text),
            ),
            Error::MissingQueryParams { .. } => {
                (StatusCode::BAD_REQUEST, "No query parameters".to_string())
            }
            Error::MultipleAuthnHeaders => (
                StatusCode::BAD_REQUEST,
                "Multiple authorization headers".to_string(),
            ),
            Error::NotUtf8 { source, .. } => (
                StatusCode::BAD_REQUEST,
                format!("Bad UTF-8 encoding: {:?}", source),
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
            Error::Unauthorized { .. } => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            Error::UnknownUser { .. } => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            Error::UnsupportedAuthScheme { .. } => {
                (StatusCode::UNAUTHORIZED, "Unauthorized".to_string())
            }
            ////////////////////////////////////////////////////////////////////////////////////////
            // Internal failure-- own up to it:
            ////////////////////////////////////////////////////////////////////////////////////////
            Error::AddPost { source, .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to add post: {}", source),
            ),
            Error::BadTagCloud {
                username, source, ..
            } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to lookup user {}'s tag cloud: {}", username, source),
            ),
            Error::DeletePosts { posts, source, .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to delete posts {:?}: {}", posts, source),
            ),
            Error::GetPosts { source, .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch posts: {}", source),
            ),
            Error::PostByUri { uri, source, .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch posts by {}: {}", uri, source),
            ),
            Error::TagCloudForUri { uri, source, .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch the tag cloud for {}: {}", uri, source),
            ),
            Error::UpdateTagCloudAdd { .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to update tag cloud on post add".to_string(),
            ),
            Error::UpdateTagCloudDel { .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to update tag cloud on post delete".to_string(),
            ),
            Error::UpdateUserPostTimes { .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to update user post times".to_string(),
            ),
            Error::User { username, source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "Internal server error looking-up user {}: {:?}",
                    username, source
                ),
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
/// I loathe putting key material on the wire (let alone passwords), but for legacy reasons we
/// support both HTTP "basic" authentication (i.e. username & password) as well as "bearer" (i.e.
/// API key). I'd love to move to something like request signing or true (OAuth) bearer tokens.
#[derive(Clone, Debug)]
enum AuthnScheme {
    Bearer((Username, UserApiKey)),
    Basic((Username, SecretString)),
}

impl AuthnScheme {
    /// Create an AuthnScheme instance from the base64 encoding of "username:password"
    fn from_basic(payload: &str) -> Result<AuthnScheme> {
        let (username, password) = BASE64_STANDARD
            .decode(payload)
            .context(BadBase64EncodingSnafu {
                text: payload.to_owned(),
            })?
            .pipe(String::from_utf8)
            .context(NotUtf8Snafu)?
            .split_once(':')
            .context(MissingColonSnafu {
                text: payload.to_string(),
            })?
            .pipe(|(u, p)| (u.to_string(), p.to_string()));

        Ok(AuthnScheme::Basic((
            Username::from_str(&username).context(BadUsernameSnafu {
                username: username.to_owned(),
            })?,
            password.into(),
        )))
    }
    /// Create an AuthnScheme instance from the the plain text "username:key-in-hex"
    fn from_bearer(payload: &str) -> Result<AuthnScheme> {
        // `payload` should be the plain text "username:key-in-hex"
        let (username, key) = payload.split_once(':').context(MissingColonSnafu {
            text: payload.to_string(),
        })?;
        Ok(AuthnScheme::Bearer((
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
            "basic" => AuthnScheme::from_basic(payload),
            "bearer" => AuthnScheme::from_bearer(payload),
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
            (AuthnScheme::Bearer((username1, key1)), AuthnScheme::Bearer((username2, key2))) => {
                username1 == username2 && key1 == key2
            }
            (
                AuthnScheme::Basic((username1, password1)),
                AuthnScheme::Basic((username2, password2)),
            ) => username1 == username2 && password1.expose_secret() == password2.expose_secret(),
            (_, _) => false,
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_authn_scheme_try_from() {
        let x = AuthnScheme::try_from(
            &HeaderValue::from_str("Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==").unwrap(/* known good */),
        );
        assert!(x.is_ok());
        assert!(
            x.unwrap()
                == AuthnScheme::Basic((
                    Username::from_str("Aladdin").unwrap(),
                    String::from("open sesame").into()
                ))
        );

        let x = AuthnScheme::try_from(
            &HeaderValue::from_str("Bearer sp1ff:010203").unwrap(/* known good */),
        );
        assert!(x.is_ok());
        assert!(
            x.unwrap()
                == AuthnScheme::Bearer((
                    Username::from_str("sp1ff").unwrap(/* known good */),
                    vec![1, 2, 3].into()
                ))
        );
    }
}

inventory::submit! { metrics::Registration::new("delicious.auth.successes", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("delicious.auth.failures", Sort::IntegralCounter) }

/// Authenticate a request to the del.icio.us API
///
/// # Introduction
///
/// The original del.icio.us API used HTTP basic authentication. The pinboard API does as well, but
/// also allows the use (preferrable, in my mind) of API keys. On pinboard, the API key goes in the
/// query string.
///
/// Generally, I prefer the use of signed, limited duration bearer tokens or request signing, but
/// I'm going to remain backward compatible with both but also accept the API token in the
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
// I made an attempt at using tower-http's `AsyncAuthorizeRequest`, but was unable to do anything
// useful with it so long as the request body was axum::body::Body because it's not Sync. Perhaps I
// missed something (it's sparsely documented), but I fail to see what it offers above & beyond
// axum::middleware::from_fn.
async fn authenticate(
    State(state): State<Arc<Indielinks>>,
    Query(params): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    // Use a nested function returning a `Result` so I can use the `?` sigil, Snafu's `ResultExt` &
    // `OptionExt` and generally write idiomatically; then have the outer implementation handle
    // converting that to an axum Response.
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
            None => {
                AuthnScheme::from_bearer(params.get("auth_token").ok_or(NoAuthTokenSnafu.build())?)?
            }
        };

        // Another method?
        match scheme {
            AuthnScheme::Bearer((username, key)) => {
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
            AuthnScheme::Basic((_username, _password)) => {
                unimplemented!("Password authentication is not (yet) implemented.")
            }
        }
    }

    match authenticate1(headers, params, state.storage.as_ref()).await {
        Ok(user) => {
            debug!("indielinks authorized user {}", user.id());
            request.extensions_mut().insert(user);
            counter_add!(state.instruments, "delicious.auth.successes", 1, &[]);
            next.run(request).await
        }
        // I want to be careful about what sort of information we reveal to our caller...
        Err(err) => {
            error!("indielinks failed to authenticate: {:?}", err);
            counter_add!(state.instruments, "delicious.auth.failures", 1, &[]);
            err.into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                     del.icio.us utilities                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GenericRsp {
    pub result_code: String,
}

/// Retrieve the authenticated [User] from the current request
///
/// All requests to the del.icio.us interface should be authenticated via middleware that attaches a
/// [User] instance to the incoming request. This method will retrieve a reference to that [User].
///
/// I'm not happy with this approach, since it depends on each handler invoking this method to
/// ensure that the request has been authenticated. I mean, at the [Router] level I attach the
/// salient middleware, which will reject any unauthenticated request, but still: I wish it were
/// possible to write the handlers in such a way as to reject any unauthenticated request. That
/// said, it would be hard to implement a handler for any endpoint in this interface *without*
/// knowing the user.
fn user_for_request<'a>(request: &'a axum::extract::Request, pth: &str) -> Result<&'a User> {
    request
        .extensions()
        .get::<User>()
        .context(UnauthorizedSnafu {
            path: pth.to_string(),
        })
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        `/posts/update`                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("delicious.updates", Sort::IntegralCounter) }

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UpdateRsp {
    pub update_time: DateTime<Utc>,
}

async fn update(
    State(state): State<Arc<Indielinks>>,
    request: axum::extract::Request,
) -> axum::response::Response {
    async fn update1(request: axum::extract::Request) -> Result<UpdateRsp> {
        let user = user_for_request(&request, "/posts/update")?;
        let update_time = user.last_update().context(NoPostsSnafu {
            username: user.username(),
        })?;

        Ok(UpdateRsp { update_time })
    }

    match update1(request).await {
        Ok(rsp) => {
            counter_add!(state.instruments, "delicious.updates", 1, &[]);
            (StatusCode::OK, Json(rsp)).into_response()
        }
        Err(err) => {
            if !matches!(err, Error::NoPosts { .. }) {
                error!("{:#?}", err)
            };
            let (status, msg) = err.as_status_and_msg();
            (status, Json(GenericRsp { result_code: msg })).into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          `/posts/add`                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("delicious.posts.added", Sort::IntegralCounter) }

/// A deserializable struct representing the query parameters for `/posts/add`
#[derive(Clone, Debug, Deserialize)]
struct PostAddReq {
    url: PostUri,
    #[serde(rename = "description")]
    title: String,
    #[serde(rename = "extended")]
    notes: Option<String>,
    tags: Option<String>,
    dt: Option<DateTime<Utc>>,
    replace: Option<bool>,
    shared: Option<bool>,
    #[serde(rename = "toread")]
    to_read: Option<bool>,
}

/// `/posts/add` handler
///
/// Add a post to a user's collection. Query parameters:
///
/// - url (required)
/// - description (required): this field would more appropriately be named "title", but
///   "description" is used for backward compatibility
/// - extended: notes (again, "extended" is not particularly descriptive, but is
///   backward compatible)
/// - tags: a list of comma-delimited tags
/// - dt: creation time to be used for this post; if omitted the current time will be used
/// - replace
/// - shared
/// - toread
///
/// Update the `first_update` & `last_update` fields for this user on success.
async fn add_post(
    State(state): State<Arc<Indielinks>>,
    Query(post_add_req): Query<PostAddReq>,
    request: axum::extract::Request,
) -> axum::response::Response {
    // Continuing to experiment with this idiom... hoist this out into it's own function?
    async fn add_post1(
        req: PostAddReq,
        request: axum::extract::Request,
        storage: &(dyn StorageBackend + Send + Sync),
    ) -> Result<bool> {
        // I'm torn as to how to handle this; given the API offered by axum, there's no way to
        // enforce this at compile-time.
        let user = user_for_request(&request, "/posts/add")?;
        let tags = req
            .tags
            .map(|tags| {
                tags.split(',')
                    .map(|s| Tagname::new(s.trim()))
                    .collect::<StdResult<HashSet<Tagname>, _>>()
            })
            .transpose()
            .context(BadTagNameSnafu)?;
        // I would have thought `futures` would have offered something here.
        let tags = match tags {
            Some(tags) => storage
                .update_tag_cloud_on_add(&user.id(), &tags)
                .await
                .context(UpdateTagCloudAddSnafu { tags })?,
            None => HashSet::new(),
        };
        let dt = req.dt.unwrap_or(Utc::now());
        let added = storage
            .add_post(
                user,
                // Question: should we resolve defaults here, or in the storage backend?
                req.replace.unwrap_or(true),
                &req.url,
                &req.title,
                &dt,
                &req.notes,
                req.shared.unwrap_or(false),
                req.to_read.unwrap_or(false),
                &tags,
            )
            .await
            .context(AddPostSnafu)?;
        if added {
            storage
                .update_user_post_times(user, &dt)
                .await
                .context(UpdateUserPostTimesSnafu)?;
        }
        Ok(added)
    }

    debug!("Add post: {:?}", post_add_req);

    // The Pinboard API seems to just return status code 200 OK no matter what-- the caller ha to
    // examine the textual `result_code` in the response body to know if the request succeeded (see
    // <https://gist.github.com/takashi/2967f9c5ec8ebab5f622#file-pydelicious-py-L117>, e.g.) This
    // seems unfortunate, so for now at least, I'm going to break backwards compatibility & actually
    // return an HTTP status code suitable to the result.
    let (status_code, status) = match add_post1(post_add_req, request, state.storage.as_ref()).await
    {
        Ok(true) => {
            counter_add!(state.instruments, "delicious.posts.added", 1, &[]);
            (StatusCode::CREATED, "done".to_string())
        }
        Ok(false) => {
            counter_add!(state.instruments, "delicious.posts.added", 1, &[]);
            (StatusCode::OK, "done".to_string())
        }
        Err(err) => {
            error!("{:#?}", err);
            err.as_status_and_msg()
        }
    };
    (
        status_code,
        Json(GenericRsp {
            result_code: status,
        }),
    )
        .into_response()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         `posts/delete`                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("delicious.posts.deleted", Sort::IntegralCounter) }

#[derive(Clone, Debug, Deserialize)]
struct PostsDeleteReq {
    url: PostUri,
}

async fn delete_post(
    State(state): State<Arc<Indielinks>>,
    Query(post_delete_req): Query<PostsDeleteReq>,
    request: axum::extract::Request,
) -> axum::response::Response {
    async fn delete_post1(
        storage: &(dyn StorageBackend + Send + Sync),
        uri: PostUri,
        request: axum::extract::Request,
    ) -> Result<bool> {
        let user = user_for_request(&request, "/posts/delete")?;
        let posts_to_be_deleted = storage
            .get_posts_by_uri(&user.id(), &uri)
            .await
            .context(PostByUriSnafu { uri: uri.clone() })?;
        debug!("Posts to be deleted: {:?}", posts_to_be_deleted);
        if posts_to_be_deleted.is_empty() {
            return Ok(false);
        }
        let post_use_map = storage
            .get_tag_cloud_for_uri(&user.id(), &uri)
            .await
            .context(TagCloudForUriSnafu { uri: uri.clone() })?;
        debug!("Tags used by these posts: {:?}", post_use_map);
        storage
            .update_tag_cloud_on_delete(&user.id(), &post_use_map)
            .await
            .context(UpdateTagCloudDelSnafu { tags: post_use_map })?;
        debug!("Tag cloud updated");
        storage
            .delete_posts(&posts_to_be_deleted)
            .await
            .context(DeletePostsSnafu {
                posts: posts_to_be_deleted,
            })?;
        debug!("Posts deleted!");
        Ok(true)
    }

    debug!("Entered handler!");
    let (status_code, status) =
        match delete_post1(state.storage.as_ref(), post_delete_req.url, request).await {
            Ok(true) => {
                counter_add!(state.instruments, "delicious.posts.deleted", 1, &[]);
                (StatusCode::OK, "done".to_string())
            }
            Ok(false) => {
                counter_add!(state.instruments, "delicious.posts.deleted", 1, &[]);
                (StatusCode::OK, "item not found".to_string())
            }
            Err(err) => {
                error!("{:#?}", err);
                err.as_status_and_msg()
            }
        };
    (
        status_code,
        Json(GenericRsp {
            result_code: status,
        }),
    )
        .into_response()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          `/posts/get`                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("delicious.posts.retrieved", Sort::IntegralCounter) }

#[derive(Clone, Debug, Deserialize)]
struct PostsGetReq {
    dt: Option<DateTime<Utc>>,
    #[serde(rename = "url")]
    _url: Option<PostUri>,
    #[serde(default, rename = "tag")]
    _tags: Vec<String>,
    #[serde(rename = "meta")]
    _meta: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PostsGetRsp {
    pub date: DateTime<Utc>,
    pub user: Username,
    pub posts: Vec<Post>,
}

/// `/posts/get` handler
///
/// Retrieve a single day's posts. They can be filtered in a few ways-- query parameters:
///
/// - tag: filter by up to three tags
/// - dt: specify the day for which bookmarks shall be returned; if omitted, the date of the
///   most recent bookmark will be used
/// - url: return the post for this URL
/// - meta: "yes" or "no"; if the former, include a change detection attribute (on which more below)
///
/// RE the "meta" attribute, from the original del.icio.us [docs]: "Clients wishing to maintain a
/// synchronized local store of bookmarks should retain the value of this attribute - its value will
/// change when any significant field of the bookmark changes."
///
/// [docs]: https://web.archive.org/web/20080908014047/http://delicious.com/help/api
///
/// I can't quite figure-out the semantics of the `dt` parameter. Both the del.icio.us and Pinboard
/// docs clearly state that if not supplied, it defaults to "the most recent date on which bookmarks
/// were saved". Yet, when I make a call to the Pinboard API without this parameter, I get a
/// response containing the timestamp of my most recent post, but *no posts*. I'm reluctantly
/// replicating that behavior for the sake of backward compaitibility.
async fn get_posts(
    State(state): State<Arc<Indielinks>>,
    Query(post_get_req): Query<PostsGetReq>,
    request: axum::extract::Request,
) -> axum::response::Response {
    // Still not sure about hoisting this out into its own function:
    async fn get_posts1(
        storage: &(dyn StorageBackend + Send + Sync),
        posts_get_req: PostsGetReq,
        request: axum::extract::Request,
    ) -> Result<PostsGetRsp> {
        let user = user_for_request(&request, "/posts/get")?;
        let last_dt = user.last_update().ok_or(
            NoPostsSnafu {
                username: user.username(),
            }
            .build(),
        )?;

        fn sort_posts_dec(lhs: &Post, rhs: &Post) -> Ordering {
            lhs.posted().cmp(&rhs.posted())
        }

        match posts_get_req.dt {
            None => Ok(PostsGetRsp {
                date: last_dt,
                user: user.username(),
                posts: Vec::new(),
            }),
            Some(dt) => {
                let mut posts = storage
                    .get_posts_by_day(
                        &user.id(),
                        &PostDay::new(&format!("{}", dt.format("%Y-%m-%d"))).unwrap(),
                    )
                    .await
                    .context(GetPostsSnafu)?;
                posts.sort_unstable_by(sort_posts_dec);
                Ok(PostsGetRsp {
                    date: dt,
                    user: user.username(),
                    posts,
                })
            }
        }
    }

    match get_posts1(state.storage.as_ref(), post_get_req, request).await {
        Ok(rsp) => {
            counter_add!(
                state.instruments,
                "delicious.posts.retrieved",
                rsp.posts.len() as u64,
                &[]
            );
            (StatusCode::OK, Json(rsp)).into_response()
        }
        Err(err) => {
            error!("{:#?}", err);
            let (status, msg) = err.as_status_and_msg();
            (status, Json(GenericRsp { result_code: msg })).into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           `tags/get`                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Serialize)]
#[serde(transparent)]
pub struct TagsGetRsp {
    pub map: HashMap<Tagname, usize>,
}

/// `tags/get` handler
///
/// Retrieve a complete list of the user's tags along with their use counts.
async fn tags_get(
    State(state): State<Arc<Indielinks>>,
    request: axum::extract::Request,
) -> axum::response::Response {
    async fn tags_get1(
        storage: &(dyn StorageBackend + Send + Sync),
        request: axum::extract::Request,
    ) -> Result<TagsGetRsp> {
        let user = user_for_request(&request, "/tags/get")?;
        Ok(TagsGetRsp {
            map: storage
                .get_tag_cloud(user)
                .await
                .context(BadTagCloudSnafu {
                    username: user.username(),
                })?,
        })
    }

    match tags_get1(state.storage.as_ref(), request).await {
        Ok(rsp) => (StatusCode::OK, Json(rsp)).into_response(),
        Err(err) => {
            error!("{:#?}", err);
            let (status, msg) = err.as_status_and_msg();
            (status, Json(GenericRsp { result_code: msg })).into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Public API                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Return a Router for the del.icio.us API
///
/// The returned router can be composed with other routers.
pub fn make_router(state: Arc<Indielinks>) -> Router<Arc<Indielinks>> {
    Router::new()
        // The del.icio.us & Pinboard APIs use the GET verb for "add", which seems odd. I'll preserve that for
        // compatibility, but also support the more idiomatic POST.
        .route("/posts/update", get(update))
        .route("/posts/add", get(add_post).merge(post(add_post)))
        .route("/posts/get", get(get_posts))
        // Decided not use `DELETE` since we're not addressing the resource being deleted, but again
        // added POST since it seems more appropriate.
        .route("/posts/delete", get(delete_post).merge(post(delete_post)))
        .route("/tags/get", get(tags_get))
        // Not sure if I should push this up the stack; as is, if a request is not authorized, the CORS
        // & Content-Ty1pe headers would be added already.
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
