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
    activity_pub::SendCreate,
    authn::{self, check_api_key, check_password, check_token, AuthnScheme},
    background_tasks::{BackgroundTasks, Sender},
    counter_add,
    entities::{self, Post, PostDay, PostId, PostUri, Tagname, User, UserApiKey, Username},
    http::{ErrorResponseBody, Indielinks},
    metrics::{self, Sort},
    origin::Origin,
    peppers::Peppers,
    signing_keys::SigningKeys,
    storage::{self, Backend as StorageBackend, DateRange},
    util::UpToThree,
};

use axum::{
    extract::{Json, State},
    http::{header::CONTENT_TYPE, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use axum_extra::extract::Query;
use chrono::{DateTime, NaiveDate, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tap::Pipe;
use tower_http::{cors::CorsLayer, set_header::SetResponseHeaderLayer};
use tracing::{debug, error};

use std::{
    backtrace::Backtrace,
    collections::{HashMap, HashSet},
    string::FromUtf8Error,
    sync::Arc,
};

/// del.icio.us module error type
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("/posts/all failed; {source}"))]
    AllPosts { source: storage::Error },
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
    #[snafu(display("Failed to delete post {uri}: {source}"))]
    DeletePosts {
        uri: PostUri,
        source: crate::storage::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to delete tag {tag}: {source}"))]
    DeleteTag {
        tag: Tagname,
        #[snafu(source(from(storage::Error, Box::new)))]
        source: Box<storage::Error>,
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
        source: authn::Error,
    },
    #[snafu(display("Invalid credentials: {source}"))]
    InvalidCredentials { source: authn::Error },
    #[snafu(display("Invalid API key"))]
    InvalidApiKey { key: UserApiKey },
    #[snafu(display("The token {value} couldn't be interpreted as an API key: {source}"))]
    InvalidQueryToken { value: String, source: authn::Error },
    #[snafu(display("Failed to find a colon in '{text}'"))]
    MissingColon { text: String, backtrace: Backtrace },
    #[snafu(display("No query parameters: this method has required query parameters"))]
    MissingQueryParams { backtrace: Backtrace },
    #[snafu(display("Multiple Authorization headers were supplied; only one is accepted."))]
    MultipleAuthnHeaders,
    #[snafu(display("No authorization token found in the query string"))]
    NoAuthToken { backtrace: Backtrace },
    #[snafu(display("No more than three tags may be given"))]
    NoMoreThanThreeTags {
        source: crate::util::NoMoreThanThree,
        backtrace: Backtrace,
    },
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
    #[snafu(display("posts by day failed: {source}"))]
    PostsByDay {
        source: storage::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to fetch posts by URI {uri}: {source}"))]
    PostByUri {
        uri: PostUri,
        source: storage::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to fetch recent posts; {source}"))]
    RecentPosts { source: storage::Error },
    #[snafu(display("Failed to rename {old} to {new}: {source}"))]
    RenameTag {
        old: Tagname,
        new: Tagname,
        #[snafu(source(from(storage::Error, Box::new)))]
        source: Box<storage::Error>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to send the Create activity for Post {postid}: {source}"))]
    SendCreate {
        postid: PostId,
        source: crate::background_tasks::Error,
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
    #[snafu(display("Failed to update the user's post counts: {source}"))]
    UpdateUserPostCounts {
        source: storage::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to update the user's post times: {source}"))]
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
            Error::InvalidQueryToken { value, source, .. } => (
                StatusCode::BAD_REQUEST,
                format!("Bad Authorization query token {}: {}", value, source),
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
            Error::NoMoreThanThreeTags { .. } => (
                StatusCode::BAD_REQUEST,
                "No more than three tags may be specified".to_string(),
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
            Error::InvalidCredentials { .. } => {
                (StatusCode::UNAUTHORIZED, "Unauthorized".to_string())
            }
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
            Error::AllPosts { source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("/posts/all failed: {source}"),
            ),
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
            Error::DeletePosts { uri, source, .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to delete post {:?}: {}", uri, source),
            ),
            Error::DeleteTag { tag, source, .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to delete {}: {}", tag, source),
            ),
            Error::GetPosts { source, .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch posts: {}", source),
            ),
            Error::PostsByDay { source, .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch posts by day: {source}"),
            ),
            Error::PostByUri { uri, source, .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch posts by {}: {}", uri, source),
            ),
            Error::TagCloudForUri { uri, source, .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch the tag cloud for {}: {}", uri, source),
            ),
            Error::RecentPosts { source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch recent posts: {}", source),
            ),
            Error::RenameTag {
                old, new, source, ..
            } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to rename {} to {}: {}", old, new, source),
            ),
            Error::SendCreate { postid: _, source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to schedule send of Create activity: {}", source),
            ),
            Error::UpdateUserPostTimes { .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to update user post times".to_string(),
            ),
            Error::UpdateUserPostCounts { .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to update user post counts".to_string(),
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
        peppers: &Peppers,
        keys: &SigningKeys,
        origin: &Origin,
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
            Some(header_val) => AuthnScheme::try_from(header_val)
                .context(InvalidAuthHeaderValueSnafu { value: header_val })?,
            None => match params.get("auth_token") {
                Some(value) => {
                    AuthnScheme::from_api_key(value).context(InvalidQueryTokenSnafu { value })?
                }
                None => {
                    return NoAuthTokenSnafu.fail();
                }
            },
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
        params,
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
            counter_add!(state.instruments, "delicious.auth.successes", 1, &[]);
            next.run(request).await
        }
        // I want to be careful about what sort of information we reveal to our caller...
        Err(err) => {
            error!("indielinks failed to authenticate this request");
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

/// Parse a `tag` parameter into a [HashSet] of [Tagname]s
fn parse_tag_parameter(tags: &Option<String>) -> Result<HashSet<Tagname>> {
    tags.as_ref()
        .map(|tags| {
            tags.split(',')
                .map(|s| Tagname::new(s.trim()))
                .collect::<StdResult<HashSet<Tagname>, _>>()
        })
        .transpose()
        .context(BadTagNameSnafu)?
        .unwrap_or(HashSet::new())
        .pipe(Ok)
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
            username: user.username().clone(),
        })?;

        Ok(UpdateRsp {
            update_time: *update_time,
        })
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
/// - replace: if false and `url` has already been posted, this method will fail
/// - shared
/// - toread
///
/// Update the `first_update` & `last_update` fields for this user on success.
async fn add_post(
    State(state): State<Arc<Indielinks>>,
    Query(post_add_req): Query<PostAddReq>,
    request: axum::extract::Request,
) -> axum::response::Response {
    async fn add_post1(
        req: PostAddReq,
        request: axum::extract::Request,
        storage: &(dyn StorageBackend + Send + Sync),
        sender: &Arc<BackgroundTasks>,
    ) -> Result<bool> {
        // I'm torn as to how to handle this; given the API offered by axum, there's no way to
        // enforce this at compile-time. OTOH, it's tough to do anything in this API *without* the
        // current user, so I don't see how I can forget this:
        let user = user_for_request(&request, "/posts/add")?;
        // Pull the `tag` parameter out of the request, separate the individual tags by comma, and
        // check that they're all legit `Tagname`s:
        let tags = parse_tag_parameter(&req.tags)?;
        // Figure-out the post time:
        let dt = req.dt.unwrap_or(Utc::now());
        // Gin-up a new `PostId`, in case this is a new post:
        let postid = PostId::default();
        let shared = req.shared.unwrap_or(false);
        let added = storage
            .add_post(
                user,
                // Question: should we resolve defaults here, or in the storage backend?
                req.replace.unwrap_or(true),
                &req.url,
                &postid,
                &req.title,
                &dt,
                &req.notes,
                shared,
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
            if shared {
                debug!(
                    "Scheduling Post {} for communication to all federated servers",
                    postid
                );
                sender
                    .send(SendCreate::new(&postid, &dt))
                    .await
                    .context(SendCreateSnafu { postid })?;
            }
        }
        Ok(added)
    }

    // The Pinboard API seems to just return status code 200 OK no matter what-- the caller has to
    // examine the textual `result_code` in the response body to know if the request succeeded (see
    // <https://gist.github.com/takashi/2967f9c5ec8ebab5f622#file-pydelicious-py-L117>, e.g.) This
    // seems unfortunate, so for now at least, I'm going to break backwards compatibility & actually
    // return an HTTP status code suitable to the result.
    let (status_code, status) = match add_post1(
        post_add_req,
        request,
        state.storage.as_ref(),
        &state.task_sender,
    )
    .await
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
        let deleted = storage
            .delete_post(user, &uri)
            .await
            .context(DeletePostsSnafu { uri })?;
        if deleted {
            storage
                .update_user_post_times(user, &Utc::now())
                .await
                .context(UpdateUserPostTimesSnafu)?;
        }
        Ok(deleted)
    }

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
    dt: Option<NaiveDate>,
    #[serde(rename = "url")]
    uri: Option<PostUri>,
    #[serde(default, rename = "tag")]
    tags: Option<String>,
    #[serde(rename = "meta")]
    _meta: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PostsGetRsp {
    pub date: DateTime<Utc>,
    pub user: Username,
    pub posts: Vec<Post>,
}

/// `/posts/get` handler
///
/// Retrieve a single day's posts. They can be filtered in a few ways-- query parameters:
///
/// - tag: filter by up to three tags; multiple tags are treated as a conjunction, not a disjunction
/// - dt: specify the day for which bookmarks shall be returned; if omitted, the date of the
///   most recent bookmark will be used
/// - url: return the post for this URL
/// - meta: "yes" or "no"; if the former, include a change detection attribute (on which more below)
///   Nb. that this feature is not yet implemented
///
/// RE the "meta" attribute, from the original del.icio.us [docs]: "Clients wishing to maintain a
/// synchronized local store of bookmarks should retain the value of this attribute - its value will
/// change when any significant field of the bookmark changes."
///
/// [docs]: https://web.archive.org/web/20080908014047/http://delicious.com/help/api
///
/// The Pinboard docs are, ahem, terse. Empirically, I've found the following:
///
/// - if you specify the `url` parameter, that uniquely names a post (together with your user id),
///   so you'll get that post back, if it exists
/// - otherwise, you *have* to specify the `dt` parameter; if you don't you'll get a response that
///   contains zero posts, but *does* contain your most recent post date
/// - if you just give the `dt` parameter, you'll get back all your posts for that day
/// - you can specify one-to-three tags *in addition*-- if present, they'll be treated as "and"
///   conditions; i.e. posts tagged with all of them will be returned
async fn get_posts(
    State(state): State<Arc<Indielinks>>,
    Query(post_get_req): Query<PostsGetReq>,
    request: axum::extract::Request,
) -> axum::response::Response {
    async fn get_posts1(
        storage: &(dyn StorageBackend + Send + Sync),
        posts_get_req: PostsGetReq,
        request: axum::extract::Request,
    ) -> Result<PostsGetRsp> {
        let user = user_for_request(&request, "/posts/get")?;
        // If the user has never made any posts, we're done:
        let last_dt = user.last_update().ok_or(
            NoPostsSnafu {
                username: user.username().clone(),
            }
            .build(),
        )?;

        match posts_get_req.dt {
            None => Ok(PostsGetRsp {
                date: *last_dt,
                user: user.username().clone(),
                posts: Vec::new(),
            }),
            Some(dt) => {
                let tags = UpToThree::new(parse_tag_parameter(&posts_get_req.tags)?.into_iter())
                    .context(NoMoreThanThreeTagsSnafu)?;
                let posts = storage
                    .get_posts(user, &tags, &dt.into(), &posts_get_req.uri)
                    .await
                    .context(GetPostsSnafu)?;
                Ok(PostsGetRsp {
                    date: *last_dt,
                    user: user.username().clone(),
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
            if !matches!(err, Error::NoPosts { .. }) {
                error!("{:#?}", err)
            };
            let (status, msg) = err.as_status_and_msg();
            (status, Json(GenericRsp { result_code: msg })).into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        `/posts/recent`                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("delicious.posts.recents", Sort::IntegralCounter) }

#[derive(Debug, Deserialize)]
struct PostsRecentReq {
    tag: Option<String>,
    count: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostsRecentRsp {
    pub date: DateTime<Utc>,
    pub user: Username,
    pub posts: Vec<Post>,
}

/// Retrieve a list of the user's most recent posts
///
/// The user may filter the results by up to three tags (which will operate as a conjunction; i.e. a
/// [Post] will only be returned if it has *all* of the specified tags) via the `tag` paramter. The
/// `count` parameter governs the number of [Post]s returned (ten by default).
async fn get_recent(
    State(state): State<Arc<Indielinks>>,
    Query(post_recent_req): Query<PostsRecentReq>,
    request: axum::extract::Request,
) -> axum::response::Response {
    async fn get_recent1(
        storage: &(dyn StorageBackend + Send + Sync),
        posts_recent_req: PostsRecentReq,
        request: axum::extract::Request,
    ) -> Result<PostsRecentRsp> {
        let user = user_for_request(&request, "/posts/recent")?;
        let tags = UpToThree::new(parse_tag_parameter(&posts_recent_req.tag)?.into_iter())
            .context(NoMoreThanThreeTagsSnafu)?;
        let count = posts_recent_req.count.unwrap_or(10);
        let update_time = user.last_update().context(NoPostsSnafu {
            username: user.username().clone(),
        })?;
        Ok(PostsRecentRsp {
            date: *update_time,
            user: user.username().clone(),
            posts: storage
                .get_recent_posts(user, &tags, count)
                .await
                .context(RecentPostsSnafu)?,
        })
    }

    match get_recent1(state.storage.as_ref(), post_recent_req, request).await {
        Ok(rsp) => {
            counter_add!(
                state.instruments,
                "delicious.posts.recents",
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
//                                         `posts/dates`                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("delicious.posts.dates", Sort::IntegralCounter) }

#[derive(Debug, Deserialize)]
pub struct PostsDatesReq {
    pub tag: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct PostsDate {
    pub count: usize,
    pub date: PostDay,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PostsDatesRsp {
    pub user: Username,
    pub tag: String,
    pub dates: Vec<PostsDate>,
}

async fn posts_dates(
    State(state): State<Arc<Indielinks>>,
    Query(posts_dates_req): Query<PostsDatesReq>,
    request: axum::extract::Request,
) -> axum::response::Response {
    async fn posts_dates1(
        storage: &(dyn StorageBackend + Send + Sync),
        posts_dates_req: PostsDatesReq,
        request: axum::extract::Request,
    ) -> Result<PostsDatesRsp> {
        let user = user_for_request(&request, "/posts/dates")?;
        let tags = UpToThree::new(parse_tag_parameter(&posts_dates_req.tag)?.into_iter())
            .context(NoMoreThanThreeTagsSnafu)?;
        Ok(PostsDatesRsp {
            user: user.username().clone(),
            tag: posts_dates_req.tag.unwrap_or("".to_string()),
            dates: storage
                .get_posts_by_day(user, &tags)
                .await
                .context(PostsByDaySnafu)?
                .iter()
                .map(|(d, n)| PostsDate {
                    count: *n,
                    date: d.clone(),
                })
                .collect::<Vec<PostsDate>>(),
        })
    }

    match posts_dates1(state.storage.as_ref(), posts_dates_req, request).await {
        Ok(rsp) => {
            counter_add!(
                state.instruments,
                "delicious.posts.dates",
                rsp.dates.len() as u64,
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
//                                          `posts/all`                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("delicious.posts.all", Sort::IntegralCounter) }

#[derive(Debug, Deserialize)]
struct PostsAllReq {
    tag: Option<String>,
    start: Option<usize>,
    results: Option<usize>,
    fromdt: Option<DateTime<Utc>>,
    todt: Option<DateTime<Utc>>,
    #[serde(rename = "meta")]
    _meta: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PostsAllRsp {
    pub user: Username,
    pub tag: String,
    pub posts: Vec<Post>,
}

fn apply_pagination(posts: Vec<Post>, start: Option<usize>, size: Option<usize>) -> Vec<Post> {
    match (start, size) {
        (None, None) => posts,
        (None, Some(n)) => {
            if n < posts.len() {
                posts[posts.len() - n..].to_vec()
            } else {
                posts
            }
        }
        (Some(s), None) => {
            if s < posts.len() {
                posts[..posts.len() - s].to_vec()
            } else {
                Vec::new()
            }
        }
        (Some(s), Some(n)) => {
            if s >= posts.len() {
                Vec::new()
            } else if n < posts.len() - s {
                posts[posts.len() - s - n..posts.len() - s].to_vec()
            } else {
                posts[..posts.len() - s].to_vec()
            }
        }
    }
}

/// Retrieve all of a user's posts.
///
/// The user may filter in a few ways:
///
/// - tag: up to three tags by which to filter; returned tags must match all of the tags listed in
///   this argument
/// - fromdt: only return [Post]s posted at or after this time
/// - todt: only return [Post]s posted before this time
/// - start: zero-based index at which to begin returning results (zero corresponds to the earliest
///   [Post] matching `tag`, `fromdt`, and/or `todt`)
/// - results: the number of results to be returned
///
/// Both the [del.icio.us] and Pinboard [docs] are quite vague on how these parameters interact.
/// Pinboard seems to not respect `start` and `results` parameters at all, so I was unable to deduce
/// this manually. This implementation queries the complete set of the user's [Post]s (i.e. a full
/// partition scan), filtering by tags & timestamps if supplied. It then applies pagination to the
/// resulting list. This is risky, since it could result in pulling a large dataset from the
/// back-end, only to throw most of it away before returning it to the caller. I figure that if this
/// application sees that much use, that's a good problem to have, and I'll deal with it when the
/// time comes.
///
/// [del.icio.us]: https://web.archive.org/web/20080908014047/http://delicious.com/help/api#posts_all
/// [docs]: https://pinboard.in/api/
async fn all_posts(
    State(state): State<Arc<Indielinks>>,
    Query(posts_all_req): Query<PostsAllReq>,
    request: axum::extract::Request,
) -> axum::response::Response {
    async fn all_posts1(
        storage: &(dyn StorageBackend + Send + Sync),
        posts_all_req: PostsAllReq,
        request: axum::extract::Request,
    ) -> Result<PostsAllRsp> {
        let user = user_for_request(&request, "/posts/all")?;
        let tags = UpToThree::new(parse_tag_parameter(&posts_all_req.tag)?.into_iter())
            .context(NoMoreThanThreeTagsSnafu)?;
        Ok(PostsAllRsp {
            user: user.username().clone(),
            tag: posts_all_req.tag.unwrap_or("".to_string()),
            posts: apply_pagination(
                storage
                    .get_all_posts(
                        user,
                        &tags,
                        &DateRange::new(posts_all_req.fromdt, posts_all_req.todt),
                    )
                    .await
                    .context(AllPostsSnafu)?,
                posts_all_req.start,
                posts_all_req.results,
            ),
        })
    }

    match all_posts1(state.storage.as_ref(), posts_all_req, request).await {
        Ok(rsp) => {
            counter_add!(
                state.instruments,
                "delicious.posts.all",
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

inventory::submit! { metrics::Registration::new("delicious.posts.tags", Sort::IntegralCounter) }

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
                    username: user.username().clone(),
                })?,
        })
    }

    match tags_get1(state.storage.as_ref(), request).await {
        Ok(rsp) => {
            counter_add!(
                state.instruments,
                "delicious.posts.tags",
                rsp.map.len() as u64,
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
//                                         `tags/rename`                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("delicious.tags.renames", Sort::IntegralCounter) }

#[derive(Debug, Deserialize)]
struct TagsRenameReq {
    old: Tagname,
    new: Tagname,
}

async fn tags_rename(
    State(state): State<Arc<Indielinks>>,
    Query(tags_rename_req): Query<TagsRenameReq>,
    request: axum::extract::Request,
) -> axum::response::Response {
    async fn tags_rename1(
        storage: &(dyn StorageBackend + Send + Sync),
        tags_rename_req: TagsRenameReq,
        request: axum::extract::Request,
    ) -> Result<()> {
        let user = user_for_request(&request, "/tags/rename")?;
        storage
            .rename_tag(user, &tags_rename_req.old, &tags_rename_req.new)
            .await
            .context(RenameTagSnafu {
                old: tags_rename_req.old,
                new: tags_rename_req.new,
            })
    }

    match tags_rename1(state.storage.as_ref(), tags_rename_req, request).await {
        Ok(_) => {
            counter_add!(state.instruments, "delicious.tags.renames", 1, &[]);
            (
                StatusCode::OK,
                Json(GenericRsp {
                    result_code: "done".to_string(),
                }),
            )
                .into_response()
        }
        Err(err) => {
            error!("{:#?}", err);
            let (status, msg) = err.as_status_and_msg();
            (status, Json(GenericRsp { result_code: msg })).into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         `tags/delete`                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("delicious.tags.deleted", Sort::IntegralCounter) }

#[derive(Debug, Deserialize)]
struct TagsDeleteReq {
    tag: Tagname,
}

async fn tags_delete(
    State(state): State<Arc<Indielinks>>,
    Query(tags_delete_req): Query<TagsDeleteReq>,
    request: axum::extract::Request,
) -> axum::response::Response {
    async fn tags_delete1(
        storage: &(dyn StorageBackend + Send + Sync),
        tags_delete_req: TagsDeleteReq,
        request: axum::extract::Request,
    ) -> Result<()> {
        let user = user_for_request(&request, "/tags/delete")?;
        storage
            .delete_tag(user, &tags_delete_req.tag)
            .await
            .context(DeleteTagSnafu {
                tag: tags_delete_req.tag,
            })
    }

    match tags_delete1(state.storage.as_ref(), tags_delete_req, request).await {
        Ok(_) => {
            counter_add!(state.instruments, "delicious.tags.deleted", 1, &[]);
            (
                StatusCode::OK,
                Json(GenericRsp {
                    result_code: "done".to_string(),
                }),
            )
                .into_response()
        }
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
        // Decided not use `DELETE` since we're not addressing the resource being deleted, but again
        // added POST since it seems more appropriate.
        .route("/posts/delete", get(delete_post).merge(post(delete_post)))
        .route("/posts/get", get(get_posts))
        .route("/posts/recent", get(get_recent))
        .route("/posts/dates", get(posts_dates))
        .route("/posts/all", get(all_posts))
        .route("/tags/get", get(tags_get))
        .route("/tags/rename", get(tags_rename).merge(post(tags_rename)))
        .route("/tags/delete", get(tags_delete).merge(post(tags_delete)))
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
