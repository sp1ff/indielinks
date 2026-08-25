// Copyright (C) 2026 Michael Herstine <sp1ff@pobox.com>
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

use std::{ops::Deref, result::Result as StdResult, sync::Arc};

use axum::{
    extract::State,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use axum_extra::extract::Query;
use http::{header::CONTENT_TYPE, HeaderValue, StatusCode};
use indielinks_shared::{
    api::{SignupReq, SignupRsp},
    entities::Username,
};
use serde::{ser::SerializeStruct, Deserialize};
use snafu::{Backtrace, IntoError, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use tower_http::{cors::CorsLayer, set_header::SetResponseHeaderLayer};
use tracing::{debug, error, info};

use crate::{
    app_logic::get_cluster_stats,
    define_metric,
    entities::{self, User},
    home_timeline::HomeTimelines,
    http::ErrorResponseBody,
    indielinks::Indielinks,
    outboxes::UserOutboxes,
    storage::Backend as StorageBackend,
};

////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to add user: {source}"))]
    AddUser { source: crate::storage::Error },
    #[snafu(display("User {username} currently has no outbox"))]
    NoOutbox {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("{source}"))]
    NoPepper { source: crate::peppers::Error },
    #[snafu(display("User {username} currently has no timeline"))]
    NoTimeline {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("While serializing a Timeline to JSON, {source}"))]
    Ser {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("There is no user named {username}"))]
    User {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("While looking up user {username}, {source}"))]
    UserLookup {
        username: String,
        source: crate::storage::Error,
    },
    #[snafu(display("Failed to create user: {source}"))]
    UserSignup { source: entities::Error },
}

pub type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       Operator interface                                       //
////////////////////////////////////////////////////////////////////////////////////////////////////

// General-purpose request to dump a datastructure that is maintained on a per-user basis; if the
// username is omitted, dump everything.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DumpRequest {
    user: Option<Username>,
}

struct HomeTimelinesJsonRepr<'a>(&'a HomeTimelines);

impl<'a> serde::ser::Serialize for HomeTimelinesJsonRepr<'a> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let t = self.0;
        let mut st = serializer.serialize_struct("HomeTimelines", 4)?;

        struct AsPairs<'a>(&'a HomeTimelines);

        impl<'a> serde::ser::Serialize for AsPairs<'a> {
            fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.collect_seq(
                    self.0
                        .into_iter()
                        .map(|(k, v)| (*k, crate::home_timeline::JsonRepr(v))),
                )
            }
        }

        st.serialize_field("items", &AsPairs(t))?;
        st.serialize_field("num_items", &t.len())?;
        st.serialize_field("capacity", &t.cap())?;
        st.end()
    }
}

/// Dump the home timelines, perhaps filtering by user
async fn dump_timelines(
    State(state): State<Arc<Indielinks>>,
    Query(dump_req): Query<DumpRequest>,
) -> axum::response::Response {
    async fn dump_timelines1(
        state: Arc<Indielinks>,
        dump_req: DumpRequest,
    ) -> Result<axum::response::Response> {
        match dump_req.user {
            Some(username) => {
                let user = state
                    .storage
                    .user_for_name(username.as_ref())
                    .await
                    .context(UserLookupSnafu {
                        username: username.clone(),
                    })?
                    .context(UserSnafu {
                        username: username.clone(),
                    })?;
                Json(
                    serde_json::to_value(crate::home_timeline::JsonRepr(
                        state
                            .home_timelines
                            .lock()
                            .await
                            .get(user.id())
                            .context(NoTimelineSnafu { username })?,
                    ))
                    .context(SerSnafu)?,
                )
                .into_response()
                .pipe(Ok)
            }
            None => Json(
                serde_json::to_value(HomeTimelinesJsonRepr(
                    state.home_timelines.lock().await.deref(),
                ))
                .context(SerSnafu)?,
            )
            .into_response()
            .pipe(Ok),
        }
    }

    match dump_timelines1(state.clone(), dump_req).await {
        Ok(response) => response,
        Err(err) => {
            error!("{err:#?}");
            Json(ErrorResponseBody {
                error: format!("{err}"),
            })
            .into_response()
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DropRequest {
    user: Option<Username>,
}

/// Drop the home timelines, or just the timeline for a particular user
async fn drop_timelines(
    State(state): State<Arc<Indielinks>>,
    Query(drop_req): Query<DropRequest>,
) -> axum::response::Response {
    async fn drop_timelines1(state: Arc<Indielinks>, name: Option<Username>) -> Result<()> {
        match name {
            Some(username) => {
                let user = state
                    .storage
                    .user_for_name(username.as_ref())
                    .await
                    .context(UserLookupSnafu {
                        username: username.clone(),
                    })?
                    .context(UserSnafu {
                        username: username.clone(),
                    })?;
                let _ = state
                    .home_timelines
                    .lock()
                    .await
                    .pop(user.id())
                    .or_else(|| {
                        debug!("User {} had no timeline, anyway.", username);
                        None
                    });
            }
            None => {
                state.home_timelines.lock().await.clear();
            }
        }
        Ok(())
    }

    match drop_timelines1(state, drop_req.user).await {
        Ok(_) => http::StatusCode::ACCEPTED.into_response(),
        Err(err) => {
            error!("{err:#?}");
            Json(ErrorResponseBody {
                error: format!("{err}"),
            })
            .into_response()
        }
    }
}

struct UserOutboxesJsonRepr<'a>(&'a UserOutboxes);

impl<'a> serde::ser::Serialize for UserOutboxesJsonRepr<'a> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let t = self.0;
        let mut st = serializer.serialize_struct("UserOutboxes", 4)?;

        struct AsPairs<'a>(&'a UserOutboxes);

        impl<'a> serde::ser::Serialize for AsPairs<'a> {
            fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.collect_seq(
                    self.0
                        .into_iter()
                        .map(|(k, v)| (*k, crate::outboxes::JsonRepr(v))),
                )
            }
        }

        st.serialize_field("items", &AsPairs(t))?;
        st.serialize_field("num_items", &t.len())?;
        st.serialize_field("capacity", &t.cap())?;
        st.end()
    }
}

/// Dump the user outboxes, perhaps filtering by user
async fn dump_outboxes(
    State(state): State<Arc<Indielinks>>,
    Query(dump_req): Query<DumpRequest>,
) -> axum::response::Response {
    async fn dump_outboxes1(
        state: Arc<Indielinks>,
        dump_req: DumpRequest,
    ) -> Result<axum::response::Response> {
        match dump_req.user {
            Some(user) => {
                let user = state
                    .storage
                    .user_for_name(user.as_ref())
                    .await
                    .context(UserLookupSnafu {
                        username: user.clone(),
                    })?
                    .context(UserSnafu {
                        username: user.clone(),
                    })?;
                Json(
                    serde_json::to_value(crate::outboxes::JsonRepr(
                        state
                            .user_outboxes
                            .lock()
                            .await
                            .get(user.id())
                            .context(NoOutboxSnafu {
                                username: user.username().clone(),
                            })?,
                    ))
                    .context(SerSnafu)?,
                )
                .into_response()
                .pipe(Ok)
            }
            None => Json(
                serde_json::to_value(UserOutboxesJsonRepr(
                    state.user_outboxes.lock().await.deref(),
                ))
                .context(SerSnafu)?,
            )
            .into_response()
            .pipe(Ok),
        }
    }

    match dump_outboxes1(state.clone(), dump_req).await {
        Ok(response) => response,
        Err(err) => {
            error!("{err:#?}");
            Json(ErrorResponseBody {
                error: format!("{err}"),
            })
            .into_response()
        }
    }
}

/// Get cluster stats-- this is the same as the `/users/cluster-stats` endpoint, but unauthenticated
/// (since we're presumably being hit from localhost)
async fn cluster_stats(State(state): State<Arc<Indielinks>>) -> axum::response::Response {
    match get_cluster_stats(state).await {
        Ok(stats) => (StatusCode::OK, Json(stats)).into_response(),
        Err(err) => {
            error!("{err:#?}");
            Json(ErrorResponseBody {
                error: format!("{err}"),
            })
            .into_response()
        }
    }
}

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
        use secrecy::ExposeSecret;
        let user = User::new(
            &pepper_ver,
            &pepper_key,
            &signup_req.username,
            // Arrrgghhh!!!
            &signup_req.password.expose_secret().0.clone().into(),
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
                        error: format!("Insufficient password strength: {feedback}"),
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
                let err = UserSignupSnafu.into_error(err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponseBody {
                        error: format!("{err}"),
                    }),
                )
                    .into_response()
            }
        },
        Err(Error::AddUser { source }) => match source {
            crate::storage::Error::UsernameClaimed { username, .. } => {
                info!("Username {} already claimed", username);
                user_signups_failures.add(1, &[]);
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponseBody {
                        error: format!("Username {username} is already claimed; sorry"),
                    }),
                )
                    .into_response()
            }
            err => {
                error!("{:#?}", err);
                user_signups_failures.add(1, &[]);
                let err = AddUserSnafu.into_error(err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponseBody {
                        error: format!("{err}"),
                    }),
                )
                    .into_response()
            }
        },
        Err(err) => {
            error!("{:#?}", err);
            user_signups_failures.add(1, &[]);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponseBody {
                    error: format!("{err}"),
                }),
            )
                .into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn make_router(state: Arc<Indielinks>) -> Router<Arc<Indielinks>> {
    Router::new()
        .route("/timelines/dump", get(dump_timelines))
        .route("/timelines/drop", get(drop_timelines))
        .route("/outboxes/dump", get(dump_outboxes))
        .route("/cluster-stats", get(cluster_stats))
        .route("/users/signup", post(signup))
        .layer(SetResponseHeaderLayer::if_not_present(
            CONTENT_TYPE,
            HeaderValue::from_static("text/json; charset=utf-8"),
        ))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
