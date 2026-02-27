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

use axum::{extract::State, response::IntoResponse, routing::get, Router};
use axum_extra::extract::Query;
use http::{header::CONTENT_TYPE, HeaderValue};
use indielinks_shared::entities::Username;
use serde::{ser::SerializeStruct, Deserialize};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use tower_http::{cors::CorsLayer, set_header::SetResponseHeaderLayer};
use tracing::error;

use crate::{
    http::ErrorResponseBody,
    indielinks::{HomeTimelines, Indielinks},
};

////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
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
        username: String,
        backtrace: Backtrace,
    },
    #[snafu(display("While looking up user {username}, {source}"))]
    UserLookup {
        username: String,
        source: crate::storage::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       Operator interface                                       //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DumpRequest {
    user: Option<String>,
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
            Some(user) => {
                let user = state
                    .storage
                    .user_for_name(&user)
                    .await
                    .context(UserLookupSnafu {
                        username: user.clone(),
                    })?
                    .context(UserSnafu {
                        username: user.clone(),
                    })?;
                Ok(axum::Json(
                    serde_json::to_value(crate::home_timeline::JsonRepr(
                        state.home_timelines.lock().await.get(user.id()).context(
                            NoTimelineSnafu {
                                username: user.username().clone(),
                            },
                        )?,
                    ))
                    .context(SerSnafu)?,
                )
                .into_response())
            }
            None => Ok(axum::Json(
                serde_json::to_value(HomeTimelinesJsonRepr(
                    state.home_timelines.lock().await.deref(),
                ))
                .context(SerSnafu)?,
            )
            .into_response()),
        }
    }

    match dump_timelines1(state.clone(), dump_req).await {
        Ok(response) => response,
        Err(err) => {
            error!("{err:#?}");
            axum::Json(ErrorResponseBody {
                error: format!("{err}"),
            })
            .into_response()
        }
    }
}

pub fn make_router(state: Arc<Indielinks>) -> Router<Arc<Indielinks>> {
    Router::new()
        .route("/dump", get(dump_timelines))
        .layer(SetResponseHeaderLayer::if_not_present(
            CONTENT_TYPE,
            HeaderValue::from_static("text/json; charset=utf-8"),
        ))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
