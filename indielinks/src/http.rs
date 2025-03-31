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

use std::sync::Arc;

use crate::{
    background_tasks::BackgroundTasks, metrics, origin::Origin, peppers::Peppers,
    signing_keys::SigningKeys, storage::Backend as StorageBackend,
};

use axum::Json;
use chrono::Duration;
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, ResultExt, Snafu};
use tap::Pipe;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        Error Responses                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A serializable struct for use in HTTP error responses
///
/// This is intended to be used in the [IntoResponse] implementations for whatever error type
/// an axum handler is using.
///
/// [IntoResponse]: https://docs.rs/axum/latest/axum/response/trait.IntoResponse.html
///
/// This may be a violation of the YNGNI! principle, but I'd like to return a JSON body for errors.
/// I can't see a way to enforce the rule that all axum handlers do this, but I can at least
/// setup a standard representation of an error response.
#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorResponseBody {
    pub error: String,
}

impl axum::response::IntoResponse for ErrorResponseBody {
    fn into_response(self) -> axum::response::Response {
        Json(self).into_response()
    }
}

#[allow(type_alias_bounds)]
pub type Result<T: axum::response::IntoResponse> = std::result::Result<T, ErrorResponseBody>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to interpret a header value as a UTF-8 string: {source}"))]
    HeaderValue {
        source: http::header::ToStrError,
        backtrace: Backtrace,
    },
    #[snafu(display("{value} is not supported as an Accept header value"))]
    UnsupportedAccept { value: String, backtrace: Backtrace },
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                modelling "Accept" header values                                //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Supported values for the request Accept header
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Accept {
    ActivityPub,
    Html,
}

impl std::fmt::Display for Accept {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Accept::Html => write!(f, "text/html"),
            Accept::ActivityPub => write!(f, "application/activity+json"),
        }
    }
}

impl Accept {
    /// Lookup the header value corresponding to the "Accept" header in a [HeaderMap], defaulting to
    /// [Accept::Html]. If there are more than one "Accept" headers, only the first will be
    /// examined. If the value specifies a a MIME type not supported by indielinks, fail.
    ///
    /// [HeaderMap]: https://docs.rs/http/latest/http/header/struct.HeaderMap.html
    pub fn lookup_from_header_map(headers: &http::HeaderMap) -> std::result::Result<Accept, Error> {
        headers
            .get(http::header::ACCEPT)
            .map(http::HeaderValue::to_str)
            .transpose()
            .context(HeaderValueSnafu)?
            .map(|s| {
                if s.contains("application/ld+json") || s.contains("application/activity+json") {
                    Ok(Accept::ActivityPub)
                } else if s == "text/hml" {
                    Ok(Accept::Html)
                } else {
                    UnsupportedAcceptSnafu {
                        value: s.to_owned(),
                    }
                    .fail()
                }
            })
            .transpose()?
            .unwrap_or(Accept::Html)
            .pipe(Ok)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       Application State                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Application state available to all handlers
// Not sure this is going to stay here.
pub struct Indielinks {
    pub origin: Origin,
    pub storage: Arc<dyn StorageBackend + Send + Sync>,
    pub registry: prometheus::Registry,
    pub instruments: Arc<metrics::Instruments>,
    pub pepper: Peppers,
    pub token_lifetime: Duration,
    pub signing_keys: SigningKeys,
    pub client: reqwest::Client,
    pub collection_page_size: usize,
    pub task_sender: Arc<BackgroundTasks>,
}
