// Copyright (C) 2024-2025 Michael Herstine <sp1ff@pobox.com>
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
// You should have received a copy of the GNU General Public License along with mpdpopm.  If not,
// see <http://www.gnu.org/licenses/>.

//! indielinks webfinger implementation
//!
//! This is a minimal webfinger implementation; it ignores the `rel` query parameter and just
//! returns the "application/activity+json" link.
//!
//! It also neglects the CORS headers: "servers MUST include the Access-Control-Allow-Origin HTTP
//! header in responses. Servers SHOULD support the least restrictive setting by allowing any domain
//! access to the WebFinger resource: Access-Control-Allow-Origin: *"

use std::sync::Arc;

use crate::http::Indielinks;
use crate::{acct::Account, http::ErrorResponseBody};

use axum::extract::{Query, State};
use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use tracing::error;
use url::Url;

/// Link Relation Type
///
/// This is a trivial subset of the [registered] link relation types sufficient for indielinks. I'll
/// build it out, later.
///
/// [registered]: https://www.iana.org/assignments/link-relations/link-relations.xhtml
#[derive(Debug, Serialize)]
pub enum LinkRelation {
    #[serde(rename = "self")]
    Myself,
}

/// Media Type
///
/// This is a trivial subset of the [registered] media types sufficient for indielinks. I'll build
/// it out, later.
///
/// [registered]: https://www.iana.org/assignments/media-types/media-types.xhtml
#[derive(Debug, Serialize)]
pub enum MediaType {
    #[serde(rename = "application/activity+json")]
    ActivityPub,
}

#[derive(Debug, Serialize)]
pub struct Link {
    rel: LinkRelation,
    r#type: MediaType,
    href: Url,
}

impl Link {
    fn from_account(value: &Account) -> Self {
        Link {
            rel: LinkRelation::Myself,
            r#type: MediaType::ActivityPub,
            href: value.home(),
        }
    }
}

/// Webfinger response body
///
/// According to the Webfinger [RFC], the response shall be in the form of a [JRD]-- a JSON Resource
/// Descriptor. A JRD is "a JSON object that comprises the following name/value pairs:
///
/// - subject
/// - aliases
/// - properties
/// - links"
///
/// [RFC]: https://www.rfc-editor.org/rfc/rfc7033
/// [JRD]: https://www.rfc-editor.org/rfc/rfc7033#page-11
///
/// A [ResponseBody] is a struct that, when serialized to JSON, may be used as the response
/// body to a Webfinger request.
///
/// For now, the implementation is quite simple; it will return a response of the following form:
///
/// ```text
/// {
///   "subject": "<username>@<domain>"
///   "links": [
///      {
///        "rel": "self",
///        "type": "application/activity+json",
///        "href": "https://<domain>/~<username>"
///      }
///    ]
/// }
/// ```
///
/// We can expect a follow-up request to "https://domain/~username" to retrieve the user's
/// ActivityPub profile.
#[derive(Serialize)]
pub struct ResponseBody {
    subject: Account,
    links: Vec<Link>,
}

impl std::convert::From<Account> for ResponseBody {
    fn from(value: Account) -> Self {
        ResponseBody {
            links: vec![Link::from_account(&value)],
            subject: value,
        }
    }
}

impl axum::response::IntoResponse for ResponseBody {
    fn into_response(self) -> axum::response::Response {
        Json(self).into_response()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       webfinger handler                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct WebFingerQueryParams {
    pub resource: Account,
    #[serde(default)]
    _rel: Vec<Url>,
}

/// `/.well-known/webfinger` handler
///
/// This is a first, minimal implementation of the webfinger [protocol].
///
/// [protocol]: https://www.rfc-editor.org/rfc/rfc7033
pub async fn webfinger(
    State(state): State<Arc<Indielinks>>,
    params: Query<WebFingerQueryParams>,
) -> axum::response::Response {
    // An inelegant implementation, but I'm going to wait for the full implementation to optimize.
    // One thing to note: Mastodon does not send back an error message on failure (just an empty
    // response).
    if params.resource.host() != state.domain {
        return (
            StatusCode::NOT_FOUND,
            ErrorResponseBody {
                error: "Hostname mismatch".to_string(),
            },
        )
            .into_response();
    }

    match state.storage.user_for_name(params.resource.user()).await {
        Ok(Some(_user)) => {
            let rsp: ResponseBody = params.resource.clone().into();
            (StatusCode::OK, rsp).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            ErrorResponseBody {
                error: "No such user".to_string(),
            },
        )
            .into_response(),
        Err(err) => {
            error!("{:?}", err);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
