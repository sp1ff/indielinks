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
// You should have received a copy of the GNU General Public License along with indielinks.  If not,
// see <http://www.gnu.org/licenses/>.

//! indielinks webfinger implementation
//!
//! This is a minimal webfinger implementation; it ignores the `rel` query parameter and just
//! returns the "self" link.
//!
//! CORS headers: "servers MUST include the Access-Control-Allow-Origin HTTP header in responses.
//! Servers SHOULD support the least restrictive setting by allowing any domain access to the
//! WebFinger resource: Access-Control-Allow-Origin: *" I handle this at the router layer with a
//! [CorsLayer].
//!
//! [CorsLayer]: tower_http::cors::CorsLayer
//!
//! Clients can request different formats via the Accept header. That said, we can silently ignore
//! any incoming "Accept" header value other than "application/jrd+json".
//!
//! # The `rel` Parameter
//!
//! This lets the caller specify the link relations in which they are interested. Even if specified,
//! the other attributes, like aliases & properties, are still returned; it's just the "links"
//! collection that's filtered. Support for this feature is *not* required, and Mastodon does not
//! implement it. The `rel` parameter may be specified more than once (once for each link type
//! desired). From the [RFC]:
//!
//! [RFC]: https://www.rfc-editor.org/rfc/rfc7033
//!
//! "If there are no matching link relation types defined for the resource, the 'links' array in the
//! JRD will be either absent or empty. All other information present in a resource descriptor
//! remains present, even when 'rel' is employed."
//!
//! I take this to mean that if a caller names a link type I don't know about in a `rel` parameter I
//! am to return no links at all. Indeed, further down: "Note that if a client requests a particular
//! link relation type for which the server has no information, the server MAY return a JRD with an
//! empty 'links' array or no 'links' array."

use std::sync::Arc;

use crate::ap_entities::make_user_id;
use crate::entities::User;
use crate::http::Indielinks;
use crate::origin::Origin;
use crate::storage::Backend as StorageBackend;
use crate::{acct::Account, http::ErrorResponseBody};
use crate::{define_metric, storage};

use axum::extract::{Query, State};
use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, ResultExt, Snafu};
use tracing::{error, info};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The account name {name} is not a valid username: {source}"))]
    BadUsername {
        name: String,
        #[snafu(source(from(crate::entities::Error, Box::new)))]
        source: Box<crate::entities::Error>,
    },
    #[snafu(display("Could not interpret {domain} as an acct host: {source}"))]
    Domain {
        domain: String,
        source: url::ParseError,
    },
    #[snafu(display("Mismatched hostname for webfinger"))]
    Hostname { backtrace: Backtrace },
    #[snafu(display("Unknown user for webfinger"))]
    NoSuchUser { backtrace: Backtrace },
    #[snafu(display("Storage failure: {source}"))]
    Storage { source: storage::Error },
    #[snafu(display("Failed to form an URL: {source}"))]
    UrlParse {
        source: url::ParseError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to create an AP user ID from the account {name}: {source}"))]
    UserId {
        name: String,
        #[snafu(source(from(crate::ap_entities::Error, Box::new)))]
        source: Box<crate::ap_entities::Error>,
    },
}

type Result<T> = std::result::Result<T, Error>;

/// Link Relation Type
///
/// This is a trivial subset of the [registered] link relation types sufficient for indielinks. I'll
/// build it out, later. Later, we'll want:
///
/// - `http://webfinger.net/rel/profile-page`
/// - `http://webfinger.net/rel/avatar`
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
/// it out, later. Later we'll also want `text/html` and `image/jpeg`.
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
    /// Create a "self" [Link] from a [User]
    fn from_user(user: &User, origin: &Origin) -> Result<Self> {
        Ok(Link {
            rel: LinkRelation::Myself,
            r#type: MediaType::ActivityPub,
            href: make_user_id(user.username(), origin).context(UserIdSnafu {
                name: user.username().to_string(),
            })?,
        })
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
///   "subject": "<username>@<domain>",
///   "aliases": [
///     "https://<domain>/@<username>",
///     "https://<domain>/users/<username>",
///   ],
///   "links": [
///      {
///        "rel": "self",
///        "type": "application/activity+json",
///        "href": "https://<domain>/users/<username>"
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
    aliases: Vec<Url>,
    links: Vec<Link>,
}

impl ResponseBody {
    pub fn new(user: &User, acct: &Account, origin: &Origin) -> Result<ResponseBody> {
        Ok(ResponseBody {
            links: vec![Link::from_user(user, origin)?],
            aliases: vec![
                Url::parse(&format!("{}/@{}", origin, user.username())).context(UrlParseSnafu)?,
                make_user_id(user.username(), origin).context(UserIdSnafu {
                    name: user.username().to_string(),
                })?,
            ],
            subject: acct.clone(),
        })
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

define_metric! { "webfinger.served", webfinger_served, Sort::IntegralCounter }
define_metric! { "webfinger.not_found", webfinger_not_found, Sort::IntegralCounter }
define_metric! { "webfinger.errors", webfinger_errors, Sort::IntegralCounter }

#[derive(Debug, Deserialize)]
pub struct WebFingerQueryParams {
    pub resource: Account,
    #[serde(default, rename = "rel")]
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
    async fn webfinger1(
        account: &Account,
        origin: &Origin,
        storage: &(dyn StorageBackend + Send + Sync),
    ) -> Result<ResponseBody> {
        if *account.host() != *origin.host() {
            return HostnameSnafu.fail();
        }

        match storage
            .user_for_name(account.user().as_ref())
            .await
            .context(StorageSnafu)?
        {
            Some(user) => Ok(ResponseBody::new(&user, account, origin)?),
            None => NoSuchUserSnafu.fail(),
        }
    }

    match webfinger1(&params.resource, &state.origin, state.storage.as_ref()).await {
        Ok(rsp) => {
            webfinger_served.add(1, &[]);
            (StatusCode::OK, Json(rsp)).into_response()
        }
        Err(Error::Hostname { .. }) => {
            webfinger_not_found.add(1, &[]);
            info!("Mismatched hostname");
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponseBody {
                    error: "Mismatched host name".to_owned(),
                }),
            )
                .into_response()
        }
        Err(Error::NoSuchUser { .. }) => {
            webfinger_not_found.add(1, &[]);
            info!("No such user");
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponseBody {
                    error: "No such user here".to_owned(),
                }),
            )
                .into_response()
        }
        Err(err) => {
            error!("{:?}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponseBody {
                    error: "Internal server error".to_owned(),
                }),
            )
                .into_response()
        }
    }
}
