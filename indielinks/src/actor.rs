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

//! The ActivityPub Actor input

use std::sync::Arc;

use axum::{
    extract::State,
    http::{
        header::{ToStrError, ACCEPT},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{Html, IntoResponse},
    Json,
};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, IntoError, ResultExt, Snafu};
use tap::Pipe;
use tracing::error;
use url::Url;

use crate::{
    counter_add,
    entities::{self, User, Username},
    http::{ErrorResponseBody, Indielinks},
    metrics::{self, Sort},
    storage::{self, Backend as StorageBackend},
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

inventory::submit! { metrics::Registration::new("actor.retrieved", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("actor.errors", Sort::IntegralCounter) }

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to interpret a header value as a UTF-8 string: {source}"))]
    HeaderValue {
        source: ToStrError,
        backtrace: Backtrace,
    },
    NoUser {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to obtain public key in PEM format; {source}"))]
    Pem { source: entities::Error },
    #[snafu(display("Storage backend error: {source}"))]
    Storage { source: storage::Error },
    #[snafu(display("Failed to parse an URL: {source}"))]
    UrlParse {
        source: url::ParseError,
        backtrace: Backtrace,
    },
    #[snafu(display("{value} is not supported as an Accept header value"))]
    UnsupportedAccept { value: String, backtrace: Backtrace },
}

type Result<T> = std::result::Result<T, Error>;

/// Supported values for the request Accept header
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Accept {
    Html,
    ActivityPub,
}

impl Accept {
    pub fn new(headers: &HeaderMap) -> Result<Accept> {
        headers
            .get(ACCEPT)
            .map(HeaderValue::to_str)
            .transpose()
            .context(HeaderValueSnafu)?
            .map(|s| {
                if s == "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\""
                    || s == "application/activity+json"
                {
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
//                                  `/users/{username}` handler                                   //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EndpointsJrd {
    #[serde(rename = "sharedInbox")]
    shared_inbox: Url,
}

impl EndpointsJrd {
    pub fn new(domain: &str) -> Result<EndpointsJrd> {
        Ok(EndpointsJrd {
            shared_inbox: Url::parse(&format!("https://{}/inbox", domain))
                .context(UrlParseSnafu)?,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PublicKeyJrd {
    id: Url,
    owner: Url,
    #[serde(rename = "publicKeyPem")]
    public_key_pem: String,
}

impl PublicKeyJrd {
    pub fn new(user: &User, domain: &str) -> Result<PublicKeyJrd> {
        Ok(PublicKeyJrd {
            id: Url::parse(&format!(
                "https://{}/users/{}#main-key",
                domain,
                user.username()
            ))
            .context(UrlParseSnafu)?,
            owner: Url::parse(&format!("https://{}/users/{}", domain, user.username()))
                .context(UrlParseSnafu)?,
            public_key_pem: user.pub_key().to_pem().context(PemSnafu)?,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
// Build to serialize to the appropriate JRD
pub struct ActorJrd {
    #[serde(rename = "@context")]
    context: Vec<Url>,
    id: Url,
    r#type: String,
    inbox: Url,
    outbox: Url,
    following: Url,
    followers: Url,
    liked: Url,
    endpoints: EndpointsJrd,
    #[serde(rename = "publicKey")]
    public_key: PublicKeyJrd,
}

impl ActorJrd {
    pub fn new(user: &User, domain: &str) -> Result<ActorJrd> {
        Ok(ActorJrd {
            context: vec![
                Url::parse("https://www.w3.org/ns/activitystreams").unwrap(/* known good */),
                Url::parse("https://w3id.org/security/v1").unwrap(/* known good */),
            ],
            id: Url::parse(&format!("https://{}/users/{}", domain, &user.username()))
                .context(UrlParseSnafu)?,
            r#type: "person".to_owned(),
            inbox: Url::parse(&format!(
                "https://{}/users/{}/inbox",
                domain,
                &user.username()
            ))
            .context(UrlParseSnafu)?,
            outbox: Url::parse(&format!(
                "https://{}/users/{}/outbox",
                domain,
                &user.username()
            ))
            .context(UrlParseSnafu)?,
            following: Url::parse(&format!(
                "https://{}/users/{}/following",
                domain,
                &user.username()
            ))
            .context(UrlParseSnafu)?,
            followers: Url::parse(&format!(
                "https://{}/users/{}/followers",
                domain,
                &user.username()
            ))
            .context(UrlParseSnafu)?,
            liked: Url::parse(&format!(
                "https://{}/users/{}/liked",
                domain,
                &user.username()
            ))
            .context(UrlParseSnafu)?,
            endpoints: EndpointsJrd::new(domain)?,
            public_key: PublicKeyJrd::new(user, domain)?,
        })
    }
}

trait Actor {
    fn as_jrd(&self, domain: &str) -> Result<ActorJrd>;
    fn as_html(&self) -> String;
}

impl Actor for User {
    fn as_jrd(&self, domain: &str) -> Result<ActorJrd> {
        ActorJrd::new(self, domain)
    }
    fn as_html(&self) -> String {
        format!(
            "<html><body>{} ({}@indiemark.sh)</body></html>",
            self.display_name(),
            self.username()
        )
    }
}

/// `/users/{username}` handler
pub async fn actor(
    State(state): State<Arc<Indielinks>>,
    axum::extract::Path(username): axum::extract::Path<Username>,
    headers: HeaderMap,
) -> axum::response::Response {
    async fn actor1(
        storage: &(dyn StorageBackend + Send + Sync),
        username: &Username,
        headers: &HeaderMap,
    ) -> Result<(User, Accept)> {
        let accept = Accept::new(headers)?;
        let user = storage
            .user_for_name(username)
            .await
            .map_err(|err| StorageSnafu.into_error(err))?
            .ok_or(
                NoUserSnafu {
                    username: username.clone(),
                }
                .build(),
            )?;
        Ok((user, accept))
    }

    fn handle_err(err: Error, instruments: &metrics::Instruments) -> axum::response::Response {
        error!("{:#?}", err);
        counter_add!(instruments, "actor.errors", 1, &[]);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponseBody {
                error: format!("{}", err),
            }),
        )
            .into_response()
    }

    match actor1(state.storage.as_ref(), &username, &headers).await {
        Ok((user, Accept::ActivityPub)) => match user.as_jrd(&state.domain) {
            Ok(jrd) => {
                counter_add!(state.instruments, "actor.retrieved", 1, &[]);
                (StatusCode::OK, Json(jrd)).into_response()
            }
            Err(err) => handle_err(err, &state.instruments),
        },
        Ok((user, Accept::Html)) => {
            counter_add!(state.instruments, "actor.retrieved", 1, &[]);
            (StatusCode::OK, Html(user.as_html())).into_response()
        }
        Err(err) => handle_err(err, &state.instruments),
    }
}
