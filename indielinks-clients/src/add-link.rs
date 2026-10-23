// Copyright (C) 2025-2026 Michael Herstine <sp1ff@pobox.com>
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

use std::fmt::Debug;

use http::{header::AUTHORIZATION, Method, Request};
use itertools::Itertools;
use secrecy::SecretString;
use snafu::{Backtrace, ResultExt, Snafu};
use tower::{Service, ServiceExt};
use url::Url;

use indielinks_shared::{
    api::PostAddReq, entities::Tagname, nonempty_string::NonEmptyString, origin::Origin,
};

use crate::service::RequestBody;

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    #[snafu(display("failed to invoke the client Service"))]
    Call {
        source: indielinks_shared::service::Error,
    },
    #[snafu(display("the client Service failed to become ready"))]
    Ready {
        source: indielinks_shared::service::Error,
    },
    #[snafu(display("failed to build an HTTP request"))]
    Request {
        source: http::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("indielinks returned status {status}"))]
    Response {
        status: http::StatusCode,
        backtrace: Backtrace,
    },
    #[snafu(display("failed to encode {url} to a query string"))]
    UrlEncoding {
        url: Box<Url>,
        #[snafu(source(from(serde_urlencoded::ser::Error, Box::new)))]
        source: Box<serde_urlencoded::ser::Error>,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         adding a link                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Add a link
#[allow(clippy::too_many_arguments)]
pub async fn add_link<'a, C, I>(
    mut client: C,
    api: &Origin,
    token: &SecretString,
    url: Url,
    title: NonEmptyString,
    notes: Option<&NonEmptyString>,
    tags: I,
    replace: bool,
    shared: bool,
    to_read: bool,
) -> Result<()>
where
    // It would be nice to have an alias for this, but the trait_alias feature is only on nightly
    C: Service<
            http::Request<RequestBody>,
            Response = http::Response<bytes::Bytes>,
            Error = indielinks_shared::service::Error,
        > + Clone,
    I: Iterator<Item = &'a Tagname>,
{
    use secrecy::ExposeSecret;

    let tags = tags.map(|tag| tag.to_string()).join(",");

    let add_request = PostAddReq {
        url,
        title,
        notes: notes.cloned(),
        tags: if tags.is_empty() { None } else { Some(tags) },
        dt: None,
        replace: Some(replace),
        shared: Some(shared),
        to_read: Some(to_read),
    };

    let request = Request::builder()
        .method(Method::POST)
        .uri(format!(
            "{api}/api/v1/posts/add?{}",
            serde_urlencoded::to_string(&add_request).context(UrlEncodingSnafu {
                url: add_request.url
            })?
        ))
        .header(AUTHORIZATION, format!("Bearer {}", token.expose_secret()))
        .body(RequestBody::None)
        .context(RequestSnafu)?;

    let response = client
        .ready()
        .await
        .context(ReadySnafu)?
        .call(request)
        .await
        .context(CallSnafu)?;

    if response.status().is_success() {
        // Response body ignored
        Ok(())
    } else {
        ResponseSnafu {
            status: response.status(),
        }
        .fail()
    }
}
