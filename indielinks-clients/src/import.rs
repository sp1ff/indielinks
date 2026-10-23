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

use futures::stream::{FuturesUnordered, StreamExt};
use http::{header::AUTHORIZATION, Method, Request};
use indielinks_shared::{api::PostAddReq, origin::Origin};
use secrecy::SecretString;
use snafu::{Backtrace, ResultExt, Snafu};
use tower::{Service, ServiceExt};
use url::Url;

use crate::service::RequestBody;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    #[snafu(display("failed to invoke the client Service"))]
    Call {
        source: indielinks_shared::service::Error,
    },
    #[snafu(display("failed response from the `/posts/add` endpoint: {status}"))]
    Failure {
        status: http::StatusCode,
        backtrace: Backtrace,
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
//                                   logic for importing posts                                    //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Add a [post](ExportedPost) to indielinks
async fn import_post<C>(
    mut client: C,
    api: &Origin,
    token: &SecretString,
    add_req: PostAddReq,
) -> Result<()>
where
    C: Service<
            http::Request<RequestBody>,
            Response = http::Response<bytes::Bytes>,
            Error = indielinks_shared::service::Error,
        > + Clone,
{
    use secrecy::ExposeSecret;

    let req = Request::builder()
        .method(Method::POST)
        .uri(format!(
            "{api}/api/v1/posts/add?{}",
            serde_urlencoded::to_string(&add_req).context(UrlEncodingSnafu { url: add_req.url })?
        ))
        .header(AUTHORIZATION, format!("Bearer {}", token.expose_secret()))
        .body(RequestBody::None)
        .context(RequestSnafu)?;
    let rsp = client
        .ready()
        .await
        .context(ReadySnafu)?
        .call(req)
        .await
        .context(CallSnafu)?;
    if rsp.status().is_success() {
        Ok(())
    } else {
        FailureSnafu {
            status: rsp.status(),
        }
        .fail()
    }
    // Response body ignored
}

/// Import a (fixed-size, presumably small) collection of posts
pub async fn import_posts<C, I>(
    client: C,
    api: &Origin,
    token: &SecretString,
    posts: I,
) -> Result<usize>
where
    C: Service<
            http::Request<RequestBody>,
            Response = http::Response<bytes::Bytes>,
            Error = indielinks_shared::service::Error,
        > + Clone,
    I: Iterator<Item = PostAddReq>,
{
    // Here, we want the requests to be in-flight simultaneously (subject to rate-limiting, of course).
    let futs = FuturesUnordered::new();
    posts.for_each(|post| {
        let client = client.clone();
        futs.push(import_post(client, api, token, post))
    });
    let res = futs
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    Ok(res.len())
}
