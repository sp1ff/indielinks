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

use futures::stream::{FuturesUnordered, StreamExt};
use http::{Method, Request, header::AUTHORIZATION};
use indielinks::origin::Origin;
use indielinks_shared::PostAddReq;
use secrecy::SecretString;
use snafu::{Backtrace, ResultExt, Snafu};
use tower::{Service, ServiceExt};
use url::Url;

use crate::service::ReqBody;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The tower service returns {source}"))]
    Call {
        source: Box<dyn std::error::Error + Send + Sync>,
        backtrace: Backtrace,
    },
    #[snafu(display("posts/add response: {status}"))]
    Failure {
        status: http::StatusCode,
        backtrace: Backtrace,
    },
    #[snafu(display("While waiting for the tower service to be ready: {source}"))]
    Ready {
        source: Box<dyn std::error::Error + Send + Sync>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to build an http::Request: {source}"))]
    Request {
        source: http::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While encoding {url} to a query string: {source}"))]
    UrlEncoding {
        url: Url,
        source: serde_urlencoded::ser::Error,
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
            http::Request<ReqBody>,
            Response = http::Response<bytes::Bytes>,
            Error = Box<dyn std::error::Error + Send + Sync>,
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
        .body(ReqBody::None)
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
            http::Request<ReqBody>,
            Response = http::Response<bytes::Bytes>,
            Error = Box<dyn std::error::Error + Send + Sync>,
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
