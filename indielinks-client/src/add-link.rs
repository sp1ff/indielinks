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

use std::{collections::HashSet, fmt::Debug};

use http::{Method, Request, header::AUTHORIZATION};
use indielinks::origin::Origin;
use itertools::Itertools;
use secrecy::SecretString;
use snafu::{Backtrace, ResultExt, Snafu};
use tower::{Service, ServiceExt};
use url::Url;

use indielinks_shared::{PostAddReq, Tagname};

use crate::service::ReqBody;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The API origin must be specified, either in config or on the command line"))]
    Api,
    #[snafu(display("The tower service returns {source}"))]
    Call {
        source: Box<dyn std::error::Error + Send + Sync>,
        backtrace: Backtrace,
    },
    #[snafu(display("Exhausted the buffer without matching the predicate"))]
    Eob { backtrace: Backtrace },
    #[snafu(display("Failed to find the initial '[']"))]
    InitialParse,
    #[snafu(display("The API key must be specified, either in config or on the command line"))]
    MissingToken,
    #[snafu(display("No sub-command given; try --help"))]
    NoSubCommand,
    #[snafu(display(
        "Missing token; specify it with the --token option or in your configuration file"
    ))]
    NoToken,
    #[snafu(display("Failed to find the next ',' or ']'"))]
    Parse,
    #[snafu(display("Premature EOF while deserializing array elements"))]
    PrematureEof { backtrace: Backtrace },
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
//                                         adding a link                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Add a link
#[allow(clippy::too_many_arguments)]
pub async fn add_link<C>(
    mut client: C,
    api: &Origin,
    token: &SecretString,
    url: &Url,
    title: &str,
    notes: Option<&str>,
    tags: HashSet<Tagname>,
    replace: Option<bool>,
    shared: Option<bool>,
    to_read: Option<bool>,
) -> Result<()>
where
    // It would be nice to have an alias for this, but the trait_alias feature is only on nightly
    C: Service<
            http::Request<ReqBody>,
            Response = http::Response<Vec<u8>>,
            Error = Box<dyn std::error::Error + Send + Sync>,
        > + Clone,
{
    use secrecy::ExposeSecret;

    let add_req = PostAddReq {
        url: url.into(),
        title: title.to_owned(),
        notes: notes.map(|s| s.to_owned()),
        tags: if tags.is_empty() {
            None
        } else {
            Some(tags.into_iter().map(|tag| tag.to_string()).join(","))
        },
        dt: None,
        replace,
        shared,
        to_read,
    };
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!(
            "{api}/api/v1/posts/add?{}",
            serde_urlencoded::to_string(&add_req).context(UrlEncodingSnafu { url: add_req.url })?
        ))
        .header(AUTHORIZATION, format!("Bearer {}", token.expose_secret()))
        .body(ReqBody::None)
        .context(RequestSnafu)?;
    client
        .ready()
        .await
        .context(ReadySnafu)?
        .call(req)
        .await
        .context(CallSnafu)?;

    // Response body ignored
    Ok(())
}
