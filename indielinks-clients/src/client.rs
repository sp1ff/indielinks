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

use std::{num::NonZero, result::Result as StdResult, time::Duration};

use bytes::Bytes;
use governor::Quota;
use http::{
    header::{ACCEPT, AUTHORIZATION, USER_AGENT},
    HeaderValue,
};
use indielinks_shared::service::{
    Body as BodyTranslation, ExponentialBackoffPolicy, ReqwestServiceLayer,
};
use nonzero::nonzero;
use secrecy::{ExposeSecret, SecretString};
use snafu::{prelude::*, Backtrace};
use tap::Pipe;
use tower::{retry::backoff::MakeBackoff, Service, ServiceBuilder};
use tower_gcra::direct::Layer as GovernorLayer;
use tower_http::set_header::SetRequestHeaderLayer;

use crate::service::RequestBody;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    #[snafu(display("invalid client backoff configuration; this is a bug & should be reported"))]
    Backoff {
        source: tower::retry::backoff::InvalidBackoff,
        backtrace: Backtrace,
    },
    #[snafu(display("the API key cannot be interpreted as a header value"))]
    InvalidToken {
        source: http::header::InvalidHeaderValue,
        backtrace: Backtrace,
    },
}

pub type Result<T> = StdResult<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        client creation                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Create an [indielinks] client
///
/// [indielinks]: ../indielinksd/index.html
///
/// Return a [reqwest::Client] wrapped in tower layers that will:
///
/// - set Authorization, User-Agent, and Accept headers on all outgoing requests
/// - rate-limit requests according to the `rate_limit` argument
/// - retry failed requests with exponential backoff & a maximum of three attempts
// Might be nice to make the backoff policy configurable, too.
pub fn make_indielinks_client(
    user_agent: HeaderValue,
    token: Option<&SecretString>,
    requests_per_minute: Option<NonZero<u32>>,
) -> Result<
    impl Service<
            http::Request<RequestBody>,
            Response = http::Response<Bytes>,
            // This 👇 is a happy accident: everything in our stack simply passes their inner
            // `Service`'s error on, all the way down to `ReqwestService`. If I install a timeout
            // layer, however, this will change.
            Error = indielinks_shared::service::Error,
        > + Clone,
> {
    let token = token
        .map(|token| HeaderValue::from_str(&format!("Bearer {}", token.expose_secret())))
        .transpose()
        .context(InvalidTokenSnafu)?;

    ServiceBuilder::new()
        // Only set the Authorization if we have a token
        .layer(SetRequestHeaderLayer::overriding(AUTHORIZATION, token))
        .layer(SetRequestHeaderLayer::overriding(USER_AGENT, user_agent))
        .layer(SetRequestHeaderLayer::overriding(
            ACCEPT,
            HeaderValue::from_static("application/json"),
        ))
        .layer(tower::retry::RetryLayer::new(ExponentialBackoffPolicy {
            backoff: tower::retry::backoff::ExponentialBackoffMaker::new(
                // I need to add these to config
                Duration::from_secs(1),
                Duration::from_secs(3),
                10.0,
                tower::util::rng::HasherRng::new(),
            )
            .context(BackoffSnafu)?
            .make_backoff(),
            num_attempts: 3,
        }))
        .layer(GovernorLayer::default(Quota::per_minute(
            requests_per_minute.unwrap_or(nonzero!(120u32)),
        )))
        // Later: add some instrumentation? For debugging? Maybe add a timeout layer?
        .layer(ReqwestServiceLayer::new(BodyTranslation))
        .service(reqwest::Client::new())
        .pipe(Ok)
}
