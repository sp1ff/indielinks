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

//! # indielinks clients

use std::{
    borrow::Cow,
    num::NonZeroU32,
    result::Result as StdResult,
    time::{self, Duration},
};

use bytes::Bytes;
use governor::Quota;
use http::{
    header::{ACCEPT, AUTHORIZATION, USER_AGENT},
    HeaderValue,
};
use indielinks_shared::service::{
    Body as BodyTranslation, BoxedError, ExponentialBackoffPolicy, ReqwestServiceLayer,
};
use nonzero::nonzero;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use snafu::{prelude::*, Backtrace};
use tap::Pipe;
use tower::{
    retry::backoff::MakeBackoff, timeout::TimeoutLayer, util::BoxCloneSyncService, ServiceBuilder,
};
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

pub type ClientType =
    BoxCloneSyncService<http::Request<RequestBody>, http::Response<Bytes>, BoxedError>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Configuration {
    // the minimum backoff time when retrying
    pub min_backoff: time::Duration,
    // the maximum backoff time when retrying
    pub max_backoff: time::Duration,
    // jitter introduced into the exponential progression; must be in [0, 100]
    pub backoff_jitter: f64,
    // the maximum number of retries to be attempted (so the total number of requests will be this
    // value plus one)
    pub num_retry_attempts: usize,
    // the maximum number of requests per minute allowed (metered via GCRA)
    pub requests_per_minute: NonZeroU32,
    // timeout applied to any single request
    pub request_timeout: time::Duration,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(5),
            backoff_jitter: 10.0,
            num_retry_attempts: 3,
            requests_per_minute: nonzero!(120u32),
            request_timeout: Duration::from_secs(3),
        }
    }
}

/// Create an [indielinks] client
///
/// [indielinks]: ../indielinksd/index.html
///
/// Return a [reqwest::Client] wrapped in tower layers that will:
///
/// - set Authorization, User-Agent, and Accept headers on all outgoing requests
/// - rate-limit requests according to the `rate_limit` argument
/// - retry failed requests with exponential backoff & a maximum of three attempts
/// - apply a timeout
pub fn make_indielinks_client(
    user_agent: HeaderValue,
    token: Option<&SecretString>,
    configuration: Option<&Configuration>,
) -> Result<ClientType> {
    let token = token
        .map(|token| HeaderValue::from_str(&format!("Bearer {}", token.expose_secret())))
        .transpose()
        .context(InvalidTokenSnafu)?;

    let configuration =
        configuration.map_or_else(|| Cow::Owned(Configuration::default()), Cow::Borrowed);

    // The indielinks client stack: set headers first, since that only needs to be done once. Then:
    // the retry layer. Inside the retries, the rate-limiting & timeout layers.
    //
    //                            requests
    //                               |
    //                               v
    //     +-------------- Set Authorization header --------------+
    //     | +------------   Set User-Agent header  ------------+ |
    //     | | +----------     Set Accept header    ----------+ | |
    //     | | | +--------     Retry on failure     --------+ | | |
    //     | | | | +------   Rate-limiting layer    ------+ | | | |
    //     | | | | | +----     Timeout layer        ----+ | | | | |
    //     | | | | | | +--     Reqwest layer        --+ | | | | | |
    //     | | | | | | |                              | | | | | | |
    //     | | | | | | |           remote             | | | | | | |
    //     | | | | | | |                              | | | | | | |
    //     | | | | | | +->     Reqwest layer        <-+ | | | | | |
    //     | | | | | +--->     Timeout layer        <---+ | | | | |
    //     | | | | +----->   Rate-limiting layer    <-----+ | | | |
    //     | | | +------->    Retry on failure      <-------+ | | |
    //     | | +--------->   Set Accept header      <---------+ | |
    //     | +----------->  Set User-Agent header   <-----------+ |
    //     +-------------> Set Authorization header <-------------+
    //                               |
    //                               v
    //                           responses
    ServiceBuilder::new()
        // Set headers at the outset (so this is done only once)-- only set the Authorization if we
        // have a token
        .layer(SetRequestHeaderLayer::overriding(AUTHORIZATION, token))
        .layer(SetRequestHeaderLayer::overriding(USER_AGENT, user_agent))
        .layer(SetRequestHeaderLayer::overriding(
            ACCEPT,
            HeaderValue::from_static("application/json"),
        ))
        // Next, apply the retry layer; make sure this is "outside" the rate limiter so that our
        // retries get rate-limited properly.
        .layer(tower::retry::RetryLayer::new(ExponentialBackoffPolicy {
            backoff: tower::retry::backoff::ExponentialBackoffMaker::new(
                configuration.min_backoff,
                configuration.max_backoff,
                configuration.backoff_jitter,
                tower::util::rng::HasherRng::new(),
            )
            .context(BackoffSnafu)?
            .make_backoff(),
            num_attempts: configuration.num_retry_attempts,
        }))
        .layer(GovernorLayer::default(Quota::per_minute(
            configuration.requests_per_minute,
        )))
        // `TimeoutLayer`, as per usual for tower middleware, returns an error of type
        // `tower::BoxError`, which is just an alias for `Box<dyn Error + Send + Sync>`.
        // Surprisingly, `Box<dyn Error...>` does NOT implement `Error`, which is a problem because,
        // the services above expect the `Err` variant of our service return value to, you know,
        // *implement Error*.
        //
        // This situation is documented in <https://github.com/rust-lang/rust/issues/68185>: Box<dyn
        // Error...> does not implement `Error` (!). As a workaround, I created a newtype to wrap
        // the boxed error & manually implemented `Error` on that.
        .map_err(BoxedError)
        .layer(TimeoutLayer::new(configuration.request_timeout))
        // Later: add some instrumentation? This is intended to be used in CLIs, so perhaps not
        // metrics, but log messages?
        .layer(ReqwestServiceLayer::new(BodyTranslation))
        .service(reqwest::Client::new())
        .pipe(ClientType::new)
        .pipe(Ok)
}

#[cfg(test)]
mod test {
    use super::*;

    use http::Request;
    use tower::timeout::error::Elapsed;
    use tower::{Service, ServiceExt};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_that_timeouts_are_retried() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/slow"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(250)))
            .expect(4)
            .mount(&server)
            .await;

        let configuration = Configuration {
            request_timeout: time::Duration::from_millis(100),
            ..Default::default()
        };

        let mut client = make_indielinks_client(
            HeaderValue::from_static("indielinks-client integration tests"),
            None,
            Some(&configuration),
        )
        .expect("client should build");

        let request = Request::builder()
            .method("GET")
            .uri(&format!("{}/slow", server.uri()))
            .body(RequestBody::None)
            .expect("the request should build");

        let response = client
            .ready()
            .await
            .expect("the client should become ready")
            .call(request)
            .await;

        eprintln!("{response:#?}");
        assert!(response.is_err());
        assert!(response.unwrap_err().0.downcast_ref::<Elapsed>().is_some());
    }
}
