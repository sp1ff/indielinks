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

//! # indielinks as an HTTP client
//!
//! More documentation [here].
//!
//! [here]: crate::_docs#indielinks-as-client
//!
//! ## Digest Headers
//!
//! ActivityPub uses HTTP signatures to authenticate messages. While HTTP signatures have gone
//! through a number of revisions, the implementation in wide use across the Fediverse is the
//! "[draft cavage]" specification. This, in turn, uses the `Digest` header as defined in
//! [RFC-3230]. The reader may do a double-take here, as this is similar to but not the same as the
//! `Content-Digest` header introduced in [RFC-9530] (which obsoleted [RFC-3230])).
//!
//! [draft cavage]: https://datatracker.ietf.org/doc/html/draft-cavage-http-signatures-12
//! [RFC-3230]: https://datatracker.ietf.org/doc/html/rfc3230
//! [RFC-9530]: https://www.ietf.org/archive/id/draft-ietf-httpbis-digest-headers-12.html#name-the-content-digest-field

use std::{convert::Infallible, fmt::Debug, hash::Hash, ops::Deref, result::Result as StdResult};

use bytes::Bytes;
use either::Either;
use governor::{clock::Clock, middleware::RateLimitingMiddleware, state::StateStore, RateLimiter};
use http::{
    header::{HOST, USER_AGENT},
    HeaderName, HeaderValue, Request,
};
use itertools::Itertools;
use opentelemetry::KeyValue;
use pin_project::pin_project;
use snafu::{Backtrace, ResultExt, Snafu};
use tap::Pipe;
use tower::{
    retry::{
        backoff::{ExponentialBackoffMaker, MakeBackoff},
        RetryLayer,
    },
    Layer, Service, ServiceBuilder,
};
use tower_gcra::keyed::{KeyExtractor, Layer as GovernorLayer};
use tower_http::set_header::SetRequestHeaderLayer;
use tracing::{debug, error, Level};

use indielinks_shared::{
    entities::{UserPrivateKey, Username},
    origin::{NetLoc, Origin},
    service::{Body, ExponentialBackoffParameters, ExponentialBackoffPolicy, ReqwestServiceLayer},
};

use crate::{
    ap_entities::{make_instance_actor_key_id, make_key_id},
    authn::{compute_signature, AddSha256DigestIfNotPresentLayer},
    define_metric,
    entities::User,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Authentication fialure: {source}"))]
    Authn {
        source: crate::authn::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid backoff configuration: {source}"))]
    Backoff {
        source: tower::retry::backoff::InvalidBackoff,
        backtrace: Backtrace,
    },
    #[snafu(display("While creating the key ID for the instance actor, {source}"))]
    InstanceActor { source: crate::ap_entities::Error },
    #[snafu(display("Failed to create a KeyId for user {username}: {source}"))]
    KeyId {
        username: Username,
        #[snafu(source(from(crate::ap_entities::Error, Box::new)))]
        source: Box<crate::ap_entities::Error>,
    },
    #[snafu(display("Failed to create an HTTP client: {source}"))]
    ReqwestClient {
        source: reqwest::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("The generate HTTP signature was not a valid header value: {source}"))]
    SignatureToString {
        source: http::header::InvalidHeaderValue,
    },
}

type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       InstrumentedService                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "client.requests",                client_requests,                Sort::IntegralCounter }
define_metric! { "client.errors",                  client_errors,                  Sort::IntegralCounter }
define_metric! { "client.responses.informational", client_responses_informational, Sort::IntegralCounter }
define_metric! { "client.responses.success",       client_responses_success,       Sort::IntegralCounter }
define_metric! { "client.responses.redirect",      client_responses_redirect,      Sort::IntegralCounter }
define_metric! { "client.responses.client_error",  client_responses_client_error,  Sort::IntegralCounter }
define_metric! { "client.responses.server_error",  client_responses_server_error,  Sort::IntegralCounter }
define_metric! { "client.responses.unknown",       client_responses_unknown,       Sort::IntegralCounter }
define_metric! { "client.responses.errors",        client_responses_errors,        Sort::IntegralCounter }

/// A [Future] that wraps an inner future associated with a service that will log & emit metrics for
/// each request
///
/// [Future]: std::future::Future
// I suppose I could have used tower_http::TraceLayer, but trying to communicate state among the
// handlers seemed more complex than just implementing my own Service & associated Future.
#[pin_project]
pub struct InstrumentedServiceFuture<InnerFut> {
    host: String,
    span: tracing::Span,
    #[pin]
    inner: InnerFut,
}

impl<InnerFut> InstrumentedServiceFuture<InnerFut> {
    pub fn new<ReqBody, S>(
        service: &mut S,
        request: http::Request<ReqBody>,
    ) -> InstrumentedServiceFuture<<S as Service<http::Request<ReqBody>>>::Future>
    where
        S: Service<http::Request<ReqBody>>,
        ReqBody: std::fmt::Debug,
    {
        let host = request.uri().host().unwrap_or("localhost").to_owned();
        let span = tracing::span!(Level::DEBUG, "indielinks-client-call");
        let _ = span.enter();
        debug!("Sending request to {host}: {request:?}");
        client_requests.add(1, &[KeyValue::new("host", host.clone())]);
        InstrumentedServiceFuture {
            host,
            span,
            inner: service.call(request),
        }
    }
}

impl<RspBody, E, InnerFut> std::future::Future for InstrumentedServiceFuture<InnerFut>
where
    InnerFut: std::future::Future<Output = std::result::Result<http::Response<RspBody>, E>>,
    E: std::error::Error,
    RspBody: std::fmt::Debug,
{
    type Output = std::result::Result<http::Response<RspBody>, E>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        let _guard = this.span.enter();
        match this.inner.poll(cx) {
            std::task::Poll::Ready(rsp) => match rsp {
                Ok(rsp) => {
                    let instrument = match rsp.status().as_u16() {
                        100..=199 => client_responses_informational.deref(),
                        200..=299 => client_responses_success.deref(),
                        300..=399 => client_responses_redirect.deref(),
                        400..=499 => client_responses_client_error.deref(),
                        500..=599 => client_responses_server_error.deref(),
                        _ => client_responses_unknown.deref(),
                    };
                    instrument.add(1, &[KeyValue::new("host", this.host.clone())]);
                    debug!(
                        "Response {rsp:?} from {} returned with status {}",
                        this.host,
                        rsp.status()
                    );
                    std::task::Poll::Ready(Ok(rsp))
                }
                Err(err) => {
                    error!("While sending a request to {}, got {}", this.host, err);
                    client_errors.add(1, &[KeyValue::new("host", this.host.clone())]);
                    std::task::Poll::Ready(Err(err))
                }
            },
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

#[derive(Clone, Debug)]
pub struct InstrumentedService<S> {
    inner: S,
}

impl<S, ReqBody, RspBody> Service<http::Request<ReqBody>> for InstrumentedService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<RspBody>>,
    <S as Service<http::Request<ReqBody>>>::Error: std::error::Error,
    RspBody: std::fmt::Debug,
    ReqBody: std::fmt::Debug,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future = InstrumentedServiceFuture<S::Future>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        InstrumentedServiceFuture::<<S as Service<http::Request<ReqBody>>>::Future>::new(
            &mut self.inner,
            request,
        )
    }
}

#[derive(Clone, Debug)]
pub struct InstrumentedLayer;

impl<S> Layer<S> for InstrumentedLayer {
    type Service = InstrumentedService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        InstrumentedService { inner }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Add an Activity Pub signature to `request` if there is an `(Either<User, UserPrivateKey>,
/// Origin)` pair in the request extensions.
fn maybe_add_signature<B: AsRef<[u8]>>(request: &http::Request<B>) -> Option<HeaderValue> {
    fn maybe_add_signature1<B: AsRef<[u8]>>(
        request: &http::Request<B>,
        principal: &Either<User, UserPrivateKey>,
        origin: &Origin,
    ) -> Result<HeaderValue> {
        match principal {
            Either::Left(user) => compute_signature::<B>(
                request,
                make_key_id(user.username(), origin)
                    .context(KeyIdSnafu {
                        username: user.username(),
                    })?
                    .as_str(),
                user.priv_key().as_ref(),
            ),
            Either::Right(private_key) => compute_signature::<B>(
                request,
                make_instance_actor_key_id(origin)
                    .context(InstanceActorSnafu)?
                    .as_str(),
                private_key.as_ref(),
            ),
        }
        .context(AuthnSnafu)
    }

    request
        .extensions()
        .get::<(Either<User, UserPrivateKey>, Origin)>()
        .map(|(principal, origin)| maybe_add_signature1(request, principal, origin))
        .and_then(Result::ok)
}

fn add_date<B: AsRef<[u8]>>(_request: &http::Request<B>) -> Option<HeaderValue> {
    Some(
        HeaderValue::from_str(
            chrono::Utc::now()
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string()
                .as_ref(),
        ).unwrap(/* known good */),
    )
}

fn add_host<B: AsRef<[u8]>>(request: &http::Request<B>) -> Option<HeaderValue> {
    // Take care to include the port, if present.
    let netloc: Option<NetLoc> = request.uri().try_into().ok();
    netloc
        .map(|netloc| HeaderValue::from_str(&format!("{netloc}")))
        .transpose()
        .unwrap_or(None)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum HostKey {
    Null,
    Host(NetLoc),
}

#[derive(Clone)]
pub struct HostExtractor;

impl<B> KeyExtractor<Request<B>> for HostExtractor {
    type Key = HostKey;
    // Extracting the key from a `Request` *is* fallible, but if you return the `Err` variant, the
    // request as a whole will be failed, so I'll never fail this operation:
    type Error = Infallible;
    fn extract(&self, req: &Request<B>) -> StdResult<Self::Key, Self::Error> {
        // Try the "Host" header first, then fall-back to the URI.
        req.headers()
            .get_all(HOST)
            .iter()
            .at_most_one()
            .ok()
            .flatten()
            .and_then(|header_value| header_value.to_str().ok())
            .and_then(|text| text.parse::<NetLoc>().ok())
            .or_else(|| req.uri().try_into().ok())
            .map(HostKey::Host)
            .unwrap_or(HostKey::Null)
            .pipe(Ok)
    }
}

/// Build a [tower] [Service] based on [reqwest::Client]
///
/// This function starts with a [reqwest::Client] and then:
/// - adds the Host header, if it's not present
/// - if the caller has requested ActivityPub support:
///     - adds Date & SHA-256 digest headers, if they're not present
///     - add a "draft Cavage" HTTP signature
/// - set the User Agent header
/// - retry failed requests
/// - rate limit outgoing requests
/// - instrument all requests
///
/// Request & response bodies are modelled as [Bytes]. Rate-limiting & exponential backoff are configurable.
pub fn make_client<KE, S, C, MW>(
    user_agent: &str,
    support_ap: bool,
    key_extractor: KE,
    rate_limiter: RateLimiter<<KE as KeyExtractor<Request<Bytes>>>::Key, S, C, MW>,
    backoff_parameters: &ExponentialBackoffParameters,
) -> Result<crate::client_types::GenericClientType<KE, S, C, MW>>
where
    KE: KeyExtractor<Request<Bytes>> + Clone,
    <KE as KeyExtractor<Request<Bytes>>>::Key: Clone + Eq + Hash,
    S: StateStore<Key = <KE as KeyExtractor<Request<Bytes>>>::Key>,
    C: Clock,
    MW: RateLimitingMiddleware<<KE as KeyExtractor<Request<Bytes>>>::Key, C::Instant>,
{
    let (add_date, add_digest, add_signature) = if support_ap {
        (
            Some(SetRequestHeaderLayer::if_not_present(
                http::header::DATE,
                add_date as for<'a> fn(&'a http::Request<bytes::Bytes>) -> Option<HeaderValue>,
            )),
            Some(AddSha256DigestIfNotPresentLayer),
            Some(SetRequestHeaderLayer::if_not_present(
                HeaderName::from_static("signature"),
                maybe_add_signature
                    as for<'a> fn(&'a http::Request<bytes::Bytes>) -> Option<HeaderValue>,
            )),
        )
    } else {
        (None, None, None)
    };

    ServiceBuilder::new()
        // Apply the signing middleware first, so that the outgoing request is only signed once. Then
        // apply the retry middleware, and finally the instrumentation (so that metrics will be emitted
        // on each retry):
        //
        //                              requests
        //                                  |
        //                                  v
        // +---------------------      Add Date header      ---------------------+
        // | +-------------------      Add Host header      -------------------+ |
        // | | +-----------------    Add SHA-256 digest     -----------------+ | |
        // | | | +--------------- Add ActivityPub signature ---------------+ | | |
        // | | | | +-------------   Set User-Agent header   -------------+ | | | |
        // | | | | | +-----------     retry on failure      -----------+ | | | | |
        // | | | | | | | +-------    rate-limiting layer    -------+ | | | | | | |
        // | | | | | | | | +-----      instrumentation      -----+ | | | | | | | |
        // | | | | | | | | | +---       Reqwest layer       ---+ | | | | | | | | |
        // | | | | | | | | | |                                 | | | | | | | | | |
        // | | | | | | | | | |             remote              | | | | | | | | | |
        // | | | | | | | | | |                                 | | | | | | | | | |
        // | | | | | | | | | +-->       Reqwest layer       <--+ | | | | | | | | |
        // | | | | | | | | +---->      instrumentation      <----+ | | | | | | | |
        // | | | | | | | +------>    rate-limiting layer    <------+ | | | | | | |
        // | | | | | +---------->     retry on failure      <----------+ | | | | |
        // | | | | +------------>   Set User-Agent header   <------------+ | | | |
        // | | | +--------------> Add ActivityPub signature <--------------+ | | |
        // | | +---------------->    Add SHA-256 digest     <----------------+ | |
        // | +------------------>      Add Host header      <------------------+ |
        // +-------------------->      Add Date header      <--------------------+
        //                                   |
        //                                   v
        //                               responses
        .layer(SetRequestHeaderLayer::if_not_present(
            http::header::HOST,
            add_host as for<'a> fn(&'a http::Request<bytes::Bytes>) -> Option<HeaderValue>,
        ))
        .option_layer(add_date)
        .option_layer(add_digest)
        .option_layer(add_signature)
        .layer(SetRequestHeaderLayer::overriding(
            USER_AGENT,
            HeaderValue::from_str(user_agent).unwrap(/* known good*/),
        ))
        .layer(RetryLayer::new(ExponentialBackoffPolicy {
            backoff: ExponentialBackoffMaker::new(
                *backoff_parameters.lower(),
                *backoff_parameters.upper(),
                backoff_parameters.jitter(),
                tower::util::rng::HasherRng::new(),
            )
            .context(BackoffSnafu)?
            .make_backoff(),
            num_attempts: backoff_parameters.num_attempts(),
        }))
        .layer(GovernorLayer::new_with_limiter(key_extractor, rate_limiter))
        .layer(InstrumentedLayer)
        .layer(ReqwestServiceLayer::new(Body))
        .service(reqwest::Client::new())
        .pipe(Ok)
}

#[cfg(test)]
mod test {

    use super::*;

    use governor::Quota;
    use http::Method;
    use nonzero::nonzero;
    use tower::ServiceExt;
    use wiremock::{Match, Mock, MockServer, Request, ResponseTemplate};

    /// Type on which to hang a [Match] implementation that sanity-checks the incoming request
    struct SimpleChecker;

    impl Match for SimpleChecker {
        fn matches(&self, request: &Request) -> bool {
            debug!("SimpleChecker: {request:?}");
            match request.headers.get("digest") {
                Some(value) => value.to_str().unwrap().starts_with("sha-256="),
                None => false,
            }
        }
    }

    #[tokio::test]
    async fn client_smoke_tests() {
        let mock_server = MockServer::start().await;

        Mock::given(SimpleChecker)
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let mut client = make_client(
            "indielinks unit tests/0.0.1; +sp1ff@pobox.com",
            true,
            HostExtractor,
            RateLimiter::keyed(Quota::per_second(nonzero!(10u32))),
            &ExponentialBackoffParameters::default(),
        )
        .unwrap();

        let request = http::Request::builder()
            .method(Method::GET)
            .uri(&mock_server.uri())
            .body(Bytes::default())
            .unwrap();

        let response = client.ready().await.unwrap().call(request).await.unwrap();

        assert!(response.status() == 200);

        // Later: build out this test suite
    }
}
