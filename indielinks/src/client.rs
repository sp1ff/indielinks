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

use std::ops::Deref;

use bytes::Bytes;
use http::{HeaderName, HeaderValue, header::USER_AGENT};
use opentelemetry::KeyValue;
use pin_project::pin_project;
use snafu::{Backtrace, ResultExt, Snafu};
use tap::Pipe;
use tower::{
    Layer, Service, ServiceBuilder,
    buffer::BufferLayer,
    limit::RateLimitLayer,
    retry::{
        Retry, RetryLayer,
        backoff::{ExponentialBackoffMaker, MakeBackoff},
    },
};
use tower_http::set_header::{SetRequestHeader, SetRequestHeaderLayer};
use tracing::{Level, debug, error};

use indielinks_shared::{
    Username,
    service::{
        Body, ExponentialBackoffParameters, ExponentialBackoffPolicy, RateLimit,
        ReqwestServiceFuture, ReqwestServiceLayer,
    },
};

use crate::{
    ap_entities::make_key_id,
    authn::{AddSha256DigestIfNotPresent, AddSha256DigestIfNotPresentLayer, compute_signature},
    define_metric,
    entities::User,
    origin::Origin,
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
    {
        let host = request.uri().host().unwrap_or("localhost").to_owned();
        let span = tracing::span!(Level::DEBUG, "indielinks-client-call");
        let _ = span.enter();
        debug!("Sending request to {host}");
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
                        "Response from {} returned with status {}",
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

/// Add an Activity Pub signature to `request` if there is a `(User, Origin)` pair in the request
/// extensions
fn maybe_add_signature<B: AsRef<[u8]>>(request: &http::Request<B>) -> Option<HeaderValue> {
    fn maybe_add_signature1<B: AsRef<[u8]>>(
        request: &http::Request<B>,
        user: &User,
        origin: &Origin,
    ) -> Result<HeaderValue> {
        compute_signature::<B>(
            request,
            make_key_id(user.username(), origin)
                .context(KeyIdSnafu {
                    username: user.username(),
                })?
                .as_str(),
            user.priv_key().as_ref(),
        )
        .context(AuthnSnafu)
    }

    request
        .extensions()
        .get::<(User, Origin)>()
        .map(|(user, origin)| maybe_add_signature1(request, user, origin))
        .and_then(Result::ok)
}

// In indielinks-client, I make the client type generic, with a type constraint like:
//
//     impl Service<
//         http::Request<Bytes>,
//         Response = http::Response<Bytes>,
//         Error = Box<dyn std::error::Error + Send + Sync>,
//     > + Clone,
//
// In this crate, however, I find myself naming the type more frequently, so I'm going with a type
// alias. This is also inconvenient, but better than making every type and function that deals with
// our client type generic.
//
// If you need to update this, say due to adding or removing a layer in `make_client()`, just do a
// `cargo build`; the `make_client()` return type won't type-check, but the compiler will write the
// expected type to a text file in the build directory (it will be the second one)-- just copy it
// from there over this one:
pub type ClientType = AddSha256DigestIfNotPresent<
    SetRequestHeader<
        SetRequestHeader<
            Retry<
                ExponentialBackoffPolicy,
                tower::buffer::Buffer<
                    http::Request<bytes::Bytes>,
                    InstrumentedServiceFuture<
                        ReqwestServiceFuture<reqwest::Client, indielinks_shared::service::Body>,
                    >,
                >,
            >,
            http::HeaderValue,
        >,
        // A function item is a special zero-sized type tied to exactly one function in your program.
        // This is useful because it compiles down to just hard-coding the correct function when the
        // function item is used, which is more efficient than calling a dynamic function pointer. In
        // some cases, the compiler will automatically cast function items to function pointers
        for<'a> fn(&'a http::Request<bytes::Bytes>) -> std::option::Option<http::HeaderValue>,
    >,
>;

/// Build a [tower] [Service] based on [reqwest::Client] that will:
///     - add a SHA-256 digest, if it's not present
///     - add a "draft Cavage" HTTP signature
///     - set the User Agent header
///     - retry failed requests
///     - rate limit requests
///     - instrument all requests
/// Request & response bodies are modelled as [Bytes]. Rate-limiting & exponential backoff are configurable.
pub fn make_client(
    user_agent: &str,
    rate_limit: &RateLimit,
    backoff_parameters: &ExponentialBackoffParameters,
) -> Result<ClientType> {
    ServiceBuilder::new()
        // Apply the signing middleware first, so that the outgoing request is only signed once. Then
        // apply the retry middleware, and finally the instrumentation (so that metrics will be emitted
        // on each retry):
        //
        //                          requests
        //                              |
        //                              v
        // +-----------------    Add SHA-256 digest     -----------------+
        // | +--------------- Add ActivityPub signature ---------------+ |
        // | | +-------------   Set User-Agent header   -------------+ | |
        // | | | +-----------     retry on failure      -----------+ | | |
        // | | | | +---------       Buffer layer        ---------+ | | | |
        // | | | | | +-------      RateLimit layer      -------+ | | | | |
        // | | | | | | +-----      instrumentation      -----+ | | | | | |
        // | | | | | | | +---       Reqwest layer       ---+ | | | | | | |
        // | | | | | | | |                                 | | | | | | | |
        // | | | | | | | |             remote              | | | | | | | |
        // | | | | | | | |                                 | | | | | | | |
        // | | | | | | | +-->       Reqwest layer       <--+ | | | | | | |
        // | | | | | | +---->      instrumentation      <----+ | | | | | |
        // | | | | | +------>      RateLimit layer      <------+ | | | | |
        // | | | | +-------->       Buffer layer        <--------+ | | | |
        // | | | +---------->     retry on failure      <----------+ | | |
        // | | +------------>   Set User-Agent header   <------------+ | |
        // | +--------------> Add ActivityPub signature <--------------+ |
        // +---------------->    Add SHA-256 digest     <----------------+
        //                               |
        //                               v
        //                           responses
        .layer(AddSha256DigestIfNotPresentLayer)
        .layer(SetRequestHeaderLayer::if_not_present(
            HeaderName::from_static("signature"),
            maybe_add_signature
                as for<'a> fn(&'a http::Request<bytes::Bytes>) -> Option<HeaderValue>,
        ))
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
        // `RetryLayer` requries that the `Service` it wraps is `Clone`... which `RateLimitLayer` is
        // not. Per https://github.com/tokio-rs/axum/discussions/987#discussioncomment-2678595, we
        // wrap it in a `BufferLayer`. Regrettably, it changes the error type from
        // `indielinks_shared::service::Error` to `Box<Error + Send + Sync>`
        .layer(BufferLayer::<http::Request<Bytes>>::new(1024))
        .layer(RateLimitLayer::new(rate_limit.num, rate_limit.duration))
        .layer(InstrumentedLayer)
        .layer(ReqwestServiceLayer::new(Body))
        .service(reqwest::Client::new())
        .pipe(Ok)
}

#[cfg(test)]
mod test {

    use super::*;

    use http::Method;
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
            &RateLimit::default(),
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
