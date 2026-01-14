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

//! # Assorted http utilities
//!
//! This is a low-level module containing assorted HTTP-related utilities that don't depend on much
//! of anything else.

use std::{convert::Infallible, ops::Deref};

use axum::Json;
use http::{header::HOST, Request};
use indielinks_shared::origin::NetLoc;
use itertools::Itertools;
use opentelemetry::KeyValue;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, ResultExt, Snafu};
use tap::Pipe;
use tower::{Layer, Service};
use tower_gcra::keyed::KeyExtractor;
use tracing::{debug, error, Level};

use crate::define_metric;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        Error Responses                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A serializable struct for use in HTTP error responses
///
/// This is intended to be used in the [IntoResponse] implementations for whatever error type
/// an axum handler is using.
///
/// [IntoResponse]: https://docs.rs/axum/latest/axum/response/trait.IntoResponse.html
///
/// This may be a violation of the YNGNI! principle, but I'd like to return a JSON body for errors.
/// I can't see a way to enforce the rule that all axum handlers do this, but I can at least
/// setup a standard representation of an error response.
#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorResponseBody {
    pub error: String,
}

impl axum::response::IntoResponse for ErrorResponseBody {
    fn into_response(self) -> axum::response::Response {
        Json(self).into_response()
    }
}

#[allow(type_alias_bounds)]
pub type Result<T: axum::response::IntoResponse> = std::result::Result<T, ErrorResponseBody>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to interpret a header value as a UTF-8 string: {source}"))]
    HeaderValue {
        source: http::header::ToStrError,
        backtrace: Backtrace,
    },
    #[snafu(display("Request to {path} unauthorized"))]
    Unauthorized { path: String, backtrace: Backtrace },
    #[snafu(display("{value} is not supported as an Accept header value"))]
    UnsupportedAccept { value: String, backtrace: Backtrace },
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                modeling "Accept" header values                                 //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Supported values for the request Accept header
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Accept {
    ActivityPub,
    Html,
}

impl std::fmt::Display for Accept {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Accept::Html => write!(f, "text/html"),
            Accept::ActivityPub => write!(f, "application/activity+json"),
        }
    }
}

impl Accept {
    /// Lookup the header value corresponding to the "Accept" header in a [HeaderMap], defaulting to
    /// [Accept::Html]. If there are more than one "Accept" headers, only the first will be
    /// examined. If the value specifies a a MIME type not supported by indielinks, fail.
    ///
    /// [HeaderMap]: https://docs.rs/http/latest/http/header/struct.HeaderMap.html
    pub fn lookup_from_header_map(headers: &http::HeaderMap) -> std::result::Result<Accept, Error> {
        headers
            .get(http::header::ACCEPT)
            .map(http::HeaderValue::to_str)
            .transpose()
            .context(HeaderValueSnafu)?
            .map(|s| {
                tracing::warn!("s is ``{}''", s);
                if s.contains("application/ld+json") || s.contains("application/activity+json") {
                    Ok(Accept::ActivityPub)
                } else if s == "text/html" || s == "*/*" {
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum SameSite {
    Strict,
    Lax,
    None,
}

impl std::fmt::Display for SameSite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                SameSite::Strict => "Strict",
                SameSite::Lax => "Lax",
                SameSite::None => "None",
            }
        )
    }
}

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
    fn extract(&self, req: &Request<B>) -> std::result::Result<Self::Key, Self::Error> {
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
