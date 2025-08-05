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

use std::sync::Arc;

use http::{HeaderValue, Method};
use opentelemetry::KeyValue;
use reqwest::IntoUrl;
use reqwest_middleware::{ClientWithMiddleware, Middleware, RequestBuilder};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use snafu::{Backtrace, ResultExt, Snafu};
use tap::Pipe;
use tracing::error;

use indielinks_shared::Username;

use crate::{
    ap_entities::make_key_id,
    authn::{ensure_sha_256, sign_request},
    counter_add,
    entities::User,
    metrics::{self, Sort},
    origin::Origin,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create a KeyId for user {username}: {source}"))]
    KeyId {
        username: Username,
        source: crate::ap_entities::Error,
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
//                                     InstrumentedMiddleware                                     //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// [reqwest-middleware] [Middleware] that emits [indielinks] [metrics] for client requests
///
/// [reqwest-middleware]: reqwest_middleware
/// [Middleware]: reqwest_middleware::Middleware
/// [indielinks]: crate
/// [metrics]: crate::metrics
#[derive(Clone)]
pub struct InstrumentedMiddleware {
    instruments: Arc<metrics::Instruments>,
}

impl InstrumentedMiddleware {
    pub fn new(instruments: Arc<metrics::Instruments>) -> InstrumentedMiddleware {
        InstrumentedMiddleware { instruments }
    }
}

inventory::submit! { metrics::Registration::new("client.requests",                Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("client.errors",                  Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("client.responses.informational", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("client.responses.success",       Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("client.responses.redirect",      Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("client.responses.client_error",  Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("client.responses.server_error",  Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("client.responses.unknown",       Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("client.responses.errors",        Sort::IntegralCounter) }

#[async_trait::async_trait]
impl Middleware for InstrumentedMiddleware {
    async fn handle(
        &self,
        req: reqwest::Request,
        extensions: &mut http::Extensions,
        next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let attrs = req
            .url()
            .host()
            .map(|host| vec![KeyValue::new("host".to_owned(), host.to_string())])
            .unwrap_or_default();
        counter_add!(self.instruments, "client.requests", 1, attrs.as_slice());
        let res = next.run(req, extensions).await;
        match &res {
            Ok(rsp) => {
                counter_add!(
                    self.instruments,
                    &format!(
                        "client.responses.{}",
                        match rsp.status().as_u16() {
                            100..=199 => "informational",
                            200..=299 => "success",
                            300..=399 => "redirect",
                            400..=499 => "client_error",
                            500..=599 => "server_error",
                            _ => "unknown",
                        }
                    ),
                    1,
                    attrs.as_slice()
                );
            }
            Err(err) => {
                error!("While sending a request, got {}", err);
                counter_add!(self.instruments, "client.errors", 1, attrs.as_slice());
            }
        }
        res
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       SigningMiddleware                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// [reqwest-middleware] [Middleware] that signs client requests
///
/// [reqwest-middleware]: reqwest_middleware
/// [Middleware]: reqwest_middleware::Middleware
///
/// In order to sign the request, the [User] on whose behalf this request is being sent needs to be
/// in the extensions.
// Not sure I want to implement signing as a middleware, but I'm attracted by the elegance-- if
// there's a `User` in the extensions, sign it.
pub struct SigningMiddleware;

#[async_trait::async_trait]
impl Middleware for SigningMiddleware {
    async fn handle(
        &self,
        req: reqwest::Request,
        extensions: &mut http::Extensions,
        next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        // Sign if we can get a `User` out of the extensions:
        if let Some((user, origin)) = extensions.get::<(User, Origin)>() {
            // `picky-rs` really wants an http::Request...
            let http_req: http::Request<reqwest::Body> = req
                .try_into()
                .map_err(reqwest_middleware::Error::middleware)?;
            // & now ensure we've got a SHA-256 content digest in the request.
            let http_req =
                ensure_sha_256(http_req).map_err(reqwest_middleware::Error::middleware)?;
            // With that, we can compute a signature...
            let (http_req, http_signature) = sign_request(
                http_req,
                make_key_id(user.username(), origin)
                    .map_err(reqwest_middleware::Error::middleware)?
                    .as_str(),
                user.priv_key().as_ref(),
            )
            .map_err(reqwest_middleware::Error::middleware)?;

            let (mut parts, body) = http_req.into_parts();
            parts.headers.append(
                "Signature",
                HeaderValue::from_str(&http_signature.to_string()[10..])
                    .map_err(reqwest_middleware::Error::middleware)?,
            );

            next.run(
                http::Request::from_parts(parts, body)
                    .try_into()
                    .map_err(reqwest_middleware::Error::middleware)?,
                extensions,
            )
            .await
        } else {
            next.run(req, extensions).await
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       Utility Functions                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Create a [ClientWithMiddleware] that is instrumented, will conditionally sign outgoing requests
/// (if there's a [User] in the request extensions) and automatically retry.
pub fn make_client(
    user_agent: &str,
    instruments: Arc<metrics::Instruments>,
    max_retries: Option<u32>,
) -> Result<ClientWithMiddleware> {
    reqwest_middleware::ClientBuilder::new(
        reqwest::ClientBuilder::new()
            .user_agent(user_agent)
            .build()
            .context(ReqwestClientSnafu)?,
    )
    // Apply the signing middleware first, so that the outgoing request is only signed once. Then
    // apply the retry middleware, and finally the instrumentation (so that metrics will be emitted
    // on each retry):
    //
    //                    requests
    //                       |
    //                       v
    // +---------      SigningMiddleware     ---------+
    // | +-------  RetryTransientMiddleware  -------+ |
    // | | +-----   InstrumentedMiddleware   -----+ | |
    // | | |                                      | | |
    // | | |               remote                 | | |
    // | | |                                      | | |
    // | | +-----   InstrumentedMiddleware   -----+ | |
    // | +-------  RetryTransientMiddleware  -------+ |
    // +---------      SigningMiddleware     ---------+
    //                       |
    //                       v
    //                   responses
    .with(SigningMiddleware)
    .with(RetryTransientMiddleware::new_with_policy(
        ExponentialBackoff::builder().build_with_max_retries(max_retries.unwrap_or(3)),
    ))
    .with(InstrumentedMiddleware { instruments })
    .build()
    .pipe(Ok)
}

/// Begin building a [Request] with the guarantee that the client's initializer stack as been run. If given,
/// insert the [User] into the request extensions.
///
/// [Request]: reqwest::Request
pub fn request_builder<U: IntoUrl>(
    client: &ClientWithMiddleware,
    method: Method,
    url: U,
    user: Option<(&User, &Origin)>,
) -> RequestBuilder {
    let mut builder = client.request(method, url);
    if let Some((user, origin)) = user {
        builder.extensions().insert((user.clone(), origin.clone()));
    }
    builder
}
