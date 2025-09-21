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

//! Client-Side tower Services
//!
//! # Introduction
//!
//! When writing any non-trivial web service, one quickly discovers operations one would like to
//! perform on requests and/or responses that are independent of their destinations. For instance,
//! we may want to log all incoming requests in a uniform way, or set certain headers on all
//! outgoing responses.
//!
//! We colloquially refer to the code that performs such operations as "middleware", and in the Rust
//! ecosystem, the canonical implementation is [tower]. [tower] provides a general set of
//! abstractions for building middleware on top of any request/response-style service, and
//! [tower-http] provides middleware specific to the HTTP protocol.
//!
//! The problem is, the same dynamic occurs on the *client* side. In indielinks, for example we want
//! to respect rate limits, set certain headers on all outgoing requests, and retry failed requests.
//! Regrettably, the situation on the client side is *not* so clear-cut. The problem is that
//! [tower-http] chose to use "the http and http-body crates as the HTTP abstractions." That's not a
//! problem in itself; http and http-body are fine libraries. The issue is that the canonical HTTP
//! client in Rust is [reqwest]. [reqwest], while also a fine library, doesn't use http... at all.
//! It uses its own abstractions for pretty-much everything the http crate provides.
//!
//! This has long [been](https://github.com/seanmonstar/reqwest/issues/1370)
//! [recognized](https://github.com/seanmonstar/reqwest/issues/155) as an
//! [issue](https://github.com/seanmonstar/reqwest/issues/491#issuecomment-865365282), but as far
//! as I know there's no work ongoing within the [reqwest] project to remedy this.
//!
//! I've found a few attempts to improve this situation.
//! [reqwest-middleware](https://docs.rs/reqwest-middleware/latest/reqwest_middleware/index.html)
//! [creates](https://truelayer.com/blog/engineering/adding-middleware-support-to-rust-reqwest/)
//! its own, client-side middleware abstractions in the style of [tower], but implemented in terms
//! of [reqwest]. I've used it, and it does the job, but... it's not [tower], meaning that any [tower]
//! functionality it wants to provide has to be re-implemented-- inelegant.
//!
//! There's a new crate,
//! [tower-reqwest](https://docs.rs/tower-reqwest/latest/tower_reqwest/index.html]) that "provides
//! adapters to use \[the\] reqwest client with tower_http layers." I spent some time working with
//! this crate, as well. The author has re-implemented the [reqwest] `Client` API on top of the [tower]
//! `Service` trait, which I'm not sure I want to sign-up for.
//!
//! Still. At its heart, [tower-reqwest] has an interesting idea: `reqwest::Client` implements
//! [Service]; what if we could just write a [tower] service designed to wrap a [reqwest] client and
//! that did nothing but translate between [reqwest] requests & responses and [http] requests &
//! responses? We could stack all the [tower] & [tower-http] middleware we wanted on top of such a
//! thing.
//!
//! This module builds-out this idea. I wrote about the implementation extensively [here]. If this
//! implementation works out, I might consider moving it into its own crate.
//!
//! [here]: https://www.unwoundstack.com/blog/rust-client-middleware.html

use async_trait::async_trait;
use pin_project::pin_project;
use serde::Deserialize;
use snafu::{Backtrace, IntoError, OptionExt, ResultExt, Snafu};
use tower::{Service, retry::backoff::Backoff};

use std::{
    error::Error as StdError,
    future::Future,
    ops::Deref,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("While waiting for the response body, {source}"))]
    Body {
        source: reqwest::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("[{min:?}, {max:?}] is not a valid Duration range"))]
    DurationRange {
        min: std::time::Duration,
        max: std::time::Duration,
        backtrace: Backtrace,
    },
    #[snafu(display("{value} is not a valid Jitter value"))]
    Jitter { value: f64, backtrace: Backtrace },
    #[snafu(display("The wrapped service speaking reqwest errored-out on poll_ready: {source:?}"))]
    PollReady {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("The reqwest service reported an error: {source}"))]
    Reqwest {
        // Would be nice to *not* erase this
        source: Box<dyn StdError + Send + Sync>,
    },
    #[snafu(display("While extracting a reqwest body, {source}"))]
    ReqwestBody {
        source: reqwest::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("When building an http response, {source}"))]
    Response {
        source: http::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("The response builder yielded no extensions"))]
    ResponseExtensions { backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         ReqwestService                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Implementing this trait signals the ability to fallibly convert a given HTTP response type into
/// an http [http::Response] with a given response body type.
#[async_trait]
pub trait FromResponse: Clone {
    type InnerResponse;
    type ResponseBody;
    async fn try_into_response(
        // Note that the receiver is now `self` (instead of `&self`)-- this is to keep the resulting
        // future from taking a reference on the trait object. Adjust for that by demanding that
        // implementors also implement `Clone`
        self,
        _: Self::InnerResponse,
    ) -> Result<http::Response<Self::ResponseBody>>;
}

/// Wrap a [tower] [Service] that deals in [reqwest] requests & responses so as to produce one that
/// works in terms of [http] requests & responses
#[derive(Clone, Debug)]
pub struct ReqwestService<S, R>
where
    S: tower::Service<reqwest::Request>,
    // make sure their associated types match-up right off the bat:
    R: FromResponse<InnerResponse = S::Response>,
{
    inner: S,
    from_response: R,
}

// Our future will operate like a state machine. My first inclination was to embed state in the
// variant payload (e.g. give InitialConversionFailed a reqwest::Error), but that worked out badly
// in terms of access to state inside the future.
enum State {
    InitialConversionFailed,
    InnerPending,
    ResponseBodyPending,
}

// Note that, at this point, this struct is not Unpin!
#[pin_project]
pub struct ReqwestServiceFuture<S, R>
where
    S: tower::Service<reqwest::Request>,
    R: FromResponse<InnerResponse = S::Response>,
{
    from_response: R,
    state: State,
    // The `Option`s are a hack to produce a type that implements `Default`, enabling us to use
    // `std::mem::take`
    first_err: Option<reqwest::Error>,
    #[pin]
    inner_fut: Option<S::Future>,
    #[pin]
    #[allow(clippy::type_complexity)]
    body_fut: Option<Pin<Box<dyn Future<Output = Result<http::Response<R::ResponseBody>>> + Send>>>,
}

impl<S, R> ReqwestServiceFuture<S, R>
where
    S: tower::Service<reqwest::Request>,
    R: FromResponse<InnerResponse = S::Response>,
{
    pub fn new(
        from_response: R,
        res: StdResult<<S as Service<reqwest::Request>>::Future, reqwest::Error>,
    ) -> ReqwestServiceFuture<S, R> {
        match res {
            Ok(fut) => Self {
                from_response,
                state: State::InnerPending,
                first_err: None,
                inner_fut: Some(fut),
                body_fut: None,
            },
            Err(err) => Self {
                from_response,
                state: State::InitialConversionFailed,
                first_err: Some(err),
                inner_fut: None,
                body_fut: None,
            },
        }
    }
}

impl<S, R> Future for ReqwestServiceFuture<S, R>
where
    S: tower::Service<reqwest::Request>,
    S::Error: StdError + Send + Sync + 'static,
    R: FromResponse<InnerResponse = S::Response> + Clone + 'static,
{
    type Output = Result<http::Response<R::ResponseBody>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.state {
            State::InitialConversionFailed => {
                let this = self.project();
                let first_err = this.first_err;
                Poll::Ready(Err(ReqwestBodySnafu.into_error(first_err.take().unwrap())))
            }
            State::InnerPending => {
                let inner_fut = unsafe {
                    self.as_mut()
                        .map_unchecked_mut(|this| this.inner_fut.as_mut().unwrap())
                };
                match inner_fut.poll(cx) {
                    Poll::Ready(Ok(rsp)) => {
                        let from_response = self.from_response.clone();
                        let mut this = self.project();
                        *this.state = State::ResponseBodyPending;
                        *this.body_fut = Some(from_response.try_into_response(rsp));
                        match Pin::new(this.body_fut.get_mut().as_mut().unwrap()).poll(cx) {
                            res @ Poll::Ready(_) => res,
                            Poll::Pending => Poll::Pending,
                        }
                    }
                    Poll::Ready(Err(err)) => {
                        Poll::Ready(Err(ReqwestSnafu.into_error(Box::new(err))))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            State::ResponseBodyPending => {
                let body_fut = unsafe {
                    self.as_mut()
                        .map_unchecked_mut(|this| this.body_fut.as_mut().unwrap())
                };
                match body_fut.poll(cx) {
                    res @ Poll::Ready(_) => res,
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}

/// [ReqwestService] is a [tower] [Service]
// We still restrict the body type to types `B: Into<reqwest::Body>`
impl<S, ReqBody, R> tower::Service<http::Request<ReqBody>> for ReqwestService<S, R>
where
    ReqBody: Into<reqwest::Body>,
    // Again, make sure the response that our inner service is returning matches-up with what our
    // translation trait expects,
    R: FromResponse<InnerResponse = S::Response>,
    // Since an `R` instance will be moved into a Future, it can't have any references that would
    // limit its lifetime.
    R: FromResponse + Clone + 'static,
    // However, `S::Response` no longer needs to be static! Nor does `S::Future`.
    S: tower::Service<reqwest::Request>,
    S::Error: StdError + Send + Sync + 'static,
{
    // Our response type (see above)
    type Response = http::Response<R::ResponseBody>;
    // We need to declare our error type; may as well use our own.
    type Error = Error;
    // The problem with using `TryFutureExt::and_then` is that we can't *name* the future
    // it returns; instead, we erase the type & just return a trait object.
    type Future = ReqwestServiceFuture<S, R>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<StdResult<(), Self::Error>> {
        self.inner
            .poll_ready(cx)
            .map(|res| res.map_err(|err| PollReadySnafu.into_error(Box::new(err))))
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        ReqwestServiceFuture::new(
            self.from_response.clone(),
            reqwest::Request::try_from(req).map(|r| self.inner.call(r)),
        )
    }
}

// Finally, give ourselves a `Layer` implementation for convenience
pub struct ReqwestServiceLayer<R: FromResponse> {
    from_response: R,
}

impl<R: FromResponse> ReqwestServiceLayer<R> {
    pub fn new(from_response: R) -> ReqwestServiceLayer<R> {
        Self { from_response }
    }
}

impl<S, R> tower::Layer<S> for ReqwestServiceLayer<R>
where
    S: tower::Service<reqwest::Request, Response = reqwest::Response>,
    R: FromResponse<InnerResponse = S::Response> + Clone,
{
    type Service = ReqwestService<S, R>;

    fn layer(&self, inner: S) -> Self::Service {
        ReqwestService {
            inner,
            from_response: self.from_response.clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                      exponential backoffs                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

// Apparently, we need to build our own Policy. Actually, this makes sense, because it enables
// us to decide when to retry versus when to give up.
#[derive(Clone, Debug)]
pub struct ExponentialBackoffPolicy {
    pub backoff: tower::retry::backoff::ExponentialBackoff,
    pub num_attempts: usize,
}

impl<Req: Clone, Res: std::fmt::Debug>
    tower::retry::Policy<
        Req,
        Res,
        /*indielinks_shared::service::Error*/
        Box<(dyn std::error::Error + Send + Sync + 'static)>,
    > for ExponentialBackoffPolicy
{
    type Future =
        <tower::retry::backoff::ExponentialBackoff as tower::retry::backoff::Backoff>::Future;

    // This trait is poorly documented, but I gather that we return `None` to not retry, and
    // `Some(F)` to retry. `F:Future<Output = ()>`, the actual retry will only take place when the
    // future resolves (I guess).
    fn retry(
        &mut self,
        _: &mut Req,
        result: &mut std::result::Result<Res, Box<(dyn std::error::Error + Send + Sync + 'static)>>,
    ) -> Option<Self::Future> {
        // Regrettably, at this time, I'm reduced to using a `Buffer` layer between my `Retry` layer
        // and my `RateLimit` layer, because the latter is not Clone, and the former requires that
        // it wrap a Clonable. This is regrettable because `Buffer` erases the type of any `Error`
        // thrown by its inner service, so we have, at *this* point, no way to distinguish between
        // retryable and non-retryable errors.
        match result {
            Ok(_) => None,
            Err(_) => {
                if self.num_attempts > 0 {
                    self.num_attempts -= 1;
                    Some(self.backoff.next_backoff())
                } else {
                    None
                }
            }
        }
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        Some(req.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "backend")]
#[derive(Clone, Debug)]
pub struct Body;

#[cfg(feature = "backend")]
#[async_trait]
impl FromResponse for Body {
    type InnerResponse = reqwest::Response;
    type ResponseBody = bytes::Bytes;
    async fn try_into_response(
        self,
        rsp: Self::InnerResponse,
    ) -> Result<http::Response<Self::ResponseBody>> {
        let mut builder = rsp.headers().iter().fold(
            http::Response::builder()
                .status(rsp.status())
                .version(rsp.version()),
            |builder, (name, value)| builder.header(name, value),
        );
        let extensions = builder.extensions_mut().context(ResponseExtensionsSnafu)?;
        *extensions = rsp.extensions().clone();
        builder
            .body(rsp.bytes().await.context(BodySnafu)?)
            .context(ResponseSnafu)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Express a rate limit in terms of requests per duration
#[derive(Clone, Debug, Deserialize)]
pub struct RateLimit {
    pub num: u64,
    pub duration: std::time::Duration,
}

impl Default for RateLimit {
    fn default() -> Self {
        RateLimit {
            num: 3,
            duration: std::time::Duration::from_secs(1),
        }
    }
}

/// A pair of [Duration]s that carries with it the guarantee that the first is less than or equal to
/// the second, and that the second is non-zero
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DurationRange {
    min: Duration,
    max: Duration,
}

impl DurationRange {
    pub fn new(min: Duration, max: Duration) -> Result<DurationRange> {
        if min > max || max == Duration::from_millis(0) {
            DurationRangeSnafu { min, max }.fail()
        } else {
            Ok(DurationRange { min, max })
        }
    }
    pub fn lower(&self) -> &Duration {
        &self.min
    }
    pub fn upper(&self) -> &Duration {
        &self.max
    }
}

/// A refinement of [f64] that asserts that it is also in the range [0, 100), as well as being
/// neither infinite nor NaN.
#[derive(Clone, Copy, Debug, Deserialize)]
pub struct Jitter(f64);

impl Default for Jitter {
    fn default() -> Self {
        Jitter(0.0)
    }
}

impl std::fmt::Display for Jitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{Jitter {}%}}", self.0)
    }
}

impl TryFrom<f64> for Jitter {
    type Error = Error;

    fn try_from(value: f64) -> std::result::Result<Self, Self::Error> {
        if !value.is_finite() || !(0.0..100.0).contains(&value) {
            JitterSnafu { value }.fail()
        } else {
            Ok(Jitter(value))
        }
    }
}

impl Deref for Jitter {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<f64> for Jitter {
    fn as_ref(&self) -> &f64 {
        self.deref()
    }
}

/// Parameters for an exponential backoff policy
#[derive(Clone, Debug, Deserialize)]
pub struct ExponentialBackoffParameters {
    durations: DurationRange,
    jitter: Jitter,
    #[serde(rename = "num-attempts")]
    num_attempts: usize,
}

impl ExponentialBackoffParameters {
    pub fn jitter(&self) -> f64 {
        *self.jitter.as_ref()
    }
    pub fn lower(&self) -> &Duration {
        self.durations.lower()
    }
    pub fn num_attempts(&self) -> usize {
        self.num_attempts
    }
    pub fn upper(&self) -> &Duration {
        self.durations.upper()
    }
}

impl Default for ExponentialBackoffParameters {
    fn default() -> Self {
        ExponentialBackoffParameters {
            durations: DurationRange::new(Duration::from_secs(1), Duration::from_secs(3)).unwrap(/* known good */),
            jitter: Jitter::try_from(10.0).unwrap(/* known good */),
            num_attempts: 3,
        }
    }
}
