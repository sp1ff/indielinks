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

use std::{fmt::Debug, path::PathBuf};

use async_trait::async_trait;
use snafu::{Backtrace, ResultExt, Snafu};
use tower::retry::backoff::Backoff;
use url::Url;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The API origin must be specified, either in config or on the command line"))]
    Api,
    #[snafu(display("The tower service returns {source}"))]
    Call {
        source: Box<dyn std::error::Error + Send + Sync>,
        backtrace: Backtrace,
    },
    #[snafu(display("While parsing the configuration file, {source}"))]
    Config {
        source: toml::de::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize from JSON: {source}"))]
    De {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Exhausted the buffer without matching the predicate"))]
    Eob { backtrace: Backtrace },
    #[snafu(display("Failed to open {path:?}: {source}"))]
    ImportFile {
        path: PathBuf,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to find the initial '[']"))]
    InitialParse,
    #[snafu(display("I/O error: {source}"))]
    Io {
        source: std::io::Error,
        backtrace: Backtrace,
    },
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
    #[snafu(display("Failed to setup the tracing global subscriber: {source}"))]
    Subscriber {
        source: tracing::dispatcher::SetGlobalDefaultError,
        backtrace: Backtrace,
    },
    #[snafu(display("Bad tagname: {source}"))]
    Tagname {
        source: indielinks_shared::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid API key"))]
    Token {
        source: http::header::InvalidHeaderValue,
        backtrace: Backtrace,
    },
    #[snafu(display("While encoding {url} to a query string: {source}"))]
    UrlEncoding {
        url: Url,
        source: serde_urlencoded::ser::Error,
        backtrace: Backtrace,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             client                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

// At the time of this writing, this is my third copy of this logic. The first is in the Github
// project accompanying my post on the topic. This is the second time I've applied it in anger (the
// first was my personal project "tune-truffle"). If this application works out, I should consider
// moving it into its own crate.
pub mod proto_reqwest_tower {

    use async_trait::async_trait;
    use pin_project::pin_project;
    use snafu::{Backtrace, IntoError, Snafu};
    use tower::Service;

    use std::{
        error::Error as StdError,
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub(crate)))]
    pub enum Error {
        #[snafu(display("Failed to form the http Response: {source}"))]
        Body {
            source: http::Error,
            backtrace: Backtrace,
        },
        #[snafu(display(
            "The wrapped service speaking reqwest errored-out on poll_ready: {source:?}"
        ))]
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
    }

    pub type Result<T> = std::result::Result<T, Error>;
    type StdResult<T, E> = std::result::Result<T, E>;

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
        body_fut:
            Option<Pin<Box<dyn Future<Output = Result<http::Response<R::ResponseBody>>> + Send>>>,
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

    // We still restrict the body type to types `B: Into<Body>`
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
}

// Apparently, we need to build our own Policy. Actually, this makes sense, because it enables
// us to decide when to retry versus when to give up.
#[derive(Clone, Debug)]
pub struct ExponentialBackoffPolicy {
    pub backoff: tower::retry::backoff::ExponentialBackoff,
    pub num_attempts: usize,
}

impl<Req: Clone, Res: Debug>
    tower::retry::Policy<
        Req,
        Res,
        /*proto_reqwest_tower::Error*/ Box<(dyn std::error::Error + Send + Sync + 'static)>,
    > for ExponentialBackoffPolicy
{
    type Future =
        <tower::retry::backoff::ExponentialBackoff as tower::retry::backoff::Backoff>::Future;

    // This trait is poorly documented, but I gather that we return `None` to not retry, and
    // `Some(F)` to retry. `F:Future<Output = ()>`, the actual retry will only take place when the
    // future resolves (I guess).
    fn retry(
        &mut self,
        _req: &mut Req,
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

// I'm not sure what other request bodies this tool will need to send, but for now I'm experimenting
// with a sum type
#[derive(Clone, Debug)]
pub enum ReqBody {
    None,
}

impl From<ReqBody> for reqwest::Body {
    fn from(value: ReqBody) -> Self {
        match value {
            ReqBody::None => (&[] as &'static [u8]).into(),
        }
    }
}

/// Convert a `reqwest::Response` into an `http::Response<Vec<u8>`
#[derive(Clone)]
pub struct GenericRspBody;

#[async_trait]
impl proto_reqwest_tower::FromResponse for GenericRspBody {
    type InnerResponse = reqwest::Response;
    type ResponseBody = Vec<u8>;
    async fn try_into_response(
        self,
        rsp: Self::InnerResponse,
    ) -> proto_reqwest_tower::Result<http::Response<Self::ResponseBody>> {
        let status = rsp.status();
        // I still need to copy the headers over!
        use proto_reqwest_tower::ReqwestBodySnafu;
        Ok(http::Response::builder()
            .status(status)
            .body(rsp.bytes().await.context(ReqwestBodySnafu)?.to_vec())
            .context(proto_reqwest_tower::BodySnafu)?)
    }
}
