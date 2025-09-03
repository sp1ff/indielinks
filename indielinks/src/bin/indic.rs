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

//! # indic
//!
//! [indic](crate) (think "Indie-C") is a general purpose [indielinks] client. Currently, it's only
//! function is to import bookmarks from Pinboard, but I anticipate it growing quickly.
//!
//! indielinks: ::indielinksd

use std::{
    collections::HashSet,
    ffi::OsStr,
    fmt::Debug,
    fs::{self, File},
    io::{self, BufRead, BufReader, Read},
    path::{Path, PathBuf},
    result::Result as StdResult,
    str::FromStr,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clap::{
    Arg, ArgAction, Command, crate_authors, crate_version, parser::ValueSource, value_parser,
};
use futures::{
    StreamExt,
    stream::{FuturesUnordered, iter},
};
use http::{
    HeaderValue, Method, Request,
    header::{ACCEPT, AUTHORIZATION, USER_AGENT},
};
use indielinks::origin::Origin;
use itertools::Itertools;
use secrecy::SecretString;
use serde::{Deserialize, de::DeserializeOwned};
use snafu::{Backtrace, IntoError, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use tower::{
    Service, ServiceBuilder, ServiceExt,
    buffer::BufferLayer,
    limit::RateLimitLayer,
    retry::backoff::{Backoff, MakeBackoff},
};
use tower_http::set_header::SetRequestHeaderLayer;
use tracing::{Level, level_filters::LevelFilter};
use tracing_subscriber::{Registry, fmt, layer::SubscriberExt};
use url::Url;

use indielinks_shared::{PostAddReq, Tagname};

#[derive(Snafu)]
enum Error {
    #[snafu(display("The API origin must be specified, either in config or on the command line"))]
    Api,
    #[snafu(display("While attempting to read {path:?}, {source}"))]
    BadConfig {
        path: PathBuf,
        source: std::io::Error,
        backtrace: Backtrace,
    },
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

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> StdResult<(), std::fmt::Error> {
        write!(f, "{self}")
    }
}

type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             import                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

// Adapted heavily from https://stackoverflow.com/questions/68641157/how-can-i-stream-elements-from-inside-a-json-array-using-serde-json

/// This differs from [BufRead::skip_until] in that it returns the delimiter byte, rather than the number of bytes consumed
// With apologies to https://doc.rust-lang.org/stable/src/std/io/mod.rs.html#2235
fn skip_until_pred<R: BufRead, F: Fn(&u8) -> bool>(r: &mut R, f: F) -> Result<u8> {
    loop {
        let (done, used) = {
            let available = match r.fill_buf() {
                Ok(n) => n,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(IoSnafu.into_error(err)),
            };
            match available.iter().copied().find_position(&f) {
                Some((i, delim)) => (Some(delim), i + 1),
                None => (None, available.len()),
            }
        };
        r.consume(used);
        match (done, used) {
            (Some(delim), _) => return Ok(delim),
            (_, 0) => return EobSnafu.fail(),
            _ => (),
        }
    }
}

fn skip_ws<R: BufRead>(r: &mut R) -> Result<u8> {
    skip_until_pred(r, |x| !x.is_ascii_whitespace())
}

/// Deserialize a single `T` (of an array) from a [BufRead], assuming we're at the initial `{`
fn deserialize_one<T: DeserializeOwned, R: BufRead>(reader: R) -> Result<T> {
    match serde_json::Deserializer::from_reader(reader)
        .into_iter::<T>()
        .next()
    {
        Some(result) => result.context(DeSnafu),
        None => PrematureEofSnafu.fail(),
    }
}

/// Find the next `T` in a [BufRead], skipping the intial `[` and any whitepsace
// The game here is to leverage
// [serde_json::Deserializer](https://docs.rs/serde_json/latest/serde_json/struct.Deserializer.html),
// but we need to provide some scaffolding.
fn yield_next_elt<T: DeserializeOwned, R: BufRead>(
    mut reader: R,
    ready_for_elt: &mut bool,
) -> Result<Option<T>> {
    if *ready_for_elt {
        match skip_ws(&mut reader)? {
            b',' => deserialize_one(reader),
            b']' => Ok(None),
            _ => ParseSnafu.fail(),
        }
    } else {
        *ready_for_elt = true;
        if skip_ws(&mut reader)? == b'[' {
            let peek = skip_ws(&mut reader)?;
            if peek == b']' {
                // Empty array-- we're done!
                Ok(None)
            } else {
                Ok(Some(deserialize_one(
                    io::Cursor::new([peek]).chain(reader),
                )?))
            }
        } else {
            InitialParseSnafu.fail()
        }
    }
}

/// Given a [BufRead]er at the start of a JSON array, return an iterator over the enclosed elements
fn iterator_for_json_array<T: DeserializeOwned, R: BufRead>(
    mut reader: R,
) -> impl Iterator<Item = Result<T>> {
    // I had originally thought to consume the initial '[' here, handling the case of an empty array
    // here, also. Thing is, you end-up having to return one type of iterator in the first case
    // (i.e. a non-empty array) & a second type in the second (i.e. an empty array), which doesn't
    // type-check: we're can can branch & return different things in each branch, but the *types*
    // have to be the same.
    let mut ready_for_elt = false;
    std::iter::from_fn(move || yield_next_elt(&mut reader, &mut ready_for_elt).transpose())
}

#[cfg(test)]
mod test {
    use std::io::BufReader;

    use super::*;

    #[test]
    fn test_skip_until_pred() {
        let mut r = BufReader::new(b"    Hello, world!".as_slice());
        let res = skip_until_pred(&mut r, |x| !x.is_ascii_whitespace());
        assert!(res.is_ok());
        assert!(res.unwrap() == b'H');
    }

    #[test]
    fn test_json_array_iter() {
        let data = r#"[
{
  "a": 1,
  "b": "Hello"
},
{
  "a": 2,
  "b": "world"
}
]
"#;

        #[derive(Debug, Deserialize, Eq, PartialEq)]
        struct S {
            a: i32,
            b: String,
        }

        let mut i = iterator_for_json_array::<S, _>(io::Cursor::new(&data));
        let x = i.next().transpose();
        assert!(x.is_ok());
        let x = x.unwrap();
        assert_eq!(
            x,
            Some(S {
                a: 1,
                b: "Hello".to_owned()
            })
        );
        let x = i.next().transpose();
        assert!(x.is_ok());
        let x = x.unwrap();
        assert_eq!(
            x,
            Some(S {
                a: 2,
                b: "world".to_owned()
            })
        );
        let x = i.next().transpose();
        assert!(x.is_ok());
        let x = x.unwrap();
        assert_eq!(x, None);
    }
}

#[derive(Debug, Deserialize)]
struct ExportedPost {
    href: Url,
    description: String,
    extended: String,
    time: DateTime<Utc>,
    #[serde(deserialize_with = "de_exported_post::de_bool")]
    shared: bool,
    #[serde(deserialize_with = "de_exported_post::de_bool")]
    toread: bool,
    #[serde(deserialize_with = "de_exported_post::de_tags")]
    tags: HashSet<Tagname>,
}

mod de_exported_post {
    use super::*;
    use serde::de;

    pub fn de_bool<'de, D>(deserializer: D) -> StdResult<bool, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s: /*&str*/ String = de::Deserialize::deserialize(deserializer)?;
        match s.as_str() {
            "yes" => Ok(true),
            "no" => Ok(false),
            _ => Err(de::Error::unknown_variant(s.as_str(), &["yes", "no"])),
        }
    }

    pub fn de_tags<'de, D>(deserializer: D) -> StdResult<HashSet<Tagname>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let tags: String = de::Deserialize::deserialize(deserializer)?;
        Ok(HashSet::from_iter(
            tags.split_ascii_whitespace()
                .map(Tagname::new)
                .collect::<StdResult<Vec<_>, _>>()
                .map_err(|err| {
                    de::Error::invalid_value(
                        de::Unexpected::Other(&format!("{err:?}")),
                        &"Tag name",
                    )
                })?,
        ))
    }
}

impl From<ExportedPost> for PostAddReq {
    fn from(value: ExportedPost) -> Self {
        PostAddReq {
            url: value.href.into(),
            title: value.description,
            notes: if value.extended.is_empty() {
                None
            } else {
                Some(value.extended)
            },
            tags: if value.tags.is_empty() {
                None
            } else {
                Some(
                    value
                        .tags
                        .into_iter()
                        .map(|tagname| tagname.to_string())
                        .join(","),
                )
            },
            dt: Some(value.time),
            replace: Some(true),
            shared: Some(value.shared),
            to_read: Some(value.toread),
        }
    }
}

/// Add a [post](ExportedPost) to indielinks
async fn import_post<C>(
    mut client: C,
    api: &Origin,
    token: &SecretString,
    post: ExportedPost,
) -> Result<()>
where
    C: Service<
            http::Request<ReqBody>,
            Response = http::Response<Vec<u8>>,
            Error = Box<dyn std::error::Error + Send + Sync>,
        > + Clone,
{
    use secrecy::ExposeSecret;

    let add_req: PostAddReq = post.into();
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

/// Import a (fixed-size, presumably small) collection of posts
async fn import_posts<C, I>(
    client: C,
    api: &Origin,
    token: &SecretString,
    posts: I,
) -> Result<usize>
where
    C: Service<
            http::Request<ReqBody>,
            Response = http::Response<Vec<u8>>,
            Error = Box<dyn std::error::Error + Send + Sync>,
        > + Clone,
    I: Iterator<Item = ExportedPost>,
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

/// Import a Pinboard bookmark file in JSON format
///
/// You can export your Pinboard backsups [here](https://pinboard.in/settings/backup). Pinboard
/// offers XML, HTML, or JSON format. This implementation only supports JSON.
///
/// Since the export file can be quite large, this implementation will not read the entire file into
/// memory, but will process it in chunks.
async fn import<C, P>(
    client: C,
    api: &Origin,
    token: &SecretString,
    file: P,
    skip: Option<usize>,
    num_to_send: Option<usize>,
    chunk_size: Option<usize>,
) -> Result<()>
where
    // It would be nice to have an alias for this, but the trait_alias feature is only on nightly
    C: Service<
            http::Request<ReqBody>,
            Response = http::Response<Vec<u8>>,
            Error = Box<dyn std::error::Error + Send + Sync>,
        > + Clone,
    P: AsRef<Path>,
{
    let _ = iter(
        // Create an iterator that yields a `Result<ExportedPost>` on each invocation of `next()` by
        // deserializing `file` one post at a time,
        &iterator_for_json_array::<ExportedPost, _>(BufReader::new(File::open(&file).context(
            ImportFileSnafu {
                path: file.as_ref().to_path_buf(),
            },
        )?))
        // optionally skip some number of them,
        .skip(skip.unwrap_or(0))
        // and then limit the number we'll yield.
        .take(num_to_send.unwrap_or(usize::MAX))
        // At this point, we have an `Iterator` that respects `skip` & `num_to_send`. Now chunk it:
        .chunks(chunk_size.unwrap_or(8)),
        // and turn it into a `Stream`.
        //
        // This results in an iterator that yields "chunks". Each chunk is itself an iterator, this time
        // over `chunk_size` `Result<ExportedPost>`s. The game here is to process each such chunk
        // asynchronously, while *synchronously* going from chunk to chunk. I.e we'll synchronously read
        // `chunk_size` posts, asynchronously send-out `chunk_size` requests to indielinks (respecting
        // rate limits), and only once they've all completed, produce the next chunk.
    )
    // to produce an "iterator of iterators".
    //
    // Now, we *could* just spawn the `import_posts()` invocation off on to the async runtime &
    // continue consuming the file, but given that network comms, especially over the internet, are
    // an order of magnitude or more slower than local disk access, I suspect we'd wind-up with most
    // or all of the file in-memory, waiting on a massive number of async tasks to complete.
    //
    // I guess the right thing to do would be to set a maximum of memory overhead and a maximum
    // number of "in-flight" requests. When either is hit, we pause and wait for the async runtime
    // to drain until we have a certain amount of headroom, then resume processing. That seems hard,
    // however, so at least for this, first, implementation, I'll just use the proxy of throttling
    // via chunk.
    .then(|chunk| {
        let client = client.clone();
        async move {
            match chunk.collect::<Result<Vec<_>>>() {
                Ok(posts) => import_posts(client, api, token, posts.into_iter()).await,
                Err(err) => Err(err),
            }
        }
    })
    .collect::<Vec<_>>()
    .await;
    Ok(())
}

/// Add a link
#[allow(clippy::too_many_arguments)]
async fn add_link<C>(
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

// At the time of this writing, this is my third copy of this logic. The first is in the Github
// project accompanying my post on the topic. This is the second time I've applied it in anger (the
// first was my personal project "tune-truffle"). If this application works out, I should consider
// moving it into its own crate.
mod proto_reqwest_tower {

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
struct ExponentialBackoffPolicy {
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
struct GenericRspBody;

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

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         configuration                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Express a rate limit in terms of requests per duration
#[derive(Clone, Debug, Deserialize)]
struct RateLimit {
    pub num: u64,
    pub duration: std::time::Duration,
}

impl Default for RateLimit {
    fn default() -> Self {
        RateLimit {
            num: 3,
            duration: Duration::from_secs(1),
        }
    }
}

/// Current configuration
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigV1 {
    // For this one item, I'll allow it both in config and on the command line
    pub token: Option<SecretString>,
    // alright, well for these *two* items...
    pub api: Option<Origin>,
    /// Rate limit for requests to indielinks
    #[serde(rename = "rate-limit")]
    pub rate_limit: RateLimit,
}

impl ConfigV1 {
    pub fn set_api(self, api: Origin) -> Self {
        ConfigV1 {
            api: Some(api),
            ..self
        }
    }
    pub fn set_token(self, token: SecretString) -> Self {
        ConfigV1 {
            token: Some(token),
            ..self
        }
    }
    pub fn api(&self) -> Option<&Origin> {
        self.api.as_ref()
    }
    pub fn token(&self) -> Option<&SecretString> {
        self.token.as_ref()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "version", deny_unknown_fields)] // tag "internally"
enum Configuration {
    #[serde(rename = "1")]
    V1(ConfigV1),
}

impl Configuration {
    pub fn rate_limit(&self) -> &RateLimit {
        match self {
            Configuration::V1(config_v1) => &config_v1.rate_limit,
        }
    }
    pub fn set_api(self, api: Origin) -> Self {
        match self {
            Configuration::V1(config_v1) => Configuration::V1(config_v1.set_api(api)),
        }
    }
    pub fn set_token(self, token: SecretString) -> Self {
        match self {
            Configuration::V1(config_v1) => Configuration::V1(config_v1.set_token(token)),
        }
    }
    pub fn api(&self) -> Option<&Origin> {
        match self {
            Configuration::V1(config_v1) => config_v1.api(),
        }
    }
    pub fn token(&self) -> Option<&SecretString> {
        match self {
            Configuration::V1(config_v1) => config_v1.token(),
        }
    }
}

impl Default for Configuration {
    fn default() -> Self {
        Configuration::V1(ConfigV1::default())
    }
}

/// Create an [indielinks] client
///
/// Return a [reqwest::Client] wrapped in tower layers that will:
///
/// - set Authorization, User-Agent, and Accept headers on all outgoing requests
/// - rate-limit requests according to the `rate_limit` argument
/// - retry failed requests with exponential backoff
async fn make_indielinks_client(
    user_agent: &str,
    token: SecretString,
    rate_limit: &RateLimit,
) -> Result<
    impl Service<
        http::Request<ReqBody>,
        Response = http::Response<Vec<u8>>,
        Error = Box<dyn std::error::Error + Send + Sync>,
    > + Clone,
> {
    use proto_reqwest_tower::ReqwestServiceLayer;
    use secrecy::ExposeSecret;

    ServiceBuilder::new()
        .layer(tower::retry::RetryLayer::new(ExponentialBackoffPolicy {
            backoff: tower::retry::backoff::ExponentialBackoffMaker::new(
            // I need to add these to config
            Duration::from_secs(1),
            Duration::from_secs(3),
            10.0,
            tower::util::rng::HasherRng::new(),
        )
        .unwrap(/* known good */)
                .make_backoff(),
            num_attempts: 3,
        }))
        // `RetryLayer` requries that the `Service` it wraps is `Clone`... which `RateLimitLayer` is
        // not. Per https://github.com/tokio-rs/axum/discussions/987#discussioncomment-2678595, we
        // wrap it in a `BufferLayer`. Regrettably, it changes the error type from
        // `proto_reqwest_tower::Error` to `Box<Error + Send + Sync>`
        .layer(BufferLayer::<http::Request<ReqBody>>::new(1024))
        .layer(RateLimitLayer::new(rate_limit.num, rate_limit.duration))
        .layer(SetRequestHeaderLayer::overriding(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", token.expose_secret()))
                .context(TokenSnafu)?,
        ))
        .layer(SetRequestHeaderLayer::overriding(
            USER_AGENT,
            HeaderValue::from_str(user_agent).unwrap(/* known good*/),
        ))
        .layer(SetRequestHeaderLayer::overriding(
            ACCEPT,
            HeaderValue::from_static("application/json"),
        ))
        .layer(ReqwestServiceLayer::new(GenericRspBody))
        .service(reqwest::Client::new())
        .pipe(Ok)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                              main                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

fn parse_tags(text: &str) -> Result<HashSet<Tagname>> {
    HashSet::from_iter(
        text.split(',')
            .map(Tagname::from_str)
            .collect::<StdResult<Vec<_>, _>>()
            .context(TagnameSnafu)?,
    )
    .pipe(Ok)
}

#[tokio::main]
async fn main() -> Result<()> {
    let matches = Command::new("indic")
        .version(crate_version!())
        .author(crate_authors!())
        .about("The indielinks client")
        .long_about(
            "General-purpose client for working with indielinks.

indic (think \"indie-c\") will be a general client for indielinks. Currently, its only
sub-command is 'import', but it will be built-out as circumstances warrant.",
        )
        // In the past, I've pulled run-time configuration from command line options, environment
        // variables, and a configuration file (in that order of precedence). I'm not sure that's
        // really worth the effort. For now, I'm just going to put frequently configured items in
        // the command line (perhaps with an environment variable backup) and leave items that are
        // likely to change less frequently in a configuration file.
        .arg(
            Arg::new("api")
                .short('A')
                .long("api")
                .num_args(1)
                .value_parser(value_parser!(Origin))
                .env("INDIC_API")
                .help("Specify the location of the indielinks API to which you wish to speak")
        )
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .num_args(1)
                .value_parser(value_parser!(PathBuf))
                .default_value(OsStr::new("/home/mgh/.indic.toml"))
                .env("INDIC_CONFIG")
                .help("Specify the path to the configuration file")
        )
        .arg(
            Arg::new("token")
                .short('t')
                .long("token")
                .num_args(1)
                .value_parser(value_parser!(SecretString))
                .env("INDIC_TOKEN")
                .help("The indielinks API token to be used for authentication")
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .num_args(0)
                .action(ArgAction::SetTrue)
                .help("produce more prolix output"),
        )
        .subcommand(
            Command::new("import")
                .about("Import links from another source")
                .long_about(
                    "Import links from another source.

At the time of this writing, the only import format supported is Pinboard JSON. You
can download your Pinboard links in JSON format by following the \"JSON\" link on
the page https://pinboard.in/settings/backup.
",
                )
                .arg(
                    Arg::new("skip")
                        .short('s')
                        .long("skip")
                        .required(false)
                        .num_args(1)
                        .value_parser(value_parser!(usize))
                        .help("Number of bookmarks in the input file to skip"),
                )
                .arg(
                    Arg::new("send")
                        .short('N')
                        .long("send")
                        .required(false)
                        .num_args(1)
                        .value_parser(value_parser!(usize))
                        .help("Number of bookmarks in the input file to send (after --skip'ping)"),
                )
                .arg(
                    Arg::new("chunk-size")
                        .short('c')
                        .long("chunk-size")
                        .required(false)
                        .num_args(1)
                        .value_parser(value_parser!(usize))
                        .help("The maximum number of /posts/add requests to indielinks that may be in-flight at any time"),
                )
                .arg(
                    Arg::new("FILE")
                        .required(true)
                        .value_parser(value_parser!(PathBuf))
                        .index(1) /* Better to be explicit, I think */
                        .help("Pinboard JSON-formatted file containing links to be imported to indielinks"),
                ),
        )
        .subcommand(
            Command::new("add-link")
                .about("Add a link")
                .long_about("Add a single link to indielinks. The URL & title do not need
to be escaped; the implementation will handle that.")
                .arg(
                    Arg::new("replace")
                        .short('r')
                        .long("replace")
                        .num_args(0)
                        .action(ArgAction::SetTrue)
                        .help("Replace the current link, if present")
                )
                .arg(
                    Arg::new("shared")
                        .short('s')
                        .long("shared")
                        .num_args(0)
                        .action(ArgAction::SetTrue)
                        .help("Mark this link as public/shared")
                )
                .arg(
                    Arg::new("tags")
                        .short('t')
                        .long("tags")
                        .num_args(1)
                        .value_parser(parse_tags)
                        .help("One or more tags, separated by commas")
                )
                .arg(
                    Arg::new("title")
                        .short('T')
                        .long("title")
                        .required(true)
                        .num_args(1)
                        .value_parser(value_parser!(String))
                        .help("Title to be used for this link")
                )
                .arg(
                    Arg::new("to-read")
                        .short('R')
                        .long("to-read")
                        .num_args(0)
                        .action(ArgAction::SetTrue)
                        .help("Mark this link as unread")
                )
                .arg(
                    Arg::new("URL")
                        .required(true)
                        .value_parser(value_parser!(Url))
                        .index(1) /* Better to be explicit, I think */
                        .help("The URL to be added to indielinks"))
        )
        .get_matches();

    tracing::subscriber::set_global_default(
        Registry::default()
            .with(LevelFilter::from_level(if matches.get_flag("verbose") {
                Level::DEBUG
            } else {
                Level::INFO
            }))
            .with(
                fmt::Layer::default()
                    .compact()
                    .without_time()
                    .with_level(false)
                    .with_file(false)
                    .with_line_number(false)
                    .with_target(false),
            ),
    )
    .context(SubscriberSnafu)?;

    // Next-up: configuration:
    let mut cfg = match matches.get_one::<PathBuf>("config") {
        Some(path) => match fs::read_to_string(path) {
            Ok(config_text) => toml::from_str(&config_text).context(ConfigSnafu)?,
            Err(err) => match (err.kind(), matches.value_source("config")) {
                (io::ErrorKind::NotFound, Some(ValueSource::DefaultValue)) => {
                    Configuration::default()
                }
                _ => {
                    return Err(BadConfigSnafu {
                        path: path.to_path_buf(),
                    }
                    .into_error(err));
                }
            },
        },
        None => Configuration::default(), // We got nada-- just whip-up a default instance.
    };

    if let Some(api) = matches.get_one::<Origin>("api").cloned() {
        cfg = cfg.set_api(api);
    }
    if let Some(token) = matches.get_one::<SecretString>("token").cloned() {
        cfg = cfg.set_token(token);
    }

    let client = make_indielinks_client(
        &format!("indic/{} ( sp1ff@pobox.com )", crate_version!()),
        cfg.token().context(NoTokenSnafu)?.clone(),
        cfg.rate_limit(),
    )
    .await?;

    match matches.subcommand() {
        Some(("import", matches)) => {
            import(
                client,
                cfg.api().context(ApiSnafu)?,
                cfg.token().context(MissingTokenSnafu)?,
                matches.get_one::<PathBuf>("FILE").unwrap(/* impossible */),
                matches.get_one::<usize>("skip").copied(),
                matches.get_one::<usize>("send").copied(),
                matches.get_one::<usize>("chunk-size").copied(),
            )
            .await
        }
        Some(("add-link", matches)) => {
            add_link(
                client,
                cfg.api().context(ApiSnafu)?,
                cfg.token().context(MissingTokenSnafu)?,
                matches.get_one::<Url>("URL").unwrap(/* impossible */),
                matches.get_one::<String>("title").unwrap(/* impossible */),
                None, // no notes, as yet
                matches
                    .get_one::<HashSet<Tagname>>("tags")
                    .cloned()
                    .unwrap_or(HashSet::new()),
                matches.get_one::<bool>("replace").copied(),
                matches.get_one::<bool>("shared").copied(),
                matches.get_one::<bool>("to-read").copied(),
            )
            .await
        }
        Some(_) => unimplemented!(/* impossible */),
        None => NoSubCommandSnafu.fail(),
    }
}
