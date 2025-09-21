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

use std::{
    collections::HashSet,
    fmt::Debug,
    fs::File,
    io::{self, BufRead, BufReader, Read},
    path::{Path, PathBuf},
    result::Result as StdResult,
};

use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::iter};
use indielinks::origin::Origin;
use itertools::Itertools;
use secrecy::SecretString;
use serde::{Deserialize, de::DeserializeOwned};
use snafu::{Backtrace, IntoError, ResultExt, Snafu};
use tower::Service;
use url::Url;

use indielinks_shared::{PostAddReq, Tagname};

use crate::{import::import_posts, service::ReqBody};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

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
    #[snafu(display("While importing posts, {source}"))]
    Import {
        source: crate::import::Error,
        backtrace: Backtrace,
    },
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

/// Import a Pinboard bookmark file in JSON format
///
/// You can export your Pinboard backsups [here](https://pinboard.in/settings/backup). Pinboard
/// offers XML, HTML, or JSON format. This implementation only supports JSON.
///
/// Since the export file can be quite large, this implementation will not read the entire file into
/// memory, but will process it in chunks.
pub async fn import_pinboard<C, P>(
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
            Response = http::Response<bytes::Bytes>,
            Error = Box<dyn std::error::Error + Send + Sync>,
        > + Clone,
    P: AsRef<Path>,
{
    iter(
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
            match chunk
                .map(|x| x.map(Into::into))
                .collect::<Result<Vec<PostAddReq>>>()
            {
                Ok(posts) => import_posts(client, api, token, posts.into_iter())
                    .await
                    .context(ImportSnafu),
                Err(err) => Err(err),
            }
        }
    })
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<Result<Vec<_>>>()
    .map(|_| ())
}
