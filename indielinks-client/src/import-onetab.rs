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

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    str::FromStr,
};

use futures::{StreamExt, stream::iter};
use indielinks::origin::Origin;
use indielinks_shared::{PostAddReq, Tagname};
use itertools::Itertools;
use secrecy::SecretString;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use tower::Service;
use url::Url;

use crate::{import::import_posts, service::ReqBody};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
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
    #[snafu(display("While reading one line from the input file, {source}"))]
    Line {
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to parse {line}"))]
    Parse { line: String, backtrace: Backtrace },
    #[snafu(display("Failed to parse {text} as an URL: {source}"))]
    Url {
        text: String,
        source: url::ParseError,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
struct ExportedPost {
    url: Url,
    title: String,
}

impl ExportedPost {
    fn into_post_add_req(self, tags: &str, private: bool, to_read: bool) -> PostAddReq {
        PostAddReq {
            url: self.url.into(),
            title: self.title,
            notes: None,
            tags: Some(tags.to_owned()),
            dt: None,
            replace: Some(true),
            shared: Some(!private),
            to_read: Some(to_read),
        }
    }
}

impl FromStr for ExportedPost {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (a, b) = s
            .split_once('|')
            .context(ParseSnafu { line: s.to_owned() })?;
        Ok(ExportedPost {
            url: Url::parse(a).context(UrlSnafu { text: a.to_owned() })?,
            title: b.trim().to_owned(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                  Importing Links from OneTab                                   //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Import bookmarks in OneTab format
// This parameter list is waaaay too long (along with those of `add_link()` & `import_pinboard()`);
// just not sure how I want to re-organize them.
#[allow(clippy::too_many_arguments)]
pub async fn import_onetab<'a, C, P, I>(
    client: C,
    api: &Origin,
    token: &SecretString,
    file: P,
    skip: Option<usize>,
    num_to_send: Option<usize>,
    chunk_size: Option<usize>,
    to_read: bool,
    private: bool,
    tags: Option<I>,
) -> Result<()>
where
    // It would be nice to have an alias for this, but the trait_alias feature is only on nightly
    C: Service<
            http::Request<ReqBody>,
            Response = http::Response<bytes::Bytes>,
            Error = Box<dyn std::error::Error + Send + Sync>,
        > + Clone,
    P: AsRef<Path>,
    I: Iterator<Item = &'a Tagname>,
{
    let tags = tags.map(|x| x.cloned().join(",")).unwrap_or("".to_owned());
    iter(
        &BufReader::new(File::open(&file).context(ImportFileSnafu {
            path: file.as_ref().to_path_buf(),
        })?)
        .lines() // optionally skip some number of them,
        .skip(skip.unwrap_or(0))
        // and then limit the number we'll yield.
        .take(num_to_send.unwrap_or(usize::MAX))
        .map(|res| {
            res.context(LineSnafu)
                .and_then(|s| s.parse::<ExportedPost>())
        })
        .chunks(chunk_size.unwrap_or(8)),
    )
    .then(|chunk| {
        let client = client.clone();
        let tags = tags.clone();
        async move {
            match chunk
                .map(|res| res.map(|post| post.into_post_add_req(tags.as_str(), private, to_read)))
                .collect::<Result<Vec<_>>>()
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
