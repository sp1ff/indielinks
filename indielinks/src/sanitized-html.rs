// Copyright (C) 2026 Michael Herstine <sp1ff@pobox.com>
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

//! # Sanitized HTML
//!
//! [indielinks](crate) in a few places needs to render free form text as HTML, or sanitize HTML
//! obtained from an untrusted source. This module offers types & utilities for doing so.
//! Sanitization is performed by the [ammonia] crate, and transliteration by [comrak].
//!
//! [ammonia]: https://docs.rs/ammonia/latest/ammonia/index.html
//! [comrak]: https://docs.rs/comrak/latest/comrak/index.html
//!
//! ## Extant Work
//!
//! It turns out there's a dark art to parsing URIs, mentions, hashtags &c out of "posts". The
//! granddaddy of libraries for doing so is [twitter-text], ported to Rust in the [twitter_text]
//! crate. Besides being Twitter- (sorry, X-) specific, it failed to recognize Fediverse handles,
//! and provided, AFAICT, no extension points where I could plug-in such functionality.
//!
//! [twitter-text]: https://github.com/twitter/twitter-text
//! [twitter_text]: https://docs.rs/twitter-text/latest/twitter_text/index.html
//!
//! [pulldown-cmark] is a popular choice for parsing Markdown (CommonMark), and is capable of
//! rendering HTML from the resulting AST, but, again AFAICT, provides no full round-trip
//! capability: parsing to an AST, modifying the AST, then rendering the new AST to HTML.
//!
//! [pulldown-cmark]: https://docs.rs/pulldown-cmark/latest/pulldown_cmark/index.html
//!
//! [markdown-it] and its fork [markdown-that] are Rust ports of the popular JavaScript [library] by
//! the same name, but are too rarely used & dormant for me to feel comfortable using them.
//!
//! [markdown-it]: https://docs.rs/markdown-it/latest/markdown_it/index.html
//! [markdown-that]: https://docs.rs/markdown-that/latest/markdown_that/index.html
//! [library]: https://github.com/markdown-it/markdown-it

use std::{cell::RefCell, collections::HashMap, fmt::Display, result::Result as StdResult};

use comrak::{
    arena_tree::Node,
    format_html,
    nodes::{Ast, NodeValue},
    options::Render,
    parse_document, Arena,
};
use lazy_static::lazy_static;

use indielinks_shared::entities::Tagname;
use serde::{Deserialize, Deserializer, Serialize};

use indielinks_shared::acct::Account;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       HTML Sanitization                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

lazy_static! {
    // I use `ammonia::Builder` rather than the convenience function `ammonia::clean()` both to make
    // the parameters explicit, as well as to tighten them up a bit from the defaults (which are
    // general-purpose).
    static ref AMMONIA_OPTIONS: ammonia::Builder<'static> = {
        let mut builder = ammonia::Builder::default();
        builder
            .tags(["a", "b", "blockquote", "br", "code", "del", "em", "i", "li", "mark", "ol", "p",
                   "pre", "s", "small", "span", "strong", "sub", "sup", "time", "u", "ul",].into())
            .clean_content_tags(["script", "style", "iframe", "object", "embed", "svg", "math",
                                 "template", "form", "textarea"].into())
            .tag_attributes([("a", ["href", "hreflang"].into()),
                             ("blockquote", ["cite"].into()),
                             ("time", ["datetime"].into())].into())
            .generic_attributes(["lang", "title"].into())
            .url_schemes(["http", "https", "mailto"].into())
            .url_relative(ammonia::UrlRelative::Deny)
            .link_rel(Some("noopener noreferrer nofollow ugc"))
            .strip_comments(true)
            .allowed_classes(HashMap::new());
        builder
    };
}

/// Refined type that can only be instantiated by sanitizing input HTML
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SanitizedHtml(String);

impl SanitizedHtml {
    // It bothers me that `text` can be an arbitrary UTF-8 string, but, then, that's the type
    // ammonia accepts, so 🤷.
    pub fn new(text: &str) -> Self {
        Self(AMMONIA_OPTIONS.clean(text).into())
    }
}

impl Display for SanitizedHtml {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for SanitizedHtml {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl From<&str> for SanitizedHtml {
    fn from(value: &str) -> Self {
        SanitizedHtml::new(value)
    }
}

impl From<String> for SanitizedHtml {
    fn from(value: String) -> Self {
        SanitizedHtml::new(&value)
    }
}

impl From<SanitizedHtml> for String {
    fn from(value: SanitizedHtml) -> String {
        value.0
    }
}

impl<'de> Deserialize<'de> for SanitizedHtml {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        // Will sanitize
        Ok(s.into())
    }
}

/// The results of parsing Markdown into an [indielinks](crate)-specific form
pub struct ParseResult {
    pub html: SanitizedHtml,
    pub mentions: Vec<Account>,
    pub tags: Vec<Tagname>,
}

// I may need to make the parsing & rendering parameters configurable at runtime, but for now I
// don't so make 'em static (and immutable):
lazy_static! {
    static ref COMRAK_OPTIONS: comrak::Options<'static> = comrak::Options {
        render: Render::builder().r#unsafe(true).build(),
        ..Default::default()
    };
}

/// Parse Markdown; render it to HTML, collect mentions & tags
pub fn parse(text: &str) -> StdResult<ParseResult, std::fmt::Error> {
    // `comrak`, building a tree structure to represent the AST, uses an arena allocated. All tree
    // nodes are parameterized by the arena's lifetime.q
    let arena = Arena::new();
    // Parse the document into an Abstract Syntax Tree.
    let ast = parse_document(&arena, text, &COMRAK_OPTIONS);

    // I want to walk the tree, processing every `Text` node for mentions & tags. Let's look at it
    // like a fold operation:
    let mut mentions: Vec<Account> = Vec::new();
    let mut tags: Vec<Tagname> = Vec::new();
    ast.descendants().fold(
        (&mut mentions, &mut tags),
        |(mentions, tags), node: &Node<'_, RefCell<Ast>>| {
            if let NodeValue::Text(ref text) = node.data.borrow().value {
                // text: &Cow<'static, str>
                text.split_ascii_whitespace().for_each(|word| {
                    if word.starts_with("#") {
                        // If it doesn't parse as a `Tagname`; it's not a tag. I'm not sure this
                        // is how we want to handle this, but let's keep it simple for now.
                        let _ = word
                                .split_once("#")
                                .unwrap(/* known good */)
                                .1
                                .parse::<Tagname>()
                                .map(|tagname| tags.push(tagname));
                    } else if word.starts_with("@") {
                        // Likewise, if we can't parse it as an `Account`, it's not a mention.
                        let _ = word
                                .split_once("@")
                                .unwrap(/* known good */)
                                .1
                                .parse::<Account>()
                                .map(|acct| mentions.push(acct));
                    }
                });
            };
            (mentions, tags)
        },
    );

    // Now, we render the AST as HTML:
    let mut html = String::new();
    format_html(ast, &COMRAK_OPTIONS, &mut html)?;

    Ok(ParseResult {
        html: html.into(),
        mentions,
        tags,
    })
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_sanitization() {
        let sanitized: SanitizedHtml =
            "<b><img src='' onerror=alert('hax')>I'm not trying to XSS you</b>".into();
        assert_eq!(sanitized.as_ref(), "<b>I'm not trying to XSS you</b>");

        // Is sanitization idempotent?
        let already_sanitized: String = sanitized.clone().into(); // infallible
        let resanitized: SanitizedHtml = already_sanitized.into();

        assert_eq!(sanitized, resanitized);
    }

    #[test]
    fn test_parsing() {
        let result = parse(
            r#"<a href="https://example.com">example</a>

@sp1ff@indieweb.social #example #indielinks Blah.
Blahblah<script>alert("Hello, world!")</script>blah.
@coffeehouse-talker@sfba.social

https://foo.com is a cool site.

"#,
        );
        assert!(result.is_ok());
        let result = result.unwrap(/* known good */);
        assert_eq!(result.html.as_ref(), "<p><a href=\"https://example.com\" rel=\"noopener noreferrer nofollow ugc\">example</a></p>\n<p>@sp1ff@indieweb.social #example #indielinks Blah.\nBlahblahblah.\n@coffeehouse-talker@sfba.social</p>\n<p>https://foo.com is a cool site.</p>\n");
        assert_eq!(
            result.mentions,
            vec![
                Account::from_uri("acct:sp1ff@indieweb.social").unwrap(/* known good */),
                Account::from_uri("acct:coffeehouse-talker@sfba.social").unwrap(/* known good */),
            ]
        );
        assert_eq!(
            result.tags,
            vec![
                "example".try_into().unwrap(/* known good */),
                "indielinks".try_into().unwrap(/* known good */)
            ]
        );
    }
}
