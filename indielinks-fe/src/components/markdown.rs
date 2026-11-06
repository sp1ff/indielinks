// Copyright (C) 2025-2026 Michael Herstine <sp1ff@pobox.com>
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

//! # Markdown rendering
//!
//! A small component wrapping the pure [markdown_to_html] conversion from [crate::content].

use crate::content::markdown_to_html;
use leptos::prelude::*;

/// Render `markdown` as sanitized HTML inside a semantic `<article>`.
///
/// The conversion policy implemented by [markdown_to_html] is:
///
/// - headings, paragraphs, emphasis, strong text, lists, block quotes, code spans, fenced code
///   blocks, thematic breaks, and tables are rendered;
/// - raw and inline HTML is escaped to plain text;
/// - link destinations are restricted to `http`, `https`, `mailto`, and fragment or relative
///   application paths; unsafe schemes such as `javascript` or `data` are stripped so only the
///   link label remains;
/// - images are omitted to avoid scriptable content.
///
/// The resulting HTML is inserted via a single tightly-scoped `inner_html` call. The input is
/// treated as display content, not trusted DOM.
#[component]
pub fn MarkdownDocument(markdown: String) -> impl IntoView {
    let html = markdown_to_html(&markdown);
    view! {
        <article class="markdown-document" inner_html=html />
    }
}
