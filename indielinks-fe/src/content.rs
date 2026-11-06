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

//! # Pure content logic
//!
//! Markdown conversion, document routing, and mailto helpers that have no browser dependencies and
//! can be unit-tested on the host.

use pulldown_cmark::{Event, Parser, Tag, TagEnd};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                     Document routing enum                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// One of the four operator-authored instance documents.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Document {
    About,
    Documentation,
    Privacy,
    Terms,
}

impl Document {
    pub const ALL: [Self; 4] = [Self::About, Self::Documentation, Self::Privacy, Self::Terms];

    pub fn label(self) -> &'static str {
        match self {
            Self::About => "About",
            Self::Documentation => "Documentation",
            Self::Privacy => "Privacy",
            Self::Terms => "Terms",
        }
    }

    pub fn heading(self) -> &'static str {
        match self {
            Self::About => "About this instance",
            Self::Documentation => "Documentation",
            Self::Privacy => "Privacy Policy",
            Self::Terms => "Terms of Service",
        }
    }

    pub fn markdown<'a>(self, documents: &'a indielinks_shared::api::InstanceDocuments) -> &'a str {
        match self {
            Self::About => &documents.about,
            Self::Documentation => &documents.documentation,
            Self::Privacy => &documents.privacy_policy,
            Self::Terms => &documents.terms_of_service,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                  Markdown-to-HTML conversion                                   //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Escape `&`, `<`, `>`, and `"` for safe insertion into HTML text.
fn escape_html(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

/// Return `Some(url)` if the destination is safe to link, otherwise `None`.
///
/// Allowed schemes: `http`, `https`, `mailto`, plus fragment-only (`#…`) and relative paths
/// (`/…`). Everything else is rejected.
fn sanitize_href(url: &str) -> Option<String> {
    if url.starts_with('#') || url.starts_with('/') {
        return Some(url.to_string());
    }
    if let Some(pos) = url.find(':') {
        let scheme = &url[..pos].to_lowercase();
        match scheme.as_str() {
            "http" | "https" | "mailto" => Some(url.to_string()),
            _ => None,
        }
    } else {
        Some(url.to_string())
    }
}

/// Convert Markdown to a small, sanitized HTML fragment.
///
/// The conversion policy is:
///
/// - headings, paragraphs, emphasis, strong text, lists, block quotes, code spans, fenced code
///   blocks, thematic breaks, and tables are rendered;
/// - raw and inline HTML is escaped to plain text;
/// - link destinations are restricted to `http`, `https`, `mailto`, and fragment or relative
///   application paths; unsafe schemes such as `javascript` or `data` are stripped so only the
///   link label remains;
/// - images are omitted to avoid scriptable content.
pub fn markdown_to_html(markdown: &str) -> String {
    use pulldown_cmark::Options;

    let mut html = String::new();
    let options =
        Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_SMART_PUNCTUATION;
    let parser = Parser::new_ext(markdown, options);
    let mut in_image = false;
    let mut image_alt = String::new();
    let mut in_unsafe_link = false;

    for event in parser {
        if in_image {
            match event {
                Event::Text(text) => image_alt.push_str(&text),
                Event::End(TagEnd::Image) => {
                    in_image = false;
                    html.push_str(&escape_html(&image_alt));
                }
                _ => {}
            }
            continue;
        }

        match event {
            Event::Start(tag) => match tag {
                Tag::Paragraph => html.push_str("<p>"),
                Tag::Heading { level, .. } => {
                    html.push_str(&format!("<h{level}>", level = level as u8))
                }
                Tag::BlockQuote(_) => html.push_str("<blockquote>"),
                Tag::CodeBlock(_) => html.push_str("<pre><code>"),
                Tag::List(None) => html.push_str("<ul>"),
                Tag::List(Some(1)) => html.push_str("<ol>"),
                Tag::List(Some(start)) => html.push_str(&format!("<ol start=\"{start}\">")),
                Tag::Item => html.push_str("<li>"),
                Tag::Emphasis => html.push_str("<em>"),
                Tag::Strong => html.push_str("<strong>"),
                Tag::Strikethrough => html.push_str("<del>"),
                Tag::Link { dest_url, .. } => {
                    if let Some(href) = sanitize_href(&dest_url) {
                        html.push_str(&format!(r#"<a href="{href}">"#));
                    } else {
                        in_unsafe_link = true;
                    }
                }
                Tag::Image { .. } => {
                    in_image = true;
                    image_alt.clear();
                }
                Tag::Table(_) => html.push_str("<table>"),
                Tag::TableHead => html.push_str("<thead>"),
                Tag::TableRow => html.push_str("<tr>"),
                Tag::TableCell => html.push_str("<td>"),
                Tag::HtmlBlock => {}
                _ => {}
            },
            Event::End(tag_end) => match tag_end {
                TagEnd::Paragraph => html.push_str("</p>"),
                TagEnd::Heading(level) => {
                    html.push_str(&format!("</h{level}>", level = level as u8))
                }
                TagEnd::BlockQuote(_) => html.push_str("</blockquote>"),
                TagEnd::CodeBlock => html.push_str("</code></pre>"),
                TagEnd::List(false) => html.push_str("</ul>"),
                TagEnd::List(true) => html.push_str("</ol>"),
                TagEnd::Item => html.push_str("</li>"),
                TagEnd::Emphasis => html.push_str("</em>"),
                TagEnd::Strong => html.push_str("</strong>"),
                TagEnd::Strikethrough => html.push_str("</del>"),
                TagEnd::Link => {
                    if in_unsafe_link {
                        in_unsafe_link = false;
                    } else {
                        html.push_str("</a>");
                    }
                }
                TagEnd::Image => {
                    in_image = false;
                    html.push_str(&escape_html(&image_alt));
                }
                TagEnd::Table => html.push_str("</table>"),
                TagEnd::TableHead => html.push_str("</thead>"),
                TagEnd::TableRow => html.push_str("</tr>"),
                TagEnd::TableCell => html.push_str("</td>"),
                TagEnd::HtmlBlock => {}
                _ => {}
            },
            Event::Text(text) => html.push_str(&escape_html(&text)),
            Event::Code(code) => {
                html.push_str("<code>");
                html.push_str(&escape_html(&code));
                html.push_str("</code>");
            }
            Event::Html(text) | Event::InlineHtml(text) => {
                html.push_str(&escape_html(&text));
            }
            Event::SoftBreak | Event::HardBreak => html.push_str("\n"),
            Event::Rule => html.push_str("<hr />"),
            _ => {}
        }
    }

    html
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                    Contact-link helpers                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Build a `mailto:` URL with an optional subject.
///
/// The address and subject are percent-encoded so the result is safe for an `href` attribute.
pub fn mailto_url(email: &indielinks_shared::entities::UserEmail, subject: Option<&str>) -> String {
    let mut url = format!("mailto:{email}");
    if let Some(subject) = subject {
        let encoded = percent_encode(subject);
        url += &format!("?subject={encoded}");
    }
    url
}

/// Percent-encode a string for use in a URL query parameter value.
fn percent_encode(s: &str) -> String {
    let mut encoded = String::new();
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte))
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Tests                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use indielinks_shared::api::InstanceDocuments;

    #[test]
    fn paragraphs_are_wrapped() {
        let input = "Hello, world!";
        let html = markdown_to_html(input);
        assert_eq!(html, "<p>Hello, world!</p>");
    }

    #[test]
    fn headings_at_all_levels() {
        let input = "# H1\n## H2\n### H3";
        let html = markdown_to_html(input);
        assert_eq!(html, "<h1>H1</h1><h2>H2</h2><h3>H3</h3>");
    }

    #[test]
    fn emphasis_and_strong() {
        let input = "*em* and **strong**";
        let html = markdown_to_html(input);
        assert_eq!(html, "<p><em>em</em> and <strong>strong</strong></p>");
    }

    #[test]
    fn lists_render() {
        let input = "- a\n- b\n";
        let html = markdown_to_html(input);
        assert_eq!(html, "<ul><li>a</li><li>b</li></ul>");
    }

    #[test]
    fn ordered_lists_render() {
        let input = "1. a\n2. b\n";
        let html = markdown_to_html(input);
        assert_eq!(html, "<ol><li>a</li><li>b</li></ol>");
    }

    #[test]
    fn code_spans_and_blocks() {
        let input = "`inline` and\n\n```rust\nlet x = 1;\n```";
        let html = markdown_to_html(input);
        assert!(html.contains("<code>inline</code>"));
        assert!(html.contains("<pre><code>let x = 1;\n</code></pre>"));
    }

    #[test]
    fn blockquotes_render() {
        let input = "> quote";
        let html = markdown_to_html(input);
        assert_eq!(html, "<blockquote><p>quote</p></blockquote>");
    }

    #[test]
    fn thematic_break_renders() {
        let input = "---";
        let html = markdown_to_html(input);
        assert_eq!(html, "<hr />");
    }

    #[test]
    fn tables_render() {
        let input = "|a|b|\n|---|---|\n|c|d|";
        let html = markdown_to_html(input);
        assert!(html.contains("<table>"));
        assert!(html.contains("<tr>"));
        assert!(html.contains("<td>c</td>"));
    }

    #[test]
    fn raw_html_is_escaped() {
        let input = "<script>alert('xss')</script>";
        let html = markdown_to_html(input);
        assert!(!html.contains("<script>"));
        assert!(html.contains("&lt;script&gt;"));
    }

    #[test]
    fn safe_links_allowed() {
        let input = "[link](https://example.com)";
        let html = markdown_to_html(input);
        assert!(html.contains(r#"<a href="https://example.com">"#));
    }

    #[test]
    fn fragment_links_allowed() {
        let input = "[anchor](#section)";
        let html = markdown_to_html(input);
        assert!(html.contains(r##"<a href="#section">"##));
    }

    #[test]
    fn relative_links_allowed() {
        let input = "[home](/about)";
        let html = markdown_to_html(input);
        assert!(html.contains(r#"<a href="/about">"#));
    }

    #[test]
    fn mailto_links_allowed() {
        let input = "[email](mailto:a@b.com)";
        let html = markdown_to_html(input);
        assert!(html.contains(r#"<a href="mailto:a@b.com">"#));
    }

    #[test]
    fn unsafe_schemes_stripped() {
        let input = "[bad](javascript:alert(1))";
        let html = markdown_to_html(input);
        assert!(!html.contains("href="));
        assert!(html.contains("bad"));
    }

    #[test]
    fn data_uri_stripped() {
        let input = "[bad](data:text/html,<script>alert(1)</script>)";
        let html = markdown_to_html(input);
        assert!(!html.contains("href="));
    }

    #[test]
    fn images_omitted_alt_preserved() {
        let input = "![alt text](https://example.com/img.png)";
        let html = markdown_to_html(input);
        assert!(!html.contains("<img"));
        assert!(html.contains("alt text"));
    }

    #[test]
    fn blank_input_yields_empty() {
        assert_eq!(markdown_to_html(""), "");
        assert_eq!(markdown_to_html("   "), "");
    }

    #[test]
    fn strikethrough_renders() {
        let input = "~~deleted~~";
        let html = markdown_to_html(input);
        assert!(html.contains("<del>deleted</del>"));
    }

    fn sample_documents() -> InstanceDocuments {
        InstanceDocuments {
            about: "About text".to_string(),
            documentation: "Docs text".to_string(),
            privacy_policy: "Privacy text".to_string(),
            terms_of_service: "Terms text".to_string(),
            motd: None,
        }
    }

    #[test]
    fn document_about_selects_about_field() {
        let docs = sample_documents();
        assert_eq!(Document::About.markdown(&docs), "About text");
    }

    #[test]
    fn document_documentation_selects_documentation_field() {
        let docs = sample_documents();
        assert_eq!(Document::Documentation.markdown(&docs), "Docs text");
    }

    #[test]
    fn document_privacy_selects_privacy_policy_field() {
        let docs = sample_documents();
        assert_eq!(Document::Privacy.markdown(&docs), "Privacy text");
    }

    #[test]
    fn document_terms_selects_terms_of_service_field() {
        let docs = sample_documents();
        assert_eq!(Document::Terms.markdown(&docs), "Terms text");
    }

    #[test]
    fn all_documents_are_distinct() {
        let all = Document::ALL;
        assert_eq!(all.len(), 4);
        assert_ne!(all[0], all[1]);
        assert_ne!(all[0], all[2]);
        assert_ne!(all[0], all[3]);
        assert_ne!(all[1], all[2]);
        assert_ne!(all[1], all[3]);
        assert_ne!(all[2], all[3]);
    }

    #[test]
    fn document_labels_and_headings_are_non_empty() {
        for doc in Document::ALL {
            assert!(!doc.label().is_empty());
            assert!(!doc.heading().is_empty());
        }
    }

    #[test]
    fn mailto_url_plain_address() {
        let email = indielinks_shared::entities::UserEmail::new("admin@example.com").unwrap();
        assert_eq!(mailto_url(&email, None), "mailto:admin@example.com");
    }

    #[test]
    fn mailto_url_with_subject() {
        let email = indielinks_shared::entities::UserEmail::new("a@b.com").unwrap();
        assert_eq!(
            mailto_url(&email, Some("Hello world")),
            "mailto:a@b.com?subject=Hello%20world"
        );
    }

    #[test]
    fn mailto_url_subject_with_plus_and_ampersand() {
        let email = indielinks_shared::entities::UserEmail::new("x@y.com").unwrap();
        assert_eq!(
            mailto_url(&email, Some("A + B & C")),
            "mailto:x@y.com?subject=A%20%2B%20B%20%26%20C"
        );
    }

    #[test]
    fn percent_encode_spaces_and_specials() {
        let encoded = super::percent_encode("foo bar+baz%qux");
        assert_eq!(encoded, "foo%20bar%2Bbaz%25qux");
    }
}
