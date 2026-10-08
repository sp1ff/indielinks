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

//! # bookmarklets
//!
//! API for the indielinks bookmarklet
//!
//! ## Introduction
//!
//! The most frequent use case for saving bookmarks to indielinks will be doing so from the user's
//! browser. I first thought to offer a browser extension for this, but this has a few downsides:
//!
//! - either an extension per browser, or the overhead of cross-browser support
//! - the process of extension signing & review
//!
//! While playing around with my Pinboard [bookmarklet], I realized they weren't using an extension
//! at all: the link uses a bit of Javascript to collect the page's URL & title and submits them to
//! an API endpoint that responds with an HTML form inviting me to add tags & notes. On submit, the
//! form goes to a different endpoint that saves the page to my account.
//!
//! This seemed much simpler than authoring an extension. A bookmarklet, unlike an extension, has
//! limited access to the page on which its invoked, but it has enough for our purposes. It turns
//! out that this approach was [used] by del.icio.us, as well. This module implements the same
//! approach for [indielinks](crate).
//!
//! [bookmarklet]: https://grokipedia.com/page/Bookmarklet
//! [used]: https://grokipedia.com/page/Bookmarklet#naming-and-popularization
//!
//! ## Authentication
//!
//! Since indielinks doesn't use session-based authentication cookies (see [here](crate::users)),
//! authenticating requests to this API is a little trickier. When the bookmarklet sends a request
//! to this API, it _will_ include the refresh token, from which we can both extract a username, and
//! establish that the user is logged-in. Depending only on that would, of course, leave us open to
//! CSRF attacks, but we can mitigate that in the traditional way:
//!
//! - generate a nonce & sign it on the server side
//! - include it in the generated form transmitted back to the user
//! - _and_ include it in a new cookie set in the response
//!
//! In a CSRF attack, the attacker could induce the victim to transmit the cookie, but would have no
//! way to mint a matching token in the forged form.

use std::{collections::HashSet, result::Result as StdResult, sync::Arc};

use askama::Template;
use axum::{
    extract::{Query, State},
    response::IntoResponse,
    routing::{get, post},
    Extension, Form, Router,
};
use axum_extra::extract::{cookie::Cookie, CookieJar};
use chrono::Utc;
use crypto_common::KeyInit;
use hmac::HmacReset;
use http::{
    header::{CONTENT_TYPE, SET_COOKIE},
    HeaderMap, HeaderValue, Method, Request, StatusCode,
};
use indielinks_shared::{
    api::REFRESH_COOKIE,
    entities::{Tagname, Username},
    nonempty_string::NonEmptyString,
};
use itertools::Itertools;
use jwt::VerifyWithKey;
use opentelemetry::KeyValue;
use rand::Rng;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use tower::{Service, ServiceExt};
use tower_http::set_header::SetResponseHeaderLayer;
use tracing::{debug, error, warn};
use url::Url;
use uuid::Uuid;

use crate::{
    app_logic::add_post,
    client_types::{BoxedError, ClientType},
    define_metric,
    indielinks::Indielinks,
    signing_keys::SigningKey,
    token::{verify_refresh_token, RefreshCsrfToken, Verified},
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("While adding this post, {source}"))]
    AddPost {
        #[snafu(source(from(crate::app_logic::Error, Box::new)))]
        source: Box<crate::app_logic::Error>,
    },
    #[snafu(display("While making the Pinboard request, {source}"))]
    Client {
        #[snafu(source(from(either::Either<std::convert::Infallible, BoxedError>, Box::new)))]
        source: Box<either::Either<std::convert::Infallible, BoxedError>>,
        backtrace: Backtrace,
    },
    #[snafu(display("While setting a cookie, {source}"))]
    CookieAsHeader {
        source: http::header::InvalidHeaderValue,
        backtrace: Backtrace,
    },
    #[snafu(display("CSRF token mismatch; possible Cross-Site Request Forgery!"))]
    CsrfMismatch {
        cookie: String,
        token: String,
        backtrace: Backtrace,
    },
    #[snafu(display("While deriving the signature key, {source}"))]
    Hmac {
        source: crypto_common::InvalidLength,
        backtrace: Backtrace,
    },
    #[snafu(display("A required authentication cookie was missing from the request"))]
    MissingCookie { backtrace: Backtrace },
    #[snafu(display("No user by this name"))]
    NoSuchUser { backtrace: Backtrace },
    #[snafu(display("Pinboard request failed: {result_code}"))]
    Pinboard {
        result_code: String,
        backtrace: Backtrace,
    },
    #[snafu(display("While encoding the Pinboard request parameters, {source}"))]
    PinboardParams {
        source: serde_urlencoded::ser::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While building the Pinboard HTTP request, {source}"))]
    PinboardRequest {
        source: http::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to deserialize a Pinboard response body: {source}"))]
    PinboardResponse {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While lookingup this post, {source}"))]
    PostLookup { source: crate::storage::Error },
    #[snafu(display("While rendering the Askana template, {source}"))]
    Render {
        source: askama::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While signing the CSRF token, {source}"))]
    Signature { source: crate::token::Error },
    #[snafu(display("While finding the signing key, {source}"))]
    SigningKey { source: crate::signing_keys::Error },
    #[snafu(display("Invalid tag name: {source}"))]
    Tag {
        source: indielinks_shared::entities::Error,
    },
    #[snafu(display("Bad refresh token: {source}"))]
    Token { source: crate::token::Error },
    #[snafu(display("While lookingup this user, {source}"))]
    UserLookup { source: crate::storage::Error },
    #[snafu(display("Invalid CSRF token: {source}"))]
    Verification {
        source: jwt::Error,
        backtrace: Backtrace,
    },
}

type Result<T> = StdResult<T, Error>;

/// The canonical palette shared by the browser frontend and bookmarklet pages.
const THEME_STYLES: &str = include_str!("../../indielinks-shared/assets/theme.css");

/// Bookmarklet-specific layout and control treatment.
const BOOKMARKLET_STYLES: &str = r#"
:root {
    color-scheme: light;
    font-family: system-ui, sans-serif;
}
:root[data-theme="dark"] {
    color-scheme: dark;
}
body.bookmarklet-page {
    min-height: 100vh; margin: 0; padding: 24px 16px; box-sizing: border-box;
    background: var(--indielinks-background-3); color: var(--indielinks-foreground-1); font-size: 15px; line-height: 22px;
}
.bookmarklet-card, .bookmarklet-status {
    width: min(100%, 640px); margin: 0 auto; padding: 24px; box-sizing: border-box;
    border: 1px solid var(--indielinks-stroke-2); border-top: 3px solid var(--indielinks-heritage); border-radius: 6px;
    background: var(--indielinks-background-1); box-shadow: var(--indielinks-shadow);
}
.bookmarklet-status { width: min(100%, 440px); text-align: center; }
.bookmarklet-mark { display: inline-block; margin-bottom: 18px; font-size: 20px; font-weight: 700; line-height: 24px; }
.bookmarklet-mark span { color: var(--indielinks-heritage); }
.bookmarklet-card__heading, .bookmarklet-status__heading { margin: 0; font-size: 24px; font-weight: 650; line-height: 30px; }
.bookmarklet-card__introduction, .bookmarklet-status__message { margin: 6px 0 20px; color: var(--indielinks-foreground-2); }
.bookmarklet-optional { color: var(--indielinks-foreground-2); font-weight: 400; }
.indielinks-form, .form-field { display: grid; min-width: 0; }
.indielinks-form { gap: 16px; }
.form-field { gap: 6px; }
.form-field__label, .form-options__legend { font-size: 14px; font-weight: 600; line-height: 20px; }
.form-control {
    width: 100%; min-width: 0; min-height: 40px; padding: 8px 10px; box-sizing: border-box;
    border: 1px solid var(--indielinks-stroke-1); border-radius: 6px; background: var(--indielinks-background-1); color: inherit;
    font: inherit; line-height: 22px;
}
.form-control:hover { border-color: var(--indielinks-stroke-1-hover); }
.form-control:focus { outline: none; border-color: var(--indielinks-focus); box-shadow: 0 0 0 1px var(--indielinks-focus); }
.form-control--textarea { min-height: 104px; resize: vertical; }
.form-options { display: grid; min-width: 0; margin: 0; padding: 12px; gap: 10px; border: 1px solid var(--indielinks-stroke-2); border-radius: 6px; }
.form-options__legend { padding-inline: 4px; }
.form-options--inline { grid-template-columns: repeat(2, minmax(0, max-content)); gap: 8px 20px; }
.form-check { display: flex; align-items: flex-start; min-width: 0; gap: 8px; cursor: pointer; }
.form-check input { flex: 0 0 auto; width: 16px; height: 16px; margin: 2px 0 0; accent-color: var(--indielinks-brand-background); }
.form-actions { display: flex; align-items: center; flex-wrap: wrap; margin-top: 4px; gap: 8px; }
.form-button {
    display: inline-flex; align-items: center; justify-content: center; min-height: 40px; padding: 8px 14px;
    box-sizing: border-box; border: 1px solid transparent; border-radius: 6px; font: inherit;
    font-size: 14px; font-weight: 600; line-height: 20px; cursor: pointer;
}
.form-button:focus-visible { outline: 2px solid var(--indielinks-focus); outline-offset: 2px; }
.form-button--primary { border-color: var(--indielinks-brand-background); background: var(--indielinks-brand-background); color: var(--indielinks-foreground-on-brand); }
.form-button--primary:hover { border-color: var(--indielinks-brand-background-hover); background: var(--indielinks-brand-background-hover); }
.form-button--secondary { border-color: var(--indielinks-stroke-1); background: var(--indielinks-background-1); color: var(--indielinks-foreground-1); }
.form-button--secondary:hover { background: var(--indielinks-background-3); }
.bookmarklet-status__details { margin: 0 0 20px; color: var(--indielinks-foreground-2); text-align: left; }
.bookmarklet-status__details pre { max-height: 12rem; margin: 8px 0 0; overflow: auto; padding: 8px; border: 1px solid var(--indielinks-stroke-2); background: var(--indielinks-background-3); font: 12px/16px ui-monospace, monospace; white-space: pre-wrap; }
@media (max-width: 479px) {
    body.bookmarklet-page { padding: 16px 12px; }
    .bookmarklet-card, .bookmarklet-status { padding: 18px; }
    .form-options--inline { grid-template-columns: minmax(0, 1fr); }
    .form-actions { align-items: stretch; flex-direction: column; }
    .form-button { width: 100%; }
}
"#;

/// Set the bookmarklet document's palette before its body can paint.
const BOOKMARKLET_THEME_SCRIPT: &str = r#"
(() => {
    let theme;
    try {
        theme = localStorage.getItem("indielinks.theme.v1");
    } catch (_) {}
    if (theme !== "light" && theme !== "dark") {
        theme = typeof matchMedia === "function" && matchMedia("(prefers-color-scheme: dark)").matches
            ? "dark"
            : "light";
    }
    document.documentElement.dataset.theme = theme;
})();
"#;

/// Server-rendered status page for bookmarklet completion and failures.
#[derive(Template)]
#[template(ext = "html", in_doc = true)]
/// ```askama
/// <!doctype html>
/// <html lang="en">
/// <head>
///     <meta charset="UTF-8">
///     <meta name="viewport" content="width=device-width, initial-scale=1">
///     <title>{{ title }} · indielinks</title>
///     <style>{{ theme_styles|safe }}</style>
///     <style>{{ styles|safe }}</style>
///     <script>{{ theme_script|safe }}</script>
/// </head>
/// <body class="bookmarklet-page">
///     <main class="bookmarklet-status" {% if error %}role="alert"{% endif %}>
///         <span aria-hidden="true" class="bookmarklet-mark">indie<span>links</span></span>
///         <h1 class="bookmarklet-status__heading">{{ title }}</h1>
///         <p class="bookmarklet-status__message">{{ message }}</p>
///         {% if details.is_some() %}
///         <details class="bookmarklet-status__details">
///             <summary>Technical details</summary>
///             <pre>{{ details.as_ref().unwrap() }}</pre>
///         </details>
///         {% endif %}
///         <button autofocus class="form-button form-button--secondary" onclick="window.close()" type="button">Close</button>
///         {% if close %}<script>window.close();</script>{% endif %}
///     </main>
/// </body>
/// </html>
/// ```
struct StatusPage {
    theme_styles: &'static str,
    styles: &'static str,
    theme_script: &'static str,
    title: &'static str,
    message: &'static str,
    details: Option<String>,
    error: bool,
    close: bool,
}

impl StatusPage {
    fn error(details: impl Into<String>) -> Self {
        Self {
            theme_styles: THEME_STYLES,
            styles: BOOKMARKLET_STYLES,
            theme_script: BOOKMARKLET_THEME_SCRIPT,
            title: "Something went wrong",
            message: "The link could not be saved. Try again, or close this window and return to the page.",
            details: Some(details.into()),
            error: true,
            close: false,
        }
    }

    fn not_logged_in() -> Self {
        Self {
            theme_styles: THEME_STYLES,
            styles: BOOKMARKLET_STYLES,
            theme_script: BOOKMARKLET_THEME_SCRIPT,
            title: "Sign in required",
            message: "Sign in to indielinks, then try saving this link again.",
            details: None,
            error: false,
            close: false,
        }
    }

    fn saved() -> Self {
        Self {
            theme_styles: THEME_STYLES,
            styles: BOOKMARKLET_STYLES,
            theme_script: BOOKMARKLET_THEME_SCRIPT,
            title: "Link saved",
            message: "This window will close automatically.",
            details: None,
            error: false,
            close: true,
        }
    }
}

fn render_status(status: StatusCode, page: StatusPage) -> axum::response::Response {
    match page.render() {
        Ok(html) => (status, html).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#?}")).into_response(),
    }
}

pub const CSRF_TOKEN_COOKIE: &str = "indielinks-add-csrf";

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         authentication                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "bookmarklets.authn.successes", bookmarklets_auth_successes, Sort::IntegralCounter }
define_metric! { "bookmarklets.auth.not_logged_in", bookmarklets_auth_not_logged_in, Sort::IntegralCounter }
define_metric! { "bookmarklets.auth.failures", bookmarklets_auth_failures, Sort::IntegralCounter }

/// Authenticate a request to the bookmarklets API
///
/// We demand that the caller at least have a valid & current refresh cookie. See the [users](users)
/// module documentation for more information on this.
async fn authenticate(
    State(state): State<Arc<Indielinks>>,
    jar: CookieJar,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    async fn authenticate1(state: Arc<Indielinks>, jar: &CookieJar) -> Result<Username> {
        let refresh_token_str = jar
            .get(REFRESH_COOKIE)
            .context(MissingCookieSnafu)?
            .value_trimmed();
        let (token, _, _, _) =
            verify_refresh_token(refresh_token_str, &state.signing_keys, state.origin.host())
                .context(TokenSnafu)?;
        Ok(token.claims().subject().clone())
    }

    match authenticate1(state, &jar).await {
        Ok(username) => {
            debug!("Authenticated user {}", username);
            bookmarklets_auth_successes.add(1, &[KeyValue::new("username", username.to_string())]);
            request.extensions_mut().insert(username);
            next.run(request).await
        }
        Err(Error::MissingCookie { .. }) => {
            warn!("No login cookie!");
            bookmarklets_auth_not_logged_in.add(1, &[]);
            render_status(StatusCode::UNAUTHORIZED, StatusPage::not_logged_in())
        }
        Err(err) => {
            error!("Failed to authenticate this request: {err:?}");
            bookmarklets_auth_failures.add(1, &[]);

            render_status(
                StatusCode::INTERNAL_SERVER_ERROR,
                StatusPage::error(format!("{err:#?}")),
            )
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                              save                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "bookmarklets.save.successes", bookmarklets_save_successes, Sort::IntegralCounter }
define_metric! { "bookmarklets.save.failures", bookmarklets_save_failures, Sort::IntegralCounter }

/// Incoming "save" request
#[derive(Clone, Debug, Deserialize)]
struct SaveRequest {
    url: Url,
    title: NonEmptyString,
    description: String,
}

/// This is an [askama] "template"-- a type-safe Jinja-like form.
///
/// ```askama
/// <!doctype html>
/// <html lang="en">
/// <head>
///     <meta charset="UTF-8">
///     <meta name="viewport" content="width=device-width, initial-scale=1">
///     <title>Save to indielinks</title>
///     <style>{{ theme_styles|safe }}</style>
///     <style>{{ styles|safe }}</style>
///     <script>{{ theme_script|safe }}</script>
/// </head>
/// <body class="bookmarklet-page">
///     <main class="bookmarklet-card">
///         <span aria-hidden="true" class="bookmarklet-mark">indie<span>links</span></span>
///         <h1 class="bookmarklet-card__heading">Save link</h1>
///         <p class="bookmarklet-card__introduction">Review the details before adding this page to your links.</p>
///     <form method="POST" action="add" class="indielinks-form">
///         <input type="hidden" name="csrf_token" value="{{ csrf_token }}">
///
///         <div class="form-field">
///             <label class="form-field__label" for="url">URL</label>
///             <input class="form-control" id="url" name="url" readonly value="{{ url }}">
///         </div>
///         <div class="form-field">
///             <label class="form-field__label" for="title">Title</label>
///             <input class="form-control" id="title" name="title" required value="{{ title }}">
///         </div>
///         <div class="form-field">
///             <label class="form-field__label" for="description">Notes <span class="bookmarklet-optional">(optional)</span></label>
///             <textarea class="form-control form-control--textarea" id="description" name="description" rows="4">{{ description }}</textarea>
///         </div>
///         <div class="form-field">
///             <label class="form-field__label" for="tags">Tags <span class="bookmarklet-optional">(optional)</span></label>
///             <input class="form-control" id="tags" name="tags" value="{{ tags }}">
///         </div>
///         <fieldset class="form-options form-options--inline">
///             <legend class="form-options__legend">Link options</legend>
///             <label class="form-check"><input id="private" name="private" type="checkbox" value="true" {% if private %}checked{% endif %}><span>Private</span></label>
///             <label class="form-check"><input id="to-read" name="to_read" type="checkbox" value="true" {% if to_read %}checked{% endif %}><span>Read later</span></label>
///         </fieldset>
///         <div class="form-actions">
///             <button class="form-button form-button--primary" type="submit">Save link</button>
///             <button class="form-button form-button--secondary" onclick="window.close()" type="button">Cancel</button>
///         </div>
///     </form>
///     </main>
/// </body>
/// </html>
/// ```
#[derive(Template)]
#[template(ext = "html", in_doc = true)]
struct SaveForm {
    theme_styles: &'static str,
    styles: &'static str,
    theme_script: &'static str,
    csrf_token: String,
    url: Url,
    title: NonEmptyString,
    description: String,
    // I feel like this type is incorrect, but I guess we'd need something like a `HashSet<TagName`
    // that formats to a space-delimited list?
    tags: String,
    private: bool,
    to_read: bool,
}

/// Given a signing key, mint a CSRF token, sign it & base64-encode it for transmission
// It irritates me that this is so similar to `mint_refresh_and_csrf_token()`, yet just different
// enough that it's difficult to refactor.
fn mint_csrf_token(signing_key: &SigningKey) -> Result<String> {
    // Arguably abusing `RefreshCsrfToken`, here...
    let session = Uuid::new_v4();
    let nonce = rand::rng().next_u64();
    let key: HmacReset<Sha256> =
        HmacReset::new_from_slice(signing_key.as_ref().expose_secret()).context(HmacSnafu)?;
    RefreshCsrfToken::new(session, nonce)
        .sign_with_key(&key)
        .context(SignatureSnafu)?
        .as_str()
        .to_owned()
        .pipe(Ok)
}

/// Serve a dynamically-generated HTML Form prompting the user to save the current page
async fn save(
    State(state): State<Arc<Indielinks>>,
    username: Extension<Username>,
    headers: HeaderMap,
    Query(request): Query<SaveRequest>,
) -> axum::response::Response {
    async fn save1(
        state: Arc<Indielinks>,
        username: &Username,
        mut headers: HeaderMap,
        request: SaveRequest,
    ) -> Result<(HeaderMap, String)> {
        let (_, signing_key) = state.signing_keys.current().context(SigningKeySnafu)?;
        let csrf_token = mint_csrf_token(&signing_key)?;
        let csrf_cookie = Cookie::build((CSRF_TOKEN_COOKIE, csrf_token.clone()))
            .path("/")
            .secure(true)
            .http_only(true);

        headers.insert(
            SET_COOKIE,
            format!("{csrf_cookie}")
                .try_into()
                .context(CookieAsHeaderSnafu)?,
        );

        let user = state
            .storage
            .as_ref()
            .user_for_name(username.as_ref())
            .await
            .context(UserLookupSnafu)?
            .context(NoSuchUserSnafu)?;

        let post = state
            .storage
            .get_post(user.id(), &request.url.clone().into())
            .await
            .context(PostLookupSnafu)?;

        // The URL & title given in the request always win, and we'll overwrite the description, if
        // it's given.
        let (description, tags, private, to_read) = if let Some(post) = post {
            let description = if !request.description.is_empty() {
                request.description
            } else {
                post.notes().map(str::to_owned).unwrap_or(String::new())
            };
            (
                description,
                post.tags().join(" "),
                !post.public(),
                post.unread(),
            )
        } else {
            (request.description, Default::default(), true, false)
        };

        let html = SaveForm {
            theme_styles: THEME_STYLES,
            styles: BOOKMARKLET_STYLES,
            theme_script: BOOKMARKLET_THEME_SCRIPT,
            csrf_token,
            url: request.url,
            title: request.title,
            description,
            tags,
            private,
            to_read,
        }
        .render()
        .context(RenderSnafu)?;

        Ok((headers, html))
    }

    match save1(state, &username.0, headers, request).await {
        Ok((headers, html)) => {
            debug!("Sending back a save form for {}", username.to_string());
            bookmarklets_save_successes.add(1, &[KeyValue::new("username", username.to_string())]);
            (StatusCode::OK, headers, html).into_response()
        }
        Err(err) => {
            error!("Failed to save this link: {err:?}");
            bookmarklets_save_failures.add(1, &[]);
            render_status(
                StatusCode::INTERNAL_SERVER_ERROR,
                StatusPage::error(format!("{err:#?}")),
            )
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                              add                                               //
////////////////////////////////////////////////////////////////////////////////////////////////////

define_metric! { "bookmarklets.add.successes", bookmarklets_add_successes, Sort::IntegralCounter }
define_metric! { "bookmarklets.add.failures", bookmarklets_add_failures, Sort::IntegralCounter }

#[derive(Clone, Debug, Deserialize)]
struct AddRequest {
    csrf_token: String,
    url: Url,
    title: NonEmptyString,
    // This has to be a string, because the bookmarklet will send the description regardless
    description: String,
    // Will show-up whitespace-delimited
    tags: String,
    private: Option<bool>,
    to_read: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
struct PinboardAddRequest {
    url: Url,
    description: NonEmptyString,
    notes: Option<NonEmptyString>,
    // Again whitespace-delimited
    tags: String,
    private: bool,
    toread: bool,
    auth_token: String,
    r#format: String,
}

#[derive(Clone, Debug, Deserialize)]
struct PinboardAddResponse {
    result_code: String,
}

async fn send_to_pinboard(
    token: &SecretString,
    mut client: ClientType,
    request: AddRequest,
    tags: HashSet<Tagname>,
) -> Result<()> {
    let request = PinboardAddRequest {
        url: request.url,
        description: request.title,
        notes: if request.description.is_empty() {
            None
        } else {
            Some(request.description.try_into().unwrap(/* known good */))
        },
        tags: tags.into_iter().map(|t| t.to_string()).join(" "),
        private: request.private.unwrap_or(true),
        toread: request.to_read.unwrap_or(true),
        auth_token: token.expose_secret().to_owned(),
        r#format: "json".to_owned(),
    };
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "https://api.pinboard.in/v1/posts/add?{}",
            serde_urlencoded::to_string(&request).context(PinboardParamsSnafu)?
        ))
        .body(Default::default())
        .context(PinboardRequestSnafu)?;
    debug!("Pinboard request: {request:#?}");
    let response = client
        .ready()
        .await
        .context(ClientSnafu)?
        .call(request)
        .await
        .context(ClientSnafu)?;

    // Regrettably, the Pinboard API often returns 200 even on failure-- we have to examine the
    // response body to determine whether our request succeeded or not.
    if !response.status().is_success() {
        let response_text =
            String::from_utf8_lossy(response.into_body().to_vec().as_slice()).into_owned();
        return PinboardSnafu {
            result_code: response_text,
        }
        .fail();
    }

    let response: PinboardAddResponse =
        serde_json::from_slice(response.into_body().to_vec().as_slice())
            .context(PinboardResponseSnafu)?;
    if response.result_code != "done" {
        return PinboardSnafu {
            result_code: response.result_code,
        }
        .fail();
    }

    Ok(())
}

/// Complete the bookmarklet form submission
async fn add(
    State(state): State<Arc<Indielinks>>,
    jar: CookieJar,
    username: Extension<Username>,
    Form(request): Form<AddRequest>,
) -> axum::response::Response {
    async fn add1(
        state: Arc<Indielinks>,
        jar: CookieJar,
        username: &Username,
        request: AddRequest,
    ) -> Result<()> {
        // Let's begin by validating the CSRF token (this should really be factored-out):
        let csrf_cookie = jar
            .get(CSRF_TOKEN_COOKIE)
            .context(MissingCookieSnafu)?
            .value();
        if csrf_cookie != request.csrf_token {
            error!(
                "WARNING: CSRF token mismatch! cookie: {csrf_cookie}, token: {}.",
                request.csrf_token
            );
            return CsrfMismatchSnafu {
                cookie: csrf_cookie,
                token: request.csrf_token,
            }
            .fail();
        }

        let (_, signing_key) = state.signing_keys.current().context(SigningKeySnafu)?;
        let key: HmacReset<Sha256> =
            HmacReset::new_from_slice(signing_key.as_ref().expose_secret()).context(HmacSnafu)?;
        let _: RefreshCsrfToken<Verified> = csrf_cookie
            .verify_with_key(&key)
            .context(VerificationSnafu)?;

        // Deserialize any tags,
        let tags = request
            .tags
            .split_whitespace()
            .map(Tagname::new)
            .collect::<StdResult<HashSet<Tagname>, _>>()
            .context(TagSnafu)?;

        // lookup the `User`,
        let user = state
            .storage
            .as_ref()
            .user_for_name(username.as_ref())
            .await
            .context(UserLookupSnafu)?
            .context(NoSuchUserSnafu)?;

        // and add the post:
        let description: Option<NonEmptyString> = if request.description.is_empty() {
            None
        } else {
            Some(request.description.clone().try_into().unwrap(/* known good */))
            // Ugh
        };
        let _ = add_post(
            state.clone(),
            &user,
            &request.url,
            &request.title,
            Some(Utc::now()),
            description.as_ref(),
            &tags,
            true,
            !request.private.unwrap_or(false),
            request.to_read.unwrap_or(false),
            state.storage.as_ref(),
            state.task_sender.clone(),
        )
        .await
        .context(AddPostSnafu)?;

        // This is a "feature" I've added essentially for my own use-- if indielinks is configured
        // with a Pinboard API token, we'll send this link there, too.
        if let Some(token) = &state.pinboard_token {
            send_to_pinboard(token, state.general_purpose_client.clone(), request, tags).await?;
        }

        Ok(())
    }

    debug!("incoming form: {request:#?}");

    match add1(state, jar, &username.0, request).await {
        Ok(_) => {
            debug!("Successfully added a link for {}.", username.0);
            bookmarklets_add_successes.add(1, &[KeyValue::new("username", username.0.to_string())]);
            render_status(StatusCode::ACCEPTED, StatusPage::saved())
        }
        Err(err) => {
            error!("Failed to add this link: {err:?}");
            bookmarklets_add_failures.add(1, &[]);
            render_status(
                StatusCode::INTERNAL_SERVER_ERROR,
                StatusPage::error(format!("{err:#?}")),
            )
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Public API                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn make_router(state: Arc<Indielinks>) -> Router<Arc<Indielinks>> {
    Router::new()
        .route("/save", get(save))
        .route("/add", post(add))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            authenticate,
        ))
        .layer(SetResponseHeaderLayer::overriding(
            CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        ))
        .with_state(state)
}
