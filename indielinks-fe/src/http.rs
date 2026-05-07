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

//! # indielinks-fe HTTP utilities
// This module is in transition; see below

use std::{result::Result as StdResult, sync::Arc};

use gloo_net::http::Request;
use leptos::prelude::*;
use secrecy::{ExposeSecret, SecretString};
use snafu::{IntoError, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use wasm_cookies::FromUrlEncodingError;

use indielinks_shared::api::{LoginRsp, REFRESH_CSRF_COOKIE, REFRESH_CSRF_HEADER_NAME};

use crate::types::{Api, Token, USER_AGENT};

// To be removed (?)
pub fn string_for_status(
    rsp: gloo_net::http::Response,
) -> Result<gloo_net::http::Response, String> {
    let status = rsp.status();
    if status >= 200 && status < 300 {
        Ok(rsp)
    } else {
        Err(rsp.status_text())
    }
}

// To be removed
pub fn error_for_status(
    rsp: gloo_net::http::Response,
) -> Result<gloo_net::http::Response, gloo_net::Error> {
    let status = rsp.status();
    if status >= 200 && status < 300 {
        Ok(rsp)
    } else {
        Err(gloo_net::Error::GlooError(rsp.status_text()))
    }
}

// To be removed
pub async fn refresh_token1() -> Result<(), gloo_net::Error> {
    let api = use_context::<Api>()
        .expect("No context for the API location!?")
        .0;
    let token = use_context::<Token>().expect("No context for the access token!?");

    // We need to prove that we have code execution privileges by copying the CSRF token from it's cookie to
    // a request header
    let csrf_token = wasm_cookies::get(REFRESH_CSRF_COOKIE)
        .ok_or(gloo_net::Error::GlooError(
            "Missing refresh CSRF cookie".to_owned(),
        ))?
        .map_err(|_| gloo_net::Error::GlooError("Invalid refresh CSRF cookie value".to_owned()))?;
    let rsp = Request::post(&format!("{api}/api/v1/users/refresh"))
        .credentials(web_sys::RequestCredentials::Include)
        .header("User-Agent", USER_AGENT)
        .header(REFRESH_CSRF_HEADER_NAME, &csrf_token)
        .send()
        .await
        .and_then(error_for_status)?
        .json::<LoginRsp>()
        .await?;
    token.set(Some(rsp.token));
    Ok(())
}

/// Attempt a request; if the request is denied with 401 Unauthorized, refresh our access token &
/// re-try
// To be removed
pub async fn send_with_retry<F, Fut>(
    make_request: F,
) -> Result<gloo_net::http::Response, gloo_net::Error>
where
    F: Fn() -> Fut,
    // It would be nice to derive the error type from F. The approach: Change refresh_token() to
    // return a dedicated error type (it could be a sum type). Make callers implement `From<this
    // type>` for their error types. Then, they can write their functors to return their own error
    // types.
    Fut: Future<Output = Result<gloo_net::http::Response, gloo_net::Error>>,
{
    // Huh. Seems prolix.
    // Also, do I want to return a distinguishable error code for "token expired", to trigger the refresh?
    match make_request().await {
        Ok(rsp) => {
            if rsp.status() == 401 {
                refresh_token1().await?;
                make_request().await
            } else {
                Ok(rsp)
            }
        }
        err @ Err(_) => {
            return err;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

// `Backtrace` isn't `Clone`, so just forgoing it until I know I need it.
#[derive(Clone, Debug, Snafu)]
pub enum Error {
    #[snafu(display("While decoding the CSRF cookie, {source}"))]
    CookieDecoding {
        #[snafu(source(from(FromUrlEncodingError, Arc::new)))]
        source: Arc<FromUrlEncodingError>,
    },
    #[snafu(display("While deserializing the login resource, {source}"))]
    LoginResponseDeser {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    #[snafu(display("No authorization token available & unable to refresh it"))]
    NoRefresh {},
    #[snafu(display("No authorization token available"))]
    NoToken {},
    #[snafu(display("Failed to send an HTTP request: {source}"))]
    Send {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    #[snafu(display("A request returned HTTP status code {status_code} {status_text}"))]
    Status {
        status_code: u16,
        status_text: String,
    },
}

// Soon:
// type Result<T> = std::result::Result<T, Error>;

// Rename this to just `error_for_status`
pub fn error_for_status1(rsp: gloo_net::http::Response) -> Result<gloo_net::http::Response, Error> {
    let status = rsp.status();
    if status >= 200 && status < 300 {
        Ok(rsp)
    } else {
        StatusSnafu {
            status_code: status,
            status_text: rsp.status_text(),
        }
        .fail()
    }
}

/// Attempt to refresh an expired token, or obtain one in the first place, based on the CSRF cookie.
///
/// This function returns an `Option<SecretString>` on success, `None` on successful completion, but
/// inability to refresh the token (no CSRF cookie, for instance), and an `Error` on, well, error
/// (no network, for instance). On successful refresh, the `Token` context will be updated.
///
/// This function expects the `Token` and `Api` contexts to be available.
pub async fn refresh_token() -> StdResult<Option<SecretString>, Error> {
    let api = expect_context::<Api>().0;
    let token = expect_context::<Token>();

    // We need to prove that we have code execution privileges by copying the CSRF token from it's cookie to
    // a request header
    let csrf_token = match wasm_cookies::get(REFRESH_CSRF_COOKIE) {
        None => return Ok(None),
        Some(Err(err)) => {
            return Err(CookieDecodingSnafu {}.into_error(err));
        }
        Some(Ok(cookie)) => cookie,
    };

    let response = Request::post(&format!("{api}/api/v1/users/refresh"))
        .credentials(web_sys::RequestCredentials::Include)
        .header("User-Agent", USER_AGENT)
        .header(REFRESH_CSRF_HEADER_NAME, &csrf_token)
        .send()
        .await
        .context(SendSnafu)?
        .pipe(error_for_status1)?
        .json::<LoginRsp>()
        .await
        .context(LoginResponseDeserSnafu)?;

    token.set(Some(response.token.clone()));

    Ok(Some(response.token.into()))
}

/// Attempt a request; if the request is denied with 401 Unauthorized, refresh our access token &
/// re-try
// To be renamed
pub async fn send_with_retry1<F>(make_request: F) -> Result<gloo_net::http::Response, Error>
where
    F: Fn() -> gloo_net::http::RequestBuilder,
{
    let send_request = |token: &str| {
        make_request()
            .header("User-Agent", USER_AGENT)
            .header("Authorization", &format!("Bearer {token}"))
            .send()
    };

    let token = expect_context::<Token>();

    match token.get() {
        Some(token) => {
            // We have a token laying around, so use it. Retry with refresh on 401.
            let response = send_request(&token).await.context(SendSnafu)?;
            if response.status() == 401 {
                match refresh_token().await? {
                    Some(token) => send_request(token.expose_secret()).await.context(SendSnafu),
                    None => NoRefreshSnafu.fail(),
                }
            } else {
                Ok(response)
            }
        }
        None => match refresh_token().await? {
            Some(token) => {
                // We just refreshed-- make the request & let the chips fall where they may.
                send_request(token.expose_secret()).await.context(SendSnafu)
            }
            // No token, no refresh cooke-- we're done.
            None => NoRefreshSnafu.fail(),
        },
    }
}
