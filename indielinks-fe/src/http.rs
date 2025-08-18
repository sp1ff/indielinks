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

//! # indielinks-fe HTTP utilities

use gloo_net::http::Request;
use leptos::prelude::*;

use indielinks_shared::{LoginRsp, REFRESH_CSRF_COOKIE, REFRESH_CSRF_HEADER_NAME};

use crate::types::{Api, Token, USER_AGENT};

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

pub async fn refresh_token() -> Result<(), gloo_net::Error> {
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
pub async fn send_with_retry<F, Fut>(
    make_request: F,
) -> Result<gloo_net::http::Response, gloo_net::Error>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<gloo_net::http::Response, gloo_net::Error>>,
{
    // Huh. Seems prolix.
    // Also, do I want to return a distinguishable error code for "token expired", to trigger the refresh?
    match make_request().await {
        Ok(rsp) => {
            if rsp.status() == 401 {
                refresh_token().await?;
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
