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

//! # add-user
//!
//! Sub-command implementation for provisioning an indielinks instance with a new user.

use chrono::{Duration, Utc};
use http::{
    header::{AUTHORIZATION, CONTENT_TYPE},
    Method, Request,
};
use secrecy::{SecretBox, SecretString};
use snafu::{Backtrace, ResultExt, Snafu};
use tower::{Service, ServiceExt};

use indielinks_shared::{
    api::{LoginReq, LoginRsp, MintKeyReq, MintKeyRsp, Password, SignupReq},
    entities::{UserEmail, Username},
    origin::Origin,
};

use crate::service::ReqBody;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The tower service returns {source}"))]
    Call {
        source: Box<dyn std::error::Error + Send + Sync>,
        backtrace: Backtrace,
    },
    #[snafu(display("While deserializing the login response body, {source}"))]
    LoginBody {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While deserializing the \"mint key\" response body, {source}"))]
    MintKeyBody {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
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
    #[snafu(display("indielinks returned status {status}"))]
    Response {
        status: http::StatusCode,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

pub async fn add_user<C>(
    mut client: C,
    api: &Origin,
    username: &Username,
    password: &SecretBox<Password>,
    email: &UserEmail,
) -> Result<SecretString>
where
    // It would be nice to have an alias for this, but the trait_alias feature is only on nightly
    C: Service<
            http::Request<ReqBody>,
            Response = http::Response<bytes::Bytes>,
            Error = Box<dyn std::error::Error + Send + Sync>,
        > + Clone,
{
    // If the response body can be deserialized to a JSON document with an "error" field, log it.
    //
    // This is kind of lame; but I don't want to push `ErrorResponseBody` down into the
    // indielinks-shared crate, because then I can't implement `axum::IntoResponse` (without taking
    // a dependency on axum, at any rate). I'm not sure what the best approach is, here, so I'm
    // going to leave this here for now. If it threatens to spread, I'll have to tighten-up the
    // contracts between the API & clients.
    fn log_error(rsp: &http::Response<bytes::Bytes>) {
        if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_slice::<serde_json::Value>(rsp.body())
        {
            if let Some(serde_json::Value::String(msg)) = map.get("error") {
                tracing::error!("{msg}")
            }
        }
    }

    let request = SignupReq {
        username: username.clone(),
        password: password.clone(),
        email: email.clone(),
        discoverable: Some(true),
        display_name: None,
        summary: None,
    };
    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("{api}/api/v1/users/signup"))
        .header(CONTENT_TYPE, "application/json")
        .body(ReqBody::Signup(request))
        .context(RequestSnafu)?;
    let response = client
        .ready()
        .await
        .context(ReadySnafu)?
        .call(request)
        .await
        .context(CallSnafu)?;

    if !response.status().is_success() {
        log_error(&response);
        return ResponseSnafu {
            status: response.status(),
        }
        .fail();
    }

    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("{api}/api/v1/users/login"))
        .header(CONTENT_TYPE, "application/json")
        .body(ReqBody::Login(LoginReq {
            username: username.clone(),
            password: password.clone(),
        }))
        .context(RequestSnafu)?;
    let response = client
        .ready()
        .await
        .context(ReadySnafu)?
        .call(request)
        .await
        .context(CallSnafu)?;

    if !response.status().is_success() {
        log_error(&response);
        return ResponseSnafu {
            status: response.status(),
        }
        .fail();
    }

    let token = serde_json::from_slice::<LoginRsp>(response.body())
        .context(LoginBodySnafu)?
        .token;

    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("{api}/api/v1/users/mint-key"))
        .header(CONTENT_TYPE, "application/json")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(ReqBody::MintKey(MintKeyReq {
            // Should probably make the key expiry configurable
            expiry: Utc::now() + Duration::days(365),
        }))
        .context(RequestSnafu)?;

    let response = client
        .ready()
        .await
        .context(ReadySnafu)?
        .call(request)
        .await
        .context(CallSnafu)?;

    if !response.status().is_success() {
        log_error(&response);
        return ResponseSnafu {
            status: response.status(),
        }
        .fail();
    }

    Ok(serde_json::from_slice::<MintKeyRsp>(response.body())
        .context(MintKeyBodySnafu)?
        .key_text
        .into())
}
