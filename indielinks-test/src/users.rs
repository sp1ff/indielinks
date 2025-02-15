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
// You should have received a copy of the GNU General Public License along with mpdpopm.  If not,
// see <http://www.gnu.org/licenses/>.

//! Integration tests for the user API.
//!
//! Backend-agnostic test logic for the user API goes here.

use indielinks::{http::ErrorResponseBody, users::SignupRsp};
use libtest_mimic::Failed;
use reqwest::{Client, StatusCode, Url};
use serde_json::json;

/// Test `/user/signup`
pub async fn test_signup(url: Url) -> Result<(), Failed> {
    let client = Client::new();

    let rsp = client
        .post(url.join("/api/v1/users/signup")?)
        .json(
            &json!({"username": "johndoe", "password":"f00 b@r sp1at", "email": "jdoe@gmail.com"}),
        )
        .send()
        .await?;
    assert_eq!(StatusCode::CREATED, rsp.status());

    let body = rsp.json::<SignupRsp>().await?;
    assert_eq!("Welcome to indielinks!", body.greeting);

    let rsp = client
        .post(url.join("/api/v1/users/signup")?)
        .json(
            &json!({"username": "johndoe", "password":"f00 b@r sp1at", "email": "jdoe@gmail.com"}),
        )
        .send()
        .await?;
    assert_eq!(StatusCode::BAD_REQUEST, rsp.status());

    let body = rsp.json::<ErrorResponseBody>().await?;
    assert_eq!("Username johndoe is already claimed; sorry", body.error);

    // OK-- now attempt to get a token for our new user
    let rsp = client
        .get(url.join("/api/v1/users/login")?)
        .json(&json!({"username": "johndoe", "password": "f00 b@r sp1at"}))
        .send()
        .await?;
    assert_eq!(StatusCode::OK, rsp.status());

    Ok(())
}
