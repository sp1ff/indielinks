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

use std::sync::Arc;

use libtest_mimic::Failed;
use reqwest::{Client, StatusCode, Url};
use serde_json::json;

use indielinks_shared::{
    api::{LoginRsp, MintKeyRsp, SignupRsp},
    entities::Username,
};

use indielinks::http::ErrorResponseBody;

use crate::Helper;

/// Test `/user/signup`
pub async fn test_signup(url: Url, utils: Arc<dyn Helper + Send + Sync>) -> Result<(), Failed> {
    // Cleanup the test user we'll create if he's around from a previous test
    let _ = utils.remove_user(&Username::new("johndoe").unwrap()).await;

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

/// Test `/users/mint-key`
pub async fn test_mint_key(url: Url, utils: Arc<dyn Helper + Send + Sync>) -> Result<(), Failed> {
    // Cleanup the test user we'll create if he's around from a previous test
    let _ = utils.remove_user(&Username::new("johndoe").unwrap()).await;

    let client = Client::new();

    // Alright-- let's create a user...
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

    // authenticate him...
    let rsp = client
        .post(url.join("/api/v1/users/login")?)
        .json(&json!({"username":"johndoe","password":"f00 b@r sp1at"}))
        .send()
        .await?;
    assert_eq!(StatusCode::OK, rsp.status());
    let token = rsp.json::<LoginRsp>().await?.token;

    // and mint a key...
    let rsp = client
        .get(url.join("/api/v1/users/mint-key")?)
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(StatusCode::CREATED, rsp.status());
    let key_text0 = rsp.json::<MintKeyRsp>().await?.key_text;

    // and a second key...
    let rsp = client
        .get(url.join("/api/v1/users/mint-key")?)
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(StatusCode::CREATED, rsp.status());
    let key_text1 = rsp.json::<MintKeyRsp>().await?.key_text;

    // and a third-- this should push the first key off the end of the list.
    let rsp = client
        .get(url.join("/api/v1/users/mint-key")?)
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(StatusCode::CREATED, rsp.status());
    let key_text2 = rsp.json::<MintKeyRsp>().await?.key_text;

    let rsp = client
        .get(url.join("/api/v1/posts/update")?)
        .header("Authorization", format!("Bearer johndoe:{key_text0}"))
        .send()
        .await?;
    assert_eq!(StatusCode::UNAUTHORIZED, rsp.status());

    let rsp = client
        .get(url.join("/api/v1/posts/update")?)
        .header("Authorization", format!("Bearer johndoe:{key_text1}"))
        .send()
        .await?;
    assert_eq!(StatusCode::OK, rsp.status());

    let rsp = client
        .get(url.join("/api/v1/posts/update")?)
        .header("Authorization", format!("Bearer johndoe:{key_text2}"))
        .send()
        .await?;
    assert_eq!(StatusCode::OK, rsp.status());

    Ok(())
}
