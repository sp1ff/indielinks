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

//! # Integration tests for the indielinks bookmarklets

use std::{collections::HashSet, sync::Arc};

use http::{header::COOKIE, StatusCode};
use lazy_static::lazy_static;
use libtest_mimic::Failed;
use reqwest::Client;
use secrecy::{ExposeSecret, SecretString};
use serde_json::json;
use url::Url;

use indielinks_shared::{
    api::{LoginRsp, SaveRequest},
    entities::Username,
    known_good,
};

use indielinks::peppers::{Pepper, Version as PepperVersion};

use crate::helper::Helper;

lazy_static! {
    static ref TEST_PASSWORD: SecretString = "8046ee8074145ee8".to_owned().into();
}

/// Test the `/save` bookmarklet
// Regrettably, this test turned-out to be useless: it was *meant* to ensure that the DDB backend
// correctly handled querying on the "url" attribute (which is a reserved word in attribute
// expressions). Thing is, ScyllaDB/Alternator happily accepts it. Still: better to leave it-- it
// tests a legitimate function, and could be the basis for more involved bookmarklet tests in the
// future.
pub async fn test_save_bookmarklet(
    indielinks: Url,
    pepper_version: PepperVersion,
    pepper_key: Pepper,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    let username = known_good!(Username::new("test-save-bookmark-user"));

    let _api_key = helper
        .create_user(
            &pepper_version,
            &pepper_key,
            &username,
            &TEST_PASSWORD,
            &HashSet::new(),
            &HashSet::new(),
        )
        .await?;

    let client = Client::builder()
        .user_agent(concat!(
            "indielinks integration tests/",
            env!("CARGO_PKG_VERSION"),
            "; +sp1ff@pobox.com"
        ))
        .build()?;

    // the bookmarklets API just takes a login cookie
    let response = client
        .get(indielinks.join("/api/v1/users/login")?)
        .json(
            &json!({"username":"test-save-bookmark-user","password":TEST_PASSWORD.expose_secret()}),
        )
        .send()
        .await?;
    assert_eq!(StatusCode::OK, response.status());

    let refresh_cookie = response
        .cookies()
        .find(|cookie| cookie.name() == "indielinks-refresh")
        .expect("There should be a refresh token present");
    let refresh_cookie = format!("{}={}", refresh_cookie.name(), refresh_cookie.value());

    let token = response.json::<LoginRsp>().await?.token;

    let mut url = indielinks.join("/bookmarklets/save")?;
    url.set_query(Some(
        known_good!(serde_urlencoded::to_string(SaveRequest {
            url: known_good!(Url::parse("https://example.com")),
            title: known_good!("Example.com".to_owned().try_into()),
            description: "This is a test.".to_owned(),
        }))
        .as_ref(),
    ));

    tracing::debug!("Sending request to: {}", url.as_str());

    let response = client
        .get(url)
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .header(COOKIE, refresh_cookie)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    Ok(())
}
