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

//! Integration tests for the webfinger endpoint.

use indielinks_shared::{entities::Username, origin::Origin};

use indielinks::acct::Account;

use libtest_mimic::Failed;
use reqwest::{StatusCode, Url};

/// Exercise webfinger in a few simple ways
pub async fn webfinger_smoke(url: Url, username: Username, origin: Origin) -> Result<(), Failed> {
    // No or malformed `resource` query param => 400 Bad Request
    let rsp = reqwest::get(url.join("/.well-known/webfinger")?)
        .await?
        .status();
    assert_eq!(rsp, StatusCode::BAD_REQUEST);

    // This is kind of lame; I'm not guaranteed that user "sp1ff2" doesn't actually exist.
    let acct = Account::from_user_and_host("sp1ff2", origin.host())?;

    // `resource` names someone unknown => 404 Not Found
    let rsp = reqwest::get(url.join(&format!(
        "/.well-known/webfinger?resource={}",
        acct.to_uri()
    ))?)
    .await?
    .status();
    assert_eq!(rsp, StatusCode::NOT_FOUND);

    let acct = Account::from_user_and_host(username.as_ref(), origin.host())?;

    let rsp = reqwest::get(url.join(&format!(
        "/.well-known/webfinger?resource={}",
        acct.to_uri()
    ))?)
    .await?;

    assert_eq!(rsp.status(), StatusCode::OK);

    Ok(())
}
