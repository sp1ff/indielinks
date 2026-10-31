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

//! Integration tests for the actor API

use std::{collections::HashSet, sync::Arc};

use libtest_mimic::Failed;
use picky::key::PrivateKey;
use reqwest::{Client, Url};
use secrecy::SecretString;
use uuid::Uuid;
use wiremock::MockServer;

use indielinks_shared::{entities::Username, origin::Origin};

use indielinks::{
    ap_entities::{ActorField, Jld, Like},
    peppers::{Pepper, Version as PepperVersion},
};

use crate::{helper::Helper, make_signed_request, peer_actor, PeerUser, TEST_USER_AGENT};

/// Reject a Like whose body names an actor different from the signing actor.
///
/// Mallory signs a request with her own key, but the `Like` activity inside the request body
/// claims Alice as its actor. The handler compares the authenticated actor (Mallory) against the
/// actor declared in the payload (Alice) and must reject the request with 400 Bad Request.
pub async fn like_actor_mismatch(
    indielinks: Url,
    pepper_version: PepperVersion,
    pepper_key: Pepper,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    let username = Username::new("like_actor_mismatch_user").unwrap(/* known good */);
    let password: SecretString = "0534e7529239fed032a49953ee6ba4d9".to_owned().into();

    helper.remove_user(&username).await?;

    // Mallory: an honest peer, signing with her own key.
    let mallory = PeerUser {
        name: Username::new("mallory").unwrap(/* known good */),
        priv_key: PrivateKey::generate_rsa(2048)?,
    };

    let mock_server = MockServer::start().await;
    let mock_origin: Origin = mock_server.uri().try_into()?;
    peer_actor(&mallory, &mock_origin)
        .await?
        .mount(&mock_server)
        .await;

    let client = Client::builder().user_agent(TEST_USER_AGENT).build()?;

    // Create a local user whose inbox Mallory will target.
    helper
        .create_user(
            &pepper_version,
            &pepper_key,
            &username,
            &password,
            &HashSet::new(),
            &HashSet::new(),
        )
        .await?;

    // Mallory signs a Like, but the Like claims Alice is the actor.
    let like_object = indielinks.join(&format!("/users/{username}/posts/{}", Uuid::new_v4()))?;
    let like_id = Url::parse(&format!("{mock_origin}/{}", Uuid::new_v4().hyphenated()))?;
    let alice_actor_id = Url::parse(&format!("{mock_origin}/users/alice"))?;
    let like = Like::new(like_object, like_id, ActorField::Iri(alice_actor_id));

    let request = make_signed_request(
        axum::http::Method::POST,
        indielinks.join(&format!("/users/{username}/inbox"))?,
        Jld::new(&like, None)?.to_string().into(),
        &mock_origin,
        mallory.priv_key(),
        mallory.name(),
    )
    .await?;

    let rsp = client.execute(request).await?;
    assert_eq!(
        rsp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "Like with mismatched actor should be rejected with 400 Bad Request"
    );

    Ok(())
}
