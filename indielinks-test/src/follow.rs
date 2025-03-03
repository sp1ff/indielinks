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

//! Integration tests for ActivityPub follows
//!
//! I'm collecting integration tests for sending & receiving ActivityPub [Follow]s.
//!
//! [Follow]: https://www.w3.org/TR/activitystreams-vocabulary/#dfn-follow

use axum::{
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use chrono::Utc;
use indielinks::{ap_entities, authn::sign_request, entities::Username};
use libtest_mimic::Failed;
use reqwest::{Client, Url};
use uuid::Uuid;

/// First integration test for ActivityPub [Follow]s.
///
/// [Follow]: https://www.w3.org/TR/activitystreams-vocabulary/#dfn-follow
pub async fn accept_follow_smoke(
    url: Url,
    username: Username,
    local_port: u16,
) -> Result<(), Failed> {
    // This test takes the form of a conversation between a mock ActivityPub server implemented in
    // this test, and indielinks. We'll begin by forming a `Follow` request for the test indielinks
    // user:
    let follow = ap_entities::Follow::new(
        url.join("/users/sp1ff/inbox")?,
        Url::parse(&format!(
            "http://localhost:{}/{}",
            local_port,
            Uuid::new_v4().hyphenated()
        ))?,
        Url::parse(&format!("http://localhost:{}/users/test-user", local_port))?,
    );
    // Serialize that to JSON-LD, and coerce *that* into an axum `Body`:
    let body: axum::body::Body =
        ap_entities::to_jrd(follow, ap_entities::Type::Follow, None)?.into();
    // Now, finally, build an axum `Request` containing the `Follow`:
    let request = axum::http::Request::builder()
        .method(axum::http::Method::POST)
        .uri(url.join(&format!("/users/{}/inbox", &username))?.as_str())
        .header(reqwest::header::CONTENT_TYPE, "application/activity+json")
        .header(
            reqwest::header::DATE,
            Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
        )
        .header(reqwest::header::HOST, "localhost")
        .body(body)?;
    // We need a keypair for our "test user". We start by generating a 2048 bit RSA private key...
    let priv_key = picky::key::PrivateKey::generate_rsa(2048)?;
    // and using that to sign the request.
    let request = sign_request(
        request,
        &format!("http://localhost:{}/users/test-user", local_port),
        &priv_key,
    )
    .await?;
    // `request` is now a *reqwest* `Request`, not an axum `Request`!

    // Now: spin-up a local web server...
    let listener = tokio::net::TcpListener::bind(&format!("localhost:{}", local_port)).await?;
    // which will require the corresponding public key.
    let pub_key = priv_key.to_public_key()?;
    let and_port = format!(":{}", local_port);
    // Our server will serve two endpoints...
    let app = Router::<()>::new()
        .route(
            // the endpoint corresponding to the above "key ID"; it will return an `Actor`
            // representing our "test user", which will include the public key (so indielinks can
            // validate our signatures)...
            "/users/test-user",
            get(|| async move {
                (
                    http::status::StatusCode::OK,
                    ap_entities::to_jrd(
                        ap_entities::Actor::from_username_and_key(
                            &Username::new("test-user").unwrap(),
                            "http",
                            "localhost",
                            &and_port,
                            &pub_key,
                        )
                        .unwrap(),
                        ap_entities::Type::Actor,
                        None,
                    )
                    .unwrap(),
                )
                    .into_response()
            }),
        )
        .route(
            // and the endpoint corresponding to our test user's public inbox.
            "/users/test-user/inbox",
            post(
                |axum::extract::Json(_accept): axum::extract::Json<ap_entities::Accept>| async {
                    // We need to validate the signature on `accept`!
                    (http::status::StatusCode::ACCEPTED, ()).into_response()
                },
            ),
        );
    // It will use a "one shot" channel to handle graceful shutdown.
    let (shutdown_sender, shutdown_reader) = tokio::sync::oneshot::channel::<()>();
    // Alright-- spawn the server on a Tokio task...
    let handle = tokio::task::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_reader.await;
            })
            .await
            .expect("Failed to serve requests");
    });
    // send our Follow request to indielinks...
    let client = Client::builder()
        .user_agent("indielinks integration tests/0.0.1; +sp1ff@pobox.com")
        .build()?;
    let rsp = client.execute(request).await?;

    assert!(rsp.status() == reqwest::StatusCode::CREATED);

    // We need to check that our follow was acepted!

    // Shut-down the server...
    shutdown_sender
        .send(())
        .expect("Failed to send shutdown signal");
    // and wait to be sure it's done.
    handle.await.expect("Spawned server panic'd");

    Ok(())
}
