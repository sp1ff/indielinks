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
//! I'm collecting integration tests for sending & receiving ActivityPub [Follow]s here.
//!
//! [Follow]: https://www.w3.org/TR/activitystreams-vocabulary/#dfn-follow

use std::sync::{Arc, Mutex};

use axum::{
    extract::State,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use chrono::Utc;
use indielinks::{
    actor::CollectionPage,
    ap_entities::{self, to_jrd},
    authn::sign_request,
    entities::Username,
};
use libtest_mimic::Failed;
use picky::key::PrivateKey;
use reqwest::{Client, Url};
use tap::Pipe;
use uuid::Uuid;

/// Take an HTTP verb/method, URL and an axum [Body]. Return a signed reqwest Request. The signature
/// uses a key ID of "http://localhost:{}/users/test-user".
async fn make_request(
    method: axum::http::Method,
    url: Url,
    body: axum::body::Body,
    local_port: u16,
    priv_key: &PrivateKey,
) -> Result<reqwest::Request, Failed> {
    axum::http::Request::builder()
        .method(method)
        .uri(url.as_ref())
        .header(reqwest::header::CONTENT_TYPE, "application/activity+json")
        .header(
            reqwest::header::DATE,
            Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
        )
        .header(reqwest::header::HOST, "localhost")
        .body(body)?
        .pipe(|request| async move {
            sign_request(
                request,
                &format!("http://localhost:{}/users/test-user", local_port),
                priv_key,
            )
            // drive this future to completion before key_id goes out of scope
            .await
        })
        .await?
        .pipe(Ok)
}

struct TestState {
    pub accepted: Arc<Mutex<usize>>,
}

async fn inbox_taking_accept(
    State(state): State<Arc<TestState>>,
    axum::extract::Json(_accept): axum::extract::Json<ap_entities::Accept>,
) -> axum::response::Response {
    // We need to validate the signature on `accept`!
    let mut data = state.accepted.lock().expect("Failed to lock the mutex");
    *data += 1;
    (http::status::StatusCode::ACCEPTED, ()).into_response()
}

/// First integration test for ActivityPub [Follow]s.
///
/// [Follow]: https://www.w3.org/TR/activitystreams-vocabulary/#dfn-follow
///
/// Very simple: send a follow, validate that we receive an [Accept], then hit the followers
/// endpoint & make sure we show-up.
///
/// [Accept]: https://www.w3.org/TR/activitystreams-vocabulary/#dfn-accept
pub async fn accept_follow_smoke(
    url: Url,
    username: Username,
    domain: String,
    local_port: u16,
) -> Result<(), Failed> {
    // This test takes the form of a conversation between a mock ActivityPub server implemented in
    // this test, and indielinks. We need a keypair for our "test user". We start by generating a
    // 2048 bit RSA private key...
    let priv_key = picky::key::PrivateKey::generate_rsa(2048)?;

    // We'll also need a client:
    let client = Client::builder()
        .user_agent("indielinks integration tests/0.0.1; +sp1ff@pobox.com")
        .build()?;

    // We'll begin by forming a `Follow` request for the test indielinks
    // user.
    let follow = ap_entities::Follow::new(
        url.join("/users/sp1ff/inbox")?,
        Url::parse(&format!(
            "http://localhost:{}/{}",
            local_port,
            Uuid::new_v4().hyphenated()
        ))?,
        Url::parse(&format!("http://localhost:{}/users/test-user", local_port))?,
    );
    // Nb. that `request` is now a *reqwest* `Request`, not an axum `Request`!
    let request = make_request(
        axum::http::Method::POST,
        url.join(&format!("/users/{}/inbox", &username))?,
        to_jrd(&follow, ap_entities::Type::Follow, None)?.into(),
        local_port,
        &priv_key,
    )
    .await?;

    // OK-- that's our Follow request. Now spin-up a local web server...
    let listener = tokio::net::TcpListener::bind(&format!("localhost:{}", local_port)).await?;
    // which will require the corresponding public key.
    let pub_key = priv_key.to_public_key()?;
    let hostname = format!("localhost:{}", local_port);
    let state = Arc::new(TestState {
        accepted: Arc::new(Mutex::new(0)),
    });
    // Our server will serve two endpoints...
    let app = Router::<Arc<TestState>>::new()
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
                            &hostname,
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
            post(inbox_taking_accept),
        )
        .with_state(state.clone());
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

    // That's it-- we're ready to follow the test user:
    let rsp = client.execute(request).await?;

    assert!(rsp.status() == reqwest::StatusCode::CREATED);
    assert_eq!(*state.accepted.lock().expect("Failed to lock mutex"), 1);

    // Let's at least check that the follower now shows-up!
    let request = make_request(
        axum::http::Method::GET,
        url.join(&format!("/users/{}/followers", &username))?,
        axum::body::Body::default(),
        local_port,
        &priv_key,
    )
    .await?;

    let rsp = client.execute(request).await?;
    assert_eq!(rsp.status(), http::StatusCode::OK);

    let page = rsp.json::<CollectionPage>().await?;
    assert_eq!(page.total_items, 1);

    let first = page.first.unwrap();

    let request = make_request(
        axum::http::Method::GET,
        first,
        axum::body::Body::default(),
        local_port,
        &priv_key,
    )
    .await?;

    let rsp = client.execute(request).await?;
    assert_eq!(rsp.status(), http::StatusCode::OK);
    let page = rsp.json::<CollectionPage>().await?;
    assert_eq!(
        page.first.unwrap(),
        Url::parse(&format!("https://{}/users/sp1ff/followers?page=0", domain))?
    );
    assert!(page.next.is_none());
    assert_eq!(
        page.part_of.unwrap(),
        Url::parse(&format!("https://{}/users/sp1ff/followers", domain))?
    );
    let items = page.ordered_items.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].as_ref(),
        &format!("http://localhost:{}/users/test-user", local_port)
    );

    // Shut-down the server...
    shutdown_sender
        .send(())
        .expect("Failed to send shutdown signal");
    // and wait to be sure it's done.
    handle.await.expect("Spawned server panic'd");

    Ok(())
}
