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

//! # The indielinks Integration Tests
//!
//! # Introduction
//!
//! The Rust unit & integration testing framework is really oriented toward testing *libraries*, not
//! programs. There's no notion of test fixtures, nor even of simple setup & teardown operations
//! that apply to multiple tests. [Nextest] provides a great number of features above & beyond what
//! `cargo test` offers, but 1) is implemented as a Cargo plugin (i.e. to use it one needs to say
//! `cargo nextest` rather than `cargo test`) and 2) still provides nothing in terms of test
//! fixtures.
//!
//! [Nextest]: https://nexte.st
//!
//! T.J. Telan [found] himself in the same boat when building [Fluvio] and lays out an approach that
//! takes more work, but offers limitless opportunities for customization while still fitting into
//! the `cargo test` framework: change-out the default *test harness*. In Cargo.toml, one is free to
//! explicitly define tests, and opt-out of `libharness`, the default testing harness provided by
//! Rust:
//!
//! ```toml
//! [[test]]
//!     name = "delicious"
//!     harness = false
//! ```
//!
//! [found]: https://tjtelan.com/blog/rust-custom-test-harness/
//! [Fluvio]: https://github.com/infinyon/fluvio
//!
//! With this configuration, `cargo test` expects to find a file named `delicious.rs` in the `tests`
//! subdirectory containing a `main()`. `cargo test` will compile it and execute it, passing any
//! command-line parameters passed to `cargo test` after the `--`. It is expected to exit with
//! status zero if all tests passed, and one else. That's it-- that's the contract.
//!
//! [Advanced Testing in Rust] notes that a compliant implementation could simply ignore the
//! command-line parameters, albeit at the cost of surprising one's users, and suggests the use of
//! [libtest-mimic] to avoid that.
//!
//! [Advanced Testing in Rust]: https://rust-exercises.com/advanced-testing/00_intro/00_welcome.html
//! [libtest-mimic]: https://docs.rs/libtest-mimic/latest/libtest_mimic/index.html
//!
//! For all that guidance, I'm still feeling my way, and already this implementation has grown
/// far beyond what I'd envisioned when I started down this path. My general idea is to build a set of
/// integration test programs, each exercising some aspect of indielinks. For now, I'm going to
/// treat each integration test program as a fixture unto itself. I don't really see a lot of
/// difference between this (one integration test per fixture) and allowing multiple fixtures per
/// integration test; I just prefer to present a finer-grained test suite to the Rust test
/// framework.
///
/// # Project Structure
///
/// The crate that owns this file, `indielinks-libtest` is a crate that produces a library package
/// (this one) and multiple integration test packages (one binary for each). Code that's applicable
/// to all integration tests belongs here ([test_healthcheck], e.g., can be usefully registered as a
/// test by any integration test).
///
/// Code relating to the test framework itself (e.g. the `Test` struct) belongs in `tests/common`.
/// Integration test programs themselves go in `tests`.
use std::{
    collections::HashSet,
    convert::Infallible,
    result::Result as StdResult,
    sync::atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use base64::{prelude::BASE64_STANDARD, Engine};
use chrono::Utc;
use http::{header, HeaderValue};
use itertools::Itertools;
use libtest_mimic::Failed;
use once_cell::sync::Lazy;
use picky::{
    hash::HashAlgorithm,
    http::{http_signature::HttpSignatureBuilder, HttpSignature},
    key::{PrivateKey, PublicKey},
    signature::SignatureAlgorithm,
};
use reqwest::Url;
use secrecy::SecretString;
use sha2::Digest;
use wiremock::{
    matchers::{method, path},
    Mock, ResponseTemplate,
};

use indielinks_shared::{
    entities::{StorUrl, Username},
    origin::Origin,
};

use indielinks::{
    ap_entities::{self, make_user_id},
    entities::FollowId,
    peppers::{Pepper, Version as PepperVersion},
};

pub static TEST_USER_AGENT: &str = "indielinks integration tests/0.0.1; +sp1ff@pobox.com";

#[path = "activity-pub.rs"]
pub mod activity_pub;
pub mod actor;
pub mod background;
pub mod cache;
pub mod delicious;
pub mod follow;
pub mod users;
pub mod webfinger;

/// Hit the `indielinks` healthcheck endpoint; panic on anything other than success.
///
/// This is a legit test that also demonstrates a few things that can be done with this framework.
///
/// Firstly, it's async: when registering it, the `Test` instance can use
/// [futures::executor::block_on], like this:
///
/// ```ignore
/// inventory::submit!(Test {
///     name: "000test_healthcheck",
///     test_fn: |cfg, _drop_posts| { futures::executor::block_on(test_healthcheck(&cfg.url)) },
/// });
/// ```
///
/// Secondly, it returns a `Result<(), Failed>` ([Failed] comes from [libtest-mimic]). This let's me
/// use the `?` sigil conveniently for fallible code that I nevertheless expect to succeed and am
/// not interested in testing.
///
/// [libtest-mimic]: https://docs.rs/libtest-mimic/latest/libtest_mimic/index.html
pub async fn test_healthcheck(url: Url) -> Result<(), Failed> {
    let request_text = reqwest::get(url.join("/healthcheck")?)
        .await?
        .text()
        .await?;
    assert!("GOOD" == request_text);
    Ok(())
}

/// Implementations of this trait will be passed to each test function to enable them to do things
/// like talk directly to the back-end, ensure a known starting point for the test, & so on. Each
/// integration test will need to provide an implemetnation.
#[async_trait]
pub trait Helper {
    /// Remove all posts belonging to `username`
    async fn clear_posts(&self, username: &Username) -> Result<(), Failed>;
    /// Create an indielinks user with given followers on federated servers
    async fn create_user(
        &self,
        pepper_version: &PepperVersion,
        pepper_key: &Pepper,
        username: &Username,
        password: &SecretString,
        followers: &HashSet<StorUrl>,
        following: &HashSet<(StorUrl, FollowId)>,
    ) -> Result<String, Failed>;
    /// Remove a user
    async fn remove_user(&self, username: &Username) -> Result<(), Failed>;
}

/// A mocked-up user on a peer ActivityPub server
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerUser {
    name: Username,
    priv_key: PrivateKey,
}

static PEER_USER_COUNTER: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));

impl PeerUser {
    pub fn new() -> Result<PeerUser, Failed> {
        Ok(PeerUser {
            name: Username::new(
                &format!("mock-user-{}", PEER_USER_COUNTER.fetch_add(1, Ordering::Relaxed)),
            ).unwrap(/* known good */),
            priv_key: picky::key::PrivateKey::generate_rsa(2048)?,
        })
    }
    pub fn id(&self, origin: &Origin) -> Result<Url, Failed> {
        Ok(make_user_id(&self.name, origin)?)
    }
    pub fn name(&self) -> &Username {
        &self.name
    }
    pub fn priv_key(&self) -> &PrivateKey {
        &self.priv_key
    }
    pub fn pub_key(&self) -> Result<PublicKey, Failed> {
        Ok(self.priv_key.to_public_key()?)
    }
}

/// Return a [Mock] for a [PeerUser]'s inbox that will match on GET requests
/// to `/users/{username}`.
pub async fn peer_actor(user: &PeerUser, origin: &Origin) -> Result<Mock, Failed> {
    Ok(Mock::given(method("GET"))
        .and(path(format!("/users/{}", user.name())))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(
                ap_entities::Jld::new(
                    &ap_entities::Actor::from_username_and_key(
                        user.name(),
                        origin,
                        &user.pub_key()?,
                    )
                    .unwrap(),
                    None,
                )
                .unwrap(/* known good */)
                .to_string(),
                "application/activity+json",
            ),
        ))
}

// Not sure this is needed anymore, in light of my cleanup of ActivityPub logic. It's been heavily
// hacked-up so as to compile outside the indielinks::authn module.
pub fn ensure_sha_256(
    request: http::Request<reqwest::Body>,
) -> StdResult<http::Request<reqwest::Body>, Infallible> {
    // Seems like a *lot* of work just to check for the presence of a SHA-256 digest... Ah: the joys
    // of HTTP.
    match request
        .headers()
        .get_all("digest")
        .iter()
        .filter_map(|h| {
            h.to_str()
                .map(|s| {
                    s.to_lowercase()
                        .starts_with("sha-256=")
                        .then_some(s.to_owned())
                })
                .unwrap_or(None)
        })
        .at_most_one()
        .unwrap()
    {
        Some(_) => Ok(request),
        None => {
            let (mut parts, body) = request.into_parts();
            let mut hasher = sha2::Sha256::new();
            hasher.update(body.as_bytes().unwrap());
            let sha_256_result = format!(
                "sha-256={}",
                BASE64_STANDARD.encode(hasher.finalize().as_slice())
            );
            parts
                .headers
                .append("digest", HeaderValue::from_str(&sha_256_result).unwrap());
            Ok(http::Request::from_parts(parts, body))
        }
    }
}

// Not sure this is needed anymore, in light of my cleanup of ActivityPub logic.
// It's been hacked-up heavily to get it to compile outside the authn module.
pub fn sign_request(
    request: http::Request<reqwest::Body>,
    key_id: &str,
    private_key: &picky::key::PrivateKey,
) -> StdResult<(http::Request<reqwest::Body>, HttpSignature), Infallible> {
    let (parts, body) = request.into_parts();

    assert!(parts.headers.contains_key("Date"));

    assert!(parts.headers.contains_key("Host"));

    assert!(parts.headers.contains_key("Content-Type"));

    let http_signature = HttpSignatureBuilder::new()
        .key_id(key_id)
        .signature_method(
            private_key,
            SignatureAlgorithm::RsaPkcs1v15(HashAlgorithm::SHA2_256),
        )
        // `picky::http::http_request::HttpRequest` trait is implemented for
        // `http::request::Parts` for `http` crate with `http_trait_impl` feature gate
        .generate_signing_string_using_http_request(&parts)
        .request_target()
        .http_header(header::CONTENT_TYPE.as_str())
        .http_header(header::DATE.as_str())
        .http_header("digest")
        .http_header(header::HOST.as_str())
        .build()
        .unwrap();

    Ok((http::Request::from_parts(parts, body), http_signature))
}

/// Take an HTTP verb/method, URL and a reqwest [Body]. Return a signed reqwest Request. The signature
/// uses a key ID of "http://localhost:{}/users/test-user".
///
/// [Body]: reqwest::Body
pub async fn make_signed_request(
    method: http::Method,
    url: Url,
    body: reqwest::Body,
    origin: &Origin,
    priv_key: &PrivateKey,
    username: &Username,
) -> Result<reqwest::Request, Failed> {
    let req = http::Request::builder()
        .method(method)
        .uri(url.as_ref())
        .header(reqwest::header::CONTENT_TYPE, "application/activity+json")
        .header(reqwest::header::ACCEPT, "application/activity+json")
        .header(
            reqwest::header::DATE,
            Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
        )
        .header(reqwest::header::HOST, "localhost")
        .body(body)?;
    let req = ensure_sha_256(req)?;
    let (mut req, sig) = sign_request(req, &format!("{}/users/{}", origin, username), priv_key)?;
    req.headers_mut().append(
        "Signature",
        http::HeaderValue::from_str(&sig.to_string()[10..])?,
    );
    Ok(req.try_into()?)
}
