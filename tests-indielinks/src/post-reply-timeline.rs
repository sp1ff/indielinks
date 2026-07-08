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

//! # Integration test: own posts & replies appear in the author's home timeline and outbox
//!
//! `app_logic::add_post()` and `app_logic::reply()` insert the new activity into the author's
//! materialized home timeline and outbox. A home timeline is only ever *materialized* from the
//! outboxes of the people the user follows, so a user's own activities surface there exclusively
//! via these in-memory inserts (into an already-loaded timeline). Likewise the in-memory outbox
//! insert keeps a loaded outbox fresh without a storage rebuild — and the reply's durable write
//! happens asynchronously in `SendReply`, so we cannot rely on a rebuild observing it promptly.
//!
//! This test therefore *primes* both structures (one timeline request + one outbox request,
//! each returning empty) before creating any content, so every subsequent post/reply lands as a
//! deterministic in-memory insert that the following fetch observes.

use std::{collections::HashSet, iter::once, sync::Arc};

use chrono::{SecondsFormat, Utc};
use lazy_static::lazy_static;
use libtest_mimic::Failed;
use reqwest::{Body, Client, Method, StatusCode, Url};
use secrecy::SecretString;
use tracing::debug;
use uuid::Uuid;
use wiremock::MockServer;

use indielinks_shared::{
    api::{PostsAllRsp, ReplyRequest, TimelineInitialRsp, TimelineReq},
    entities::{PostId, Username},
    origin::Origin,
};

use indielinks::{
    ap_entities::{
        make_user_followers, make_user_post_id, Create, CreateObject, FirstField, Jld, Note,
        NoteField, Outbox, OutboxPage, Replies, RepliesPage,
    },
    peppers::{Pepper, Version as PepperVersion},
};

use crate::{activity_pub, helper::Helper, make_signed_request, peer_actor, PeerUser};

lazy_static! {
    static ref TEST_PASSWORD: SecretString = "4d9c0b1a2f3e4d5c6b7a8901f2e3d4c5".to_owned().into();
}

/// Fetch the initial page of `username`'s home timeline and return the number of posts it holds
/// (0 if the timeline is empty).
async fn timeline_count(
    client: &Client,
    indielinks: &Url,
    username: &Username,
    api_key: &str,
) -> Result<usize, Failed> {
    let rsp = client
        .post(format!("{indielinks}api/v1/users/timeline"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {username}:{api_key}"),
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_string(&TimelineReq::Initial {
            max_posts: None,
        })?)
        .send()
        .await?
        .error_for_status()?
        .json::<TimelineInitialRsp>()
        .await?;
    Ok(rsp.map(|page| page.posts.len().get()).unwrap_or(0))
}

/// Fetch `username`'s outbox via an HTTP-signed (authorized-fetch) request as `peer_user`,
/// paginating to completion, and return the total number of activities.
async fn outbox_count(
    client: &Client,
    indielinks: &Url,
    username: &Username,
    mock_origin: &Origin,
    peer_user: &PeerUser,
) -> Result<usize, Failed> {
    let rsp = client
        .execute(
            make_signed_request(
                Method::GET,
                indielinks.join(&format!("/users/{username}/outbox"))?,
                Body::default(),
                mock_origin,
                peer_user.priv_key(),
                peer_user.name(),
            )
            .await?,
        )
        .await?;
    assert_eq!(rsp.status(), StatusCode::OK);
    let outbox: Outbox = rsp.json().await?;

    let mut total = 0usize;
    let mut next_url = Some(outbox.first.clone());
    let mut num_pages = 0usize;
    while let Some(page_url) = next_url {
        let rsp = client
            .execute(
                make_signed_request(
                    Method::GET,
                    page_url,
                    Body::default(),
                    mock_origin,
                    peer_user.priv_key(),
                    peer_user.name(),
                )
                .await?,
            )
            .await?;
        assert_eq!(rsp.status(), StatusCode::OK);
        let page: OutboxPage = rsp.json().await?;
        num_pages += 1;
        // Failsafe so a pagination bug can't loop forever.
        assert!(num_pages < 16);
        if page.ordered_items.is_empty() {
            break;
        }
        total += page.ordered_items.len();
        next_url = page.next.clone();
    }
    Ok(total)
}

/// Create a public bookmark on behalf of `username` via the del.icio.us-compatible API.
async fn create_post(
    client: &Client,
    indielinks: &Url,
    username: &Username,
    api_key: &str,
    url: &str,
    description: &str,
    dt: &str,
) -> Result<(), Failed> {
    let status = client
        .get(format!(
            "{indielinks}api/v1/posts/add?url={url}&description={description}&dt={dt}&shared=true"
        ))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {username}:{api_key}"),
        )
        .send()
        .await?
        .status();
    assert_eq!(status, StatusCode::CREATED, "posts/add for {url}");
    Ok(())
}

/// Reply, on behalf of `username`, to a (synthetic) note published by `peer_user`.
///
/// The reply target's exact identity doesn't affect the home-timeline/outbox counts under test;
/// we point it at the resolvable mock peer so the asynchronous federation task has a real actor
/// to resolve.
async fn create_reply(
    client: &Client,
    indielinks: &Url,
    username: &Username,
    api_key: &str,
    mock_origin: &Origin,
    peer_user: &PeerUser,
) -> Result<(), Failed> {
    let request = ReplyRequest {
        id: Url::parse(&format!(
            "{mock_origin}/users/{}/posts/{}",
            peer_user.name(),
            Uuid::new_v4()
        ))?,
        actor: peer_user.id(mock_origin)?,
        text: "Nice post!".to_owned(),
    };
    let status = client
        .post(format!("{indielinks}api/v1/users/reply"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {username}:{api_key}"),
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_string(&request)?)
        .send()
        .await?
        .status();
    assert_eq!(status, StatusCode::ACCEPTED, "users/reply");
    Ok(())
}

/// A user posts and replies; both land in the user's own home timeline and outbox. A second
/// post + reply then show up alongside the first pair.
pub async fn post_reply_timeline(
    indielinks: Url,
    pepper_version: PepperVersion,
    pepper_key: Pepper,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    let username = Username::new("post-reply-timeline-user").unwrap(/* known good */);
    // `setup_test` removes any pre-existing user and mounts a mock peer actor (needed so the
    // server can verify the signed outbox fetches below).
    let (_mock_server, peer_user, client) =
        activity_pub::setup_test(&username, Arc::clone(&helper)).await?;

    let api_key = helper
        .create_user(
            &pepper_version,
            &pepper_key,
            &username,
            &TEST_PASSWORD,
            &HashSet::new(),
            &HashSet::new(),
        )
        .await?;

    let mock_origin: Origin = _mock_server.uri().try_into()?;

    // Prime both structures: materialize an (empty) home timeline and outbox so that the
    // in-memory inserts performed by add_post()/reply() have a target. Both must be empty now.
    assert_eq!(
        timeline_count(&client, &indielinks, &username, &api_key).await?,
        0,
        "primed timeline should be empty"
    );
    assert_eq!(
        outbox_count(&client, &indielinks, &username, &mock_origin, &peer_user).await?,
        0,
        "primed outbox should be empty"
    );

    let t = Utc::now();
    let dt = |offset_secs: i64| {
        (t - chrono::Duration::seconds(offset_secs)).to_rfc3339_opts(SecondsFormat::Secs, true)
    };

    // Round 1: one post, one reply.
    create_post(
        &client,
        &indielinks,
        &username,
        &api_key,
        "https://example.com/post1",
        "Post1",
        &dt(40),
    )
    .await?;
    create_reply(
        &client,
        &indielinks,
        &username,
        &api_key,
        &mock_origin,
        &peer_user,
    )
    .await?;

    assert_eq!(
        timeline_count(&client, &indielinks, &username, &api_key).await?,
        2,
        "home timeline should contain the post and the reply (and nothing else)"
    );
    assert_eq!(
        outbox_count(&client, &indielinks, &username, &mock_origin, &peer_user).await?,
        2,
        "outbox should contain the post and the reply (and nothing else)"
    );

    // Round 2: another post, another reply — the two new items should show up.
    create_post(
        &client,
        &indielinks,
        &username,
        &api_key,
        "https://example.com/post2",
        "Post2",
        &dt(20),
    )
    .await?;
    create_reply(
        &client,
        &indielinks,
        &username,
        &api_key,
        &mock_origin,
        &peer_user,
    )
    .await?;

    assert_eq!(
        timeline_count(&client, &indielinks, &username, &api_key).await?,
        4,
        "home timeline should contain both posts and both replies"
    );
    assert_eq!(
        outbox_count(&client, &indielinks, &username, &mock_origin, &peer_user).await?,
        4,
        "outbox should contain both posts and both replies"
    );

    helper.remove_user(&username).await?;
    Ok(())
}

/// Stand-up a fresh mock ActivityPub server hosting a single mock user, with that user's actor
/// document mounted at `/users/{name}` so the server-under-test can resolve its public key while
/// verifying signed requests.
async fn spawn_mock_peer() -> Result<(MockServer, PeerUser, Origin), Failed> {
    let server = MockServer::start().await;
    let peer = PeerUser::new()?;
    let origin: Origin = server.uri().try_into()?;
    peer_actor(&peer, &origin).await?.mount(&server).await;
    Ok((server, peer, origin))
}

/// Discover the (server-generated) [PostId] of `username`'s single bookmark via the del.icio.us
/// `posts/all` endpoint. `posts/add` returns only "done", so we read the post back to learn its id.
async fn only_post_id(
    client: &Client,
    indielinks: &Url,
    username: &Username,
    api_key: &str,
) -> Result<PostId, Failed> {
    let rsp = client
        .get(format!("{indielinks}api/v1/posts/all"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {username}:{api_key}"),
        )
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await?
        .error_for_status()?
        .json::<PostsAllRsp>()
        .await?;
    assert_eq!(rsp.posts.len(), 1, "expected exactly one post");
    Ok(rsp.posts[0].id())
}

/// Deliver a reply to `in_reply_to` (a local post's AP id) on behalf of `peer`, by forming a public
/// `Create{Note}` addressed to `followers` and POSTing it, HTTP-signed as `peer`, to the shared
/// inbox. Returns the AP id of the reply `Note` (which is what surfaces in the replies collection).
async fn send_reply(
    client: &Client,
    indielinks: &Url,
    in_reply_to: &Url,
    followers: &Url,
    peer: &PeerUser,
    peer_origin: &Origin,
    text: &str,
) -> Result<Url, Failed> {
    let public = Url::parse("https://www.w3.org/ns/activitystreams#Public")?;
    let note_id = Url::parse(&format!(
        "{peer_origin}/users/{}/posts/{}",
        peer.name(),
        Uuid::new_v4()
    ))?;
    let note_replies = {
        let mut url = note_id.clone();
        url.path_segments_mut().unwrap(/* known good */).push("replies");
        url
    };
    let note = Note::new_from_parts(
        note_id.clone(),
        Some(in_reply_to.clone()),
        None,
        peer.id(peer_origin)?,
        once(public.clone()),
        once(followers.clone()),
        text.into(),
        Replies::new(note_replies, None, None),
    )?;
    let create = Create::from_parts(
        Url::parse(&format!(
            "{peer_origin}/users/{}/activities/{}",
            peer.name(),
            Uuid::new_v4()
        ))?,
        peer.id(peer_origin)?,
        once(public),
        once(followers.clone()),
        CreateObject::Note(note),
    )?;

    let request = make_signed_request(
        Method::POST,
        indielinks.join("/inbox")?,
        Body::from(Jld::new(&create, None)?.to_string()),
        peer_origin,
        peer.priv_key(),
        peer.name(),
    )
    .await?;
    assert_eq!(
        client.execute(request).await?.status(),
        StatusCode::ACCEPTED,
        "shared inbox should accept the reply"
    );
    debug!("Sent a reply: {note_id}");
    Ok(note_id)
}

/// Fetch a single page (of `page_size`) from `page_url`, which must already carry a `page=<token>`
/// query, and return the set of reply-`Note` ids it holds. Also returns the `next`-page URL.
async fn replies_page(
    client: &Client,
    page_url: Url,
    page_size: usize,
) -> Result<(HashSet<Url>, Option<Url>), Failed> {
    let mut page_url = page_url;
    page_url
        .query_pairs_mut()
        .append_pair("page_size", &page_size.to_string());
    let page = client
        .get(page_url)
        .send()
        .await
        .unwrap()
        // .error_for_status()?
        .json::<RepliesPage>()
        .await
        .unwrap();
    debug!("Retrieved replies page: {page:#?}");
    let ids = page
        .items
        .iter()
        .map(|item| match item {
            NoteField::Inline(note) => note.id().clone(),
            NoteField::Iri(url) => url.clone(),
        })
        .collect::<HashSet<Url>>();
    Ok((ids, page.next))
}

/// Two remote actors each reply twice to a local post; paginating the post's replies collection
/// with a page size of two returns the four replies across two disjoint pages.
pub async fn post_replies_endpoint(
    indielinks: Url,
    pepper_version: PepperVersion,
    pepper_key: Pepper,
    helper: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    let username = Username::new("post-replies-endpoint-user").unwrap(/* known good */);
    helper.remove_user(&username).await?;

    let client = Client::builder()
        .user_agent("indielinks integration tests/0.0.1; +sp1ff@pobox.com")
        .build()?;

    let api_key = helper
        .create_user(
            &pepper_version,
            &pepper_key,
            &username,
            &TEST_PASSWORD,
            &HashSet::new(),
            &HashSet::new(),
        )
        .await?;

    // Create one public bookmark, then read it back to learn its (server-assigned) id.
    create_post(
        &client,
        &indielinks,
        &username,
        &api_key,
        "https://example.com/replies-target",
        "RepliesTarget",
        &Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
    )
    .await?;
    let post_id = only_post_id(&client, &indielinks, &username, &api_key).await?;

    let indielinks_origin: Origin = (&indielinks).try_into()?;
    let in_reply_to = make_user_post_id(&username, &post_id, &indielinks_origin)?;
    let followers = make_user_followers(&username, &indielinks_origin)?;

    // Two mock peers, each replying twice: four replies in all.
    let (_peer1_server, peer1, peer1_origin) = spawn_mock_peer().await?;
    let (_peer2_server, peer2, peer2_origin) = spawn_mock_peer().await?;

    let mut expected = HashSet::new();
    for (peer, origin, text) in [
        (&peer1, &peer1_origin, "peer1 first reply"),
        (&peer1, &peer1_origin, "peer1 second reply"),
        (&peer2, &peer2_origin, "peer2 first reply"),
        (&peer2, &peer2_origin, "peer2 second reply"),
    ] {
        let reply_id = send_reply(
            &client,
            &indielinks,
            &in_reply_to,
            &followers,
            peer,
            origin,
            text,
        )
        .await?;
        assert!(expected.insert(reply_id), "reply ids should be distinct");
    }

    // Request the collection (no page token): its `first` link names the first page.
    let collection = client
        .get(indielinks.join(&format!("/users/{username}/posts/{post_id}/replies"))?)
        .send()
        .await?
        .error_for_status()?
        .json::<Replies>()
        .await?;
    debug!("Collection: {collection:#?}");
    let first = match collection.first() {
        Some(FirstField::Iri(url)) => url.clone(),
        other => return Err(Failed::from(format!("unexpected `first` field: {other:?}"))),
    };

    // Page one (size 2), then page two via its `next` link.
    let (page1, next) = replies_page(&client, first, 2).await?;
    let next = next.ok_or_else(|| Failed::from("expected a second page"))?;
    let (page2, _) = replies_page(&client, next, 2).await?;

    assert_eq!(page1.len(), 2, "page one should hold two distinct replies");
    assert_eq!(page2.len(), 2, "page two should hold two distinct replies");
    assert!(page1.is_disjoint(&page2), "the two pages must not overlap");
    assert_eq!(
        page1.union(&page2).cloned().collect::<HashSet<Url>>(),
        expected,
        "the two pages together should yield all four replies"
    );

    helper.remove_user(&username).await?;
    Ok(())
}
