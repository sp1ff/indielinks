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

//! # ap-resolution
//!
//! [ActivityPub] entity resolution service.
//!
//! [ActivityPub]: https://www.w3.org/TR/activitypub/
//!
//! # Introduction
//!
//! [indielinks](crate) frequently needs to resolve an [ActivityPub] identifier (i.e. an URL naming
//! some AP entity) to either the entire entity (a [Note], say), or to some attribute of that entity
//! (given an URL naming an [Actor], return the location of the actor's inbox, say). So as to avoid
//! excessive, unecessary network round trips, I've setup a caching service, built on top of the
//! [Cache] mechanism.

use std::sync::Arc;

use either::Either;
use http::method::Method;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use url::Url;

use indielinks_shared::{entities::UserPrivateKey, origin::Origin};

use indielinks_cache::cache::Cache;

use crate::{
    acct::Account,
    ap_entities::{ap_request, Actor, Note, WebfingerResponse},
    cache::{GrpcClient, GrpcClientFactory},
    client_types::ClientType,
    entities::User,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("While sending an ActivityPub request, {source}"))]
    Ap {
        source: crate::ap_entities::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While making a Grpc call, {source}"))]
    Grpc {
        source: indielinks_cache::cache::Error<GrpcClient>,
        backtrace: Backtrace,
    },
    #[snafu(display("No 'self' Link from a webfinger"))]
    NoSelf { backtrace: Backtrace },
    #[snafu(display("While forming an URL, {source}"))]
    Url {
        source: url::ParseError,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                    ActivityPub entity cache                                    //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// The [indielinks](crate) [ActivityPub] resolution cache
///
/// [ActivityPub]: https://www.w3.org/TR/activitypub/
// There will presumably only ever be one instance of this type in a given process, but enforcing
// singleton semantics is messy, and IMO is now understood as an anti-pattern.
#[derive(Clone)]
pub struct ApResolver {
    /// The origin for this [indielinks] instance (used to form AP key identifiers)
    origin: Origin,
    client: ClientType,
    // We need to share ownership of each cache with the GRPC server; this list will grow
    // substantially
    actors: Arc<Cache<GrpcClientFactory, Url, Actor>>,
    notes: Arc<Cache<GrpcClientFactory, Url, Note>>,
    handles: Arc<Cache<GrpcClientFactory, Account, Actor>>,
}

// A note on the API: I would of course prefer to return references to these various attributes;
// e.g. have something like:
//
// ```
// pub async actor_id_to_outbox(...) -> Result<&Url>
// ```
//
// The problem is, `ApResolver` under the hood works in terms of `Cache`-- a Raft cluster working
// together to implement a key/value cache. Since the underlying `Actor` may have been hosted on
// another node, we only have a temporary copy in our method, and so can't return a reference.
impl ApResolver {
    pub fn new(
        origin: Origin,
        client: ClientType,
        actors: Arc<Cache<GrpcClientFactory, Url, Actor>>,
        notes: Arc<Cache<GrpcClientFactory, Url, Note>>,
        handles: Arc<Cache<GrpcClientFactory, Account, Actor>>,
    ) -> ApResolver {
        ApResolver {
            origin,
            client,
            actors,
            notes,
            handles,
        }
    }
    async fn get_actor(
        &mut self,
        principal: Either<&User, &UserPrivateKey>,
        url: &Url,
    ) -> Result<Actor> {
        if let Some(actor) = self.actors.get(url).await.context(GrpcSnafu)? {
            actor
        } else {
            // Cache miss
            let actor: Actor = ap_request(
                &mut self.client,
                &self.origin,
                principal,
                url,
                Method::GET,
                None,
                &(),
            )
            .await
            .context(ApSnafu)?;

            self.actors
                .insert(url.clone(), actor.clone())
                .await
                .context(GrpcSnafu)?;
            actor
        }
        .pipe(Ok)
    }
    /// Given an AP "actor ID", retrieve the location of their inbox
    pub async fn actor_id_to_inbox(
        &mut self,
        principal: Either<&User, &UserPrivateKey>,
        url: &Url,
    ) -> Result<Url> {
        let actor = self.get_actor(principal, url).await?;
        Ok(actor.inbox().clone())
    }
    /// Given an AP "actor ID", retrieve the location of their outbox
    pub async fn actor_id_to_outbox(
        &mut self,
        principal: Either<&User, &UserPrivateKey>,
        url: &Url,
    ) -> Result<Url> {
        let actor = self.get_actor(principal, url).await?;
        Ok(actor.outbox().clone())
    }
    /// Given an AP "actor ID", retrieve the location of their shared inbox
    pub async fn actor_id_to_shared_inbox(
        &mut self,
        principal: Either<&User, &UserPrivateKey>,
        url: &Url,
    ) -> Result<Option<Url>> {
        let actor = self.get_actor(principal, url).await?;
        Ok(actor.shared_inbox().cloned())
    }
    /// Resolve an ActivityPub identifer naming a Note to that Note
    pub async fn note_id_to_note(
        &mut self,
        principal: Either<&User, &UserPrivateKey>,
        url: &Url,
    ) -> Result<Note> {
        // Check the cache, first:
        if let Some(note) = self.notes.get(url).await.context(GrpcSnafu)? {
            note
        } else {
            // Cache miss
            let note: Note = ap_request(
                &mut self.client,
                &self.origin,
                principal,
                url,
                Method::GET,
                None,
                &(),
            )
            .await
            .context(ApSnafu)?;

            note
        }
        .pipe(Ok)
    }
    /// Enter a Note with a known ID into the cache
    pub async fn enter_note(&mut self, url: &Url, note: &Note) -> Result<()> {
        self.notes
            .insert(url.clone(), note.clone())
            .await
            .context(GrpcSnafu)
    }
    /// Given a handle in the form of @username@instance, retrieve the corresponding Actor
    pub async fn handle_to_actor(
        &mut self,
        principal: Either<&User, &UserPrivateKey>,
        handle: &Account,
    ) -> Result<Actor> {
        if let Some(actor) = self.handles.get(handle).await.context(GrpcSnafu)? {
            actor
        } else {
            // Cache miss-- webfinger this handle
            let url = Url::parse(&format!(
                "https://{}/.well-known/webfinger?resource={}",
                handle.host(),
                handle
            ))
            .context(UrlSnafu)?;
            let response: WebfingerResponse = ap_request(
                &mut self.client,
                &self.origin,
                principal,
                &url,
                Method::GET,
                None,
                &(),
            )
            .await
            .context(ApSnafu)?;

            // Now, we expect a "self" link
            let url = response
                .links()
                .find(|link| link.is_rel() && link.is_activity_pub())
                .context(NoSelfSnafu)?
                .href();

            let actor = self.get_actor(principal, url).await?;

            self.handles
                .insert(handle.clone(), actor.clone())
                .await
                .context(GrpcSnafu)?;

            actor
        }
        .pipe(Ok)
    }
}

#[cfg(test)]
mod test {
    use either::Either::Right;
    use governor::{Quota, RateLimiter};
    use nonzero::nonzero;
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    use indielinks_shared::{
        entities::generate_rsa_keypair,
        origin::{Host, Protocol, RegName},
    };

    use indielinks_cache::{raft::CacheNode, types::InMemoryLogStore};

    use crate::{client::make_client, http::HostExtractor};

    use super::*;

    #[tokio::test]
    async fn test_ap_resolver_actor_to_inbox() {
        // Setup a `wiremock` server that just servces my indieweb.social Actor document
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/users/sp1ff"))
            .respond_with(ResponseTemplate::new(200).set_body_string(TEST_ACTOR_DOCUMENT))
            .expect(1)
            .mount(&mock_server)
            .await;

        let cache_node = CacheNode::new(
            &Default::default(),
            GrpcClientFactory,
            InMemoryLogStore::default(),
        )
        .await
        .unwrap();
        cache_node
            .initialize([(0, Default::default())].into())
            .await
            .unwrap();
        let actor_cache: Cache<GrpcClientFactory, Url, Actor> = Cache::new(0, cache_node.clone());
        let note_cache: Cache<GrpcClientFactory, Url, Note> = Cache::new(1, cache_node.clone());
        let handle_cache: Cache<GrpcClientFactory, Account, Actor> = Cache::new(2, cache_node);

        let mut resolver: ApResolver = ApResolver::new(
            (
                Protocol::Http,
                Host::RegName(RegName::new("localhost").unwrap()),
                1234,
            )
                .into(),
            make_client(
                "ap_resolution::test Client/0.0.1 +sp1ff@pobox.com",
                true,
                HostExtractor,
                RateLimiter::keyed(Quota::per_second(nonzero!(16u32)))
                    .use_middleware(tower_gcra::extractors::KeyedDashmapMiddleware::from([])),
                &Default::default(),
            )
            .unwrap(),
            Arc::new(actor_cache),
            Arc::new(note_cache),
            Arc::new(handle_cache),
        );

        let (_, private_key) = generate_rsa_keypair().unwrap();
        let actor =
            Url::parse(&format!("{}/users/sp1ff", &mock_server.uri())).expect("Malformed URL!?");
        let url = resolver
            .actor_id_to_inbox(Right(&private_key), &actor)
            .await
            .expect("Failed to retrieve actor inbox?");
        let golden =
            Url::parse("https://indieweb.social/users/sp1ff/inbox").expect("Malformed URL!?");
        assert!(url == golden);
        let url = resolver
            .actor_id_to_inbox(Right(&private_key), &actor)
            .await
            .expect("Failed to retrieve actor inbox?");
        assert!(url == golden);
    }

    static TEST_ACTOR_DOCUMENT: &str = r#"{
    "@context":[
        "https://www.w3.org/ns/activitystreams",
        "https://w3id.org/security/v1",
        {
            "manuallyApprovesFollowers":"as:manuallyApprovesFollowers",
            "toot":"http://joinmastodon.org/ns#",
            "featured":{
                "@id":"toot:featured",
                "@type":"@id"
            },
            "featuredTags":{
                "@id":"toot:featuredTags",
                "@type":"@id"
            },
            "alsoKnownAs":{
                "@id":"as:alsoKnownAs",
                "@type":"@id"
            },
            "movedTo":{
                "@id":"as:movedTo",
                "@type":"@id"
            },
            "schema":"http://schema.org#",
            "PropertyValue":"schema:PropertyValue",
            "value":"schema:value",
            "discoverable":"toot:discoverable",
            "suspended":"toot:suspended",
            "memorial":"toot:memorial",
            "indexable":"toot:indexable",
            "attributionDomains":{
                "@id":"toot:attributionDomains",
                "@type":"@id"
            },
            "focalPoint":{
                "@container":"@list",
                "@id":"toot:focalPoint"
            }
        }
    ],
    "id":"https://indieweb.social/users/sp1ff",
    "type":"Person",
    "following":"https://indieweb.social/users/sp1ff/following",
    "followers":"https://indieweb.social/users/sp1ff/followers",
    "inbox":"https://indieweb.social/users/sp1ff/inbox",
    "outbox":"https://indieweb.social/users/sp1ff/outbox",
    "featured":"https://indieweb.social/users/sp1ff/collections/featured",
    "featuredTags":"https://indieweb.social/users/sp1ff/collections/tags",
    "preferredUsername":"sp1ff",
    "name":"Michael",
    "summary":"<p>I hack in Rust, Idris, Rocq, Lisp &amp; C++. interests include the Indieweb, the Fediverse, Emacs &amp; type theory. <a href=\"https://www.unwoundstack.com\" target=\"_blank\" rel=\"nofollow noopener\" translate=\"no\"><span class=\"invisible\">https://www.</span><span class=\"\">unwoundstack.com</span><span class=\"invisible\"></span></a></p>",
    "url":"https://indieweb.social/@sp1ff",
    "manuallyApprovesFollowers":false,
    "discoverable":true,
    "indexable":false,
    "published":"2022-03-10T00:00:00Z",
    "memorial":false,
    "publicKey":{
        "id":"https://indieweb.social/users/sp1ff#main-key",
        "owner":"https://indieweb.social/users/sp1ff",
        "publicKeyPem":"-----BEGIN PUBLIC KEY----- MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvT9wAaYPOvFLUU0eqMlz RI0ze26hkCYcYdwF3+CM986TmOEUhdQCRAQi4GruqPe6AIK9H4OjftnVbJ8B6Bwc ocIkAM4H7uk2nafcH6lVMvA2LeRQ40gY72D8n+8k1a5bZAl36Dq95uUolKsX6xq/ MnDs3N23YdNEAtIoSWGYWJGLhc0hZ/j1rmowlSTzRanpC+JhUMrG6/av9OSSoc7Y lJkG38TrPP/zPmWAUN3YYFrNR9+KlthKkBiE+ixRxMEz6WUT8m9s8zNOXfY1AgWo OBt7VRgdJucWZKvh53QLkbFN5Q7lYEAENvt2ZCNPTmrDXmtJ631J6+V8sVmR4mzf cwIDAQAB -----END PUBLIC KEY----- "
    },
    "tag":[
    ],
    "attachment":[
    ],
    "endpoints":{
        "sharedInbox":"https://indieweb.social/inbox"
    },
    "icon":{
        "type":"Image",
        "mediaType":"image/jpeg",
        "url":"https://cdn.masto.host/indiewebsocial/accounts/avatars/107/934/171/185/300/184/original/3c61e964400b8d4e.jpg"
    },
    "image":{
        "type":"Image",
        "mediaType":"image/jpeg",
        "url":"https://cdn.masto.host/indiewebsocial/accounts/headers/107/934/171/185/300/184/original/8c2ab41be165ea81.jpeg"
    }
}"#;
}
