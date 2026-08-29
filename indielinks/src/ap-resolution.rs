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
//! (given an URL naming an [Actor], return the  location of the actor's inbox, say). So as to avoid
//! excessive, unecessary network round trips, I've setup a caching service, built on top of the
//! [Cache] mechanism.

use std::sync::Arc;

use either::Either;
use http::{self, method::Method};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use tracing::debug;
use url::Url;

use indielinks_shared::{
    entities::{UserPrivateKey, Username},
    origin::Origin,
};

use bytes::Bytes;
use indielinks_cache::{cache::Cache, network::ClientFactory};

use crate::{
    acct::Account,
    ap_entities::{
        ap_request, username_and_postid_from_url, username_from_url, Actor, Note, WebfingerResponse,
    },
    cache::{GrpcClient, GrpcClientFactory},
    entities::User,
    storage::{get_note, user_for_username, Backend as StorageBackend},
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error<FC: indielinks_cache::network::Client + 'static = GrpcClient>
where
    FC::ErrorType: std::error::Error + std::fmt::Debug + 'static,
{
    #[snafu(display("Failed to produce an ActivityPub Actor from a User"))]
    Actor { source: crate::ap_entities::Error },
    #[snafu(display("While sending an ActivityPub request, {source}"))]
    Ap { source: crate::ap_entities::Error },
    #[snafu(display("While making a Grpc call, {source}"))]
    Grpc {
        source: indielinks_cache::cache::Error<FC>,
    },
    #[snafu(display("The handle username {username} isn't a valid indielinks username"))]
    Handle {
        username: crate::acct::Username,
        source: indielinks_shared::entities::Error,
    },
    #[snafu(display("No 'self' Link from a webfinger"))]
    NoSelf { backtrace: Backtrace },
    #[snafu(display("While forming an URL, {source}"))]
    Url {
        source: url::ParseError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to find a user named {username}"))]
    User {
        username: Username,
        source: crate::storage::Error,
    },
    #[snafu(display("Failed to parse an indielinks username from {}", url.as_str()))]
    Username {
        url: Url,
        source: crate::ap_entities::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to parse an indielinks username and post ID from {}", url.as_str()))]
    UsernameAndPostId {
        url: Url,
        source: crate::ap_entities::Error,
        backtrace: Backtrace,
    },
}

type Result<T, FC = GrpcClient> = std::result::Result<T, Error<FC>>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                    ActivityPub entity cache                                    //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// The [indielinks](crate) [ActivityPub] resolution cache
///
/// [ActivityPub]: https://www.w3.org/TR/activitypub/
// There will presumably only ever be one instance of this type in a given process, but enforcing
// singleton semantics is messy, and IMO is now understood as an anti-pattern.
#[derive(Clone)]
pub struct ApResolver<F: ClientFactory = GrpcClientFactory, C = crate::client_types::ClientType> {
    /// The origin for this [indielinks] instance (used to form AP key identifiers)
    origin: Origin,
    client: C,
    storage: Arc<dyn StorageBackend + Send + Sync>,
    // We need to share ownership of each cache with the GRPC server; this list will grow
    // substantially
    actors: Arc<Cache<F, Url, Actor>>,
    notes: Arc<Cache<F, Url, Note>>,
    handles: Arc<Cache<F, Account, Actor>>,
}

#[derive(Clone, Debug)]
pub struct Metrics {
    pub actor_count: u64,
    pub note_count: u64,
    pub handle_count: u64,
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
impl<F, C> ApResolver<F, C>
where
    F: ClientFactory + Send + Sync + Clone + 'static,
    F::CacheClient: Clone + Send + Sync + std::fmt::Debug + 'static,
    C: tower::Service<http::Request<Bytes>, Response = http::Response<Bytes>> + Send,
    C::Error: std::error::Error + Send + Sync + 'static,
    C::Future: Send,
{
    pub fn new(
        origin: Origin,
        client: C,
        storage: Arc<dyn StorageBackend + Send + Sync>,
        actors: Arc<Cache<F, Url, Actor>>,
        notes: Arc<Cache<F, Url, Note>>,
        handles: Arc<Cache<F, Account, Actor>>,
    ) -> ApResolver<F, C> {
        ApResolver {
            origin,
            client,
            storage,
            actors,
            notes,
            handles,
        }
    }

    pub async fn get_actor(
        &mut self,
        principal: Either<&User, &UserPrivateKey>,
        url: &Url,
    ) -> Result<Actor, F::CacheClient> {
        if let Some(actor) = self.actors.get(url).await.context(GrpcSnafu)? {
            debug!("ApResolver cache hit: {} :=> {actor:?}", url.as_str());
            actor
        } else if self.origin.matches_url(url) {
            // This Actor isn't in our cache, but it's an Actor hosted on *this* instance. Skip the
            // HTTP hop & just grab it from storage.
            debug!(
                "ApResolver cache miss: {}. However this appears to be a local user.",
                url
            );

            let username =
                username_from_url(&self.origin, url).context(UsernameSnafu { url: url.clone() })?;
            let user = user_for_username(self.storage.as_ref(), &username)
                .await
                .context(UserSnafu {
                    username: username.clone(),
                })?;
            let actor = Actor::new(&user, &self.origin).context(ActorSnafu)?;

            self.actors
                .insert(url.clone(), actor.clone())
                .await
                .context(GrpcSnafu)?;

            actor
        } else {
            debug!(
                "ApResolver cache miss: {}. This will require an HTTP call",
                url.as_str()
            );
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
    ) -> Result<Url, F::CacheClient> {
        let actor = self.get_actor(principal, url).await?;
        Ok(actor.inbox().clone())
    }
    /// Given an AP "actor ID", retrieve the location of their outbox
    pub async fn actor_id_to_outbox(
        &mut self,
        principal: Either<&User, &UserPrivateKey>,
        url: &Url,
    ) -> Result<Url, F::CacheClient> {
        let actor = self.get_actor(principal, url).await?;
        Ok(actor.outbox().clone())
    }
    /// Given an AP "actor ID", retrieve the location of their shared inbox
    pub async fn actor_id_to_shared_inbox(
        &mut self,
        principal: Either<&User, &UserPrivateKey>,
        url: &Url,
    ) -> Result<Option<Url>, F::CacheClient> {
        let actor = self.get_actor(principal, url).await?;
        Ok(actor.shared_inbox().cloned())
    }
    /// Resolve an ActivityPub identifer naming a Note to that Note
    pub async fn note_id_to_note(
        &mut self,
        principal: Either<&User, &UserPrivateKey>,
        url: &Url,
    ) -> Result<Note, F::CacheClient> {
        // Check the cache, first:
        if let Some(note) = self.notes.get(url).await.context(GrpcSnafu)? {
            debug!("Cache hit: {} :=> {note:?}", url.as_str());
            note
        } else if self.origin.matches_url(url) {
            debug!(
                "Cache miss; however {} is hosted on this intance",
                url.as_str()
            );

            let (username, postid) = username_and_postid_from_url(&self.origin, url)
                .context(UsernameAndPostIdSnafu { url: url.clone() })?;
            get_note(&self.origin, self.storage.as_ref(), &username, &postid)
                .await
                .unwrap()
        } else {
            // Cache miss
            ap_request(
                &mut self.client,
                &self.origin,
                principal,
                url,
                Method::GET,
                None,
                &(),
            )
            .await
            .context(ApSnafu)?
        }
        .pipe(Ok)
    }
    /// Enter a Note with a known ID into the cache
    pub async fn enter_note(&mut self, url: &Url, note: &Note) -> Result<(), F::CacheClient> {
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
    ) -> Result<Actor, F::CacheClient> {
        if let Some(actor) = self.handles.get(handle).await.context(GrpcSnafu)? {
            actor
        } else if self.origin.matches_host(handle.host()) {
            debug!(
                "ApResolver cache miss: {}. However this appears to be a local user.",
                handle
            );

            let username = Username::new(handle.user().as_ref()).context(HandleSnafu {
                username: handle.user().clone(),
            })?;
            let user = user_for_username(self.storage.as_ref(), &username)
                .await
                .context(UserSnafu {
                    username: username.clone(),
                })?;
            let actor = Actor::new(&user, &self.origin).context(ActorSnafu)?;

            self.handles
                .insert(handle.clone(), actor.clone())
                .await
                .context(GrpcSnafu)?;

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
    /// Retrieve some metrics about this resolver
    pub fn get_metrics(&self) -> Metrics {
        Metrics {
            actor_count: self.actors.count() as u64,
            note_count: self.notes.count() as u64,
            handle_count: self.handles.count() as u64,
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::{HashMap, HashSet},
        convert::Infallible,
        future::{ready, Ready},
        result::Result as StdResult,
        sync::Arc,
        task::Poll,
    };

    use crate::{
        entities::{
            ApiKeys, FollowId, Follower, Following, IncomingLike, IncomingLikeReplyShare,
            IncomingReply, IncomingShare, LikeReplyShare, OutgoingLike, OutgoingReply,
            OutgoingShare,
        },
        storage::{self, Backend, Counts, DateRange},
        util::UpToThree,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::{DateTime, Utc};
    use either::Either::Right;
    use futures::stream::BoxStream;
    use indielinks_cache::{
        network::null_client::NullClientFactory,
        raft::{make_shared_cache_node, InMemoryBackend},
    };
    use indielinks_shared::{
        entities::{generate_rsa_keypair, Post, PostDay, PostId, StorUrl, Tagname, UserId},
        instance_state::InstanceStateV0,
        known_good,
        nonempty_string::NonEmptyString,
        origin::{Host, Protocol, RegName},
    };
    use uuid::Uuid;

    use super::*;

    /// Canned-response HTTP client for use in unit tests.
    ///
    /// Each entry is consumed on first use via `remove()`; a second request for the same URL
    /// returns 404. This enforces that the cache is working: if `ApResolver` hits the mock twice
    /// for the same URL, the test fails rather than silently succeeding.
    struct MockHttpClient {
        responses: HashMap<String, http::Response<Bytes>>,
    }

    impl MockHttpClient {
        fn new(entries: impl IntoIterator<Item = (String, http::Response<Bytes>)>) -> Self {
            Self {
                responses: entries.into_iter().collect(),
            }
        }
    }

    impl tower::Service<http::Request<Bytes>> for MockHttpClient {
        type Response = http::Response<Bytes>;
        type Error = Infallible;
        type Future = Ready<std::result::Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _: &mut std::task::Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<Bytes>) -> Self::Future {
            let rsp = self
                .responses
                .remove(&req.uri().to_string())
                .unwrap_or_else(|| {
                    http::Response::builder()
                        .status(404)
                        .body(Bytes::new())
                        .unwrap()
                });
            ready(Ok(rsp))
        }
    }

    // An `unimplemented!()` storage backend for unit testing purposes. This is fine as things stand
    // at the time of this writing, since none of the unit tests exercise a code path that requires
    // it... which is probably not great.
    struct TestBackend;

    #[async_trait]
    impl Backend for TestBackend {
        async fn add_follower(
            &self,
            _user: &User,
            _follower: &StorUrl,
        ) -> StdResult<(), storage::Error> {
            unreachable!()
        }
        async fn add_following(
            &self,
            _user: &User,
            _following: &StorUrl,
            _id: &FollowId,
        ) -> StdResult<(), storage::Error> {
            unreachable!()
        }
        async fn add_outgoing_like(&self, _like: &OutgoingLike) -> StdResult<(), storage::Error> {
            unreachable!()
        }
        async fn add_outgoing_reply(
            &self,
            _reply: &OutgoingReply,
        ) -> StdResult<(), storage::Error> {
            unreachable!()
        }
        async fn add_outgoing_share(
            &self,
            _share: &OutgoingShare,
        ) -> StdResult<(), storage::Error> {
            unreachable!()
        }
        async fn add_post_like(&self, _reply: &IncomingLike) -> StdResult<(), storage::Error> {
            unreachable!()
        }
        #[allow(clippy::too_many_arguments)]
        async fn add_post(
            &self,
            _user: &User,
            _replace: bool,
            _uri: &Url,
            _id: &PostId,
            _title: &str,
            _dt: &DateTime<Utc>,
            _notes: Option<&NonEmptyString>,
            _shared: bool,
            _to_read: bool,
            _tags: &HashSet<Tagname>,
        ) -> StdResult<bool, storage::Error> {
            unreachable!()
        }
        async fn add_post_reply(&self, _reply: &IncomingReply) -> StdResult<(), storage::Error> {
            unreachable!()
        }
        async fn add_post_share(&self, _share: &IncomingShare) -> StdResult<(), storage::Error> {
            unreachable!()
        }
        async fn add_user(&self, _user: &User) -> StdResult<(), storage::Error> {
            unreachable!()
        }
        async fn confirm_following(
            &self,
            _user: &User,
            _following: &StorUrl,
        ) -> StdResult<bool, storage::Error> {
            unreachable!()
        }
        async fn counts(&self) -> StdResult<Counts, storage::Error> {
            unreachable!()
        }
        async fn delete_post(
            &self,
            _user: &User,
            _url: &StorUrl,
        ) -> StdResult<bool, storage::Error> {
            unreachable!()
        }
        async fn delete_tag(&self, _user: &User, _tag: &Tagname) -> StdResult<(), storage::Error> {
            unreachable!()
        }
        async fn followers_for_actor(
            &self,
            _actor_id: &StorUrl,
        ) -> StdResult<BoxStream<'static, StdResult<Following, storage::Error>>, storage::Error>
        {
            unreachable!()
        }
        async fn get_all_likes_replies_and_shares(
            &self,
            _user: &User,
        ) -> StdResult<BoxStream<'static, StdResult<LikeReplyShare, storage::Error>>, storage::Error>
        {
            unreachable!()
        }
        // I considered using `TryStream`, but not sure I see the advantage, yet. Also, there's no handy
        // type alias `BoxTryStream` (tho of course I could define it)
        async fn get_followers<'a>(
            &'a self,
            _user: &User,
        ) -> StdResult<BoxStream<'a, StdResult<Follower, storage::Error>>, storage::Error> {
            unreachable!()
        }
        async fn get_following<'a>(
            &'a self,
            _user: &User,
        ) -> StdResult<BoxStream<'a, StdResult<Following, storage::Error>>, storage::Error>
        {
            unreachable!()
        }
        async fn get_like_reply_share(
            &self,
            _id: &Uuid,
        ) -> StdResult<Option<LikeReplyShare>, storage::Error> {
            unreachable!()
        }
        async fn get_likes_replies_shares(
            &self,
            _id: &Uuid,
        ) -> StdResult<
            BoxStream<'static, StdResult<IncomingLikeReplyShare, storage::Error>>,
            storage::Error,
        > {
            unreachable!()
        }
        async fn get_post(
            &self,
            _userid: &UserId,
            _uri: &StorUrl,
        ) -> StdResult<Option<Post>, storage::Error> {
            unreachable!()
        }
        async fn get_post_by_id(&self, _id: &PostId) -> StdResult<Option<Post>, storage::Error> {
            unreachable!()
        }
        async fn get_posts(
            &self,
            _user: &User,
            _tags: &UpToThree<Tagname>,
            _day: &PostDay,
            _uri: &Option<StorUrl>,
        ) -> StdResult<Vec<Post>, storage::Error> {
            unreachable!()
        }
        async fn get_posts_by_day(
            &self,
            _user: &User,
            _tags: &UpToThree<Tagname>,
        ) -> StdResult<Vec<(PostDay, usize)>, storage::Error> {
            unreachable!()
        }
        async fn get_all_posts(
            &self,
            _user: &User,
            _tags: &UpToThree<Tagname>,
            _dates: &DateRange,
            _unread: bool,
        ) -> StdResult<BoxStream<'static, StdResult<Post, storage::Error>>, storage::Error>
        {
            unreachable!()
        }
        async fn get_recent_posts(
            &self,
            _user: &User,
            _tags: &UpToThree<Tagname>,
            _count: usize,
        ) -> StdResult<Vec<Post>, storage::Error> {
            unreachable!()
        }
        async fn get_tag_cloud(
            &self,
            _user: &User,
        ) -> StdResult<HashMap<Tagname, usize>, storage::Error> {
            unreachable!()
        }
        async fn get_user_by_id(&self, _id: &UserId) -> StdResult<Option<User>, storage::Error> {
            unreachable!()
        }
        async fn rename_tag(
            &self,
            _user: &User,
            _from: &Tagname,
            _to: &Tagname,
        ) -> StdResult<(), storage::Error> {
            unreachable!()
        }
        async fn update_user_api_keys(
            &self,
            _user: &User,
            _keys: &ApiKeys,
        ) -> StdResult<(), storage::Error> {
            unreachable!()
        }
        async fn update_user_post_times(
            &self,
            _user: &User,
            _dt: &DateTime<Utc>,
        ) -> StdResult<(), storage::Error> {
            unreachable!()
        }
        async fn user_for_name(&self, _name: &str) -> StdResult<Option<User>, storage::Error> {
            unreachable!()
        }
        async fn validate_schema_version(
            &self,
            _schema_version: u32,
        ) -> StdResult<InstanceStateV0, storage::Error> {
            unreachable!()
        }
    }

    async fn make_resolver(
        client: MockHttpClient,
    ) -> ApResolver<NullClientFactory, MockHttpClient> {
        let cache_node = make_shared_cache_node::<NullClientFactory>(
            Arc::new(InMemoryBackend::default()),
            &Default::default(),
            NullClientFactory,
        )
        .await
        .unwrap();
        cache_node
            .write()
            .await
            .initialize([(0, Default::default())], vec![])
            .await
            .unwrap();
        let actors = Arc::new(Cache::new(0, cache_node.clone()));
        let notes = Arc::new(Cache::new(1, cache_node.clone()));
        let handles = Arc::new(Cache::new(2, cache_node));
        let origin: Origin = (
            Protocol::Http,
            Host::RegName(RegName::new("localhost").unwrap()),
            1234u16,
        )
            .into();
        ApResolver::new(
            origin,
            client,
            Arc::new(TestBackend),
            actors,
            notes,
            handles,
        )
    }

    fn actor_response() -> http::Response<Bytes> {
        http::Response::builder()
            .status(200)
            .body(Bytes::from(TEST_ACTOR_DOCUMENT))
            .unwrap()
    }

    #[tokio::test]
    async fn test_actor_operations() {
        let actor_url = "https://indieweb.social/users/sp1ff";
        let client = MockHttpClient::new([(actor_url.to_string(), actor_response())]);
        let mut resolver = make_resolver(client).await;

        let (_, private_key) = generate_rsa_keypair().unwrap();
        let actor = known_good!(Url::parse(actor_url));

        let inbox = resolver
            .actor_id_to_inbox(Right(&private_key), &actor)
            .await
            .expect("Failed to retrieve actor inbox");
        let golden = known_good!(Url::parse("https://indieweb.social/users/sp1ff/inbox"));
        assert_eq!(inbox, golden);

        // Second call must hit the cache (mock entry already consumed).
        let inbox2 = resolver
            .actor_id_to_inbox(Right(&private_key), &actor)
            .await
            .expect("Failed to retrieve actor inbox on second call");
        assert_eq!(inbox2, golden);

        // What the hekc-- I'm here
        let outbox = resolver
            .actor_id_to_outbox(Right(&private_key), &actor)
            .await
            .expect("Failed to retrieve outbox with no network call");
        assert_eq!(
            outbox,
            known_good!(Url::parse("https://indieweb.social/users/sp1ff/outbox")),
        );
        let shared_inbox = resolver
            .actor_id_to_shared_inbox(Right(&private_key), &actor)
            .await
            .expect("Failed to retrieve shared inbox with no network call")
            .expect("Missing the shared inbox");
        assert_eq!(
            shared_inbox,
            known_good!(Url::parse("https://indieweb.social/inbox")),
        );

        // Now, we _should_ have the Actor in-cache, so this should work, too
        let actor = resolver
            .get_actor(Right(&private_key), &actor)
            .await
            .expect("Failed to retrieve the actor");
        assert_eq!(actor.id().as_str(), "https://indieweb.social/users/sp1ff");
    }

    #[tokio::test]
    async fn test_handle_to_actor() {
        let actor_url = "https://indieweb.social/users/sp1ff";
        let webfinger_url =
            "https://indieweb.social/.well-known/webfinger?resource=sp1ff@indieweb.social";

        let client = MockHttpClient::new([
            (
                webfinger_url.to_string(),
                http::Response::builder()
                    .status(200)
                    .body(Bytes::from(TEST_WEBFINGER_DOCUMENT))
                    .unwrap(),
            ),
            (actor_url.to_string(), actor_response()),
        ]);
        let mut resolver = make_resolver(client).await;

        let (_, private_key) = generate_rsa_keypair().unwrap();
        let handle: Account = known_good!("sp1ff@indieweb.social".parse());

        let actor = resolver
            .handle_to_actor(Right(&private_key), &handle)
            .await
            .expect("Failed to resolve handle to ctor");
        assert_eq!(actor.id().as_str(), "https://indieweb.social/users/sp1ff");

        // Should be in-cache
        let actor2 = resolver
            .handle_to_actor(Right(&private_key), &handle)
            .await
            .expect("Failed to resolve handle to actor (second call)");
        assert_eq!(actor2.id().as_str(), "https://indieweb.social/users/sp1ff");
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

    static TEST_WEBFINGER_DOCUMENT: &str = r#"{
    "subject":"acct:sp1ff@indieweb.social",
    "aliases":["https://indieweb.social/users/sp1ff"],
    "links":[
        {
            "rel":"self",
            "type":"application/activity+json",
            "href":"https://indieweb.social/users/sp1ff"
        }
    ]
}"#;
}
