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

//! # ActivityPub Entities
//!
//! ## Introduction
//!
//! [ActivityPub] [defines] a number of entities as part of the protocol, represented as [JSON-LD]
//! documents. I suppose it's to the authors' credit that they realized that JSON itself is far too
//! flexible (shall an optional attribute be simply not present? present with a null value? What
//! does the "name" field denote, precisely?), and [JSON-LD] seems better than XML namespaces, but
//! still; I would have preferred [JSON Schema].
//!
//! [ActivityPub]: https://www.w3.org/TR/activitypub/
//! [defines]: https://www.w3.org/TR/activitystreams-vocabulary
//! [JSON-LD]: https://www.w3.org/TR/json-ld/#the-context
//! [JSON Schema]: https://json-schema.org/
//!
//! ActivityPub applications such as indielinks need to both serialize internal representations of
//! entities to compliant JSON-LD representations and deserialize JSON-LD documents to internal
//! representations thereof. In some cases, the author will be in posession of a JSON-LD document
//! that could represent any of a number of acceptable entities (e.g. a user inbox can receive
//! likes, follows & boosts, among other things), so the deserialization process will have to work
//! without a priori knowledge of the type being deserialized (though it will have at least a
//! constrained set of possibilities).
//!
//! Finally, ActivityPub is [famously] [poorly] [specified], leading to a wide range of
//! implementations in the wild (though I have my suspcions as to how compliant some of those
//! implementations are), and the reality that in order to federate with other ActivityPub
//! applications, you need "tune" your implementation to theirs.
//!
//! [famously]: https://gopiandcode.uk/logs/log-writing-activitypub.html#org6cb5e5d
//! [poorly]: https://rknight.me/blog/building-an-activitypub-server/
//! [specified]: https://raphaelluckom.com/posts/Things%20I%27ve%20learned%20about%20ActivityPub%20so%20far.html
//!
//! [Kiran Gopinathan] looked to what others had done, and found that [Pleroma] had an entire
//! [folder] filled with JSON-LD documents their developers had encountered in the wild, something I
//! intend to emulate.
//!
//! [Kiran Gopinathan]: https://gopiandcode.uk/logs/log-writing-activitypub.html
//! [Pleroma]: https://git.pleroma.social/pleroma
//! [folder]: https://git.pleroma.social/pleroma/pleroma/-/blob/develop/test/fixtures/peertube/actor-person.json
//!
//! ## activitypub-federation
//!
//! There is a popular crate, [activitypub-federation], that provides a number of services for
//! authors of ActivityPub applications, including serde to & from JSON-LD representation of
//! entities. I'm not using it because I'm not sure I care for their API, in particular their
//! handling of the situation noted above of being in posession of a JSON-LD document that could
//! represent any of a given set of entities and needing to deserialize it to the correct type:
//! their crate "works by attempting to parse the received JSON data with each variant in order. The
//! first variant which parses without errors is used for receiving. This means you should avoid
//! defining multiple activities in a way that they might conflict and parse the same data." This
//! seems unappealing to me, especially in the presence of a vocabulary in which each type contains
//! a "type" field that *tells you the type of thing you're reading*.
//!
//! [activitypub-federation]: https://docs.rs/activitypub_federation/latest/activitypub_federation
//!
//! It's possible that this is all a result of my tendency to eschew libraries & simply "code it up"
//! myself; we'll see.
//!
//! ## Design
//!
//! I see two approaches, here:
//!
//! 1) This first, which I'll term the "strict" approach, would be to take the JSON-LD document,
//!    expand (fetching the schemae as needed) and validate it, convert it to either a
//!    `serde_json::Value` or a `json_syntax::Value`, and then call `from_value()` for a sum type.
//!    This has the benefit of validating the JSON-LD and handling the full JSON-LD syntax (e.g.
//!    "toot" resolving to "<http://joinmastodon.org/ns#>") at the cost of fetching schema documents
//!    (which may or may not be available at runtime), the conversion step, requiring JSON-LD
//!    compliance from all our peers, and the general hassle of the `json-ld` crate which is poorly
//!    documented & requires a number of subsidiary crates in order to do anything interesting, but
//!    doesn't publicly export them.
//!
//!    I suppose I could mitigate the problem of having to download schema documents by grabbing the
//!    commonly-used ones & packaging them with indielinks.
//!
//! 2) The second, which I'll call the "loose" approach; treat the JSON-LD document as plain JSON &
//!    deserialize a sum type from that. This has the benefit of being insensitive to JSON-LD
//!    non-compliance, not requiring the availability of schema documents at runtime, and doing
//!    serde directly against the input data. It has the cost of expecting all our peers to use the
//!    same terms in their JSON-LD documents (i.e. if Mastodon decides to write a Follow with a
//!    field of "actor", and Lemmy decides to write the same information with a name of "user"
//!    [mapping "user" to the same entity in the context] then we'll break with no recourse)
//!
//! My inclination is to gamble on 2) until we hit a snag we can't work around, on the suspicion
//! that few ActivityPub developers haven't bothered with JSON-LD; from my reading, they treat it as
//! JSON. So the scheme would be:
//!
//! 1) define a struct for each ActivityPub entity, designed to serialize & deserialize to & from the
//!    JSON-LD we expect to exchange with our peers
//!
//! 2) define an enum with a variant for each entity, using internal tagging for serde
//!
//! 3) given a JSON-LD document, treat it as JSON & deserialize to the variant already encoded via
//!    the "type" field
//!
//! 4) to serialize, equip each entity with a `new()` method taking as parameters the internal types
//!    needed to build-up the ActivityPub representation
//!
//! I'll validate against the Pleroma test suite. My _hope_ is that this is as compliant as
//! possible, while still maintaining the possibility of "fine-tuning" serde so as to maintain
//! compatibility with as many other apps as possible.
//!

use chrono::{DateTime, FixedOffset};
use picky::key::PublicKey as PickyPublicKey;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{Backtrace, ResultExt, Snafu};
use url::Url;

use crate::{
    entities::{self, User, Username},
    origin::Origin,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to deserialize an Actor: {source}"))]
    ActorDe {
        source: reqwest::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("JSON serialization error: {source}"))]
    JsonSer { source: serde_json::Error },
    #[snafu(display("AP entities serialized to unexpected JSON types"))]
    JsonTypeMismatch { backtrace: Backtrace },
    #[snafu(display("Failed to obtain public key in PEM format; {source}"))]
    Pem { source: entities::Error },
    #[snafu(display("Failed to obtain public key in PEM format; {source}"))]
    PickyPem {
        source: picky::key::KeyError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to resolve keyid {}: {source}"))]
    ResolveKeyId {
        key_id: Url,
        #[snafu(source(from(reqwest::Error, Box::new)))]
        source: Box<reqwest::Error>,
        // backtrace not included because it would make the error too large
    },
    #[snafu(display("Failed to parse an URL: {source}"))]
    UrlParse {
        source: url::ParseError,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       Standard Locations                                       //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Return an URL naming an indielinks user
///
/// `hostname` may be a DNS name, hostname or IP address. It may include a port.
pub fn make_user_id(username: &Username, origin: &Origin) -> Result<Url> {
    Url::parse(&format!("{}/users/{}", origin, username)).context(UrlParseSnafu)
}

/// Return an URL naming a public key owner
pub fn make_key_id(username: &Username, origin: &Origin) -> Result<Url> {
    Url::parse(&format!("{}/users/{}#main-key", origin, username)).context(UrlParseSnafu)
}

pub fn make_user_inbox(username: &Username, origin: &Origin) -> Result<Url> {
    Url::parse(&format!("{}/users/{}/inbox", origin, username)).context(UrlParseSnafu)
}

pub fn make_user_outbox(username: &Username, origin: &Origin) -> Result<Url> {
    Url::parse(&format!("{}/users/{}/outbox", origin, username)).context(UrlParseSnafu)
}

pub fn make_user_following(username: &Username, origin: &Origin) -> Result<Url> {
    Url::parse(&format!("{}/users/{}/following", origin, username)).context(UrlParseSnafu)
}

pub fn make_user_followers(username: &Username, origin: &Origin) -> Result<Url> {
    Url::parse(&format!("{}/users/{}/followers", origin, username,)).context(UrlParseSnafu)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            Entities                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

// The context doesn't belong in any entity; it's added to the document only at the end. This struct
// can be conveniently serialized to JSON and appended to other values on the way out.
#[derive(Clone, Debug, Serialize)]
pub struct Context {
    #[serde(rename = "@context")]
    context: Vec<Url>,
}

impl Default for Context {
    fn default() -> Self {
        Context {
            context: vec![
                Url::parse("https://www.w3.org/ns/activitystreams").unwrap(/* known good */),
                Url::parse("https://w3id.org/security/v1").unwrap(/* known good */),
            ],
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct InlineId {
    id: Url,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
struct Endpoints {
    shared_inbox: Url,
}

impl Endpoints {
    pub fn new(origin: &Origin) -> Result<Endpoints> {
        Ok(Endpoints {
            shared_inbox: Url::parse(&format!("{}/inbox", origin)).context(UrlParseSnafu)?,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
struct PublicKey {
    id: Url,
    owner: Url,
    pub public_key_pem: String,
}

impl PublicKey {
    pub fn new(user: &User, origin: &Origin) -> Result<PublicKey> {
        Ok(PublicKey {
            id: make_key_id(user.username(), origin)?,
            owner: make_user_id(user.username(), origin)?,
            public_key_pem: user.pub_key().to_pem().context(PemSnafu)?,
        })
    }
    /// Create a new [PublicKey] instance given a username, hostname & public key. `proto` selects
    /// for http or https
    // This exists solely for testing purposes-- can I re-factor?
    pub fn from_username_and_key(
        username: &Username,
        origin: &Origin,
        pub_key: &picky::key::PublicKey,
    ) -> Result<PublicKey> {
        Ok(PublicKey {
            id: make_key_id(username, origin)?,
            owner: make_user_id(username, origin)?,
            public_key_pem: pub_key.to_pem_str().context(PickyPemSnafu)?,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Actor {
    id: Url,
    // Required for Mastodon compatibility
    preferred_username: String,
    inbox: Url,
    outbox: Url,
    following: Url,
    followers: Url,
    endpoints: Option<Endpoints>,
    public_key: PublicKey,
}

impl Actor {
    /// Create a new [Actor] instance from a username & hostname; all endpoints will use https
    pub fn new(user: &User, origin: &Origin) -> Result<Actor> {
        Ok(Actor {
            id: make_user_id(user.username(), origin)?,
            preferred_username: user.username().to_string(),
            inbox: make_user_inbox(user.username(), origin)?,
            outbox: make_user_outbox(user.username(), origin)?,
            following: make_user_following(user.username(), origin)?,
            followers: make_user_followers(user.username(), origin)?,
            endpoints: Some(Endpoints::new(origin)?),
            public_key: PublicKey::new(user, origin)?,
        })
    }
    /// Create a new [Actor] instance from a username & hostname; the endpoints will use http or
    /// https according to `proto`
    // This only exists for testing purposes... can I re-factor?
    pub fn from_username_and_key(
        username: &Username,
        origin: &Origin,
        pub_key: &picky::key::PublicKey,
    ) -> Result<Actor> {
        Ok(Actor {
            id: make_user_id(username, origin)?,
            preferred_username: username.to_string(),
            inbox: make_user_inbox(username, origin)?,
            outbox: make_user_outbox(username, origin)?,
            following: make_user_following(username, origin)?,
            followers: make_user_followers(username, origin)?,
            endpoints: Some(Endpoints::new(origin)?),
            public_key: PublicKey::from_username_and_key(username, origin, pub_key)?,
        })
    }
    pub fn id(&self) -> &Url {
        &self.id
    }
    pub fn inbox(&self) -> &Url {
        &self.inbox
    }
    pub fn public_key(&self) -> Result<PickyPublicKey> {
        PickyPublicKey::from_pem_str(&self.public_key.public_key_pem).context(PickyPemSnafu)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(untagged)]
pub enum ActorField {
    Inline(Box<Actor>),
    Iri(Url),
    InlineId(InlineId),
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Announce {
    object: Url,
    id: Url,
    actor: ActorField,
    published: DateTime<FixedOffset>,
    cc: Vec<Url>,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Follow {
    object: Url,
    id: Url,
    actor: ActorField,
}

impl Follow {
    pub fn new(object: Url, id: Url, actor: Url) -> Follow {
        Follow {
            object,
            id,
            actor: ActorField::Iri(actor),
        }
    }
    /// Retrieve the `id` property of the `actor` attribute of this follow request
    pub fn actor_id(&self) -> Url {
        match &self.actor {
            ActorField::Inline(follow_actor) => follow_actor.id().clone(),
            ActorField::Iri(id) => id.clone(),
            ActorField::InlineId(inline_id) => inline_id.id.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(untagged)]
pub enum ObjectField {
    Inline(Follow),
    Iri(Url),
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Accept {
    object: ObjectField,
    actor: ActorField,
}

impl Accept {
    /// Create an [Accept] for a [Follow] request
    ///
    /// [Accept]: https://www.w3.org/TR/activitystreams-vocabulary/#dfn-accept
    /// [Follow]: https://www.w3.org/TR/activitystreams-vocabulary/#dfn-follow
    pub fn for_follow(followed: &Username, follow_req: &Follow, origin: &Origin) -> Result<Accept> {
        Ok(Accept {
            object: ObjectField::Inline(follow_req.clone()),
            actor: ActorField::Iri(make_user_id(followed, origin)?),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Like {
    object: Url,
    id: Url,
    actor: ActorField,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Undo {
    object: ObjectField,
    actor: ActorField,
}

// This is the interesting bit: when implementing the user inbox, the payload could
// be any of an `Announce`, `Follow`, or `Like`:
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "type")]
pub enum BoostFollowOrLike {
    Announce(Announce),
    Follow(Follow),
    Like(Like),
    Undo(Undo),
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_accept_mastodon() {
        serde_json::from_str::<Accept>(r##"{
  "type": "Accept",
  "signature": {
    "type": "RsaSignature2017",
    "signatureValue": "rBzK4Kqhd4g7HDS8WE5oRbWQb2R+HF/6awbUuMWhgru/xCODT0SJWSri0qWqEO4fPcpoUyz2d25cw6o+iy9wiozQb3hQNnu69AR+H5Mytc06+g10KCHexbGhbAEAw/7IzmeXELHUbaqeduaDIbdt1zw4RkwLXdqgQcGXTJ6ND1wM3WMHXQCK1m0flasIXFoBxpliPAGiElV8s0+Ltuh562GvflG3kB3WO+j+NaR0ZfG5G9N88xMj9UQlCKit5gpAE5p6syUsCU2WGBHywTumv73i3OVTIFfq+P9AdMsRuzw1r7zoKEsthW4aOzLQDi01ZjvdBz8zH6JnjDU7SMN/Ig==",
    "creator": "http://mastodon.example.org/users/admin#main-key",
    "created": "2018-02-17T14:36:41Z"
  },
  "object": {
    "type": "Follow",
    "object": "http://mastodon.example.org/users/admin",
    "id": "http://localtesting.pleroma.lol/users/lain#follows/4",
    "actor": "http://localtesting.pleroma.lol/users/lain"
  },
  "nickname": "lain",
  "id": "http://mastodon.example.org/users/admin#accepts/follows/4",
  "actor": "http://mastodon.example.org/users/admin",
  "@context": [
    "https://www.w3.org/ns/activitystreams",
    "https://w3id.org/security/v1",
    {
      "toot": "http://joinmastodon.org/ns#",
      "sensitive": "as:sensitive",
      "ostatus": "http://ostatus.org#",
      "movedTo": "as:movedTo",
      "manuallyApprovesFollowers": "as:manuallyApprovesFollowers",
      "inReplyToAtomUri": "ostatus:inReplyToAtomUri",
      "conversation": "ostatus:conversation",
      "atomUri": "ostatus:atomUri",
      "Hashtag": "as:Hashtag",
      "Emoji": "toot:Emoji"
    }
  ]
}"##).unwrap();
    }
    #[test]
    fn test_actor_pixelfed() {
        // Downloaded manually from pixelfed in late 2024 or early 2025
        serde_json::from_str::<Actor>(r##"{
    "@context":[
	    "https://w3id.org/security/v1","https://www.w3.org/ns/activitystreams",
	    {"toot":"http://joinmastodon.org/ns#",
	     "manuallyApprovesFollowers":"as:manuallyApprovesFollowers",
	     "alsoKnownAs":{"@id":"as:alsoKnownAs","@type":"@id"},
	     "movedTo":{"@id":"as:movedTo","@type":"@id"},
	     "indexable":"toot:indexable",
	     "suspended":"toot:suspended"
	    }],
    "id":"https://pixelfed.social/users/dansup",
    "type":"Person",
    "following":"https://pixelfed.social/users/dansup/following",
    "followers":"https://pixelfed.social/users/dansup/followers",
    "inbox":"https://pixelfed.social/users/dansup/inbox",
    "outbox":"https://pixelfed.social/users/dansup/outbox",
    "preferredUsername":"dansup",
    "name":"dansup",
    "summary":"Hi, I'm the developer behind <a class=\"u-url mention\" href=\"https://pixelfed.social/Pixelfed\" rel=\"external nofollow noopener\" target=\"_blank\">@Pixelfed</a>! nnAlso <a class=\"u-url list-slug\" href=\"https://pixelfed.social/@dansup@mastodon.social\" rel=\"external nofollow noopener\" target=\"_blank\">@dansup@mastodon.social</a> nnhe/him u00b7 canada u00b7 ud83cudff3ufe0fu200dud83cudf08nn<a href=\"https://pixelfed.social/discover/tags/pixelfed?src=hash\" title=\"#pixelfed\" class=\"u-url hashtag\" rel=\"external nofollow noopener\">#pixelfed</a> <a href=\"https://pixelfed.social/discover/tags/design?src=hash\" title=\"#design\" class=\"u-url hashtag\" rel=\"external nofollow noopener\">#design</a> <a href=\"https://pixelfed.social/discover/tags/webdev?src=hash\" title=\"#webdev\" class=\"u-url hashtag\" rel=\"external nofollow noopener\">#webdev</a> <a href=\"https://pixelfed.social/discover/tags/pixelfedApp?src=hash\" title=\"#pixelfedApp\" class=\"u-url hashtag\" rel=\"external nofollow noopener\">#pixelfedApp</a> <a href=\"https://pixelfed.social/discover/tags/supApp?src=hash\" title=\"#supApp\" class=\"u-url hashtag\" rel=\"external nofollow noopener\">#supApp</a> <a href=\"https://pixelfed.social/discover/tags/loops?src=hash\" title=\"#loops\" class=\"u-url hashtag\" rel=\"external nofollow noopener\">#loops</a> <a href=\"https://pixelfed.social/discover/tags/fedi22?src=hash\" title=\"#fedi22\" class=\"u-url hashtag\" rel=\"external nofollow noopener\">#fedi22</a>\",\"url\":\"https://pixelfed.social/dansup",
    "manuallyApprovesFollowers":false,
    "indexable":true,
    "published":"2018-06-01T00:00:00Z",
    "publicKey":{
	"id":"https://pixelfed.social/users/dansup#main-key",
	"owner":"https://pixelfed.social/users/dansup",
	"publicKeyPem":"-----BEGIN PUBLIC KEY-----nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAn9TEqiXOvCKBS7dK8+ZVncO/UmPRejL77hmO74sHIyteJVHUhnhzBz3PAWaQWdv9A4ViL8ghhSV50NzO6HIrknzlclmK0GeGrxRFDBLGHpa4KFErqcQgIG3uRjJ5UDhUijEsbusmnVhpLWUFEO7Atwnbhd/XVciruF6ea3ryyco6ZES7IHKdUBwM3IKpZqIb/h2ObXcVVC1I2oggHRxR+ePnqst0qU31MryUkBqX7DVcNV/yXdRUuEc+ZiK/rNkr3RCzE3J7PePH5RNpJDIfj4Jnn+lW7ErT5Susn1+VHP7NHpAK8pe8atUpXEtogAbgt7KYi0Kq+XCxLv7YZuOqSaX2pnZwIDAQABn-----END PUBLIC KEY-----n"},
    "icon":{"type":"Image","mediaType":"image/jpeg",
	    "url":"https://pixelfed.social/storage/avatars/000/000/000/000/000/000/2/mLZr2R47XEwbmasH2M3P_avatar.jpg?v=57"},
    "endpoints":{"sharedInbox":"https://pixelfed.social/f/inbox"}}
"##).unwrap();
    }

    #[test]
    fn test_actor_hubzilla() {
        serde_json::from_str::<Actor>(r##"{"@context":["https://www.w3.org/ns/activitystreams","https://w3id.org/security/v1","https://hub.somaton.com/apschema/v1.9"],"type":"Person","id":"https://hub.somaton.com/channel/testc6","preferredUsername":"testc6","name":"testc6 lala","updated":"2021-08-29T10:07:23Z","icon":{"type":"Image","mediaType":"image/png","updated":"2021-10-09T04:54:35Z","url":"https://hub.somaton.com/photo/profile/l/33","height":300,"width":300},"url":"https://hub.somaton.com/channel/testc6","publicKey":{"id":"https://hub.somaton.com/channel/testc6","owner":"https://hub.somaton.com/channel/testc6","publicKeyPem":"-----BEGIN PUBLIC KEY-----\nMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAq5ep+6MhhaAiqZSd8nXe\nUAokXNgqTr/DjUic5VDudjQgvetchaiBUieBnqpJSPNNAvvf6Qs4eDW4w2JQeA6y\nqEplKrmb8l1EyhwXeFLDUGQdf0f6hg++x5mIrO6uX0tlQGU6nutvhItn6JMZc5GU\nv3C/UW0OfHCCdHSGZ/1nIqq1P98FqF0+PA1pvTHCkLr4kcKzfpmkLjsccUSq0FGh\nQF+paW9FU89o4hkaH/X3E/Ac7DL8zgcyt29KSj4eUIvjBIEPAMdRno345fiZ+QYr\nlYQYaBC2gvozjxtxl9MyfqjBRzfl9VDHzoDvMn5+LD5dCRB1zOESv/b3EpiHYqXl\nwiPzP9az8e8cw6D72n/Mlrf27yIuVAdwaGdbAwekjIQZHIDoP0XNnA5i31RLpEMI\nbNpH47ChtjxeilQZ3va6qIShYfGlndpy/rx4i4Yt4xIG+BbGb/dWo3AbtHi64fPZ\nMoLuR71sEBe7uAvalJ+lopxuQ2qLJpCInukQ13p/G/n9tVDwbfGyumzr5hHk7JoY\nN+JqH737MCZqb9dRDof+fju58GY1VzFjBph38sHYJh0ykA+2BzYU2+nT7CDXfKWA\nsmHhizp7haoPjl/yclZG5FJwg3oqHTD14dASUs+OI4K+Q//74wfb4/6E3CDyOkW3\nUj+8TPZooKulxtQ9ezergr0CAwEAAQ==\n-----END PUBLIC KEY-----\n"},"outbox":"https://hub.somaton.com/outbox/testc6","inbox":"https://hub.somaton.com/inbox/testc6","followers":"https://hub.somaton.com/followers/testc6","following":"https://hub.somaton.com/following/testc6","endpoints":{"sharedInbox":"https://hub.somaton.com/inbox"},"discoverable":false,"signature":{"@context":["https://www.w3.org/ns/activitystreams","https://w3id.org/security/v1"],"type":"RsaSignature2017","nonce":"8d6dea03f04cbb7faaf43958a4cf39a115ff1c61c7febaa6154c463eab9a42c8","creator":"https://hub.somaton.com/channel/testc6","created":"2021-10-13T18:21:48Z","signatureValue":"N4CJBO2K/8v7KI97REyJXaSYOlLWscuEDlODDnjNYD1fbVQFO3s2JtqPcN2lVJvNTlW5HUze+owaAYNcvZe3mNm1iz05Xru3s8yRA8bNCdKBuWd/3zb3/JQVkbSb09D2PloeuoKBQmPIn+dNiTyFR0jxLsxCXXTomGKigWPtTOUIt52Dv9MFJ3jRZmfoykT9bHrAIVCASHoiluhTkPAzc6pt0lSyZd0D3X4J1K4/sLXa8HRoooMFu2dHWfqV4tyLU9WzofAhvnYg9tEbKCH42DIAbwDfjAeC4qL8xkqAlYWLvXYVGH76cZLdp9Zuv1p3NHqaPEJ85MbuaUkfnU75Bx/Fcfoi0pEieWRdFvMx5b/UFwGbJd6iSAO1zRbGYTPEMPWHzh0AEAaLeyY+g3ZmpNu88ujrIr8iJ1U4EkjOBn8ooxA5LaI2fXDiYC2NwRiAbY+xVtgJgvHDi9tXCdvzjZWfU/cgiwF/cYMbsB2BCyPRd+XZhudfXSOysFC4WYnawhiRVevba9lQ6rEP4FMepOGq4ZOSGzxgw2xNIXpu0IkrxX5mEv/ahEhDy1KGRIFc0GnPJrv3kMVxJrZ7SF8PNAGqftQBLkqQR+SEygs3XB4cd2DQ2lPeiMd8+Xv+lBjtzZtZAM/Y4CZCOdV9DHXDGNSKKFDzzna4QcUzQ+KRc8w="}}"##).unwrap();
    }

    #[test]
    fn test_actor_guppe() {
        serde_json::from_str::<Actor>(r##"{
   "@context" : [
      "https://www.w3.org/ns/activitystreams",
      "https://w3id.org/security/v1"
   ],
   "followers" : "https://gup.pe/u/bernie2020/followers",
   "following" : "https://gup.pe/u/bernie2020/following",
   "icon" : {
      "mediaType" : "image/jpeg",
      "type" : "Image",
      "url" : "https://gup.pe/f/guppe.png"
   },
   "id" : "https://gup.pe/u/bernie2020",
   "inbox" : "https://gup.pe/u/bernie2020/inbox",
   "liked" : "https://gup.pe/u/bernie2020/liked",
   "name" : "Bernie2020 group",
   "outbox" : "https://gup.pe/u/bernie2020/outbox",
   "preferredUsername" : "Bernie2020",
   "publicKey" : {
      "id" : "https://gup.pe/u/bernie2020#main-key",
      "owner" : "https://gup.pe/u/bernie2020",
      "publicKeyPem" : "-----BEGIN PUBLIC KEY-----\nMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAw4J8nSrdWWxFaipgWDhR\nbTFzHUGoFy7Gjdc6gg9ZWGWDm9ZU5Ct0C/4o72dXSWdyLbQGYMbWVHLI1LHWKSiC\nVtwIYoccQBaxfi5bCxsahWhhSNPfK8tVlySHvBy73ir8KUZm93eAYh1iE9x+Dk63\nInmi7wzjsqHSlu1KxPGYcnyxs+xxhlTUSd5LsPfO1b9sHMW+X4rEky7OC90veCdD\nsoHU+nCmf+2zJSlOrU7DAzqB4Axc9oS9Q5RlT3yARJQMeu6JyjJJP9CMbpGFbUNT\n5Gsw0km1Rc1rR4tUoz8pLUYtliEUK+/0EmHi2EHAT1ueEfMoGGbCaX/mCoMmAwYJ\nwIGYXmKn2/ARIJpw2XPmrKWXqa2AndOQdb3l44Sl3ej2rC/JQmimGCn7tbfKEZyC\n6mMkOYTIeBtyW/wXFc1+GzJxtvA3C9HjilE+O/7gLHfCLP6FRIxg/9kOLhEj64Ed\n5HZ3sylvifXXubS/lLZr6sZW6d9ICoYLZpFw9AoF2zaYWpvJqBrWinnCJzvbMCYj\nfq/RAkcQYSxkDOHquiGgbRZHGAMKLnz5fMKJIzBtdQojYCUmB14OArW+ITUE9i2a\nPAJaXEGZ+BHYp/0ScFaXwp5LIgT1S+sPKxWJU//77wQfs25i7NZHSN/jtXVmsFS6\nLFVw49LcWAz3J2Im+A+uSd8CAwEAAQ==\n-----END PUBLIC KEY-----\n"
   },
   "summary" : "I'm a group about Bernie2020. Follow me to get all the group posts. Tag me to share with the group. Create other groups by searching for or tagging @yourGroupName@gup.pe",
   "type" : "Group"
}
"##).unwrap();
    }

    #[test]
    fn test_announce_mastodon() {
        serde_json::from_str::<Announce>(r##"{
  "type": "Announce",
  "to": [
    "https://www.w3.org/ns/activitystreams#Public"
  ],
  "signature": {
    "type": "RsaSignature2017",
    "signatureValue": "T95DRE0eAligvMuRMkQA01lsoz2PKi4XXF+cyZ0BqbrO12p751TEWTyyRn5a+HH0e4kc77EUhQVXwMq80WAYDzHKVUTf2XBJPBa68vl0j6RXw3+HK4ef5hR4KWFNBU34yePS7S1fEmc1mTG4Yx926wtmZwDpEMTp1CXOeVEjCYzmdyHpepPPH2ZZettiacmPRSqBLPGWZoot7kH/SioIdnrMGY0I7b+rqkIdnnEcdhu9N1BKPEO9Sr+KmxgAUiidmNZlbBXX6gCxp8BiIdH4ABsIcwoDcGNkM5EmWunGW31LVjsEQXhH5c1Wly0ugYYPCg/0eHLNBOhKkY/teSM8Lg==",
    "creator": "http://mastodon.example.org/users/admin#main-key",
    "created": "2018-02-17T19:39:15Z"
  },
  "published": "2018-02-17T19:39:15Z",
  "object": "http://mastodon.example.org/@admin/99541947525187367",
  "id": "http://mastodon.example.org/users/admin/statuses/99542391527669785/activity",
  "cc": [
    "http://mastodon.example.org/users/admin",
    "http://mastodon.example.org/users/admin/followers"
  ],
  "atomUri": "http://mastodon.example.org/users/admin/statuses/99542391527669785/activity",
  "actor": "http://mastodon.example.org/users/admin",
  "@context": [
    "https://www.w3.org/ns/activitystreams",
    "https://w3id.org/security/v1",
    {
      "toot": "http://joinmastodon.org/ns#",
      "sensitive": "as:sensitive",
      "ostatus": "http://ostatus.org#",
      "movedTo": "as:movedTo",
      "manuallyApprovesFollowers": "as:manuallyApprovesFollowers",
      "inReplyToAtomUri": "ostatus:inReplyToAtomUri",
      "conversation": "ostatus:conversation",
      "atomUri": "ostatus:atomUri",
      "Hashtag": "as:Hashtag",
      "Emoji": "toot:Emoji"
    }
  ]
}
"##).unwrap();
    }

    #[test]
    fn test_follow_mastodon() {
        serde_json::from_str::<Follow>(r##"{
  "type": "Follow",
  "signature": {
    "type": "RsaSignature2017",
    "signatureValue": "Kn1/UkAQGJVaXBfWLAHcnwHg8YMAUqlEaBuYLazAG+pz5hqivsyrBmPV186Xzr+B4ZLExA9+SnOoNx/GOz4hBm0kAmukNSILAsUd84tcJ2yT9zc1RKtembK4WiwOw7li0+maeDN0HaB6t+6eTqsCWmtiZpprhXD8V1GGT8yG7X24fQ9oFGn+ng7lasbcCC0988Y1eGqNe7KryxcPuQz57YkDapvtONzk8gyLTkZMV4De93MyRHq6GVjQVIgtiYabQAxrX6Q8C+4P/jQoqdWJHEe+MY5JKyNaT/hMPt2Md1ok9fZQBGHlErk22/zy8bSN19GdG09HmIysBUHRYpBLig==",
    "creator": "http://mastodon.example.org/users/admin#main-key",
    "created": "2018-02-17T13:29:31Z"
  },
  "object": "http://localtesting.pleroma.lol/users/lain",
  "nickname": "lain",
  "id": "http://mastodon.example.org/users/admin#follows/2",
  "actor": "http://mastodon.example.org/users/admin",
  "@context": [
    "https://www.w3.org/ns/activitystreams",
    "https://w3id.org/security/v1",
    {
      "toot": "http://joinmastodon.org/ns#",
      "sensitive": "as:sensitive",
      "ostatus": "http://ostatus.org#",
      "movedTo": "as:movedTo",
      "manuallyApprovesFollowers": "as:manuallyApprovesFollowers",
      "inReplyToAtomUri": "ostatus:inReplyToAtomUri",
      "conversation": "ostatus:conversation",
      "atomUri": "ostatus:atomUri",
      "Hashtag": "as:Hashtag",
      "Emoji": "toot:Emoji"
    }
  ]
}"##).unwrap();
    }

    #[test]
    fn test_follow_hubzilla() {
        serde_json::from_str::<Follow>(r##"{
  "type": "Follow",
  "signature": {
    "type": "RsaSignature2017",
    "signatureValue": "Kn1/UkAQGJVaXBfWLAHcnwHg8YMAUqlEaBuYLazAG+pz5hqivsyrBmPV186Xzr+B4ZLExA9+SnOoNx/GOz4hBm0kAmukNSILAsUd84tcJ2yT9zc1RKtembK4WiwOw7li0+maeDN0HaB6t+6eTqsCWmtiZpprhXD8V1GGT8yG7X24fQ9oFGn+ng7lasbcCC0988Y1eGqNe7KryxcPuQz57YkDapvtONzk8gyLTkZMV4De93MyRHq6GVjQVIgtiYabQAxrX6Q8C+4P/jQoqdWJHEe+MY5JKyNaT/hMPt2Md1ok9fZQBGHlErk22/zy8bSN19GdG09HmIysBUHRYpBLig==",
    "creator": "https://hubzilla.example.org/channel/kaniini#main-key",
    "created": "2018-02-17T13:29:31Z"
  },
  "object": "https://localtesting.pleroma.lol/users/lain",
  "nickname": "lain",
  "id": "https://hubzilla.example.org/channel/kaniini#follows/2",
  "actor": {
    "id": "https://hubzilla.example.org/channel/kaniini"
  },
  "@context": [
    "https://www.w3.org/ns/activitystreams",
    "https://w3id.org/security/v1",
    {
      "toot": "http://joinmastodon.org/ns#",
      "sensitive": "as:sensitive",
      "ostatus": "http://ostatus.org#",
      "movedTo": "as:movedTo",
      "manuallyApprovesFollowers": "as:manuallyApprovesFollowers",
      "inReplyToAtomUri": "ostatus:inReplyToAtomUri",
      "conversation": "ostatus:conversation",
      "atomUri": "ostatus:atomUri",
      "Hashtag": "as:Hashtag",
      "Emoji": "toot:Emoji"
    }
  ]
}
"##).unwrap();
    }

    #[test]
    fn test_follow_osada() {
        serde_json::from_str::<Follow>(r##"{
  "@context": [
    "https://www.w3.org/ns/activitystreams",
    "https://w3id.org/security/v1",
    "https://apfed.club/apschema/v1.4"
  ],
  "id": "https://apfed.club/follow/9",
  "type": "Follow",
  "actor": {
    "type": "Person",
    "id": "https://apfed.club/channel/indio",
    "preferredUsername": "indio",
    "name": "Indio",
    "updated": "2019-08-20T23:52:34Z",
    "icon": {
      "type": "Image",
      "mediaType": "image/jpeg",
      "updated": "2019-08-20T23:53:37Z",
      "url": "https://apfed.club/photo/profile/l/2",
      "height": 300,
      "width": 300
    },
    "url": "https://apfed.club/channel/indio",
    "inbox": "https://apfed.club/inbox/indio",
    "outbox": "https://apfed.club/outbox/indio",
    "followers": "https://apfed.club/followers/indio",
    "following": "https://apfed.club/following/indio",
    "endpoints": {
      "sharedInbox": "https://apfed.club/inbox"
    },
    "publicKey": {
      "id": "https://apfed.club/channel/indio",
      "owner": "https://apfed.club/channel/indio",
      "publicKeyPem": "-----BEGIN PUBLIC KEY-----\nMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA77TIR1VuSYFnmDRFGHHb\n4vaGdx9ranzRX4bfOKAqa++Ch5L4EqJpPy08RuM+NrYCYiYl4QQFDSSDXAEgb5g9\nC1TgWTfI7q/E0UBX2Vr0mU6X4i1ztv0tuQvegRjcSJ7l1AvoBs8Ip4MEJ3OPEQhB\ngJqAACB3Gnps4zi2I0yavkxUfGVKr6zKT3BxWh5hTpKC7Do+ChIrVZC2EwxND9K6\nsAnQHThcb5EQuvuzUQZKeS7IEOsd0JpZDmJjbfMGrAWE81pLIfEeeA2joCJiBBTO\nglDsW+juvZ+lWqJpMr2hMWpvfrFjJeUawNJCIzsLdVIZR+aKj5yy6yqoS8hkN9Ha\n1MljZpsXl+EmwcwAIqim1YeLwERCEAQ/JWbSt8pQTQbzZ6ibwQ4mchCxacrRbIVR\nnL59fWMBassJcbY0VwrTugm2SBsYbDjESd55UZV03Rwr8qseGTyi+hH8O7w2SIaY\nzjN6AdZiPmsh00YflzlCk8MSLOHMol1vqIUzXxU8CdXn9+KsuQdZGrTz0YKN/db4\naVwUGJatz2Tsvf7R1tJBjJfeQWOWbbn3pycLVH86LjZ83qngp9ZVnAveUnUqz0yS\nhe+buZ6UMsfGzbIYon2bKNlz6gYTH0YPcr+cLe+29drtt0GZiXha1agbpo4RB8zE\naNL2fucF5YT0yNpbd/5WoV0CAwEAAQ==\n-----END PUBLIC KEY-----\n"
    }
  },
  "object": "https://pleroma.site/users/kaniini",
  "to": [
    "https://pleroma.site/users/kaniini"
  ],
  "signature": {
    "@context": [
      "https://www.w3.org/ns/activitystreams",
      "https://w3id.org/security/v1"
    ],
    "type": "RsaSignature2017",
    "nonce": "52c035e0a9e81dce8b486159204e97c22637e91f75cdfad5378de91de68e9117",
    "creator": "https://apfed.club/channel/indio/public_key_pem",
    "created": "2019-08-22T03:38:02Z",
    "signatureValue": "oVliRCIqNIh6yUp851dYrF0y21aHp3Rz6VkIpW1pFMWfXuzExyWSfcELpyLseeRmsw5bUu9zJkH44B4G2LiJQKA9UoEQDjrDMZBmbeUpiQqq3DVUzkrBOI8bHZ7xyJ/CjSZcNHHh0MHhSKxswyxWMGi4zIqzkAZG3vRRgoPVHdjPm00sR3B8jBLw1cjoffv+KKeM/zEUpe13gqX9qHAWHHqZepxgSWmq+EKOkRvHUPBXiEJZfXzc5uW+vZ09F3WBYmaRoy8Y0e1P29fnRLqSy7EEINdrHaGclRqoUZyiawpkgy3lWWlynesV/HiLBR7EXT79eKstxf4wfTDaPKBCfTCsOWuMWHr7Genu37ew2/t7eiBGqCwwW12ylhml/OLHgNK3LOhmRABhtfpaFZSxfDVnlXfaLpY1xekVOj2oC0FpBtnoxVKLpIcyLw6dkfSil5ANd+hl59W/bpPA8KT90ii1fSNCo3+FcwQVx0YsPznJNA60XfFuVsme7zNcOst6393e1WriZxBanFpfB63zVQc9u1fjyfktx/yiUNxIlre+sz9OCc0AACn94iRhBYh4bbzdleUOTnM7lnD4Dj2FP+xeDIP8CA8wXUeq5+9kopSp2kAmlUEyFUdg4no7naIeu1SZnopfUg56PsVCp9JHiUK1SYAyWbdC+FbUECu5CvI="
  }
}
"##).unwrap();
    }

    #[test]
    fn test_like_mastodon() {
        serde_json::from_str::<Like>(r##"{
  "type": "Like",
  "signature": {
    "type": "RsaSignature2017",
    "signatureValue": "fdxMfQSMwbC6wP6sh6neS/vM5879K67yQkHTbiT5Npr5wAac0y6+o3Ij+41tN3rL6wfuGTosSBTHOtta6R4GCOOhCaCSLMZKypnp1VltCzLDoyrZELnYQIC8gpUXVmIycZbREk22qWUe/w7DAFaKK4UscBlHDzeDVcA0K3Se5Sluqi9/Zh+ldAnEzj/rSEPDjrtvf5wGNf3fHxbKSRKFt90JvKK6hS+vxKUhlRFDf6/SMETw+EhwJSNW4d10yMUakqUWsFv4Acq5LW7l+HpYMvlYY1FZhNde1+uonnCyuQDyvzkff8zwtEJmAXC4RivO/VVLa17SmqheJZfI8oluVg==",
    "creator": "http://mastodon.example.org/users/admin#main-key",
    "created": "2018-02-17T18:57:49Z"
  },
  "object": "http://localtesting.pleroma.lol/objects/eb92579d-3417-42a8-8652-2492c2d4f454",
  "nickname": "lain",
  "id": "http://mastodon.example.org/users/admin#likes/2",
  "actor": "http://mastodon.example.org/users/admin",
  "@context": [
    "https://www.w3.org/ns/activitystreams",
    "https://w3id.org/security/v1",
    {
      "toot": "http://joinmastodon.org/ns#",
      "sensitive": "as:sensitive",
      "ostatus": "http://ostatus.org#",
      "movedTo": "as:movedTo",
      "manuallyApprovesFollowers": "as:manuallyApprovesFollowers",
      "inReplyToAtomUri": "ostatus:inReplyToAtomUri",
      "conversation": "ostatus:conversation",
      "atomUri": "ostatus:atomUri",
      "Hashtag": "as:Hashtag",
      "Emoji": "toot:Emoji"
    }
  ]
}"##).unwrap();
    }

    #[test]
    fn test_like_misskey() {
        serde_json::from_str::<Like>(
            r##"{
  "@context" : [
    "https://www.w3.org/ns/activitystreams",
    "https://w3id.org/security/v1",
    {"Hashtag" : "as:Hashtag"}
  ],
  "_misskey_reaction" : "pudding",
  "actor": "http://mastodon.example.org/users/admin",
  "cc" : ["https://testing.pleroma.lol/users/lain"],
  "id" : "https://misskey.xyz/75149198-2f45-46e4-930a-8b0538297075",
  "nickname" : "lain",
  "object" : "https://testing.pleroma.lol/objects/c331bbf7-2eb9-4801-a709-2a6103492a5a",
  "type" : "Like"
}
"##,
        )
        .unwrap();
    }

    #[test]
    fn test_boost_follow_or_like_1() {
        let x = serde_json::from_str::<BoostFollowOrLike>(r##"{
  "type": "Announce",
  "to": [
    "https://www.w3.org/ns/activitystreams#Public"
  ],
  "signature": {
    "type": "RsaSignature2017",
    "signatureValue": "T95DRE0eAligvMuRMkQA01lsoz2PKi4XXF+cyZ0BqbrO12p751TEWTyyRn5a+HH0e4kc77EUhQVXwMq80WAYDzHKVUTf2XBJPBa68vl0j6RXw3+HK4ef5hR4KWFNBU34yePS7S1fEmc1mTG4Yx926wtmZwDpEMTp1CXOeVEjCYzmdyHpepPPH2ZZettiacmPRSqBLPGWZoot7kH/SioIdnrMGY0I7b+rqkIdnnEcdhu9N1BKPEO9Sr+KmxgAUiidmNZlbBXX6gCxp8BiIdH4ABsIcwoDcGNkM5EmWunGW31LVjsEQXhH5c1Wly0ugYYPCg/0eHLNBOhKkY/teSM8Lg==",
    "creator": "http://mastodon.example.org/users/admin#main-key",
    "created": "2018-02-17T19:39:15Z"
  },
  "published": "2018-02-17T19:39:15Z",
  "object": "http://mastodon.example.org/@admin/99541947525187367",
  "id": "http://mastodon.example.org/users/admin/statuses/99542391527669785/activity",
  "cc": [
    "http://mastodon.example.org/users/admin",
    "http://mastodon.example.org/users/admin/followers"
  ],
  "atomUri": "http://mastodon.example.org/users/admin/statuses/99542391527669785/activity",
  "actor": "http://mastodon.example.org/users/admin",
  "@context": [
    "https://www.w3.org/ns/activitystreams",
    "https://w3id.org/security/v1",
    {
      "toot": "http://joinmastodon.org/ns#",
      "sensitive": "as:sensitive",
      "ostatus": "http://ostatus.org#",
      "movedTo": "as:movedTo",
      "manuallyApprovesFollowers": "as:manuallyApprovesFollowers",
      "inReplyToAtomUri": "ostatus:inReplyToAtomUri",
      "conversation": "ostatus:conversation",
      "atomUri": "ostatus:atomUri",
      "Hashtag": "as:Hashtag",
      "Emoji": "toot:Emoji"
    }
  ]
}
"##).unwrap();
        assert!(matches!(x, BoostFollowOrLike::Announce(_)));
    }

    #[test]
    fn test_boost_follow_or_like_2() {
        let x = serde_json::from_str::<BoostFollowOrLike>(r##"{
  "type": "Follow",
  "signature": {
    "type": "RsaSignature2017",
    "signatureValue": "Kn1/UkAQGJVaXBfWLAHcnwHg8YMAUqlEaBuYLazAG+pz5hqivsyrBmPV186Xzr+B4ZLExA9+SnOoNx/GOz4hBm0kAmukNSILAsUd84tcJ2yT9zc1RKtembK4WiwOw7li0+maeDN0HaB6t+6eTqsCWmtiZpprhXD8V1GGT8yG7X24fQ9oFGn+ng7lasbcCC0988Y1eGqNe7KryxcPuQz57YkDapvtONzk8gyLTkZMV4De93MyRHq6GVjQVIgtiYabQAxrX6Q8C+4P/jQoqdWJHEe+MY5JKyNaT/hMPt2Md1ok9fZQBGHlErk22/zy8bSN19GdG09HmIysBUHRYpBLig==",
    "creator": "http://mastodon.example.org/users/admin#main-key",
    "created": "2018-02-17T13:29:31Z"
  },
  "object": "http://localtesting.pleroma.lol/users/lain",
  "nickname": "lain",
  "id": "http://mastodon.example.org/users/admin#follows/2",
  "actor": "http://mastodon.example.org/users/admin",
  "@context": [
    "https://www.w3.org/ns/activitystreams",
    "https://w3id.org/security/v1",
    {
      "toot": "http://joinmastodon.org/ns#",
      "sensitive": "as:sensitive",
      "ostatus": "http://ostatus.org#",
      "movedTo": "as:movedTo",
      "manuallyApprovesFollowers": "as:manuallyApprovesFollowers",
      "inReplyToAtomUri": "ostatus:inReplyToAtomUri",
      "conversation": "ostatus:conversation",
      "atomUri": "ostatus:atomUri",
      "Hashtag": "as:Hashtag",
      "Emoji": "toot:Emoji"
    }
  ]
}"##).unwrap();
        assert!(matches!(x, BoostFollowOrLike::Follow(_)))
    }

    #[test]
    fn test_boost_follow_or_like_3() {
        let x = serde_json::from_str::<BoostFollowOrLike>(r##"{
  "type": "Like",
  "signature": {
    "type": "RsaSignature2017",
    "signatureValue": "fdxMfQSMwbC6wP6sh6neS/vM5879K67yQkHTbiT5Npr5wAac0y6+o3Ij+41tN3rL6wfuGTosSBTHOtta6R4GCOOhCaCSLMZKypnp1VltCzLDoyrZELnYQIC8gpUXVmIycZbREk22qWUe/w7DAFaKK4UscBlHDzeDVcA0K3Se5Sluqi9/Zh+ldAnEzj/rSEPDjrtvf5wGNf3fHxbKSRKFt90JvKK6hS+vxKUhlRFDf6/SMETw+EhwJSNW4d10yMUakqUWsFv4Acq5LW7l+HpYMvlYY1FZhNde1+uonnCyuQDyvzkff8zwtEJmAXC4RivO/VVLa17SmqheJZfI8oluVg==",
    "creator": "http://mastodon.example.org/users/admin#main-key",
    "created": "2018-02-17T18:57:49Z"
  },
  "object": "http://localtesting.pleroma.lol/objects/eb92579d-3417-42a8-8652-2492c2d4f454",
  "nickname": "lain",
  "id": "http://mastodon.example.org/users/admin#likes/2",
  "actor": "http://mastodon.example.org/users/admin",
  "@context": [
    "https://www.w3.org/ns/activitystreams",
    "https://w3id.org/security/v1",
    {
      "toot": "http://joinmastodon.org/ns#",
      "sensitive": "as:sensitive",
      "ostatus": "http://ostatus.org#",
      "movedTo": "as:movedTo",
      "manuallyApprovesFollowers": "as:manuallyApprovesFollowers",
      "inReplyToAtomUri": "ostatus:inReplyToAtomUri",
      "conversation": "ostatus:conversation",
      "atomUri": "ostatus:atomUri",
      "Hashtag": "as:Hashtag",
      "Emoji": "toot:Emoji"
    }
  ]
}"##).unwrap();
        assert!(matches!(x, BoostFollowOrLike::Like(_)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                  ActivityPub Entity Utilities                                  //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolve a key ID to a PublicKey
pub async fn resolve_key_id(key_id: &Url, client: &reqwest::Client) -> Result<Actor> {
    client
        .get(key_id.clone())
        .header(http::header::ACCEPT, "application/activity+json")
        .send()
        .await
        .context(ResolveKeyIdSnafu {
            key_id: key_id.clone(),
        })?
        .json::<Actor>()
        .await
        .context(ActorDeSnafu)
}

/// Sum type representing the permissible values for the "type" field in a JLD document
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, PartialOrd, Serialize)]
pub enum Type {
    Accept,
    Actor,
    Announce,
    Follow,
    Like,
    Person,
    OrderedCollection,
    CollectionPage,
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Type::Accept => "Accept",
                Type::Actor => "Actor",
                Type::Announce => "Announce",
                Type::Follow => "Follow",
                Type::Like => "Like",
                Type::Person => "Person",
                Type::OrderedCollection => "OrderedCollection",
                Type::CollectionPage => "CollectionPage",
            }
        )
    }
}

/// Serialize an ActivityPub entity to JLD
pub fn to_jrd<T>(value: T, ty: Type, context: Option<Context>) -> Result<String>
where
    T: Serialize,
{
    let value = serde_json::to_value(value).context(JsonSerSnafu)?;
    let context = context.unwrap_or_default();
    let ctx = serde_json::to_value(context).context(JsonSerSnafu)?;
    match (value, ctx) {
        (Value::Object(mut val_map), Value::Object(mut ctx_map)) => {
            val_map.append(&mut ctx_map);
            val_map.insert("type".to_owned(), Value::String(format!("{}", ty)));
            serde_json::to_string(&Value::Object(val_map)).context(JsonSerSnafu)
        }
        _ => JsonTypeMismatchSnafu.fail(),
    }
}
