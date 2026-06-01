// Copyright (C) 2024-2025 Michael Herstine <sp1ff@pobox.com>
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

//! # indielinks models
//!
//! ## Introduction
//!
//! I hate these sort of "catch-all" modules named "models" or "entities", but these types are truly
//! foundational (to [indielinks](crate); there's an even more foundational set of entites in
//! the [indielinks-shared](indielinks_shared) module of the same name.

use std::fmt::Display;

use argon2::{Algorithm, Argon2, Params, PasswordHash, PasswordHasher, PasswordVerifier, Version};
use chrono::{DateTime, SecondsFormat, Utc};
use password_hash::{rand_core::OsRng, PasswordHashString, SaltString};
use scylla::{
    deserialize::{
        row::ColumnIterator, value::DeserializeValue, DeserializationError, FrameSlice,
        TypeCheckError,
    },
    frame::response::result::{ColumnSpec, ColumnType},
    serialize::{
        value::SerializeValue,
        writers::{CellWriter, WrittenCellProof},
        SerializationError,
    },
    DeserializeRow,
};
use secrecy::{ExposeSecret, SecretBox, SecretSlice, SecretString};
use serde::{de::Unexpected, ser::SerializeStruct, Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha512_224};
use snafu::{prelude::*, Backtrace, IntoError};
use tap::pipe::Pipe;
use tracing::debug;
use url::Url;
use uuid::Uuid;
use zxcvbn::{feedback::Feedback, zxcvbn, Score};

use indielinks_shared::{
    define_id,
    entities::{
        generate_rsa_keypair, PostId, StorUrl, UserEmail, UserId, UserPrivateKey, UserPublicKey,
        Username,
    },
    native_type_check,
};

use crate::peppers::{self, Pepper, Peppers, Version as PepperVersion};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Attempted to deserialize an invalid value for ActivityPubPostFlavor: {n}"))]
    ActivityPubPostFlavorDe { n: i8, backtrace: Backtrace },
    #[snafu(display("Invalid API key"))]
    BadApiKey { backtrace: Backtrace },
    #[snafu(display("Incorrect password"))]
    BadPassword { backtrace: Backtrace },
    #[snafu(display("While deserializing an InReply, only one of the two fields was found"))]
    BrokenInReply { backtrace: Backtrace },
    CheckPassword {
        username: Username,
        source: password_hash::errors::Error,
        backtrace: Backtrace,
    },
    #[snafu(display(
        "Attempting to mint a key that expires at {expiry}; this is in the past or too soon in the future"
    ))]
    Expiry {
        expiry: DateTime<Utc>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to hash password: {source}"))]
    HashPassword {
        source: password_hash::errors::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Bad hash string: {source}"))]
    HashString {
        source: password_hash::errors::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to build an Argon2id password hasher: {source}"))]
    Hasher {
        source: argon2::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Attempted to deserialize an invalid value for InReplySort: {i}"))]
    InReplySortDe { i: i8, backtrace: Backtrace },
    #[snafu(display("Invalid tag value {tag}"))]
    InvalidTag { tag: i8 },
    #[snafu(display("While generating the user's keypair, {source}"))]
    Keypair {
        source: indielinks_shared::entities::Error,
    },
    #[snafu(display("Attempted to deserialize an invalid value for LsrFlavor: {i}"))]
    LsrFlavorDe { i: i8, backtrace: Backtrace },
    #[snafu(display("Missing field {field}"))]
    MissingField { field: &'static str },
    #[snafu(display("Can't deserialize a {typ} from a null frame slice"))]
    NoFrameSlice { typ: String, backtrace: Backtrace },
    #[snafu(display("No pepper found for user {username}: {source}"))]
    NoPepper {
        username: Username,
        source: peppers::Error,
    },
    #[snafu(display("Password doesn't have enough entropy: {feedback}"))]
    PasswordEntropy {
        feedback: Feedback,
        backtrace: Backtrace,
    },
    #[snafu(display("Passwords may not begin or end in whitespace"))]
    PasswordWhitespace { backtrace: Backtrace },
    #[snafu(display("Failed to parse {text} as an URL: {source}"))]
    UserUrl {
        text: String,
        source: url::ParseError,
        backtrace: Backtrace,
    },
    #[snafu(display("Attempted to deserialize an invalid value for Visibility: {i}"))]
    VisibilityDe { i: i8, backtrace: Backtrace },
    #[snafu(display("{key_material:?} doesn't match"))]
    WrongKey {
        key_material: SecretSlice<u8>,
        backtrace: Backtrace,
    },
    #[snafu(display("Incorrect key length: {source}"))]
    WrongKeyLength {
        source: std::array::TryFromSliceError,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

fn mk_de_err(err: impl std::error::Error + Send + Sync + 'static) -> DeserializationError {
    DeserializationError::new(err)
}

fn mk_ser_err(err: impl std::error::Error + Send + Sync + 'static) -> SerializationError {
    SerializationError::new(err)
}

fn mk_serde_de_err<'de, D: serde::Deserializer<'de>>(err: impl std::error::Error) -> D::Error {
    <D::Error as serde::de::Error>::custom(format!("{err:?}"))
}

define_id!(FollowId, "followid");
define_id!(FollowerId, "followerid");
define_id!(LikeId, "likeid");
define_id!(ReplyId, "replyid");
define_id!(ShareId, "shareid");

// I had, in the past, defined a few other identifiers, making it worth it to wrap the boilerplate
// up in a macro. Now that it's just `UserId`, I should probably go back to just implementing it by
// hand.

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             ApiKey                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// An indielinks API key, version 1
///
/// About the only thing this, the first version of an indielinks API key offers beyond raw key
/// material is an optional expiration date. We could, of course, track status, but for now I'm
/// going to model revocation by simply deleting the key from the datastore. Something like scope or
/// permissions would make no sense because we have no RBAC right now-- possesion of a key gives the
/// holder full permissions on the account of the user to whom it was issued.
///
/// Note that we don't store the key material in this struct: rather, we only store a *hash* of the
/// key material. See [new](ApiKeyV1::new) for details.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApiKeyV1 {
    /// SHA-512/224 hash of the key material
    key_material_hash: [u8; 28],
    /// Date this key expires; None means it lives forever
    expiry: Option<DateTime<Utc>>,
}

impl ApiKeyV1 {
    /// Create a new API key; return an [ApiKeyV1] instance along with the raw key material. The
    /// intent is that the caller will transmit or display the key material to the user and then
    /// drop it.
    pub fn new(expiry: Option<DateTime<Utc>>) -> Result<(ApiKeyV1, SecretBox<[u8]>)> {
        // Let's start with the key material
        use rand::RngCore;
        use std::ops::DerefMut;

        if let Some(expiry) = expiry {
            ensure!(
                expiry - Utc::now() >= chrono::Duration::seconds(30),
                ExpirySnafu { expiry }
            );
        }

        let mut rng = OsRng;
        let mut key_material = Box::new([0u8; 64]);
        rng.fill_bytes(key_material.deref_mut());

        Ok((
            ApiKeyV1 {
                key_material_hash: Self::hash_key_material(&key_material),
                expiry,
            },
            SecretBox::new(key_material),
        ))
    }
    /// Create a new key with infinite lifetime from pre-allocated key material
    pub fn from_key_material(key_material: &[u8; 64]) -> ApiKeyV1 {
        ApiKeyV1 {
            key_material_hash: Self::hash_key_material(key_material),
            expiry: None,
        }
    }
    /// Check `key_material` against this key
    pub fn check(&self, key_material: &SecretSlice<u8>) -> Result<()> {
        use secrecy::ExposeSecret;
        ensure!(
            self.key_material_hash
                == Self::hash_key_material(
                    key_material
                        .expose_secret()
                        .try_into()
                        .context(WrongKeyLengthSnafu)?,
                ),
            WrongKeySnafu {
                key_material: key_material.clone()
            }
        );
        Ok(())
    }
    /// Compute a SHA-512/224 hash of the provided key material
    fn hash_key_material(key_material: &[u8; 64]) -> [u8; 28] {
        let mut hasher = Sha512_224::default();
        hasher.update(key_material);
        *hasher.finalize().as_mut()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "version")] // tag "internally"
pub enum ApiKey {
    #[serde(rename = "1")]
    V1(ApiKeyV1),
}

impl ApiKey {
    pub fn new(expiry: Option<DateTime<Utc>>) -> Result<(ApiKey, String)> {
        let (key, key_material) = ApiKeyV1::new(expiry)?;
        Ok((
            ApiKey::V1(key),
            format!("v1:{}", hex::encode(key_material.expose_secret())),
        ))
    }
    /// Validate this API key against the provided key material
    pub fn check(&self, key_material: &SecretSlice<u8>) -> Result<()> {
        match self {
            ApiKey::V1(api_key_v1) => Ok(api_key_v1.check(key_material)?),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ApiKeys {
    Zero,
    One(ApiKey),
    Two((ApiKey, ApiKey)),
}

impl ApiKeys {
    pub fn check(&self, key_material: &SecretSlice<u8>) -> Result<()> {
        match &self {
            ApiKeys::Zero => panic!("This user has no API keys!"),
            ApiKeys::One(api_key) => api_key.check(key_material),
            ApiKeys::Two((api_key1, api_key2)) => api_key1
                .check(key_material)
                .or_else(|_| api_key2.check(key_material)),
        }
    }
}

// For ScyllaDB, we're just going to use MessagePack for serde, and store the keys in a column of type `blob`
impl SerializeValue for ApiKeys {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        native_type_check!(typ, Blob, SerializationError, "ApiKeys")?;
        let buf = rmp_serde::to_vec(&self).map_err(mk_ser_err)?;
        writer.set_value(buf.as_slice()).map_err(mk_ser_err)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for ApiKeys {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        native_type_check!(typ, Blob, TypeCheckError, "ApiKeys")
    }
    fn deserialize(
        _: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        v.ok_or(
            NoFrameSliceSnafu {
                typ: "ApiKeys".to_owned(),
            }
            .build(),
        )
        .map_err(mk_de_err)?
        .as_slice()
        .pipe(rmp_serde::from_slice::<ApiKeys>)
        .map_err(mk_de_err)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         UserHashString                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Newtype idiom to work around Rust's orphaned trait rule
///
/// I've chosen to serialize the hash string as a [PasswordHashString], rather than a
/// [PasswordHash], since the latter doesn't support serde.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct UserHashString(
    #[serde(serialize_with = "serde_hash_string::serialize")] PasswordHashString,
);

impl UserHashString {
    pub fn password_hash(&self) -> PasswordHash<'_> {
        self.0.password_hash()
    }
}

impl Display for UserHashString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Implement `Deserialize` by hand to fail if the serialized value isn't a legit hash string
impl<'de> Deserialize<'de> for UserHashString {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        UserHashString::try_from(s).map_err(mk_serde_de_err::<'de, D>)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for UserHashString {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        String::type_check(typ)
    }
    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        UserHashString::try_from(<String as DeserializeValue>::deserialize(typ, v)?)
            .map_err(mk_de_err)
    }
}

impl SerializeValue for UserHashString {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&self.0.as_str(), typ, writer)
    }
}

impl TryFrom<String> for UserHashString {
    type Error = Error;

    fn try_from(s: String) -> std::result::Result<Self, Self::Error> {
        Ok(UserHashString(
            PasswordHashString::new(&s).context(HashStringSnafu)?,
        ))
    }
}

mod serde_hash_string {
    use super::*;
    use serde::Serializer;

    pub fn serialize<S: Serializer>(
        hash_string: &PasswordHashString,
        ser: S,
    ) -> StdResult<S::Ok, S::Error> {
        hash_string
            .as_str()
            .pipe(|s| <str as serde::Serialize>::serialize(s, ser))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                              User                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents an indielinks user
#[derive(Clone, Debug, Deserialize, DeserializeRow, PartialEq, Serialize)]
pub struct User {
    id: UserId,
    username: Username,
    discoverable: bool,
    display_name: String,
    summary: String,
    pub_key_pem: UserPublicKey,
    priv_key_pem: UserPrivateKey,
    api_keys: ApiKeys,
    // Will be null until the first post
    first_update: Option<DateTime<Utc>>,
    // Will be null until the first post
    last_update: Option<DateTime<Utc>>,
    password_hash: UserHashString,
    pepper_version: PepperVersion,
}

/// Apply password validation rules
///
/// At this time, I don't have a lot of rules on passwords; I will reject them if they begin or end
/// with whitespace since that's likely to be a mistake on the caller's part that will drive them
/// bonkers when they try to login. I delegate most of the work to [zxcvbn] and simply reject
/// passwords that are too weak (score of less than three on a scale of zero-to-four).
fn validate_password(password: &SecretString, user_inputs: &[&str]) -> Result<()> {
    if password
        .expose_secret()
        .starts_with(|c: char| c.is_whitespace())
        || password
            .expose_secret()
            .ends_with(|c: char| c.is_whitespace())
    {
        return PasswordWhitespaceSnafu.fail();
    }

    let entropy = zxcvbn(password.expose_secret(), user_inputs);
    if entropy.score() < Score::Three {
        return PasswordEntropySnafu {
            // Feedback is set "when score <= 2", so the `unwrap()` below is safe.
            feedback: entropy.feedback().unwrap().clone(),
        }
        .fail();
    }

    debug!(
        "Password check: this password would take O({}) guesses",
        entropy.guesses_log10()
    );
    Ok(())
}

impl User {
    /// Mint a new key & add it to this users's collection, ejecting an earlier key if need be
    pub fn add_key(&self, expiry: Option<DateTime<Utc>>) -> Result<(ApiKeys, String)> {
        let (api_key, key_text) = ApiKey::new(expiry)?;
        let new_keys = match &self.api_keys {
            ApiKeys::Zero => ApiKeys::One(api_key),
            ApiKeys::One(first_api_key) => ApiKeys::Two((first_api_key.clone(), api_key)),
            ApiKeys::Two((_, last_api_key)) => ApiKeys::Two((last_api_key.clone(), api_key)),
        };
        Ok((new_keys, key_text))
    }
    pub fn api_keys(&self) -> &ApiKeys {
        &self.api_keys
    }
    /// Validate key material against this users API key(s)
    pub fn check_key(&self, key_material: &SecretSlice<u8>) -> Result<()> {
        self.api_keys.check(key_material)
    }
    /// Validate a password
    ///
    /// The caller will have to lookup the [Pepper] from configuration based on this user's pepper
    /// version.
    pub fn check_password(&self, peppers: &Peppers, password: SecretString) -> Result<()> {
        let pepper = peppers
            .find_by_version(&self.pepper_version)
            .context(NoPepperSnafu {
                username: self.username.clone(),
            })?;
        let hasher = User::create_password_hasher(&pepper)?;
        match hasher.verify_password(
            password.expose_secret().as_bytes(),
            &self.password_hash.password_hash(),
        ) {
            Ok(_) => Ok(()),
            Err(password_hash::errors::Error::Password) => BadPasswordSnafu.fail(),
            Err(err) => Err(CheckPasswordSnafu {
                username: self.username.clone(),
            }
            .into_error(err)),
        }
    }
    pub fn date_of_last_post(&self) -> Option<&DateTime<Utc>> {
        self.last_update.as_ref()
    }
    pub fn discoverable(&self) -> bool {
        self.discoverable
    }
    pub fn display_name(&self) -> &str {
        &self.display_name
    }
    pub fn first_update(&self) -> Option<DateTime<Utc>> {
        self.first_update
    }
    pub fn hash(&self) -> &UserHashString {
        &self.password_hash
    }
    pub fn id(&self) -> &UserId {
        &self.id
    }
    pub fn last_update(&self) -> Option<&DateTime<Utc>> {
        self.last_update.as_ref()
    }
    /// Create a new [User]
    ///
    /// This constructor will create a new [User] instance without validating uniqueness of the
    /// username. Otherwise, it will:
    ///
    /// - validate the password, rejecting it if it's too weak
    ///
    /// - create a 2048 bit RSA keypair; I should probably make the length configurable but 2048
    ///   seems like a good compromise between general support & acceptable security
    ///
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pepper_version: &PepperVersion,
        pepper_key: &Pepper,
        username: &Username,
        password: &SecretString,
        email: &UserEmail,
        api_key: Option<Box<[u8; 64]>>,
        discoverable: Option<bool>,
        display_name: Option<&str>,
        summary: Option<&str>,
    ) -> Result<User> {
        validate_password(password, &[username.as_ref(), email.as_ref()])?;
        let (pub_key, priv_key) = generate_rsa_keypair().context(KeypairSnafu)?;
        let password_hash = User::hash_password(pepper_key, password)?;
        let _ = display_name.unwrap_or(username.as_ref()).to_string();
        Ok(User {
            id: UserId::default(),
            username: username.clone(),
            discoverable: discoverable.unwrap_or(true),
            display_name: display_name.unwrap_or(username.as_ref()).to_string(),
            summary: summary.unwrap_or("").to_string(),
            pub_key_pem: pub_key,
            priv_key_pem: priv_key,
            api_keys: match api_key {
                Some(key_material) => {
                    ApiKeys::One(ApiKey::V1(ApiKeyV1::from_key_material(&key_material)))
                }
                None => ApiKeys::Zero,
            },
            first_update: None,
            last_update: None,
            password_hash: UserHashString(password_hash),
            pepper_version: pepper_version.clone(),
        })
    }
    pub fn password_hash(&self) -> &UserHashString {
        &self.password_hash
    }
    pub fn pepper_version(&self) -> &PepperVersion {
        &self.pepper_version
    }
    pub fn priv_key(&self) -> &UserPrivateKey {
        &self.priv_key_pem
    }
    pub fn pub_key(&self) -> &UserPublicKey {
        &self.pub_key_pem
    }
    pub fn summary(&self) -> &str {
        &self.summary
    }
    pub fn username(&self) -> &Username {
        &self.username
    }
    /// Create an indielinks user password hasher
    ///
    /// This function returns a [PasswordHasher] employing the Argon2id algorithm (with pepper) with
    /// parameters m=19456 (19 MiB), t=2, p=1 (Do not use with Argon2i) Per the OWASP Password
    /// Storage [Cheat Sheet], Argon2id is the first algorithm which should be considered, and those
    /// are one of the recommended configurations for this algorithm.
    ///
    /// [Cheat Sheet]: https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html#password-hashing-algorithms
    fn create_password_hasher(pepper: &Pepper) -> Result<Argon2<'_>> {
        Argon2::new_with_secret(
            pepper.as_ref().expose_secret(),
            Algorithm::Argon2id,
            Version::default(),
            Params::default(),
        )
        .context(HasherSnafu)
    }
    /// Hash a password
    ///
    /// This function will first salt the password, then hash it using Argon2id with the default version
    /// (19 at the time of this writing) & parameters (m=19, t=2, m=1 at the time of this writing). Note
    /// that the parameters, as of February 12, 2025, comport with the OWASP [recommendations].
    ///
    /// [recomendations]: https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html#password-hashing-algorithms
    ///
    /// Per [this] Github issue comment, the pepper is supplied via the `secret` field in the
    /// `new_with_secret()` constructor.
    ///
    /// [this]: https://github.com/RustCrypto/traits/pull/699#issuecomment-891105093
    fn hash_password(pepper: &Pepper, password: &SecretString) -> Result<PasswordHashString> {
        let salt = SaltString::generate(&mut OsRng);
        let hasher = User::create_password_hasher(pepper)?;
        Ok(hasher
            .hash_password(password.expose_secret().as_bytes(), &salt)
            .context(HashPasswordSnafu)?
            .serialize())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Follower                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents an indielinks follower; i.e. an ActivityPub entity following an indielinks [User]
#[derive(Clone, Debug, Deserialize, DeserializeRow, PartialEq, Serialize)]
pub struct Follower {
    user_id: UserId,
    actor_id: StorUrl,
    id: FollowerId,
    created: DateTime<Utc>,
    accepted: bool,
}

impl Follower {
    pub fn actor_id(&self) -> &StorUrl {
        &self.actor_id
    }
    pub fn new(user: &User, actor_id: &StorUrl) -> Follower {
        Follower {
            user_id: *user.id(),
            actor_id: actor_id.clone(),
            id: FollowerId::default(),
            created: Utc::now(),
            accepted: false,
        }
    }
    pub fn new_with_id(user: &User, actor_id: &StorUrl, id: &FollowerId) -> Follower {
        Follower {
            user_id: *user.id(),
            actor_id: actor_id.clone(),
            id: *id,
            created: Utc::now(),
            accepted: false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Following                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents an indielinks follow; i.e. an ActivityPub entity being followed by an indielinks [User]
#[derive(Clone, Debug, Deserialize, DeserializeRow, PartialEq, Serialize)]
pub struct Following {
    user_id: UserId,
    actor_id: StorUrl,
    id: FollowId,
    created: DateTime<Utc>,
    accepted: bool,
}

impl Following {
    pub fn actor_id(&self) -> &StorUrl {
        &self.actor_id
    }
    pub fn new(user: &User, actor_id: &StorUrl) -> Following {
        Following {
            user_id: *user.id(),
            actor_id: actor_id.clone(),
            id: FollowId::default(),
            created: Utc::now(),
            accepted: false,
        }
    }
    pub fn new_with_id(user: &User, actor_id: &StorUrl, id: &FollowId) -> Following {
        Following {
            user_id: *user.id(),
            actor_id: actor_id.clone(),
            id: *id,
            created: Utc::now(),
            accepted: false,
        }
    }
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Visibility                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Visibility levels for assorted messages
// I pulled this ontology from <https://seb.jambor.dev/posts/understanding-activitypub/>; I'm not
// sure if this is a general ActivityPub thing, or Mastodon-specific.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
// Chosen to match the Rust type corresponding to ScyllaDB's `tinyint`
#[repr(i8)]
pub enum Visibility {
    // Per
    // [r-items.enum.discriminant.implicit](https://doc.rust-lang.org/reference/items/enumerations.html#r-items.enum.discriminant.implicit),
    // these values are well-defined. But still. since we're this directly to the database, I'd like
    // to be explicit.
    Public = 0,
    Unlisted = 1,
    Followers = 2,
    DirectMessage = 3,
}

// If we derive `Serialize`, values of type `Visibility` will be written as strings (i.e. "Public",
// "Unlisted", and so forth). I'd prefer to write them as `i8`.
impl Serialize for Visibility {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i8(*self as i8)
    }
}

impl<'de> Deserialize<'de> for Visibility {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match <i8 as Deserialize>::deserialize(deserializer)? {
            0 => Ok(Visibility::Public),
            1 => Ok(Visibility::Unlisted),
            2 => Ok(Visibility::Followers),
            3 => Ok(Visibility::DirectMessage),
            i => Err(serde::de::Error::custom(format!(
                "Invalid Visibility encoding {i}"
            ))),
        }
    }
}

impl SerializeValue for Visibility {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&(*self as i8), typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for Visibility {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        i8::type_check(typ)
    }
    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        match <i8 as DeserializeValue>::deserialize(typ, v)? {
            0 => Ok(Visibility::Public),
            1 => Ok(Visibility::Unlisted),
            2 => Ok(Visibility::Followers),
            3 => Ok(Visibility::DirectMessage),
            i => Err(DeserializationError::new(VisibilityDeSnafu { i }.build())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           LsrFlavor                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

// We're now storing likes, shares & replies together in the same tables (one for incoming, one
// for outgoing). This requires a discriminator. Like `Visibility`, above, we'll serialize it
// as a true `i8`.

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd)]
#[repr(i8)]
pub enum LsrFlavor {
    Like = 0,
    Share = 1,
    Reply = 2,
}

impl Serialize for LsrFlavor {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i8(*self as i8)
    }
}

impl<'de> Deserialize<'de> for LsrFlavor {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match <i8 as Deserialize>::deserialize(deserializer)? {
            0 => Ok(LsrFlavor::Like),
            1 => Ok(LsrFlavor::Share),
            2 => Ok(LsrFlavor::Reply),
            i => Err(serde::de::Error::custom(format!(
                "Invalid LsrFlavor encoding {i}"
            ))),
        }
    }
}

impl SerializeValue for LsrFlavor {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&(*self as i8), typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for LsrFlavor {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        i8::type_check(typ)
    }
    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        match <i8 as DeserializeValue>::deserialize(typ, v)? {
            0 => Ok(LsrFlavor::Like),
            1 => Ok(LsrFlavor::Share),
            2 => Ok(LsrFlavor::Reply),
            i => Err(DeserializationError::new(LsrFlavorDeSnafu { i }.build())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                outgoing likes, shares & replies                                //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// The [indielinks] internal representation of a "like" generated on this instance
///
/// [indielinks]: ../indielinks/index.html
// `OutgoingLike` is not directly serializable-- it needs to be serialized as a variant of
// `OutgoingLikeReplyShare`
#[derive(Clone, Debug)]
pub struct OutgoingLike {
    user_id: UserId,
    posted: DateTime<Utc>,
    likeid: LikeId,
    // The thing being liked
    in_reply_to: StorUrl,
}

impl OutgoingLike {
    pub fn new(user_id: UserId, ap_id: &Url) -> Self {
        Self {
            user_id,
            posted: Utc::now(),
            likeid: Default::default(),
            in_reply_to: ap_id.into(),
        }
    }
    pub fn id(&self) -> LikeId {
        self.likeid
    }
    pub fn in_reply_to(&self) -> &StorUrl {
        &self.in_reply_to
    }
    pub fn posted(&self) -> DateTime<Utc> {
        self.posted
    }
}

/// The [indielinks] internal representation of a "reply" generated on this instance
///
/// [indielinks]: ../indielinks/index.html
// `OutgoingReply` is not directly serializable-- it needs to be serialized as a variant of
// `OutgoingLikeReplyShare`
#[derive(Clone, Debug)]
pub struct OutgoingReply {
    user_id: UserId,
    posted: DateTime<Utc>,
    replyid: ReplyId,
    // The thing to which we are replying
    in_reply_to: StorUrl,
    visibility: Visibility,
    // We store the raw content; possibly dangereous, but I like the faithfulness
    content: String,
}

impl OutgoingReply {
    pub fn new(
        user_id: UserId,
        replyid: ReplyId,
        ap_id: Url,
        visibility: Visibility,
        content: String,
    ) -> Self {
        Self {
            user_id,
            posted: Utc::now(),
            replyid,
            in_reply_to: ap_id.into(),
            visibility,
            content,
        }
    }
    pub fn content(&self) -> &str {
        self.content.as_ref()
    }
    pub fn id(&self) -> ReplyId {
        self.replyid
    }
    pub fn in_reply_to(&self) -> &StorUrl {
        &self.in_reply_to
    }
    pub fn posted(&self) -> DateTime<Utc> {
        self.posted
    }
    pub fn user_id(&self) -> UserId {
        self.user_id
    }
    pub fn visibility(&self) -> Visibility {
        self.visibility
    }
}

/// The [indielinks] internal representation of a "share" generated on this instance
///
/// [indielinks]: ../indielinks/index.html
// `OutgoingShare` is not directly serializable-- it needs to be serialized as a variant of
// `OutgoingLikeReplyShare`
#[derive(Clone, Debug)]
pub struct OutgoingShare {
    user_id: UserId,
    posted: DateTime<Utc>,
    shareid: ShareId,
    // The thing we are sharing
    in_reply_to: StorUrl,
    visibility: Visibility,
    // We store the raw content; possibly dangereous, but I like the faithfulness
    content: String,
}

impl OutgoingShare {
    pub fn new(user_id: UserId, ap_id: &Url, visibility: Visibility, content: String) -> Self {
        Self {
            user_id,
            posted: Utc::now(),
            shareid: Default::default(),
            in_reply_to: ap_id.into(),
            visibility,
            content,
        }
    }
    pub fn content(&self) -> &str {
        self.content.as_ref()
    }
    pub fn id(&self) -> ShareId {
        self.shareid
    }
    pub fn in_reply_to(&self) -> &StorUrl {
        &self.in_reply_to
    }
    pub fn posted(&self) -> DateTime<Utc> {
        self.posted
    }
    pub fn user_id(&self) -> UserId {
        self.user_id
    }
    pub fn visibility(&self) -> Visibility {
        self.visibility
    }
}

// The idea here is that we can instantiate a `LikeReplyShare` and write it to the
// `likes_replies_shares` table, either via the ScyllaDB client, or as `serde_dynamo::to_item`.
// Take a reference to avoid consuming the value when serializing.
#[derive(Clone, Debug)]
pub enum LikeReplyShareRef<'a> {
    Like(&'a OutgoingLike),
    Reply(&'a OutgoingReply),
    Share(&'a OutgoingShare),
}

// But on *read*, we need to deserialize to something "owned"
#[derive(Clone, Debug)]
pub enum LikeReplyShare {
    Like(OutgoingLike),
    Reply(OutgoingReply),
    Share(OutgoingShare),
}

// This is what serde calls <https://serde.rs/enum-representations.html> an "internally tagged"
// representation of the `LikeReplyShare` enum, with a few wrinkles:
//
// - the "tag" (AKA discriminator), instead of being serialized as text, is an `i8`
// - we add an additional field `posted_and_id` (derived from `posted` and `id` on write, ignored on
//   read), because we use a compond sort key when writing this to DynamoDB

impl Serialize for LikeReplyShareRef<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            LikeReplyShareRef::Like(like) => {
                let sk = format!(
                    "{}-{}",
                    like.posted.to_rfc3339_opts(SecondsFormat::Nanos, true),
                    like.likeid
                );
                let mut state = serializer.serialize_struct("LikeReplyShare", 8)?;
                state.serialize_field("kind", &LsrFlavor::Like)?;
                state.serialize_field("user_id", &like.user_id)?;
                state.serialize_field("posted_and_id", &sk)?;
                state.serialize_field("posted", &like.posted)?;
                state.serialize_field("id", &like.likeid)?;
                state.serialize_field("in_reply_to", &like.in_reply_to)?;
                state.serialize_field("visibility", &Option::<Visibility>::None)?;
                state.serialize_field("content", &Option::<String>::None)?;
                state.end()
            }
            LikeReplyShareRef::Reply(reply) => {
                let sk = format!(
                    "{}-{}",
                    reply.posted.to_rfc3339_opts(SecondsFormat::Nanos, true),
                    reply.replyid
                );
                let mut state = serializer.serialize_struct("LikeReplyShare", 8)?;
                state.serialize_field("kind", &LsrFlavor::Reply)?;
                state.serialize_field("user_id", &reply.user_id)?;
                state.serialize_field("posted_and_id", &sk)?;
                state.serialize_field("posted", &reply.posted)?;
                state.serialize_field("id", &reply.replyid)?;
                state.serialize_field("in_reply_to", &reply.in_reply_to)?;
                state.serialize_field("visibility", &Some(reply.visibility))?;
                state.serialize_field("content", &Some(&reply.content))?;
                state.end()
            }
            LikeReplyShareRef::Share(share) => {
                let sk = format!(
                    "{}-{}",
                    share.posted.to_rfc3339_opts(SecondsFormat::Nanos, true),
                    share.shareid
                );
                let mut state = serializer.serialize_struct("LikeReplyShare", 8)?;
                state.serialize_field("kind", &LsrFlavor::Share)?;
                state.serialize_field("user_id", &share.user_id)?;
                state.serialize_field("posted_and_id", &sk)?;
                state.serialize_field("posted", &share.posted)?;
                state.serialize_field("id", &share.shareid)?;
                state.serialize_field("in_reply_to", &share.in_reply_to)?;
                state.serialize_field("visibility", &Some(share.visibility))?;
                state.serialize_field("content", &Some(&share.content))?;
                state.end()
            }
        }
    }
}

struct LikeReplyShareVisitor;

impl<'de> serde::de::Visitor<'de> for LikeReplyShareVisitor {
    type Value = LikeReplyShare;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "An outgoing like, share or reply")
    }
    fn visit_map<A>(self, mut map: A) -> StdResult<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        use serde::de::Error as DeError;

        let mut kind: Option<i8> = None;
        let mut user_id: Option<UserId> = None;
        let mut posted: Option<DateTime<Utc>> = None;
        let mut id: Option<Uuid> = None;
        let mut in_reply_to: Option<StorUrl> = None;
        let mut visibility: Option<Visibility> = None;
        let mut content: Option<String> = None;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "kind" => kind = Some(map.next_value()?),
                "user_id" => user_id = Some(map.next_value()?),
                "posted" => posted = Some(map.next_value()?),
                "id" => id = Some(map.next_value()?),
                "in_reply_to" => in_reply_to = Some(map.next_value()?),
                "visibility" => visibility = map.next_value()?,
                "content" => content = map.next_value()?,
                _ => {
                    let _: String = map.next_value()?;
                }
            }
        }

        let kind = kind.ok_or(DeError::missing_field("kind"))?;

        match kind {
            0 => Ok(LikeReplyShare::Like(OutgoingLike {
                user_id: user_id.ok_or(DeError::missing_field("user_id"))?,
                posted: posted.ok_or(DeError::missing_field("posted"))?,
                likeid: LikeId::from_uuid(id.ok_or(DeError::missing_field("id"))?),
                in_reply_to: in_reply_to.ok_or(DeError::missing_field("in_reply_to"))?,
            })),
            1 => Ok(LikeReplyShare::Reply(OutgoingReply {
                user_id: user_id.ok_or(DeError::missing_field("user_id"))?,
                posted: posted.ok_or(DeError::missing_field("posted"))?,
                replyid: ReplyId::from_uuid(id.ok_or(DeError::missing_field("id"))?),
                in_reply_to: in_reply_to.ok_or(DeError::missing_field("in_reply_to"))?,
                visibility: visibility.ok_or(DeError::missing_field("visibility"))?,
                content: content.ok_or(DeError::missing_field("content"))?,
            })),
            2 => Ok(LikeReplyShare::Share(OutgoingShare {
                user_id: user_id.ok_or(DeError::missing_field("user_id"))?,
                posted: posted.ok_or(DeError::missing_field("posted"))?,
                shareid: ShareId::from_uuid(id.ok_or(DeError::missing_field("id"))?),
                in_reply_to: in_reply_to.ok_or(DeError::missing_field("in_reply_to"))?,
                visibility: visibility.ok_or(DeError::missing_field("visibility"))?,
                content: content.ok_or(DeError::missing_field("content"))?,
            })),
            i => Err(DeError::invalid_value(
                Unexpected::Signed(i as i64),
                &"An i8 between 0 & 2, inclusive",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for LikeReplyShare {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(LikeReplyShareVisitor)
    }
}

impl scylla::serialize::row::SerializeRow for LikeReplyShareRef<'_> {
    fn serialize(
        &self,
        ctx: &scylla::serialize::row::RowSerializationContext<'_>,
        writer: &mut scylla::serialize::writers::RowWriter,
    ) -> std::result::Result<(), SerializationError> {
        let tuple = match self {
            LikeReplyShareRef::Like(like) => (
                &like.user_id,
                &like.posted,
                &like.likeid.as_ref(),
                &Option::<&String>::None,
                &like.in_reply_to,
                0i8,
                &Option::<Visibility>::None,
            ),
            LikeReplyShareRef::Reply(reply) => (
                &reply.user_id,
                &reply.posted,
                &reply.replyid.as_ref(),
                &Some(&reply.content),
                &reply.in_reply_to,
                1i8,
                &Some(reply.visibility),
            ),
            LikeReplyShareRef::Share(share) => (
                &share.user_id,
                &share.posted,
                &share.shareid.as_ref(),
                &Some(&share.content),
                &share.in_reply_to,
                2i8,
                &Some(share.visibility),
            ),
        };
        scylla::serialize::row::SerializeRow::serialize(&tuple, ctx, writer)
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl<'frame, 'metadata> scylla::deserialize::row::DeserializeRow<'frame, 'metadata>
    for LikeReplyShare
{
    fn type_check(specs: &[ColumnSpec]) -> StdResult<(), TypeCheckError> {
        use scylla::deserialize::row::DeserializeRow;
        <(
            UserId,
            DateTime<Utc>,
            Uuid,
            Option<String>,
            StorUrl,
            i8,
            Option<Visibility>,
        ) as DeserializeRow>::type_check(specs)
    }

    fn deserialize(
        row: ColumnIterator<'frame, 'metadata>,
    ) -> StdResult<Self, DeserializationError> {
        use scylla::deserialize::row::DeserializeRow;
        let (user_id, posted, id, content, in_reply_to, sort, visibility) =
            <(
                UserId,
                DateTime<Utc>,
                Uuid,
                Option<String>,
                StorUrl,
                i8,
                Option<Visibility>,
            ) as DeserializeRow>::deserialize(row)?;
        match sort {
            0 => Ok(LikeReplyShare::Like(OutgoingLike {
                user_id,
                posted,
                likeid: LikeId::from_uuid(id),
                in_reply_to,
            })),
            1 => Ok(LikeReplyShare::Reply(OutgoingReply {
                user_id,
                posted,
                replyid: ReplyId::from_uuid(id),
                in_reply_to,
                visibility: visibility.ok_or(mk_de_err(
                    MissingFieldSnafu {
                        field: "visibility",
                    }
                    .build(),
                ))?,
                content: content
                    .ok_or(mk_de_err(MissingFieldSnafu { field: "content" }.build()))?,
            })),
            2 => Ok(LikeReplyShare::Share(OutgoingShare {
                user_id,
                posted,
                shareid: ShareId::from_uuid(id),
                in_reply_to,
                visibility: visibility.ok_or(mk_de_err(
                    MissingFieldSnafu {
                        field: "visibility",
                    }
                    .build(),
                ))?,
                content: content
                    .ok_or(mk_de_err(MissingFieldSnafu { field: "content" }.build()))?,
            })),
            tag => Err(mk_de_err(InvalidTagSnafu { tag }.build())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                incoming likes, shares & replies                                //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug)]
#[repr(i8)]
pub enum InReplySort {
    Post = 0,
    Reply = 1,
    Share = 2,
}

impl Serialize for InReplySort {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i8(*self as i8)
    }
}

impl<'de> Deserialize<'de> for InReplySort {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match <i8 as Deserialize>::deserialize(deserializer)? {
            0 => Ok(InReplySort::Post),
            1 => Ok(InReplySort::Reply),
            2 => Ok(InReplySort::Share),
            i => Err(serde::de::Error::custom(format!(
                "Invalid InReplySort encoding {i}"
            ))),
        }
    }
}

impl SerializeValue for InReplySort {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&(*self as i8), typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for InReplySort {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        i8::type_check(typ)
    }
    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        match <i8 as DeserializeValue>::deserialize(typ, v)? {
            0 => Ok(InReplySort::Post),
            1 => Ok(InReplySort::Reply),
            2 => Ok(InReplySort::Share),
            i => Err(DeserializationError::new(InReplySortDeSnafu { i }.build())),
        }
    }
}

#[derive(Clone, Debug)]
pub enum InReply {
    Post(PostId),
    Reply(ReplyId),
    Share(ShareId),
}

impl From<PostId> for InReply {
    fn from(value: PostId) -> Self {
        InReply::Post(value)
    }
}

impl Serialize for InReply {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            InReply::Post(postid) => {
                let mut state = serializer.serialize_struct("InReply", 2)?;
                state.serialize_field("kind", &InReplySort::Post)?;
                state.serialize_field("postid", &postid)?;
                state.end()
            }
            InReply::Reply(replyid) => {
                let mut state = serializer.serialize_struct("InReply", 2)?;
                state.serialize_field("kind", &InReplySort::Reply)?;
                state.serialize_field("replyid", &replyid)?;
                state.end()
            }
            InReply::Share(shareid) => {
                let mut state = serializer.serialize_struct("InReply", 2)?;
                state.serialize_field("kind", &InReplySort::Share)?;
                state.serialize_field("shareid", &shareid)?;
                state.end()
            }
        }
    }
}

struct InReplyVisitor;

impl<'de> serde::de::Visitor<'de> for InReplyVisitor {
    type Value = InReply;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "An InReply")
    }
    fn visit_map<A>(self, mut map: A) -> StdResult<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        use serde::de::Error as DeError;

        let mut kind: Option<i8> = None;
        let mut postid: Option<PostId> = None;
        let mut replyid: Option<ReplyId> = None;
        let mut shareid: Option<ShareId> = None;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "kind" => kind = Some(map.next_value()?),
                "postid" => postid = Some(map.next_value()?),
                "replyid" => replyid = Some(map.next_value()?),
                "shareid" => shareid = Some(map.next_value()?),
                _ => {
                    let _: String = map.next_value()?;
                }
            }
        }

        let kind = kind.ok_or(DeError::missing_field("kind"))?;

        match kind {
            0 => Ok(InReply::Post(
                postid.ok_or(DeError::missing_field("postid"))?,
            )),
            1 => Ok(InReply::Reply(
                replyid.ok_or(DeError::missing_field("replyid"))?,
            )),
            2 => Ok(InReply::Share(
                shareid.ok_or(DeError::missing_field("shareid"))?,
            )),
            i => Err(DeError::invalid_value(
                Unexpected::Signed(i as i64),
                &"An i8 between 0 & 2, inclusive",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for InReply {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(InReplyVisitor)
    }
}

/// The [indielinks] internal representation of a "like" coming to this instance from outside
///
/// [indielinks]: ../indielinks/index.html
// Same game; `IncomingLike` is not directly serializable-- it needs to be serialized as a variant
// of `IncomingLikeReplyShare`
#[derive(Clone, Debug)]
pub struct IncomingLike {
    user_id: UserId,
    received: DateTime<Utc>,
    ap_like_id: StorUrl,
    // The ID of the thing being liked; Post, Reply or Share
    in_reply_to: InReply,
}

impl IncomingLike {
    pub fn new(user_id: UserId, ap_like_id: Url, in_reply_to: InReply) -> Self {
        Self {
            user_id,
            received: Utc::now(),
            ap_like_id: ap_like_id.into(),
            in_reply_to,
        }
    }
}

/// The [indielinks] internal representation of a "reply" coming to this instance from outside
///
/// [indielinks]: ../indielinks/index.html
#[derive(Clone, Debug)]
pub struct IncomingReply {
    user_id: UserId,
    received: DateTime<Utc>,
    ap_reply_id: StorUrl,
    // The ID of the thing being liked; Post, Reply or Share, if it's on this instance
    in_reply_to: Option<InReply>,
    visibility: Visibility,
    content: String,
}

impl IncomingReply {
    pub fn new(
        user_id: UserId,
        ap_id: Url,
        in_reply_to: Option<InReply>,
        visibility: Visibility,
        content: String,
    ) -> Self {
        Self {
            user_id,
            received: Utc::now(),
            ap_reply_id: ap_id.into(),
            in_reply_to,
            visibility,
            content,
        }
    }
}

/// The [indielinks] internal representation of a "share" coming to this instance from outside
///
/// [indielinks]: ../indielinks/index.html
#[derive(Clone, Debug)]
pub struct IncomingShare {
    user_id: UserId,
    received: DateTime<Utc>,
    ap_share_id: StorUrl,
    // The ID of the thing being liked; Post, Reply or Share, if it's on this instance
    in_reply_to: Option<InReply>,
    visibility: Visibility,
    content: String,
}

// The idea here is that we can instantiate a `LikeReplyShareRef` and write it to the
// `likes_replies_shares` table, either via the ScyllaDB client, or as `serde_dynamo::to_item`
// We hold a reference to avoid consuming the entity when serializing.
#[derive(Clone, Debug)]
pub enum IncomingLikeReplyShareRef<'a> {
    Like(&'a IncomingLike),
    Reply(&'a IncomingReply),
    Share(&'a IncomingShare),
}

// However, on the deserialization side, we need to deserialize to something owned.
#[derive(Clone, Debug)]
pub enum IncomingLikeReplyShare {
    Like(IncomingLike),
    Reply(IncomingReply),
    Share(IncomingShare),
}

impl Serialize for IncomingLikeReplyShareRef<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            IncomingLikeReplyShareRef::Like(like) => {
                let sk = format!(
                    "{}-{}",
                    like.received.to_rfc3339_opts(SecondsFormat::Nanos, true),
                    like.ap_like_id
                );
                let (sort, id): (i8, Uuid) = match like.in_reply_to {
                    InReply::Post(postid) => (0i8, postid.into()),
                    InReply::Reply(replyid) => (1i8, replyid.into()),
                    InReply::Share(shareid) => (2i8, shareid.into()),
                };
                let mut state = serializer.serialize_struct("IncomingLikeReplyShare", 8)?;
                state.serialize_field("kind", &LsrFlavor::Like)?;
                state.serialize_field("user_id", &like.user_id)?;
                state.serialize_field("received_and_ap_id", &sk)?;
                state.serialize_field("received", &like.received)?;
                state.serialize_field("ap_like_id", &like.ap_like_id)?;
                state.serialize_field("in_reply_to_sort", &Some(sort))?;
                state.serialize_field("in_reply_to", &Some(id))?;
                state.serialize_field("visibility", &Option::<Visibility>::None)?;
                state.serialize_field("content", &Option::<String>::None)?;
                state.end()
            }
            IncomingLikeReplyShareRef::Reply(reply) => {
                let sk = format!(
                    "{}-{}",
                    reply.received.to_rfc3339_opts(SecondsFormat::Nanos, true),
                    reply.ap_reply_id
                );
                let (sort, id): (Option<i8>, Option<Uuid>) = match reply.in_reply_to {
                    Some(InReply::Post(postid)) => (Some(0i8), Some(postid.into())),
                    Some(InReply::Reply(replyid)) => (Some(1i8), Some(replyid.into())),
                    Some(InReply::Share(shareid)) => (Some(2i8), Some(shareid.into())),
                    None => (None, None),
                };
                let mut state = serializer.serialize_struct("IncomingReplyReplyShare", 8)?;
                state.serialize_field("kind", &LsrFlavor::Reply)?;
                state.serialize_field("user_id", &reply.user_id)?;
                state.serialize_field("received_and_ap_id", &sk)?;
                state.serialize_field("received", &reply.received)?;
                state.serialize_field("ap_reply_id", &reply.ap_reply_id)?;
                state.serialize_field("in_reply_to_sort", &sort)?;
                state.serialize_field("in_reply_to", &id)?;
                state.serialize_field("visibility", &Some(reply.visibility))?;
                state.serialize_field("content", &Some(&reply.content))?;
                state.end()
            }
            IncomingLikeReplyShareRef::Share(share) => {
                let sk = format!(
                    "{}-{}",
                    share.received.to_rfc3339_opts(SecondsFormat::Nanos, true),
                    share.ap_share_id
                );
                let (sort, id): (Option<i8>, Option<Uuid>) = match share.in_reply_to {
                    Some(InReply::Post(postid)) => (Some(0i8), Some(postid.into())),
                    Some(InReply::Reply(replyid)) => (Some(1i8), Some(replyid.into())),
                    Some(InReply::Share(shareid)) => (Some(2i8), Some(shareid.into())),
                    None => (None, None),
                };
                let mut state = serializer.serialize_struct("IncomingShareShareShare", 8)?;
                state.serialize_field("kind", &LsrFlavor::Share)?;
                state.serialize_field("user_id", &share.user_id)?;
                state.serialize_field("received_and_ap_id", &sk)?;
                state.serialize_field("received", &share.received)?;
                state.serialize_field("ap_share_id", &share.ap_share_id)?;
                state.serialize_field("in_share_to_sort", &sort)?;
                state.serialize_field("in_share_to", &id)?;
                state.serialize_field("visibility", &Some(share.visibility))?;
                state.serialize_field("content", &Some(&share.content))?;
                state.end()
            }
        }
    }
}

struct IncomingLikeReplyShareVisitor;

impl<'de> serde::de::Visitor<'de> for IncomingLikeReplyShareVisitor {
    type Value = IncomingLikeReplyShare;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "An ingoing like, share or reply")
    }
    fn visit_map<A>(self, mut map: A) -> StdResult<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        use serde::de::Error as DeError;

        let mut kind: Option<i8> = None;
        let mut user_id: Option<UserId> = None;
        let mut received: Option<DateTime<Utc>> = None;
        let mut ap_like_id: Option<StorUrl> = None;
        let mut ap_reply_id: Option<StorUrl> = None;
        let mut ap_share_id: Option<StorUrl> = None;
        let mut in_reply_to: Option<InReply> = None;
        let mut visibility: Option<Visibility> = None;
        let mut content: Option<String> = None;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "kind" => kind = Some(map.next_value()?),
                "user_id" => user_id = Some(map.next_value()?),
                "received" => received = Some(map.next_value()?),
                "ap_like_id" => ap_like_id = Some(map.next_value()?),
                "ap_reply_id" => ap_reply_id = Some(map.next_value()?),
                "ap_share_id" => ap_share_id = Some(map.next_value()?),
                "in_reply_to" => in_reply_to = Some(map.next_value()?),
                "visibility" => visibility = map.next_value()?,
                "content" => content = map.next_value()?,
                _ => {
                    let _: String = map.next_value()?;
                }
            }
        }

        let kind = kind.ok_or(DeError::missing_field("kind"))?;

        match kind {
            0 => Ok(IncomingLikeReplyShare::Like(IncomingLike {
                user_id: user_id.ok_or(DeError::missing_field("user_id"))?,
                received: received.ok_or(DeError::missing_field("received"))?,
                ap_like_id: ap_like_id.ok_or(DeError::missing_field("ap_like_id"))?,
                in_reply_to: in_reply_to.ok_or(DeError::missing_field("in_reply_to"))?,
            })),
            1 => Ok(IncomingLikeReplyShare::Reply(IncomingReply {
                user_id: user_id.ok_or(DeError::missing_field("user_id"))?,
                received: received.ok_or(DeError::missing_field("received"))?,
                ap_reply_id: ap_reply_id.ok_or(DeError::missing_field("ap_reply_id"))?,
                in_reply_to,
                visibility: visibility.ok_or(DeError::missing_field("visibility"))?,
                content: content.ok_or(DeError::missing_field("content"))?,
            })),
            2 => Ok(IncomingLikeReplyShare::Share(IncomingShare {
                user_id: user_id.ok_or(DeError::missing_field("user_id"))?,
                received: received.ok_or(DeError::missing_field("received"))?,
                ap_share_id: ap_share_id.ok_or(DeError::missing_field("ap_share_id"))?,
                in_reply_to,
                visibility: visibility.ok_or(DeError::missing_field("visibility"))?,
                content: content.ok_or(DeError::missing_field("content"))?,
            })),
            i => Err(DeError::invalid_value(
                Unexpected::Signed(i as i64),
                &"An i8 between 0 & 2, inclusive",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for IncomingLikeReplyShare {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(IncomingLikeReplyShareVisitor)
    }
}

impl scylla::serialize::row::SerializeRow for IncomingLikeReplyShareRef<'_> {
    fn serialize(
        &self,
        ctx: &scylla::serialize::row::RowSerializationContext<'_>,
        writer: &mut scylla::serialize::writers::RowWriter,
    ) -> std::result::Result<(), SerializationError> {
        let tuple = match self {
            IncomingLikeReplyShareRef::Like(like) => {
                let (sort, id): (i8, Uuid) = match like.in_reply_to {
                    InReply::Post(postid) => (0i8, postid.into()),
                    InReply::Reply(replyid) => (1i8, replyid.into()),
                    InReply::Share(shareid) => (2i8, shareid.into()),
                };
                (
                    0i8,
                    &like.user_id,
                    &like.received,
                    &like.ap_like_id,
                    Some(sort),
                    Some(id),
                    &Option::<Visibility>::None,
                    &Option::<&String>::None,
                )
            }
            IncomingLikeReplyShareRef::Reply(reply) => {
                let (sort, id): (Option<i8>, Option<Uuid>) = match reply.in_reply_to {
                    Some(InReply::Post(postid)) => (Some(0i8), Some(postid.into())),
                    Some(InReply::Reply(replyid)) => (Some(1i8), Some(replyid.into())),
                    Some(InReply::Share(shareid)) => (Some(2i8), Some(shareid.into())),
                    _ => (None, None),
                };
                (
                    1i8,
                    &reply.user_id,
                    &reply.received,
                    &reply.ap_reply_id,
                    sort,
                    id,
                    &Some(reply.visibility),
                    &Some(&reply.content),
                )
            }
            IncomingLikeReplyShareRef::Share(share) => {
                let (sort, id): (Option<i8>, Option<Uuid>) = match share.in_reply_to {
                    Some(InReply::Post(postid)) => (Some(0), Some(postid.into())),
                    Some(InReply::Reply(replyid)) => (Some(1), Some(replyid.into())),
                    Some(InReply::Share(shareid)) => (Some(2), Some(shareid.into())),
                    _ => (None, None),
                };
                (
                    2i8,
                    &share.user_id,
                    &share.received,
                    &share.ap_share_id,
                    sort,
                    id,
                    &Some(share.visibility),
                    &Some(&share.content),
                )
            }
        };
        scylla::serialize::row::SerializeRow::serialize(&tuple, ctx, writer)
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl<'frame, 'metadata> scylla::deserialize::row::DeserializeRow<'frame, 'metadata>
    for IncomingLikeReplyShare
{
    fn type_check(specs: &[ColumnSpec]) -> StdResult<(), TypeCheckError> {
        use scylla::deserialize::row::DeserializeRow;
        <(
            i8,
            UserId,
            DateTime<Utc>,
            Uuid,
            StorUrl,
            Option<Visibility>,
            Option<String>,
        ) as DeserializeRow>::type_check(specs)
    }

    fn deserialize(
        row: ColumnIterator<'frame, 'metadata>,
    ) -> StdResult<Self, DeserializationError> {
        use scylla::deserialize::row::DeserializeRow;
        let (sort, user_id, received, ap_id, in_reply_to_sort, in_reply_to, visibility, content) =
            <(
                i8,
                UserId,
                DateTime<Utc>,
                StorUrl,
                Option<i8>,
                Option<Uuid>,
                Option<Visibility>,
                Option<String>,
            ) as DeserializeRow>::deserialize(row)?;
        let in_reply_to = match (in_reply_to_sort, in_reply_to) {
            (None, None) => None,
            (Some(sort), Some(id)) => match sort {
                0 => Some(InReply::Post(PostId::from_uuid(id))),
                1 => Some(InReply::Reply(ReplyId::from_uuid(id))),
                2 => Some(InReply::Share(ShareId::from_uuid(id))),
                tag => {
                    return Err(mk_de_err(InvalidTagSnafu { tag }.build()));
                }
            },
            (_, _) => {
                return Err(mk_de_err(BrokenInReplySnafu.build()));
            }
        };
        match sort {
            0 => Ok(IncomingLikeReplyShare::Like(IncomingLike {
                user_id,
                received,
                ap_like_id: ap_id,
                in_reply_to: in_reply_to.ok_or(mk_de_err(
                    MissingFieldSnafu {
                        field: "in_reply_to",
                    }
                    .build(),
                ))?,
            })),
            1 => Ok(IncomingLikeReplyShare::Reply(IncomingReply {
                user_id,
                received,
                ap_reply_id: ap_id,
                in_reply_to,
                visibility: visibility.ok_or(mk_de_err(
                    MissingFieldSnafu {
                        field: "visibility",
                    }
                    .build(),
                ))?,
                content: content
                    .ok_or(mk_de_err(MissingFieldSnafu { field: "content" }.build()))?,
            })),
            2 => Ok(IncomingLikeReplyShare::Share(IncomingShare {
                user_id,
                received,
                ap_share_id: ap_id,
                in_reply_to,
                visibility: visibility.ok_or(mk_de_err(
                    MissingFieldSnafu {
                        field: "visibility",
                    }
                    .build(),
                ))?,
                content: content
                    .ok_or(mk_de_err(MissingFieldSnafu { field: "content" }.build()))?,
            })),
            tag => Err(mk_de_err(InvalidTagSnafu { tag }.build())),
        }
    }
}
