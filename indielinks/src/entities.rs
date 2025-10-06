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
//! foundational.

use std::{fmt::Display, str::FromStr};

use argon2::{Algorithm, Argon2, Params, PasswordHash, PasswordHasher, PasswordVerifier, Version};
use chrono::{DateTime, Utc};
use password_hash::{rand_core::OsRng, PasswordHashString, SaltString};
use scylla::{
    deserialize::{value::DeserializeValue, DeserializationError, FrameSlice, TypeCheckError},
    frame::response::result::ColumnType,
    serialize::{
        value::SerializeValue,
        writers::{CellWriter, WrittenCellProof},
        SerializationError,
    },
    DeserializeRow, SerializeRow,
};
use secrecy::{ExposeSecret, SecretBox, SecretSlice, SecretString};
use serde::{Deserialize, Deserializer, Serialize};
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
        generate_rsa_keypair, Post, StorUrl, UserEmail, UserId, UserPrivateKey, UserPublicKey,
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
    #[snafu(display("While generating the user's keypair, {source}"))]
    Keypair {
        source: indielinks_shared::entities::Error,
    },
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
    #[snafu(display("Attempted to deserialize an invalid value for Visibility: {n}"))]
    VisibilityDe { n: i8, backtrace: Backtrace },
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
    <D::Error as serde::de::Error>::custom(format!("{:?}", err))
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

/// Visibility levels for assorted messages
// I pulled this ontology from <https://seb.jambor.dev/posts/understanding-activitypub/>; I'm not
// sure if this is a general ActivityPub thing, or Mastodon-specific.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[repr(i8)]
pub enum Visibility {
    Public = 0,
    Unlisted = 1,
    Followers = 2,
    DirectMessage = 3,
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
            n => Err(DeserializationError::new(VisibilityDeSnafu { n }.build())),
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

#[derive(
    Clone, Debug, Deserialize, DeserializeRow, Eq, Hash, PartialEq, Serialize, SerializeRow,
)]
pub struct Like {
    user_id: UserId,
    url: StorUrl,
    id: LikeId,
    created: DateTime<Utc>,
    like_id: StorUrl,
}

impl Like {
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }
    pub fn url(&self) -> &StorUrl {
        &self.url
    }
    pub fn id(&self) -> &LikeId {
        &self.id
    }
    pub fn created(&self) -> &DateTime<Utc> {
        &self.created
    }
    pub fn like_id(&self) -> &StorUrl {
        &self.like_id
    }
    pub fn from_parts(user_id: impl Into<UserId>, post: &Post, like_id: &Url) -> Like {
        Like {
            user_id: user_id.into(),
            url: post.url().clone(),
            id: LikeId::default(),
            created: Utc::now(),
            like_id: like_id.into(),
        }
    }
}

#[derive(
    Clone, Debug, Deserialize, DeserializeRow, Eq, Hash, PartialEq, Serialize, SerializeRow,
)]
pub struct Reply {
    user_id: UserId,
    url: StorUrl,
    id: ReplyId,
    created: DateTime<Utc>,
    reply_id: StorUrl,
    visibility: Visibility,
}

impl Reply {
    pub fn new(
        user_id: impl Into<UserId>,
        post: &Post,
        reply_id: &Url,
        visibility: Visibility,
    ) -> Reply {
        Reply {
            user_id: user_id.into(),
            url: post.url().clone(),
            id: ReplyId::default(),
            created: Utc::now(),
            reply_id: reply_id.into(),
            visibility,
        }
    }
    pub fn url(&self) -> &url::Url {
        self.url.as_ref()
    }
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }
}

// Yes, yes... this is identical, at the time of this writing, to `Reply`. Perhaps I'll merge
// them, but I want to see how this develops.
#[derive(
    Clone, Debug, Deserialize, DeserializeRow, Eq, Hash, PartialEq, Serialize, SerializeRow,
)]
pub struct Share {
    user_id: UserId,
    url: StorUrl,
    id: ShareId,
    created: DateTime<Utc>,
    share_id: StorUrl,
    visibility: Visibility,
}

impl Share {
    pub fn new(
        user_id: impl Into<UserId>,
        post: &Post,
        share_id: &Url,
        visibility: Visibility,
    ) -> Share {
        Share {
            user_id: user_id.into(),
            url: post.url().clone(),
            id: ShareId::default(),
            created: Utc::now(),
            share_id: share_id.into(),
            visibility,
        }
    }
    pub fn url(&self) -> &url::Url {
        self.url.as_ref()
    }
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        ActivityPubPost                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, PartialOrd, Serialize)]
#[repr(i8)]
pub enum ActivityPubPostFlavor {
    Share,
    Reply,
    Mention,
    Post,
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for ActivityPubPostFlavor {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        i8::type_check(typ)
    }
    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        match <i8 as DeserializeValue>::deserialize(typ, v)? {
            0 => Ok(ActivityPubPostFlavor::Share),
            n => Err(DeserializationError::new(
                ActivityPubPostFlavorDeSnafu { n }.build(),
            )),
        }
    }
}

impl SerializeValue for ActivityPubPostFlavor {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&(*self as i8), typ, writer)
    }
}

/// Represents an external post in the ActivityPub sense of the term
#[derive(Clone, Debug, Deserialize, DeserializeRow, Eq, PartialEq, Serialize, SerializeRow)]
pub struct ActivityPubPost {
    user_id: UserId,
    post_id: StorUrl,
    posted: DateTime<Utc>,
    flavor: ActivityPubPostFlavor,
    visibility: Visibility,
}

impl ActivityPubPost {
    pub fn new(
        user_id: impl Into<UserId>,
        post_id: impl Into<StorUrl>,
        flavor: ActivityPubPostFlavor,
        visibility: Visibility,
    ) -> ActivityPubPost {
        ActivityPubPost {
            user_id: user_id.into(),
            post_id: post_id.into(),
            posted: Utc::now(),
            flavor,
            visibility,
        }
    }
}
