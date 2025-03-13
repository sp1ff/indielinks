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

use std::{collections::HashSet, fmt::Display, ops::Deref, str::FromStr};

use argon2::{Algorithm, Argon2, Params, PasswordHash, PasswordHasher, PasswordVerifier, Version};
use axum::http::Uri;
use chrono::{DateTime, NaiveDate, Utc};
use email_address::EmailAddress;
use lazy_static::lazy_static;
use password_hash::{rand_core::OsRng, PasswordHashString, SaltString};
use picky::key::{PrivateKey, PublicKey};
use pkcs8::{spki, EncodePrivateKey};
use regex::Regex;
use scylla::{
    deserialize::{DeserializationError, DeserializeValue, FrameSlice, TypeCheckError},
    frame::response::result::ColumnType,
    serialize::{
        value::SerializeValue,
        writers::{CellWriter, WrittenCellProof},
        SerializationError,
    },
    DeserializeRow,
};
use secrecy::{ExposeSecret, SecretSlice, SecretString};
use serde::{Deserialize, Deserializer, Serialize};
use snafu::{prelude::*, Backtrace, IntoError};
use tap::{conv::Conv, pipe::Pipe};
use tracing::debug;
use unicode_segmentation::UnicodeSegmentation;
use url::Url;
use uuid::Uuid;
use zxcvbn::{feedback::Feedback, zxcvbn, Score};

use crate::peppers::{self, Pepper, Peppers, Version as PepperVersion};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid API key"))]
    BadApiKey { backtrace: Backtrace },
    #[snafu(display("{email} is not a valid e-mail address"))]
    BadEmail { email: String, backtrace: Backtrace },
    #[snafu(display("Incorrect password"))]
    BadPassword { backtrace: Backtrace },
    #[snafu(display("{text} is not a valid `day` for a post"))]
    BadPostDay { text: String, backtrace: Backtrace },
    #[snafu(display("{text} is not a valid tag name"))]
    BadTagname { text: String, backtrace: Backtrace },
    #[snafu(display("{name} is not a valid indielinks username"))]
    BadUsername { name: String },
    CheckPassword {
        username: Username,
        source: password_hash::errors::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("{col_name} expected type {expected:?}; got {actual:?}"))]
    ColumnTypeMismatch {
        col_name: String,
        actual: ColumnType<'static>,
        expected: ColumnType<'static>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to import an RSA key from PKCS8 format: {source}"))]
    FromPkcs8 {
        source: picky::key::KeyError,
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
    #[snafu(display("Can't deserialize a {typ} from a null frame slice"))]
    NoFrameSlice { typ: String, backtrace: Backtrace },
    #[snafu(display("No pepper found for user {username}: {source}"))]
    NoPepper {
        username: Username,
        source: peppers::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Password doesn't have enough entropy: {feedback}"))]
    PasswordEntropy {
        feedback: Feedback,
        backtrace: Backtrace,
    },
    #[snafu(display("Passwords may not begin or end in whitespace"))]
    PasswordWhitespace { backtrace: Backtrace },
    #[snafu(display("Failed to obtain public key in PEM format; {source}"))]
    Pem {
        source: picky::key::KeyError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to export an RSA private key to PKCS8 DER format; {source}"))]
    Pkcs8Der {
        source: pkcs8::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to export an RSA public key to DER format: {source}"))]
    PublicKeyDer {
        source: spki::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to generate an RSA private key: {source}"))]
    RsaPrivateKeyGen {
        source: rsa::errors::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to parse {text} as an URL: {source}"))]
    UserUrl {
        text: String,
        source: url::ParseError,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

// We start with a series of newtype structs to both refine native types & to allow me to work
// around Rust's orphaned trait rules and implement traits not defined by this crate
// (`DeserializeValue`, `SerializeValue`) on types not defined in this crate (`PublicKey`,
// `PrivateKey` and so on). It's all boilerplate; nothing terribly complex, but it *is* tedious. I
// wonder if I'm missing some handy crate that provides macros for this...

macro_rules! type_check {
    ($var_name:ident, $column_type:ident, $err_type:ty, $column_name:expr) => {
        ($var_name == &ColumnType::$column_type)
            .then_some(())
            .ok_or(<$err_type>::new(
                ColumnTypeMismatchSnafu {
                    col_name: $column_name.to_owned(),
                    actual: $var_name.clone().into_owned(),
                    expected: ColumnType::$column_type,
                }
                .build(),
            ))
    };
}

fn mk_de_err(err: impl std::error::Error + Send + Sync + 'static) -> DeserializationError {
    DeserializationError::new(err)
}

fn mk_ser_err(err: impl std::error::Error + Send + Sync + 'static) -> SerializationError {
    SerializationError::new(err)
}

fn mk_serde_de_err<'de, D: serde::Deserializer<'de>>(err: impl std::error::Error) -> D::Error {
    <D::Error as serde::de::Error>::custom(format!("{:?}", err))
}

fn mk_serde_ser_err<S: serde::Serializer>(err: impl std::error::Error) -> S::Error {
    <S::Error as serde::ser::Error>::custom(format!("{:?}", err))
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          Identifiers                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// identifier!
///
/// # Introduction
///
/// Use this to declare a type intended to be used as an opaque identifier for some other sort of entity.
///
/// # Background
///
/// In a NoSQL world, we can't count on an auto-increment column in our tables to serve as an
/// opaque identifier. It is instead up to the application developer to assign their own ids.
/// Depending on the degree of integration with the database, I've seen implementations that assign
/// ranges of numeric identifiers to distinct nodes in the database (so as to avoid collisions), but
/// by far the most common approach is simply to move to a UUID (I guess this trades space for ease
/// of implementation).
///
/// I suppose I could have just used [Uuid] to represent this (wrapped in a newtype struct to
/// impement the ScyllaDB-related interfaces on it)... but I just couldn't bring myself to use the
/// same type to represent identifiers for users, tags and posts all at the same time.
///
/// This macro will define a newtype struct wrapping [Uuid] implementing the traits [Display],
/// [DeserializeValue] and [SerializeValue]. I thought to use a type alias, but those don't
/// work very well with newtype structs (in particular, you can't access the type's constructor
/// through the alias (not sure why)).
///
/// As an aside, [Display] will format the uuid in the form of an URN with namespace identifier
/// given by the corresponding macro argument.
macro_rules! define_id {
    ($type_name:ident, $nid:expr) => {
        #[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
        #[serde(transparent)]
        pub struct $type_name(Uuid);
        impl $type_name {
            pub fn new() -> $type_name {
                $type_name(Uuid::new_v4())
            }
            pub fn from_raw_string(s: &str) -> StdResult<$type_name, uuid::Error> {
                Ok($type_name(Uuid::parse_str(s)?))
            }
            // This seems like a landmine; I should probably just store strings in the same way I
            // display them?
            pub fn to_raw_string(&self) -> String {
                format!("{}", self.0.as_simple())
            }
        }
        impl Default for $type_name {
            fn default() -> Self {
                Self::new()
            }
        }
        impl Display for $type_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                // I'm not thrilled with this-- I'd prefer to represent it as an URN for display
                // purposes. Thing is, this is the format used by `serde-dynamo`, so I want to be
                // consistent with that.
                write!(f, "{}", self.0.as_hyphenated())
            }
        }
        // Arggghhhh... the derive macro doesn't work with newtype structs.
        impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for $type_name {
            fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
                Uuid::type_check(typ)
            }
            fn deserialize(
                typ: &'metadata ColumnType<'metadata>,
                v: Option<FrameSlice<'frame>>,
            ) -> StdResult<Self, DeserializationError> {
                Ok(Self(<Uuid as DeserializeValue>::deserialize(typ, v)?))
            }
        }

        // Again, the derive macro doesn't work with newtype structs.
        impl SerializeValue for $type_name {
            fn serialize<'b>(
                &self,
                typ: &ColumnType<'_>,
                writer: CellWriter<'b>,
            ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
                SerializeValue::serialize(&self.0, typ, writer)
            }
        }
    };
}

define_id!(UserId, "userid");
// I had, in the past, defined a few other identifiers, making it worth it to wrap the boilerplate
// up in a macro. Now that it's just `UserId`, I should probably go back to just implementing it by
// hand.

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            Username                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

// indielinks usernames must be ASCII, may be from five to sixty-four chacacters in length, and
// must match the regex "^[a-zA-Z][-_.a-zA-Z0-9]+$".
const MIN_USERNAME_LENGTH: usize = 5;
const MAX_USERNAME_LENGTH: usize = 64;

lazy_static! {
    static ref USERNAME: Regex = Regex::new("^[a-zA-Z][-_.a-zA-Z0-9]+$").unwrap(/* known good */);
    static ref BANNED_USERNAMES: HashSet<&'static str> = HashSet::from(["login", "signup", "mint-key"]);
}

fn check_username(s: &str) -> bool {
    s.is_ascii()
        && s.len() >= MIN_USERNAME_LENGTH
        && s.len() <= MAX_USERNAME_LENGTH
        && USERNAME.is_match(s)
        && (!BANNED_USERNAMES.contains(s))
}

/// A refined type representing an indielinks username
// Boy... writing refined types in Rust involves a *lot* of boilerplate. I have to wonder if there
// isn't a better way...
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Username(String);

impl Username {
    /// Construct a [Username] from a `&str`
    ///
    /// indielinks usernames must be ASCII, may be from six to sixty-four chacacters in length, and
    /// must match the regex "^[a-zA-Z][-_.a-zA-Z0-9]+$". Use this constructor to create a [Username] instance
    /// by copying from a reference to [str]. To *move* a [String] into a [Username] (with validity checking)
    /// use [TryFrom::try_from()]
    pub fn new(name: &str) -> Result<Username> {
        check_username(name)
            .then_some(Username(name.to_owned()))
            .ok_or(
                BadUsernameSnafu {
                    name: name.to_owned(),
                }
                .build(),
            )
    }
}

impl AsRef<str> for Username {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl Deref for Username {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Implement `Deserialize` by hand to fail if the serialized value isn't a legit `Username`
impl<'de> Deserialize<'de> for Username {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        Username::try_from(s).map_err(mk_serde_de_err::<'de, D>)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for Username {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        String::type_check(typ)
    }
    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        Username::try_from(<String as DeserializeValue>::deserialize(typ, v)?).map_err(mk_de_err)
    }
}

impl Display for Username {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Username {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Username::new(s)
    }
}

impl SerializeValue for Username {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&self.0, typ, writer)
    }
}

impl TryFrom<String> for Username {
    type Error = Error;

    fn try_from(name: String) -> std::result::Result<Self, Self::Error> {
        if check_username(&name) {
            Ok(Username(name))
        } else {
            BadUsernameSnafu { name }.fail()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           UserEmail                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A refiend type representing an e-mail address
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct UserEmail(String);

impl UserEmail {
    pub fn new(email: &str) -> Result<UserEmail> {
        EmailAddress::is_valid(email)
            .then_some(UserEmail(email.to_string()))
            .context(BadEmailSnafu {
                email: email.to_string(),
            })
    }
}

impl AsRef<str> for UserEmail {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl Deref for UserEmail {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Implement `Deserialize` by hand to fail if the serialized value isn't a legit `Username`
impl<'de> Deserialize<'de> for UserEmail {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        UserEmail::try_from(s).map_err(mk_serde_de_err::<'de, D>)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for UserEmail {
    fn type_check(typ: &ColumnType) -> std::result::Result<(), TypeCheckError> {
        String::type_check(typ)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> std::result::Result<Self, DeserializationError> {
        UserEmail::try_from(<String as DeserializeValue>::deserialize(typ, v)?).map_err(mk_de_err)
    }
}

impl Display for UserEmail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for UserEmail {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        UserEmail::new(s)
    }
}

impl SerializeValue for UserEmail {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> std::result::Result<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&self.0, typ, writer)
    }
}

impl TryFrom<String> for UserEmail {
    type Error = Error;

    fn try_from(email: String) -> std::result::Result<Self, Self::Error> {
        if EmailAddress::is_valid(&email) {
            Ok(UserEmail(email))
        } else {
            BadEmailSnafu { email }.fail()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         UserPublicKey                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Newtype idiom to work around Rust's orphaned trait rule
// `UserPublicKey` isn't a refined type; it's just a wrapper on which to hang trait implementations
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(transparent)]
pub struct UserPublicKey(#[serde(with = "serde_publickey")] PublicKey);

impl UserPublicKey {
    pub fn to_pem(&self) -> Result<String> {
        self.0.to_pem_str().context(PemSnafu)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for UserPublicKey {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        type_check!(typ, Ascii, TypeCheckError, "PublicKey")
    }
    fn deserialize(
        _: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        v.ok_or(
            NoFrameSliceSnafu {
                typ: "PublicKey".to_owned(),
            }
            .build(),
        )
        .map_err(mk_de_err)?
        .as_slice()
        .pipe(std::str::from_utf8)
        .map_err(mk_de_err)?
        .pipe(PublicKey::from_pem_str)
        .map_err(mk_de_err)?
        .pipe(UserPublicKey)
        .pipe(Ok)
    }
}

impl SerializeValue for UserPublicKey {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        type_check!(typ, Ascii, SerializationError, "PublicKey")?;
        self.0
            .to_pem_str()
            .map_err(mk_ser_err)?
            .as_bytes()
            .pipe(|x| writer.set_value(x))
            .map_err(mk_ser_err)?
            .pipe(Ok)
    }
}

mod serde_publickey {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(pub_key: &PublicKey, ser: S) -> StdResult<S::Ok, S::Error> {
        pub_key
            .to_pem_str()
            .map_err(mk_serde_ser_err::<S>)?
            .pipe(|s| <String as serde::Serialize>::serialize(&s, ser))
    }

    pub fn deserialize<'de, D>(de: D) -> StdResult<PublicKey, D::Error>
    where
        D: Deserializer<'de>,
    {
        <String as serde::Deserialize>::deserialize(de)
            .map_err(mk_serde_de_err::<'de, D>)?
            .pipe_ref(|s| PublicKey::from_pem_str(s))
            .map_err(mk_serde_de_err::<'de, D>)?
            .pipe(Ok)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         UserPrivateKey                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Newtype idiom to work around Rust's orphaned trait rule
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(transparent)]
// `UserPrivateKey` isn't a refined type; it's just a wrapper on which to hang trait implementations
pub struct UserPrivateKey(#[serde(with = "serde_privatekey")] PrivateKey);

impl AsRef<PrivateKey> for UserPrivateKey {
    fn as_ref(&self) -> &PrivateKey {
        &self.0
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for UserPrivateKey {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        type_check!(typ, Ascii, TypeCheckError, "PrivateKey")
    }
    fn deserialize(
        _: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        v.ok_or(
            NoFrameSliceSnafu {
                typ: "PrivateKey".to_owned(),
            }
            .build(),
        )
        .map_err(mk_de_err)?
        .as_slice()
        .pipe(std::str::from_utf8)
        .map_err(mk_de_err)?
        .pipe(PrivateKey::from_pem_str)
        .map_err(mk_de_err)?
        .pipe(UserPrivateKey)
        .pipe(Ok)
    }
}

impl SerializeValue for UserPrivateKey {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        type_check!(typ, Ascii, SerializationError, "PrivateKey")?;
        self.0
            .to_pem_str()
            .map_err(mk_ser_err)?
            .as_bytes()
            .pipe(|x| writer.set_value(x))
            .map_err(mk_ser_err)?
            .pipe(Ok)
    }
}

mod serde_privatekey {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(pub_key: &PrivateKey, ser: S) -> StdResult<S::Ok, S::Error> {
        pub_key
            .to_pem_str()
            .map_err(mk_serde_ser_err::<S>)?
            .pipe(|s| <String as serde::Serialize>::serialize(&s, ser))
    }

    pub fn deserialize<'de, D>(de: D) -> StdResult<PrivateKey, D::Error>
    where
        D: Deserializer<'de>,
    {
        <String as serde::Deserialize>::deserialize(de)
            .map_err(mk_serde_de_err::<'de, D>)?
            .pipe_ref(|s| PrivateKey::from_pem_str(s))
            .map_err(mk_serde_de_err::<'de, D>)?
            .pipe(Ok)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           UserApiKey                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Newtype idiom to work around Rust's orphaned trait rule
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(transparent)]
pub struct UserApiKey(#[serde(with = "serde_apikey")] SecretSlice<u8>);

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for UserApiKey {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        type_check!(typ, Blob, TypeCheckError, "ApiKey")
    }
    fn deserialize(
        _: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        v.ok_or(
            NoFrameSliceSnafu {
                typ: "ApiKey".to_owned(),
            }
            .build(),
        )
        .map_err(mk_de_err)?
        .as_slice()
        .conv::<Vec<u8>>()
        .conv::<SecretSlice<u8>>()
        .pipe(UserApiKey)
        .pipe(Ok)
    }
}

impl SerializeValue for UserApiKey {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        use secrecy::ExposeSecret;
        type_check!(typ, Blob, SerializationError, "ApiKey")?;
        self.0
            .expose_secret()
            .pipe(|x| writer.set_value(x))
            .map_err(mk_ser_err)?
            .pipe(Ok)
    }
}

mod serde_apikey {
    use super::*;
    use serde::{Deserializer, Serializer};
    use serde_bytes::ByteBuf;

    pub fn serialize<S: Serializer>(
        api_key: &SecretSlice<u8>,
        ser: S,
    ) -> StdResult<S::Ok, S::Error> {
        use secrecy::ExposeSecret;
        <ByteBuf as serde::Serialize>::serialize(&ByteBuf::from(api_key.expose_secret()), ser)
    }

    pub fn deserialize<'de, D>(de: D) -> StdResult<SecretSlice<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        <ByteBuf as serde::Deserialize>::deserialize(de)
            .map_err(mk_serde_de_err::<'de, D>)?
            .pipe(|x| x.into_vec())
            .conv::<SecretSlice<u8>>()
            .pipe(Ok)
    }
}

// I had originally intended to just implement serde for this time, but wound-up needing a few more
// traits
impl From<Vec<u8>> for UserApiKey {
    fn from(value: Vec<u8>) -> Self {
        UserApiKey(value.into())
    }
}

impl PartialEq for UserApiKey {
    fn eq(&self, other: &Self) -> bool {
        use secrecy::ExposeSecret;
        self.0.expose_secret().eq(other.0.expose_secret())
    }
}

impl secrecy::SerializableSecret for UserApiKey {}

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
//                                            UserUrl                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Newtype to work around Rust's orphaned traits rule
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct UserUrl(Url);

// Implement `Deserialize` by hand to fail if the serialized value isn't a legit URL
impl<'de> Deserialize<'de> for UserUrl {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        UserUrl::try_from(s).map_err(mk_serde_de_err::<'de, D>)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for UserUrl {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        String::type_check(typ)
    }
    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        UserUrl::try_from(<String as DeserializeValue>::deserialize(typ, v)?).map_err(mk_de_err)
    }
}

impl SerializeValue for UserUrl {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&self.0.as_str(), typ, writer)
    }
}

impl Deref for UserUrl {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl AsRef<str> for UserUrl {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl From<Url> for UserUrl {
    fn from(value: Url) -> Self {
        Self(value)
    }
}

impl From<&Url> for UserUrl {
    fn from(value: &Url) -> Self {
        Self(value.clone())
    }
}

impl TryFrom<String> for UserUrl {
    type Error = Error;

    fn try_from(s: String) -> std::result::Result<Self, Self::Error> {
        Ok(UserUrl(Url::parse(&s).context(UserUrlSnafu { text: s })?))
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
    api_key: Option<UserApiKey>,
    // Will be null until the first post
    first_update: Option<DateTime<Utc>>,
    // Will be null until the first post
    last_update: Option<DateTime<Utc>>,
    password_hash: UserHashString,
    pepper_version: PepperVersion,
    #[serde(default)]
    followers: HashSet<UserUrl>,
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

/// Generate a 2048-bit RSA keypair
fn generate_rsa_keypair() -> Result<(UserPublicKey, UserPrivateKey)> {
    use pkcs8::EncodePublicKey;

    let mut rng = rand::thread_rng();
    let rsa_priv_key = rsa::RsaPrivateKey::new(&mut rng, 2048).context(RsaPrivateKeyGenSnafu)?;
    let rsa_pub_key = rsa::RsaPublicKey::from(&rsa_priv_key);

    Ok((
        UserPublicKey(
            PublicKey::from_der(
                rsa_pub_key
                    .to_public_key_der()
                    .context(PublicKeyDerSnafu)?
                    .as_bytes(),
            )
            .context(FromPkcs8Snafu)?,
        ),
        UserPrivateKey(
            PrivateKey::from_pkcs8(
                rsa_priv_key
                    .to_pkcs8_der()
                    .context(Pkcs8DerSnafu)?
                    .as_bytes(),
            )
            .context(FromPkcs8Snafu)?,
        ),
    ))
}

impl User {
    /// Validate an API key
    pub fn check_key(&self, key: &UserApiKey) -> Result<()> {
        use secrecy::ExposeSecret;
        match self
            .api_key
            .as_ref()
            .map(|k| k.0.expose_secret() == key.0.expose_secret())
        {
            Some(true) => Ok(()),
            _ => BadApiKeySnafu.fail(),
        }
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
    pub fn date_of_last_post(&self) -> Option<DateTime<Utc>> {
        self.last_update
    }
    pub fn discoverable(&self) -> bool {
        self.discoverable
    }
    pub fn display_name(&self) -> String {
        self.display_name.clone()
    }
    pub fn first_update(&self) -> Option<DateTime<Utc>> {
        self.first_update
    }
    pub fn followers(&self) -> &HashSet<UserUrl> {
        &self.followers
    }
    pub fn hash(&self) -> UserHashString {
        self.password_hash.clone()
    }
    pub fn id(&self) -> UserId {
        self.id
    }
    pub fn last_update(&self) -> Option<DateTime<Utc>> {
        self.last_update
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
        discoverable: &Option<bool>,
        display_name: &Option<String>,
        summary: &Option<String>,
    ) -> Result<User> {
        validate_password(password, &[username.as_ref(), email.as_ref()])?;
        let (pub_key, priv_key) = generate_rsa_keypair()?;
        let password_hash = User::hash_password(pepper_key, password)?;
        Ok(User {
            id: UserId::new(),
            username: username.clone(),
            discoverable: discoverable.unwrap_or(true),
            display_name: display_name.clone().unwrap_or(username.to_string()),
            summary: summary.clone().unwrap_or("".to_string()),
            pub_key_pem: pub_key,
            priv_key_pem: priv_key,
            api_key: None,
            first_update: None,
            last_update: None,
            password_hash: UserHashString(password_hash),
            pepper_version: pepper_version.clone(),
            followers: HashSet::new(),
        })
    }
    pub fn pepper_version(&self) -> PepperVersion {
        self.pepper_version.clone()
    }
    pub fn priv_key(&self) -> UserPrivateKey {
        self.priv_key_pem.clone()
    }
    pub fn pub_key(&self) -> UserPublicKey {
        self.pub_key_pem.clone()
    }
    pub fn summary(&self) -> String {
        self.summary.clone()
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
    fn create_password_hasher(pepper: &Pepper) -> Result<Argon2> {
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
//                                            TagName                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

const MAX_TAGNAME_LENGTH: usize = 255;

fn check_tagname(s: &str) -> bool {
    [
        !s.is_empty(),
        UnicodeSegmentation::graphemes(s, true).count() <= MAX_TAGNAME_LENGTH,
        !s.contains(char::is_whitespace),
        !s.contains(','),
    ]
    .into_iter()
    // Ack! Does Rust really not offer the identity operator?
    .all(|x| x)
}

/// Following Pinboard conventions, tags may be up to 255 characters in length; the Pinboard [docs]
/// say "All entities are encoded as UTF-8. In the length limits below, 'characters' means logical
/// characters rather than bytes." I'm not sure what is meant by "logical characters", but I'm going
/// to use Unicode graphemes. Tags may not include whitespace. AFAICT, del.icio.us tags could
/// contain commas, but Pinboard disallows them, so I will, too.
///
/// [docs]: https://pinboard.in/api/
///
/// Pinboard also provides a feature of "private tags": if the tag begins with an ASCII period, it
/// is considered private. I model that here, although I haven't decided how to deal with it more
/// generally.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Tagname(String);

impl Tagname {
    /// Correct-by-construction [Tagname] constructor
    pub fn new(text: &str) -> Result<Tagname> {
        check_tagname(text)
            .then_some(Tagname(text.to_string()))
            .ok_or(
                BadTagnameSnafu {
                    text: text.to_string(),
                }
                .build(),
            )
    }
    pub fn private(&self) -> bool {
        self.0.as_bytes()[0] == b'.' // Index is safe
    }
}
impl AsRef<str> for Tagname {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl Deref for Tagname {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Implement `Deserialize` by hand to fail if the serialized value isn't a legit `Tagname`
impl<'de> Deserialize<'de> for Tagname {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        Tagname::try_from(s).map_err(mk_serde_de_err::<'de, D>)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for Tagname {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        String::type_check(typ)
    }
    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        Tagname::try_from(<String as DeserializeValue>::deserialize(typ, v)?).map_err(mk_de_err)
    }
}

impl std::fmt::Display for Tagname {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Tagname {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Tagname::new(s)
    }
}

impl SerializeValue for Tagname {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&self.0, typ, writer)
    }
}

impl TryFrom<String> for Tagname {
    type Error = Error;

    fn try_from(name: String) -> std::result::Result<Self, Self::Error> {
        if check_tagname(&name) {
            Ok(Tagname(name))
        } else {
            BadTagnameSnafu { text: name }.fail()
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn tagname() {
        assert!(Tagname::new("").is_err());
        assert!(Tagname::new("我翻开日记，第一行便写着：“今天晚饭以后，我就无缘无故地觉得有些怕人。”我不知道怕在哪里，可是既然怕人，便躲到屋里，不敢见谁。我越想越觉得奇怪，这里的人，同住了这么多年，他们的脸色本来并不怎样可怕，但近来却时时都换了模样似的，连那小孩子也诡秘地望着我。我想，我自己总没有得罪他们罢？因为从前是不怕的。可是一想到他们那样的眼色，心里就慌起来，似乎随时都要露出牙齿，向我扑来 我便心跳得更厉害，走到门口，看看天色还早，却又不敢出门，生怕有人看见我。于是，只好坐下来，怀疑四壁的缝隙里也藏着窥伺的眼睛。我拿起一本书想看看，却又不知翻到哪里好，觉得每一页上，都仿佛写着“吃人”两个字，阴森森地直扑到我眼前，让我毛骨悚然。").is_err());
        assert!(Tagname::new("foo bar").is_err());
        assert!(Tagname::new("foo,bar").is_err());
        assert!(Tagname::new("aws").is_ok());
        assert!(Tagname::new("我不知道怕在哪里").is_ok());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            PostUri                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PostUri(#[serde(with = "serde_uri")] Uri);

impl std::convert::From<Uri> for PostUri {
    fn from(value: Uri) -> Self {
        PostUri(value)
    }
}

impl Display for PostUri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Ugh-- need shims to call-out to the http-serde implementations
pub mod serde_uri {
    use super::Uri;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(uri: &Uri, ser: S) -> Result<S::Ok, S::Error> {
        http_serde::uri::serialize(uri, ser)
    }

    pub fn deserialize<'de, D>(de: D) -> Result<Uri, D::Error>
    where
        D: Deserializer<'de>,
    {
        http_serde::uri::deserialize(de)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for PostUri {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        type_check!(typ, Ascii, TypeCheckError, "PostUri")
    }
    fn deserialize(
        _: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        v.ok_or(
            NoFrameSliceSnafu {
                typ: "PostUri".to_owned(),
            }
            .build(),
        )
        .map_err(mk_de_err)?
        .as_slice()
        .pipe(std::str::from_utf8)
        .map_err(mk_de_err)?
        .pipe(Uri::from_str)
        .map_err(mk_de_err)?
        .pipe(PostUri)
        .pipe(Ok)
    }
}

impl SerializeValue for PostUri {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        type_check!(typ, Ascii, SerializationError, "PostUri")?;
        format!("{}", self.0)
            .as_bytes()
            .pipe(|x| writer.set_value(x))
            .map_err(mk_ser_err)?
            .pipe(Ok)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            PostDay                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

// It might be worth giving the `refined` crate a look: <https://github.com/tomoikey/refined_type>.
// My concern is that in order to implement things like `SerializeValue` & `DeserializeValue`, I'll
// have to wrap the refined type in a newtype. That might be fine, but for now I'd prefer to do that
// in a separate patch.

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct PostDay(String);

lazy_static! {
    static ref POST_DAY: Regex = Regex::new("^[0-9]{4}-[0-9]{2}-[0-9]{2}$").unwrap(/* known good */);
}

fn check_postday(s: &str) -> bool {
    POST_DAY.is_match(s)
}

impl PostDay {
    pub fn new(s: &str) -> Result<PostDay> {
        if check_postday(s) {
            Ok(PostDay(s.to_string()))
        } else {
            Err(BadPostDaySnafu {
                text: s.to_string(),
            }
            .build())
        }
    }
}

impl AsRef<str> for PostDay {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl Deref for PostDay {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Implement `Deserialize` by hand to fail if the serialized value isn't a legit `PostDay`
impl<'de> Deserialize<'de> for PostDay {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        PostDay::try_from(s).map_err(mk_serde_de_err::<'de, D>)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for PostDay {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        type_check!(typ, Ascii, TypeCheckError, "PostDay")
    }
    fn deserialize(
        _: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        v.ok_or(
            NoFrameSliceSnafu {
                typ: "PostDay".to_owned(),
            }
            .build(),
        )
        .map_err(mk_de_err)?
        .as_slice()
        .pipe(std::str::from_utf8)
        .map_err(mk_de_err)?
        .pipe(PostDay::new)
        .map_err(mk_de_err)?
        .pipe(Ok)
    }
}

impl std::fmt::Display for PostDay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::convert::From<DateTime<Utc>> for PostDay {
    fn from(value: DateTime<Utc>) -> Self {
        PostDay(value.format("%Y-%m-%d").to_string())
    }
}

impl std::convert::From<&DateTime<Utc>> for PostDay {
    fn from(value: &DateTime<Utc>) -> Self {
        PostDay(value.format("%Y-%m-%d").to_string())
    }
}

impl std::convert::From<NaiveDate> for PostDay {
    fn from(value: NaiveDate) -> Self {
        PostDay(value.format("%Y-%m-%d").to_string())
    }
}

impl std::convert::From<&NaiveDate> for PostDay {
    fn from(value: &NaiveDate) -> Self {
        PostDay(value.format("%Y-%m-%d").to_string())
    }
}

impl SerializeValue for PostDay {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        type_check!(typ, Ascii, SerializationError, "PostDay")?;
        self.0
            .to_string()
            .as_bytes()
            .pipe(|x| writer.set_value(x))
            .map_err(mk_ser_err)?
            .pipe(Ok)
    }
}

impl TryFrom<String> for PostDay {
    type Error = Error;

    fn try_from(text: String) -> std::result::Result<Self, Self::Error> {
        if check_postday(&text) {
            Ok(PostDay(text))
        } else {
            BadPostDaySnafu { text }.fail()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                              Post                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents an indielinks post
#[derive(Clone, Debug, Deserialize, DeserializeRow, Eq, PartialEq, Serialize)]
pub struct Post {
    url: PostUri,
    user_id: UserId,
    posted: DateTime<Utc>,
    day: PostDay,
    title: String,
    notes: Option<String>,
    tags: HashSet<Tagname>,
    public: bool,
    unread: bool,
}

impl Post {
    pub fn delete_tag(&mut self, tag: &Tagname) {
        self.tags.remove(tag);
    }
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        url: &PostUri,
        user_id: &UserId,
        posted: &DateTime<Utc>,
        day: &PostDay,
        title: &str,
        notes: &Option<String>,
        tags: &HashSet<Tagname>,
        public: bool,
        unread: bool,
    ) -> Post {
        Post {
            url: url.clone(),
            user_id: *user_id,
            posted: *posted,
            day: day.clone(),
            title: title.to_string(),
            notes: notes.clone(),
            tags: tags.clone(),
            public,
            unread,
        }
    }
    pub fn day(&self) -> PostDay {
        self.day.clone()
    }
    pub fn posted(&self) -> DateTime<Utc> {
        self.posted
    }
    pub fn rename_tag(&mut self, from: &Tagname, to: &Tagname) {
        if self.tags.remove(from) {
            self.tags.insert(to.clone());
        }
    }
    pub fn tags(&self) -> HashSet<Tagname> {
        self.tags.clone()
    }
    pub fn url(&self) -> PostUri {
        self.url.clone()
    }
    pub fn user_id(&self) -> UserId {
        self.user_id
    }
}
