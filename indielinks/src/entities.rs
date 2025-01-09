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
// You should have received a copy of the GNU General Public License along with mpdpopm.  If not,
// see <http://www.gnu.org/licenses/>.

//! # indielinks models
//!
//! ## Introduction
//!
//! I hate these sort of "catch-all" modules named "models" or "entities", but these types are truly
//! foundational.

use std::{fmt::Display, str::FromStr};

use axum::http::Uri;
use chrono::{DateTime, Utc};
use picky::key::{PrivateKey, PublicKey};
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
use secrecy::SecretSlice;
use serde::{Deserialize, Serialize};
use snafu::{prelude::*, Backtrace};
use tap::{conv::Conv, pipe::Pipe};
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{name} is not a valid indielinks username"))]
    BadUsername { name: String },
    #[snafu(display("{col_name} expected type {expected:?}; got {actual:?}"))]
    ColumnTypeMismatch {
        col_name: String,
        actual: ColumnType<'static>,
        expected: ColumnType<'static>,
        backtrace: Backtrace,
    },
    #[snafu(display("Can't deserialize a {typ} from a null frame slice"))]
    NoFrameSlice { typ: String, backtrace: Backtrace },
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

fn mk_serde_de_err<'de, S: serde::Deserializer<'de>>(err: impl std::error::Error) -> S::Error {
    <S::Error as serde::de::Error>::custom(format!("{:?}", err))
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
                write!(f, "urn:{}:{}", $nid, self.0.as_simple())
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
define_id!(TagId, "tagid");
define_id!(PostId, "postid");

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            Username                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A refined type representing an indielinks username
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Username(String);

fn username_char(x: u8) -> bool {
    (x > 47 && x < 58) || (x > 64 && x < 91) || (x > 96 && x < 123) || x == 45 || x == 95 || x == 46
}

impl Username {
    /// Indielinks usernamnes consist of alphanumeric characters and '-', '_' & '.'
    pub fn new(name: &str) -> Result<Username> {
        name.as_bytes()
            .iter()
            .cloned()
            .all(username_char)
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
        &self.0
    }
}

impl FromStr for Username {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Username::new(s)
    }
}

impl Display for Username {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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
        Ok(Self(<String as DeserializeValue>::deserialize(typ, v)?))
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

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         UserPublicKey                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Newtype idiom to work around Rust's orphaned trait rule
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(transparent)]
pub struct UserPublicKey(#[serde(with = "serde_publickey")] PublicKey);

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
            .to_pem()
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
            .to_pem()
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
pub struct UserPrivateKey(#[serde(with = "serde_privatekey")] PrivateKey);

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
            .to_pem()
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
            .to_pem()
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

impl From<Vec<u8>> for UserApiKey {
    fn from(value: Vec<u8>) -> Self {
        UserApiKey(value.into())
    }
}

impl secrecy::SerializableSecret for UserApiKey {}

impl PartialEq for UserApiKey {
    fn eq(&self, other: &Self) -> bool {
        use secrecy::ExposeSecret;
        self.0.expose_secret().eq(other.0.expose_secret())
    }
}

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

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                              User                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents an indielinks user
#[derive(Clone, Debug, Deserialize, DeserializeRow, PartialEq)]
pub struct User {
    id: UserId,
    username: Username,
    discoverable: bool,
    display_name: String,
    summary: String,
    pub_key_pem: UserPublicKey,
    priv_key_pem: UserPrivateKey,
    api_key: UserApiKey,
    // Will be null until the first post
    first_update: Option<DateTime<Utc>>,
    // Will be null until the first post
    last_update: Option<DateTime<Utc>>,
}

impl User {
    pub fn check_key(&self, key: &UserApiKey) -> bool {
        use secrecy::ExposeSecret;
        self.api_key.0.expose_secret() == key.0.expose_secret()
    }
    pub fn id(&self) -> UserId {
        self.id
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            PostUri                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct PostUri(Uri);

impl Display for PostUri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Ugh-- need shims to call-out to the http-serde implementations
pub mod serde_uri {
    use super::PostUri;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(uri: &PostUri, ser: S) -> Result<S::Ok, S::Error> {
        http_serde::uri::serialize(&uri.0, ser)
    }

    pub fn deserialize<'de, D>(de: D) -> Result<PostUri, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(PostUri(http_serde::uri::deserialize(de)?))
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
