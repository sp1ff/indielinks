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

//! # peppers
//!
//! Indielinks salts and peppers passwords. This module contains support for holding peppers
//! securely in memory as well as versioning & rotating them.
//!
//! The intent is that a set of currently supported peppers, along with versions, will be read by
//! the program at startup (see [here] for guidance on managing secrets like this). At least
//! initially, I'm just going to read them from configuration, so they might be written down in the
//! indielinks configuration file like:
//!
//! [here]: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
//!
//! ```toml
//! [peppers]
//! pepper-ver:2025-02-12 = [1, 2, 3, 4]
//! pepper-ver:2025-02-15 = [5, 6, 7, 8]
//! ```
//!
//! The operator can begin the process of rotating the pepper key by simply adding a new, later
//! version (the versions are compared lexicographically) and can terminate the rotation process for
//! an older key by simply removing it (I haven't quite worked-out what to do with users that
//! haven't logged-in by that point; disable their accounts?)

use std::collections::BTreeMap;

use lazy_static::lazy_static;
use regex::Regex;
use scylla::{
    deserialize::{DeserializationError, DeserializeValue, FrameSlice, TypeCheckError},
    frame::response::result::ColumnType,
    serialize::{
        value::SerializeValue,
        writers::{CellWriter, WrittenCellProof},
        SerializationError,
    },
};
use secrecy::SecretSlice;
use serde::{Deserialize, Deserializer, Serialize};
use serde_bytes::ByteBuf;
use snafu::{prelude::*, Backtrace};
use tap::{Conv, Pipe};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{text} is not a valid version string"))]
    BadVersionString { text: String, backtrace: Backtrace },
    #[snafu(display("No pepper available"))]
    NoPepper { backtrace: Backtrace },
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        Pepper Versions                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Newtype, correct by construction, version string for pepper versions
///
/// Pepper versions are strings of the form "pepper-ver:[-a-zA-Z0-9]".
///
/// Pepper versions have to be serializable so that we can write them down alongside user password hashes.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Version(String);

lazy_static! {
    static ref VERSION: Regex = Regex::new("^pepper-ver:[-a-zA-Z0-9]+$").unwrap(/* known good */);
}

impl Version {
    /// Create a new pepper version from plain text
    pub fn new(text: &str) -> Result<Version> {
        if VERSION.is_match(text) {
            Ok(Version(text.to_string()))
        } else {
            BadVersionStringSnafu {
                text: text.to_string(),
            }
            .fail()
        }
    }
}

impl<'de> Deserialize<'de> for Version {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        Version::new(&s).map_err(|err| <D::Error as serde::de::Error>::custom(format!("{:?}", err)))
    }
}

impl std::cmp::PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for Version {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for Version {
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

impl SerializeValue for Version {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&self.0, typ, writer)
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::Deref for Version {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_version() {
        assert!(Version::new("pepper-ver:20250212").is_ok());
        assert!(Version::new("pepper-ver:二月十二号").is_err());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             Pepper                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct Pepper(SecretSlice<u8>);

impl<'de> Deserialize<'de> for Pepper {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        <ByteBuf as serde::Deserialize>::deserialize(deserializer)
            .map_err(|err| <D::Error as serde::de::Error>::custom(format!("{:?}", err)))?
            .pipe(|x| x.into_vec())
            .conv::<SecretSlice<u8>>()
            .pipe(Pepper)
            .pipe(Ok)
    }
}

impl std::convert::AsRef<SecretSlice<u8>> for Pepper {
    fn as_ref(&self) -> &SecretSlice<u8> {
        &self.0
    }
}

impl Default for Pepper {
    fn default() -> Self {
        use rand::RngCore;
        let mut bytes: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        argon2::password_hash::rand_core::OsRng.fill_bytes(&mut bytes);
        Pepper(bytes.into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            Peppers                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize)]
pub struct Peppers {
    peppers: BTreeMap<Version, Pepper>,
}

impl Default for Peppers {
    fn default() -> Self {
        Peppers {
            peppers: BTreeMap::from_iter(vec![(
                Version::new(&chrono::Local::now().format("pepper-ver:%Y%m%d").to_string())
                    .unwrap(),
                Pepper::default(),
            )]),
        }
    }
}

impl Peppers {
    /// Retrieve the current (i.e. the most recent) Pepper
    pub fn current_pepper(&self) -> Result<(Version, Pepper)> {
        let (key, value) = self.peppers.last_key_value().context(NoPepperSnafu)?;
        Ok((key.clone(), value.clone()))
    }
    /// Retrieve a pepper by version
    pub fn find_by_version(&self, version: &Version) -> Result<Pepper> {
        Ok(self.peppers.get(version).context(NoPepperSnafu)?.clone())
    }
}
