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

//! # indielinks peppers
//!
//! Indielinks salts and [peppers] passwords. Salts are easy: they're generated at signup time for
//! each user & stored along with that user in the database. Peppers, however, are designed to
//! stored *separately*. This module contains support for holding peppers securely in memory as well
//! as versioning & rotating them.
//!
//! [peppers]: https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html#peppering
//!
//! The intent is that the list of currently supported peppers will be read by the program at
//! startup (see [here] for guidance on managing secrets like this). At least initially, I'm just
//! going to read them from configuration, so they might be written down in the indielinks
//! configuration file like so:
//!
//! [here]: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
//!
//! ```toml
//! [peppers]
//! pepper-ver:2025-02-12 = [1, 2, 3, 4, ..., 32] # Peppers must be 32 octets in length
//! pepper-ver:2025-02-15 = [33, 34, 35, ..., 64]
//! ```
//!
//! The operator can begin the process of rotating the pepper key by simply adding a new key with a
//! later version identifier (the versions are compared lexicographically) and either re-starting
//! the program or sending it a SIGHUP. From that point on, the new pepper will be used for any users
//! that join. Extant users have the pepper version that was current when they joined written down
//! (again in the database), so they can continue to validate their passwords.
//!
//! The operator can terminate the rotation process for an older key by simply removing it from the
//! list. I haven't quite worked-out what to do with users that haven't logged-in by that point;
//! disable their accounts? Forcibly rotate their pepper?
//!
//! See also module [signing-keys].

use std::{collections::BTreeMap, ops::Deref};

use refined::{boundable::unsigned::Equals, type_string, Refinement, TypeString};
use scylla::{
    deserialize::{DeserializationError, DeserializeValue, FrameSlice, TypeCheckError},
    frame::response::result::ColumnType,
    serialize::{
        value::SerializeValue,
        writers::{CellWriter, WrittenCellProof},
        SerializationError,
    },
};
use serde::{Deserialize, Serialize};
use snafu::{prelude::*, Backtrace};

use crate::util::{Key, RegexPredicate};

#[derive(Debug, Snafu)]
pub enum Error {
    // #[snafu(display("{text} is not a valid version string"))]
    // BadVersionString { text: String, backtrace: Backtrace },
    #[snafu(display("No pepper available"))]
    NoPepper { backtrace: Backtrace },
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        Pepper Versions                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

type_string!(VersionIdRegex, "^pepper-ver:[-a-zA-Z0-9]+$");

type RefinedString = Refinement<String, RegexPredicate<VersionIdRegex>>;

/// Newtype, correct by construction, version string for pepper versions.
///
/// Pepper versions are strings of the form "pepper-ver:[-a-zA-Z0-9]".
///
/// Pepper versions have to be serializable so that we can write them down alongside user password hashes.
// Arrrghhh... need to make this a newtype in order to implement traits on it...
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Version(RefinedString);

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for Version {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        String::type_check(typ)
    }
    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        Ok(Self(
            RefinedString::refine(<String as DeserializeValue>::deserialize(typ, v)?)
                .map_err(DeserializationError::new)?,
        ))
    }
}

impl SerializeValue for Version {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&self.0.deref(), typ, writer)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             Pepper                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A [Pepper] is a 32-octet key
pub type Pepper = Refinement<Key, Equals<32>>;

fn default_pepper() -> Pepper {
    use rand::RngCore;
    let mut bytes: Vec<u8> = vec![0; 32]; // 128 bits
    argon2::password_hash::rand_core::OsRng.fill_bytes(&mut bytes);
    Pepper::refine(bytes.into()).unwrap()
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
                Version(
                    RefinedString::refine(
                        chrono::Local::now().format("pepper-ver:%Y%m%d").to_string(),
                    )
                    .unwrap(),
                ),
                default_pepper(),
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
