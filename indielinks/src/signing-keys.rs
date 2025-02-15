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

//! # indielinks Signing Keys
//!
//! indielinks signs JWTs for authentication purposes. This module, similarly to [peppers], provides
//! abstractions for securely keeping signing keys in memory as well as versioning & rotating them.
//!
//! [peppers]: crate::peppers
//!
//! The intent is that the set of currently supported keys will be read by the program at startup
//! (see [here] for guidance on managing secrets like this). At least initially, I'm just going to
//! read them from configuration, so they might be written down in the indielinks configuration file
//! like so:
//!
//! [here]: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
//!
//! ```toml
//! [signing-keys]
//! keyid:2025-02-12 = [1, 2, 3, 4, ..., 64] # Keys must be 64 octets in length
//! keyid:2025-02-15 = [65, 66, 67,..., 128]
//! ```
//!
//! The operator can begin the process of rotating the signing key by simply adding a new key with a
//! later version identifier (the versions are compared lexicographically) and either re-starting
//! the program or sending it a SIGHUP. From that point on, the new key will be used for any users
//! that login or are otherwise issued tokens. Extant users have the key ID that was current when
//! they joined encoded in their token, so it can be looked-up in order to continue to verify their
//! token.
//!
//! The operator can terminate the rotation process for an older key by simply removing it from the
//! list. I haven't quite worked-out what to do with users that haven't updated their tokens by that
//! point; just terminate their sessions? Coordinate the token TTL with the rotation cadence (so
//! that any tokens that can now no longer be verified are expired anyway)?

use std::collections::BTreeMap;

use refined::{boundable::unsigned::Equals, type_string, Refinement, TypeString};
use serde::Deserialize;
use snafu::{prelude::*, Backtrace, Snafu};

use crate::util::{Key, RegexPredicate};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No pepper available"))]
    NoKey { backtrace: Backtrace },
}

type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             KeyId                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

type_string!(KeyIdRegex, "^keyid:[-0-9a-zA-Z]+$");

// We don't write-down `KeyId`s in the database, so no need to implement Scylla tratis on it
pub type KeyId = Refinement<String, RegexPredicate<KeyIdRegex>>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           SigningKey                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

// type RefinedKey = Refinement<Key, Equals<64>>;

/// A refined type enforcing a key length (of 64 octets)
pub type SigningKey = Refinement<Key, Equals<64>>;

fn default_signing_key() -> SigningKey {
    use rand::RngCore;
    let mut bytes: Vec<u8> = vec![0; 64];
    argon2::password_hash::rand_core::OsRng.fill_bytes(&mut bytes);
    // SigningKey(RefinedKey::refine(bytes.into()).unwrap())
    SigningKey::refine(bytes.into()).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          SigningKeys                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize)]
pub struct SigningKeys {
    keys: BTreeMap<KeyId, SigningKey>,
}

impl SigningKeys {
    /// Retrieve the current (i.e. the most recent) SigningKey
    pub fn current(&self) -> Result<(KeyId, SigningKey)> {
        let (key, value) = self.keys.last_key_value().context(NoKeySnafu)?;
        Ok((key.clone(), value.clone()))
    }
    /// Retrieve a pepper by version
    pub fn find_by_version(&self, keyid: &KeyId) -> Result<SigningKey> {
        Ok(self.keys.get(keyid).context(NoKeySnafu)?.clone())
    }
}

impl Default for SigningKeys {
    fn default() -> Self {
        SigningKeys {
            keys: BTreeMap::from_iter(vec![(
                KeyId::refine(chrono::Local::now().format("keyid:%Y%m%d").to_string()).unwrap(),
                default_signing_key(),
            )]),
        }
    }
}
