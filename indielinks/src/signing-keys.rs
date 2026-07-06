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

use std::{collections::BTreeMap, fmt::Display, result::Result as StdResult, str::FromStr};

use lazy_static::lazy_static;
use regex::Regex;
use serde::Deserialize;
use snafu::{prelude::*, Backtrace, Snafu};

use crate::util::Key;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Bad pagination token format"))]
    BadToken { backtrace: Backtrace },
    #[snafu(display("The pagination token is not base64-encoded: {source}"))]
    Base64 {
        source: base64::DecodeError,
        backtrace: Backtrace,
    },
    #[snafu(display("While decrypting the pagination token, {source}"))]
    Decrypt {
        source: chacha20poly1305::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While encrypting a sort key, {source}"))]
    Encrypt {
        source: chacha20poly1305::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While deriving the encryption key, {source}"))]
    Hkdf {
        source: hkdf::InvalidLength,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to recognize {text} as a KeyId"))]
    KeyId { text: String, backtrace: Backtrace },
    #[snafu(display("No pepper available"))]
    NoKey { backtrace: Backtrace },
    #[snafu(display("Signing keys must be 64 octets in length"))]
    SigningKey { backtrace: Backtrace },
    #[snafu(display("While deserializing the sort key, {source}"))]
    SortKeyDe {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While serializing the sort key, {source}"))]
    SortKeySer {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
}

type Result<T> = StdResult<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             KeyId                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

lazy_static! {
    static ref KEY_ID : Regex = Regex::new("^keyid:[-0-9a-zA-Z]+$").unwrap(/* known good */);
}

// We don't write-down `KeyId`s in the database, so no need to implement Scylla traits on it
#[derive(Clone, Debug, Deserialize, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct KeyId(String);

impl KeyId {
    pub fn new(s: &str) -> Result<KeyId> {
        if KEY_ID.find(s).is_none() {
            KeyIdSnafu { text: s.to_owned() }.fail()
        } else {
            Ok(KeyId(s.to_owned()))
        }
    }
}

impl Display for KeyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for KeyId {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        KeyId::new(s)
    }
}

impl AsRef<str> for KeyId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<KeyId> for String {
    fn from(value: KeyId) -> Self {
        value.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           SigningKey                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A refined type enforcing a key length (of 64 octets)
#[derive(Clone, Debug, Deserialize)]
#[serde(transparent)]
pub struct SigningKey(Key);

impl SigningKey {
    pub fn new(b: Vec<u8>) -> Result<SigningKey> {
        if b.len() == 64 {
            Ok(SigningKey(b.into()))
        } else {
            SigningKeySnafu.fail()
        }
    }
}

impl Default for SigningKey {
    fn default() -> Self {
        use rand::RngCore;
        let mut bytes: Vec<u8> = vec![0; 64];
        argon2::password_hash::rand_core::OsRng.fill_bytes(&mut bytes);
        SigningKey(bytes.into())
    }
}

impl AsRef<Key> for SigningKey {
    fn as_ref(&self) -> &Key {
        &self.0
    }
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
                KeyId(chrono::Local::now().format("keyid:%Y%m%d").to_string()),
                SigningKey::default(),
            )]),
        }
    }
}

impl<const N: usize> From<[(KeyId, SigningKey); N]> for SigningKeys {
    fn from(value: [(KeyId, SigningKey); N]) -> Self {
        Self {
            keys: BTreeMap::from(value),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       Pagination Tokens                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// # Pagination Tokens
///
/// ## Introduction
///
/// Utilities for converting internal sort keys to HTTP-friendly pagination tokens.
///
/// ## Discussion
///
/// [indielinks] routinely has to support pagination: over posts, timeline items, ActivityPub
/// collections, and so on. This brings up the question of how to represent pagination tokens in an
/// API-friendly way. I've found myself defining Rust types that function as sort keys for each
/// particular sort of collection (i.e. they implement [Ord]), but am reluctant to expose those
/// types to callers (by serializing instances to JSON, for instance).
///
/// [indielinks]: ../indielinksd/index.html
///
/// While poking around Google's API design guidelines, I came across the following bit of [advice]:
/// "Page tokens provided by APIs must be opaque (but URL-safe) strings, and must not be
/// user-parseable. This is because if users are able to deconstruct these, they will do so. This
/// effectively makes the implementation details of your API's pagination become part of the API
/// surface, and it becomes impossible to update those details without breaking users."
///
/// [advice]: https://google.aip.dev/158
///
/// I made mine opaque URL-safe and non-deconstructable by callers by first serializing instances,
/// then encrypting the resulting bytestrings, then base64-encoding them.
///
/// After going through this process a few times, I factored-out the common logic here.
pub mod pagination {

    use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};
    use chacha20poly1305::{aead::Aead, ChaCha20Poly1305, Nonce};
    use crypto_common::KeyInit;
    use hkdf::Hkdf;
    use rand::{rngs::OsRng, RngCore};
    use secrecy::ExposeSecret;
    use serde::{de::DeserializeOwned, Serialize};
    use sha2::Sha256;
    use tap::Pipe;

    use super::*;

    // Use an HMAC key derivation function to derive a 32-byte key from a 64-byte [SigningKey]
    fn derive_key(signing_key: &SigningKey) -> Result<chacha20poly1305::Key> {
        let hk = Hkdf::<Sha256>::new(None, signing_key.as_ref().expose_secret());
        let mut key = [0u8; 32];
        hk.expand(b"Token xchacha20poly1305", &mut key)
            .context(HkdfSnafu)?;
        Ok(key.into())
    }

    /// Turn a sort key into an opaque pagination token
    pub fn to_token<K, T>(sort_key: &K, signing_key: &SigningKey) -> Result<T>
    where
        K: Serialize,
        T: From<String>,
    {
        // indielinks signing keys, at the time of this writing, are 64 octets in length (don't
        // remember why I picked that ATM). Use a simple KDF to derive a 32 octet long key, which is
        // what's used by ChaCha20Poly1305.
        let key = derive_key(signing_key)?;
        let cipher = ChaCha20Poly1305::new(&key);
        // Symmetric encryption generally requires a per-message nonce. We're not really encrypting
        // for security or message integrity here, just obfuscation. And I'm not interested in
        // managing per pagination token state! So, we'll just prepend the nonce to the cipher text
        // (it's only a 12-byte nonce).
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        // OK-- serialize ourselves to JSON & encrypt.
        let mut cipher_text = cipher
            .encrypt(
                nonce,
                serde_json::to_vec(&sort_key)
                    .context(SortKeySerSnafu)?
                    .as_slice(),
            )
            .context(EncryptSnafu)?;

        // Finally, append the two (nonce + ciphertext) and base64-encode. I couldn't come-up with a
        // simpler way. Since `encode()` requires something that is `Deref<[u8]>`, I need a
        // contiguous block, meaning one copy, regardless:
        let mut out = Vec::with_capacity(12 + cipher_text.len());
        out.extend_from_slice(&nonce_bytes); // Copy here
        out.append(&mut cipher_text); // move here

        Ok(BASE64_URL_SAFE_NO_PAD.encode(out).into())
    }

    /// "Rehydrate" an opaque pagination token back into a sort key
    pub fn from_token<T, K>(token: &T, signing_key: &SigningKey) -> Result<K>
    where
        K: DeserializeOwned,
        T: AsRef<[u8]>,
    {
        // We have a base64-encoded bytestring "12-octet-nonce | encrypted JSON"
        let buf = BASE64_URL_SAFE_NO_PAD
            .decode(token.as_ref())
            .context(Base64Snafu)?;
        // Split the buffer into nonce & ciphertext
        let (nonce, cipher_text) = buf.split_at_checked(12).context(BadTokenSnafu)?;
        // indielinks signing keys, at the time of this writing, are 64 octets in length (don't
        // remember why I picked that ATM). Use a simple KDF to derive a 32 octet long key, which is
        // what's used by ChaCha20Poly1305.
        let key = derive_key(signing_key)?;
        let cipher = ChaCha20Poly1305::new(&key);
        let nonce = Nonce::from_slice(nonce);

        serde_json::from_slice::<K>(
            cipher
                .decrypt(nonce, cipher_text)
                .context(DecryptSnafu)?
                .as_slice(),
        )
        .context(SortKeyDeSnafu)?
        .pipe(Ok)
    }
}
