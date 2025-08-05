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

//! # indielinks Authentication Tokens
//!
//! The tokens used for indielinks authentication are just [JWT]s.
//!
//! [JWT]: https://www.rfc-editor.org/rfc/rfc7519.html

use std::convert::AsRef;

use chrono::{DateTime, Duration, Utc};
use hmac::{Hmac, Mac};
use jwt::{Header, SignWithKey, Token, VerifyWithKey};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use snafu::{Backtrace, prelude::*};

use crate::{
    origin::Host,
    signing_keys::{self, KeyId, SigningKey, SigningKeys},
};

use indielinks_shared::Username;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Token expired at {expires}"))]
    Expired {
        expires: DateTime<Utc>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to create an HMAC: {source}"))]
    Hmac {
        source: crypto_common::InvalidLength,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to refine a string to a KeyId: {source}"))]
    KeyId {
        source: signing_keys::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("The Key ID was missing from the JWT"))]
    MissingKeyId { backtrace: Backtrace },
    #[snafu(display("No signing key matching {keyid}: {source}"))]
    NoKey {
        keyid: KeyId,
        source: signing_keys::Error,
    },
    #[snafu(display("Invalid token: not before {not_before}"))]
    NotBefore {
        not_before: DateTime<Utc>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to parse JWT: {source}"))]
    Parse {
        source: jwt::error::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to sign JWT claims: {source}"))]
    Signature {
        source: jwt::error::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Unknown token audience {audience}"))]
    UnknownAudience {
        audience: String,
        backtrace: Backtrace,
    },
    #[snafu(display("Unknown token issuer {issuer}"))]
    UnknownIssuer { issuer: Host, backtrace: Backtrace },
    #[snafu(display("Verification failure: {source}"))]
    Verification {
        source: jwt::error::Error,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

// https://pragmaticwebsecurity.com/articles/apisecurity/hard-parts-of-jwt.html
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Claims {
    #[serde(rename = "iat")]
    issued_at: DateTime<Utc>,
    #[serde(rename = "iss")]
    issuer: Host,
    #[serde(rename = "aud")]
    audience: String,
    #[serde(rename = "nbf")]
    not_before: DateTime<Utc>,
    #[serde(rename = "exp")]
    expires: DateTime<Utc>,
    #[serde(rename = "sub")]
    subject: Username,
}

/// Mint a new JWT
///
/// Mint a new token naming `username`, signed using `signing_key` (identified by `keyid`). The
/// token will be valid for `lifetime`.
///
/// The fully serialized JWT will be returned.
pub fn mint_token(
    username: &Username,
    keyid: &KeyId,
    signing_key: &SigningKey,
    issuer: &Host,
    lifetime: &Duration,
) -> Result<String> {
    let key: Hmac<Sha256> =
        Hmac::new_from_slice(signing_key.as_ref().expose_secret()).context(HmacSnafu)?;
    let header = Header {
        key_id: Some(keyid.to_string()),
        ..Default::default()
    };
    let now = Utc::now();
    let claims = Claims {
        issued_at: now,
        issuer: issuer.clone(),
        audience: format!("api.{}", issuer),
        not_before: now,
        expires: now + *lifetime,
        subject: username.clone(),
    };
    Ok(Token::new(header, claims)
        .sign_with_key(&key)
        .context(SignatureSnafu)?
        .as_str()
        .to_owned())
}

pub fn verify_token(token_string: &str, keys: &SigningKeys, issuer: &Host) -> Result<Username> {
    let token: Token<Header, Claims, _ /* Unverified<'_> */> =
        Token::parse_unverified(token_string).context(ParseSnafu)?;
    let keyid = token
        .header()
        .key_id
        .clone()
        .ok_or(MissingKeyIdSnafu.build())?;
    let keyid = KeyId::new(&keyid).context(KeyIdSnafu)?;
    let signing_key = keys.find_by_version(&keyid).context(NoKeySnafu { keyid })?;
    let key: Hmac<Sha256> =
        Hmac::new_from_slice(signing_key.as_ref().expose_secret()).context(HmacSnafu)?;
    let token: Token<Header, Claims, _> = token_string
        .verify_with_key(&key)
        .context(VerificationSnafu)?;
    let claims = token.claims();

    let now = Utc::now();

    if now < claims.not_before {
        return NotBeforeSnafu {
            not_before: claims.not_before,
        }
        .fail();
    }
    if now > claims.expires {
        return ExpiredSnafu {
            expires: claims.expires,
        }
        .fail();
    }
    if *issuer != claims.issuer {
        return UnknownIssuerSnafu {
            issuer: claims.issuer.clone(),
        }
        .fail();
    }
    if format!("api.{}", issuer) != claims.audience {
        return UnknownAudienceSnafu {
            audience: claims.audience.clone(),
        }
        .fail();
    }

    Ok(claims.subject.clone())
}
