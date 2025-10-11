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
//! indielinks uses a few tokens for authentication. The access & refresh tokens are just [JWT]s,
//! whereas the CSRF token (used to mitigate CSRF for the refresh token) is hand-crafted (along very
//! similar lines).
//!
//! [JWT]: https://www.rfc-editor.org/rfc/rfc7519.html

use std::{convert::AsRef, result::Result as StdResult};

use chrono::{DateTime, Duration, Utc};
use hmac::{Hmac, Mac};
use jwt::{FromBase64, Header, SignWithKey, ToBase64, Token, VerifyWithKey, VerifyingAlgorithm};
use rand::{rngs::OsRng, RngCore};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use snafu::{prelude::*, Backtrace};
use uuid::Uuid;

use indielinks_shared::origin::Host;

use crate::signing_keys::{self, KeyId, SigningKey, SigningKeys};

use indielinks_shared::entities::Username;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to verify CSRF token {token}: {source}"))]
    Csrf {
        token: String,
        source: jwt::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("CSRF token mismatch!"))]
    CsrfMismatch {
        claimed: Uuid,
        token: Uuid,
        backtrace: Backtrace,
    },
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
    #[snafu(display("Failed to decode from base64: {source}"))]
    InvalidBase64 {
        source: jwt::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("The proferred CSRF token {token} is not in a valid format"))]
    InvalidCsrfTokenFmt { token: String, backtrace: Backtrace },
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

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                  the indielinks Access Token                                   //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// indielinks access [JWT] [claims]
///
/// [claims]: https://pragmaticwebsecurity.com/articles/apisecurity/hard-parts-of-jwt.html
#[derive(Clone, Debug, Deserialize, Serialize)]
struct AccessClaims {
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
/// token will be valid for duration `lifetime`.
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
    let claims = AccessClaims {
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
    let token: Token<Header, AccessClaims, _ /* Unverified<'_> */> =
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
    let token: Token<Header, AccessClaims, _> = token_string
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

#[cfg(test)]
mod access_token_tests {
    use super::*;

    #[test]
    fn verify_minted_token() {
        let username = Username::new("johndoe").unwrap(/* known good */);
        let key_id = KeyId::new("keyid:20250817").unwrap(/* known good */);
        // With apologies to J.R.R. Tolkein, but I needed 64 bytes exactly.
        let signing_key = SigningKey::new(b"All that is gold does not glitter-- Not all who wander are lost.".to_vec()).unwrap(/* known good */);
        let issuer = Host::new("indiepin.net").unwrap(/* known good */);

        let token_result = mint_token(
            &username,
            &key_id,
            &signing_key,
            &issuer,
            &Duration::seconds(300),
        );
        assert!(token_result.is_ok());

        let token = token_result.unwrap(/* known good */);

        let keys = SigningKeys::from([(key_id, signing_key)]);

        let verify_result = verify_token(&token, &keys, &issuer);
        assert!(verify_result.is_ok());

        let verified = verify_result.unwrap(/* known good */);
        assert_eq!(username, verified);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                  the indielinks Refresh Token                                  //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// indielinks refresh [JWT] [claims]
///
/// [claims]: https://pragmaticwebsecurity.com/articles/apisecurity/hard-parts-of-jwt.html
///
/// Differs from [Claims] in that it incorporates a session ID
#[derive(Clone, Debug, Deserialize, Serialize)]
struct RefreshClaims {
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
    #[serde(rename = "jti")]
    session: Uuid,
    #[serde(rename = "sub")]
    subject: Username,
}

// Similarly to the `jwt` crate, we'll use typestate to parameterize our CSRF token type

struct Unsigned;

struct Signed {
    token_string: String,
}

struct RefreshCsrfToken<S> {
    session: Uuid,
    // The OWASP cheat sheet suggests adding a nonce for "collision avoidance"; it's tough for me to
    // see what that adds to a UUID in terms of hash collisions, but out of paranoia I'll add a
    // nonce from a cryptographic source
    nonce: u64,
    signature: S,
}

impl RefreshCsrfToken<Unsigned> {
    /// Create a new CSRF token; initially, it will be unsigned
    pub fn new(session: Uuid, nonce: u64) -> RefreshCsrfToken<Unsigned> {
        RefreshCsrfToken::<Unsigned> {
            session,
            nonce,
            signature: Unsigned,
        }
    }
}

impl RefreshCsrfToken<Unsigned> {
    /// Sign a new CSRF token
    pub fn sign_with_key(
        self,
        key: &impl jwt::SigningAlgorithm,
    ) -> Result<RefreshCsrfToken<Signed>> {
        let session = format!("{}", self.session)
            .to_base64()
            .unwrap()
            .into_owned();
        // let nonce = format!("{:016x}", self.nonce)
        let nonce = self.nonce.to_base64().unwrap().into_owned();
        let signature = key.sign(&session, &nonce).context(SignatureSnafu)?;
        let token_string = format!("{}.{}.{}", session, nonce, signature);
        Ok(RefreshCsrfToken {
            session: self.session,
            nonce: self.nonce,
            signature: Signed { token_string },
        })
    }
}

impl RefreshCsrfToken<Signed> {
    pub fn as_str(&self) -> &str {
        &self.signature.token_string
    }
}

pub struct Verified;

pub struct Unverfiied<'a> {
    pub session_str: &'a str,
    pub nonce_str: &'a str,
    pub signature_str: &'a str,
}

fn split_components(token: &str) -> Result<[&str; 3]> {
    let mut components = token.split(".");
    let session = components.next().context(InvalidCsrfTokenFmtSnafu {
        token: token.to_owned(),
    })?;
    let nonce = components.next().context(InvalidCsrfTokenFmtSnafu {
        token: token.to_owned(),
    })?;
    let signature = components.next().context(InvalidCsrfTokenFmtSnafu {
        token: token.to_owned(),
    })?;

    if components.next().is_some() {
        return InvalidCsrfTokenFmtSnafu {
            token: token.to_owned(),
        }
        .fail();
    }

    Ok([session, nonce, signature])
}

impl<'a> RefreshCsrfToken<Unverfiied<'a>> {
    pub fn parse_unverified(token_str: &'a str) -> Result<RefreshCsrfToken<Unverfiied<'a>>> {
        let [session_str, nonce_str, signature_str] = split_components(token_str)?;
        let session = Uuid::from_base64(session_str).context(InvalidBase64Snafu)?;
        let nonce = u64::from_base64(nonce_str).context(InvalidBase64Snafu)?;
        let signature = Unverfiied {
            session_str,
            nonce_str,
            signature_str,
        };

        Ok(RefreshCsrfToken {
            session,
            nonce,
            signature,
        })
    }
}

impl<'a> jwt::token::verified::VerifyWithKey<RefreshCsrfToken<Verified>>
    for RefreshCsrfToken<Unverfiied<'a>>
{
    fn verify_with_key(
        self,
        key: &impl VerifyingAlgorithm,
    ) -> StdResult<RefreshCsrfToken<Verified>, jwt::error::Error> {
        let Unverfiied {
            session_str,
            nonce_str,
            signature_str,
        } = self.signature;

        if key.verify(session_str, nonce_str, signature_str)? {
            Ok(RefreshCsrfToken {
                session: self.session,
                nonce: self.nonce,
                signature: Verified,
            })
        } else {
            Err(jwt::error::Error::InvalidSignature)
        }
    }
}

impl jwt::token::verified::VerifyWithKey<RefreshCsrfToken<Verified>> for &str {
    fn verify_with_key(
        self,
        key: &impl VerifyingAlgorithm,
    ) -> StdResult<RefreshCsrfToken<Verified>, jwt::error::Error> {
        let token =
            RefreshCsrfToken::parse_unverified(self).map_err(|_| jwt::error::Error::Format)?;
        token.verify_with_key(key)
    }
}

/// Mint a refresh token and associated CSRF token
///
/// Both are returned in as strings, serialized into a form ready for transmission in the HTTP
/// response
///
/// The refresh token should be set as a cookie in the response with the Secure & HttpOnly
/// attributes; this will allow the token to survive reloads and prevent JavaScript from reading it.
/// The associated CSRF token is a CSRF mitigation strategy: it should also be a Secure cookie, but
/// not HttpOnly. The front end will prove the ability to read cookies by copying it into a custom
/// header. The CSRF token will be tied to the refresh token through the session field, and will be
/// signed.
pub fn mint_refresh_and_csrf_tokens(
    username: &Username,
    keyid: &KeyId,
    signing_key: &SigningKey,
    issuer: &Host,
    lifetime: &Duration,
) -> Result<(String, String)> {
    let now = Utc::now();
    let session = Uuid::new_v4();
    let key: Hmac<Sha256> =
        Hmac::new_from_slice(signing_key.as_ref().expose_secret()).context(HmacSnafu)?;

    // Refresh token
    let header = Header {
        key_id: Some(keyid.to_string()),
        ..Default::default()
    };
    let claims = RefreshClaims {
        issued_at: now,
        issuer: issuer.clone(),
        audience: format!("api.{}", issuer),
        not_before: now,
        expires: now + *lifetime,
        session,
        subject: username.clone(),
    };

    // CSRF token
    let nonce = OsRng.next_u64();

    Ok((
        Token::new(header, claims)
            .sign_with_key(&key)
            .context(SignatureSnafu)?
            .as_str()
            .to_owned(),
        RefreshCsrfToken::new(session, nonce)
            .sign_with_key(&key)?
            .as_str()
            .to_owned(),
    ))
}

/// Validate a refresh token & its associated CSRF token; vend a new access token on success. Return
/// the [Username] to which this refresh token corresponds as a courtesy to the caller (for logging
/// purposes, e.g.)
pub fn refresh_token(
    refresh_token_text: &str,
    refresh_csrf_token_text: &str,
    keys: &SigningKeys,
    issuer: &Host,
    lifetime: &Duration,
) -> Result<(String, Username)> {
    let now = Utc::now();
    let token: Token<Header, AccessClaims, _ /* Unverified<'_> */> =
        Token::parse_unverified(refresh_token_text).context(ParseSnafu)?;
    let keyid = token
        .header()
        .key_id
        .clone()
        .ok_or(MissingKeyIdSnafu.build())?;
    let keyid = KeyId::new(&keyid).context(KeyIdSnafu)?;
    let signing_key = keys.find_by_version(&keyid).context(NoKeySnafu {
        keyid: keyid.clone(),
    })?;
    let key: Hmac<Sha256> =
        Hmac::new_from_slice(signing_key.as_ref().expose_secret()).context(HmacSnafu)?;
    let token: Token<Header, RefreshClaims, _> = refresh_token_text
        .verify_with_key(&key)
        .context(VerificationSnafu)?;
    let claims = token.claims();

    // Alright, validate the claims in the refresh token:
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

    let csrf_token: RefreshCsrfToken<Verified> = refresh_csrf_token_text
        .verify_with_key(&key)
        .context(CsrfSnafu {
            token: refresh_csrf_token_text.to_owned(),
        })?;

    if csrf_token.session != claims.session {
        return CsrfMismatchSnafu {
            claimed: claims.session,
            token: csrf_token.session,
        }
        .fail();
    }

    Ok((
        mint_token(&claims.subject, &keyid, &signing_key, issuer, lifetime)?,
        claims.subject.clone(),
    ))
}

#[cfg(test)]
pub mod refresh_token_tests {
    use super::*;

    #[test]
    fn verify_minted_token() {
        let username = Username::new("johndoe").unwrap(/* known good */);
        let key_id = KeyId::new("keyid:20250817").unwrap(/* known good */);
        // With apologies to J.R.R. Tolkein, but I needed 64 bytes exactly.
        let signing_key = SigningKey::new(b"All that is gold does not glitter-- Not all who wander are lost.".to_vec()).unwrap(/* known good */);
        let issuer = Host::new("indiepin.net").unwrap(/* known good */);

        let tokens_result = mint_refresh_and_csrf_tokens(
            &username,
            &key_id,
            &signing_key,
            &issuer,
            &Duration::seconds(30),
        );
        assert!(tokens_result.is_ok());

        let (refresh, csrf) = tokens_result.unwrap(/* known good */);
        let keys = SigningKeys::from([(key_id, signing_key)]);

        let refresh_result = refresh_token(&refresh, &csrf, &keys, &issuer, &Duration::seconds(30));
        assert!(refresh_result.is_ok());

        let (token, parsed_username) = refresh_result.unwrap(/* known good */);
        assert_eq!(username, parsed_username);
        let verify_result = verify_token(&token, &keys, &issuer);
        assert!(verify_result.is_ok());

        let verified = verify_result.unwrap(/* known good */);
        assert_eq!(username, verified);
    }
}
