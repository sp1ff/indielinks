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

//! # indieliks authorization support
//!
//! While different parts of the indieliknks API handle authentication differently (the del.icio.us
//! API supports "basic" HTTP authentication for legacy reasons, e.g.) I've tried to standardize
//! them as much as possible. Generally useful authentication & authorization primitives go here.

use std::{str::FromStr, string::FromUtf8Error};

use axum::http::HeaderValue;
use base64::{prelude::BASE64_STANDARD, Engine};
use secrecy::SecretString;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use tap::Pipe;

use crate::{
    entities,
    entities::{User, UserApiKey, Username},
    peppers::Peppers,
    signing_keys::SigningKeys,
    storage::Backend as StorageBackend,
    token,
    token::verify_token,
    util::exactly_two,
};

/// authorization Error type
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The supplied API key couldn't be parsed"))]
    BadApiKey {
        key: String,
        source: hex::FromHexError,
        backtrace: Backtrace,
    },
    #[snafu(display("An Authorization header had a value that couldn't be parsed."))]
    BadAuthHeaderParse {
        value: HeaderValue,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to decode base64 field: {source}"))]
    BadBase64Encoding {
        text: String,
        source: base64::DecodeError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to validate password for {username}: {source}"))]
    BadPassword {
        username: Username,
        source: entities::Error,
    },
    #[snafu(display("{username} is not a valid username"))]
    BadUsername {
        username: String,
        source: crate::entities::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Incorrect password for {username}"))]
    IncorrectPassword {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid API key"))]
    InvalidApiKey {
        key: UserApiKey,
        source: entities::Error,
    },
    #[snafu(display("An Authorization header had a non-textual value: {source}"))]
    InvalidAuthHeaderValue {
        value: HeaderValue,
        source: axum::http::header::ToStrError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to find a colon in '{text}'"))]
    MissingColon { text: String, backtrace: Backtrace },
    #[snafu(display("The text was not valid UTF-8"))]
    NotUtf8 {
        source: FromUtf8Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to verify token: {source}"))]
    Token { source: token::Error },
    #[snafu(display("Unknown username {username}"))]
    UnknownUser { username: Username },
    #[snafu(display("Authorization scheme {scheme} not supported"))]
    UnsupportedAuthScheme {
        scheme: String,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to lookup user {username}: {source}"))]
    User {
        username: Username,
        source: crate::storage::Error,
    },
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                     Authorization Schemes                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Authorization schemes
///
/// I loathe putting key material on the wire (let alone passwords), but for legacy reasons we
/// support both HTTP "basic" authentication (i.e. username & password) as well as "bearer" using
/// API keys. Finally, I've implemented the more modern JWT support.
#[derive(Clone, Debug)]
pub enum AuthnScheme {
    // Authorization: Bearer <username>:<key>:: should probably impose a little more structure on
    // the key-- versioning, e.g.
    BearerApiKey((Username, UserApiKey)),
    // Authorization: Bearer base64.base64.base64:: should probably impose a little more structure
    // there-- deserialize, perhaps? I can't verify without the signing key, but I *can*
    // base64-decode it and deserialize to a (n unverified) Token, at least
    BearerToken(String),
    // Authorization: <username>:<password>:: On the wire, the username/password pair will be
    // base64-encoded
    Basic((Username, SecretString)),
}

impl AuthnScheme {
    /// Create an AuthnScheme instance from the base64 encoding of "username:password"
    pub fn from_basic(payload: &str) -> Result<AuthnScheme> {
        let (username, password) = BASE64_STANDARD
            .decode(payload)
            .context(BadBase64EncodingSnafu {
                text: payload.to_owned(),
            })?
            .pipe(String::from_utf8)
            .context(NotUtf8Snafu)?
            .split_once(':')
            .context(MissingColonSnafu {
                text: payload.to_string(),
            })?
            .pipe(|(u, p)| (u.to_string(), p.to_string()));

        Ok(AuthnScheme::Basic((
            Username::from_str(&username).context(BadUsernameSnafu {
                username: username.to_owned(),
            })?,
            password.into(),
        )))
    }
    /// Create an AuthnScheme instance from the the plain text "username:key-in-hex"
    pub fn from_api_key(payload: &str) -> Result<AuthnScheme> {
        // `payload` should be the plain text "username:key-in-hex"
        let (username, key) = payload.split_once(':').context(MissingColonSnafu {
            text: payload.to_string(),
        })?;
        Ok(AuthnScheme::BearerApiKey((
            Username::from_str(username).context(BadUsernameSnafu {
                username: username.to_owned(),
            })?,
            hex::decode(key.as_bytes())
                .context(BadApiKeySnafu {
                    key: key.to_owned(),
                })?
                .into(),
        )))
    }
    /// Create an AuthnScheme instance from the plain text "base64.base64.base64"
    pub fn from_token(payload: &str) -> Result<AuthnScheme> {
        Ok(AuthnScheme::BearerToken(payload.to_owned()))
    }
}

impl TryFrom<&HeaderValue> for AuthnScheme {
    type Error = Error;

    fn try_from(value: &HeaderValue) -> StdResult<Self, Self::Error> {
        // This seems like a lot of code to parse a `HeaderValue` into a a pair of strings... should
        // I upgrade to a proper parsing library like `nom`? I guess a regex would do it, once we've
        // converted the header value to a string.
        let (scheme, payload) = value
            .to_str()
            .context(InvalidAuthHeaderValueSnafu {
                value: value.clone(),
            })?
            .split_ascii_whitespace()
            .collect::<Vec<&str>>()
            .into_iter()
            .pipe(exactly_two)
            .map_err(|_| {
                BadAuthHeaderParseSnafu {
                    value: value.clone(),
                }
                .build()
            })?;
        match scheme.to_ascii_lowercase().as_str() {
            "basic" => AuthnScheme::from_basic(payload),
            "bearer" => {
                // OK, this could be either an API key or a token. If there's a ':', we assume it's
                // the former.
                match payload.find(':') {
                    Some(_) => AuthnScheme::from_api_key(payload),
                    None => AuthnScheme::from_token(payload),
                }
            }
            _ => UnsupportedAuthSchemeSnafu {
                scheme: scheme.to_owned(),
            }
            .fail(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                Authentication Utility Functions                                //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Authenticate a user by [Username] and API key. On success, return the full [User]; on failure
/// return error.
pub async fn check_api_key(
    storage: &(dyn StorageBackend + Send + Sync),
    username: &Username,
    key: &UserApiKey,
) -> Result<User> {
    let user = storage
        .user_for_name(username.as_ref())
        .await
        .context(UserSnafu {
            username: username.clone(),
        })?
        .context(UnknownUserSnafu {
            username: username.clone(),
        })?;
    user.check_key(key)
        .context(InvalidApiKeySnafu { key: key.clone() })?;
    Ok(user)
}

/// Authenticate a user by [Username] and JWT token. On success, return the full [User]; on failure
/// return error.
pub async fn check_token(
    storage: &(dyn StorageBackend + Send + Sync),
    token_string: &str,
    keys: &SigningKeys,
    domain: &str,
) -> Result<User> {
    let username = verify_token(token_string, keys, domain).context(TokenSnafu)?;
    storage
        .user_for_name(username.as_ref())
        .await
        .context(UserSnafu {
            username: username.clone(),
        })?
        .context(UnknownUserSnafu {
            username: username.clone(),
        })
}

pub async fn check_password(
    storage: &(dyn StorageBackend + Send + Sync),
    peppers: &Peppers,
    username: &Username,
    password: SecretString,
) -> Result<User> {
    let user = storage
        .user_for_name(username.as_ref())
        .await
        .context(UserSnafu {
            username: username.clone(),
        })?
        .context(UnknownUserSnafu {
            username: username.clone(),
        })?;
    user.check_password(peppers, password)
        .context(BadPasswordSnafu {
            username: username.clone(),
        })?;
    Ok(user)
}
