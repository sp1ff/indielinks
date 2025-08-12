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
use base64::{Engine, prelude::BASE64_STANDARD};
use http::header;
use itertools::Itertools;
use picky::{
    hash::HashAlgorithm,
    http::{HttpSignature, http_signature::HttpSignatureBuilder},
    signature::SignatureAlgorithm,
};
use secrecy::{SecretSlice, SecretString};
use sha2::Digest;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu, ensure};
use tap::Pipe;

use indielinks_shared::Username;

use crate::{
    entities::{self, User},
    origin::Host,
    peppers::Peppers,
    signing_keys::SigningKeys,
    storage::Backend as StorageBackend,
    token::{self, verify_token},
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
        #[snafu(source(from(entities::Error, Box::new)))]
        source: Box<entities::Error>,
    },
    #[snafu(display("{username} is not a valid username"))]
    BadUsername {
        username: String,
        #[snafu(source(from(indielinks_shared::Error, Box::new)))]
        source: Box<indielinks_shared::Error>,
        backtrace: Backtrace,
    },
    #[snafu(display("Computed digest did not match the reported digest"))]
    ContentDigest {
        computed: String,
        reported: String,
        backtrace: Backtrace,
    },
    #[snafu(display("(created) header with no or invalid created parameter"))]
    CreatedHeaderWithNoParam,
    #[snafu(display("(created) header with a restricted algorithm"))]
    CreatedWithRestrictedAlgo {
        algorithm: picky::http::http_signature::HttpSigAlgorithm,
    },
    #[snafu(display("Failed to convert the SHA-512 digest to a HeaderValue: {source}"))]
    DigestToHeaderValue {
        source: http::header::InvalidHeaderValue,
    },
    #[snafu(display("(expires) header with no or invalid expires parameter"))]
    ExpiresHeaderWithNoParam,
    #[snafu(display("(expires) header with a restricted algorithm"))]
    ExpiresWithRestrictedAlgo {
        algorithm: picky::http::http_signature::HttpSigAlgorithm,
    },
    #[snafu(display("Incorrect password for {username}"))]
    IncorrectPassword {
        username: Username,
        backtrace: Backtrace,
    },
    #[snafu(display("An Authorization header had a non-textual value: {source}"))]
    InvalidAuthHeaderValue {
        value: HeaderValue,
        source: axum::http::header::ToStrError,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid API key: {source}"))]
    InvalidKey {
        #[snafu(source(from(entities::Error, Box::new)))]
        source: Box<entities::Error>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to find a colon in '{text}'"))]
    MissingColon { text: String, backtrace: Backtrace },
    #[snafu(display("{payload} contains no version marker"))]
    MissingVersion {
        payload: String,
        backtrace: Backtrace,
    },
    #[snafu(display("Multiple sha-256 Digest headers found"))]
    MultipleContentDigests,
    #[snafu(display("In order to sign a request, it must contain a Content-Type header"))]
    NoContentTypeHeader,
    #[snafu(display("In order to sign a request, it must contain a Date header"))]
    NoDateHeader,
    #[snafu(display("In order to sign a request, it must contain a Host header"))]
    NoHostHeader,
    #[snafu(display("The text was not valid UTF-8"))]
    NotUtf8 {
        source: FromUtf8Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Exactly one Digest header expected"))]
    OneContentDigest,
    #[snafu(display("Failed to convert from an http request to a reqwest request: {source}"))]
    RequestConversion {
        source: reqwest::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to form an HTTP signature: {source}"))]
    Signature {
        source: picky::http::http_signature::HttpSignatureError,
    },
    #[snafu(display("The generate HTTP signature was not a valid header value: {source}"))]
    SignatureToString {
        source: http::header::InvalidHeaderValue,
    },
    #[snafu(display("Attempted to collect a body for a streaming response"))]
    StreamingBody { backtrace: Backtrace },
    #[snafu(display("Failed to read the request body as bytes: {source}"))]
    ToBytes {
        source: axum::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to verify token: {source}"))]
    Token {
        #[snafu(source(from(token::Error, Box::new)))]
        source: Box<token::Error>,
    },
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
    // Authorization: Bearer <username>:v1:<key material>. In a really irritating turn of events,
    // `SecretSlice<u8>` = `SecretBox<[u8]>` is Clonable, but `SecretBox<[u8; 28]>` is *not*.
    BearerApiKey((Username, SecretSlice<u8>)),
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
    /// Create an AuthnScheme instance from the the plain text "username:key-in-hex" or
    /// "username:v1:key-material-in-hex"
    pub fn from_api_key(payload: &str) -> Result<AuthnScheme> {
        // `payload` should be the plain text "username:key-in-hex"
        let (username, key) = payload.split_once(':').context(MissingColonSnafu {
            text: payload.to_string(),
        })?;
        ensure!(
            key.starts_with("v1:"),
            MissingVersionSnafu {
                payload: payload.to_owned()
            }
        );
        Ok(AuthnScheme::BearerApiKey((
            Username::from_str(username).context(BadUsernameSnafu {
                username: username.to_owned(),
            })?,
            hex::decode(&key[3..])
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
    key: &SecretSlice<u8>,
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
    user.check_key(key).context(InvalidKeySnafu)?;
    Ok(user)
}

/// Authenticate a user by [Username] and JWT token. On success, return the full [User]; on failure
/// return error.
pub async fn check_token(
    storage: &(dyn StorageBackend + Send + Sync),
    token_string: &str,
    keys: &SigningKeys,
    host: &Host,
) -> Result<User> {
    let username = verify_token(token_string, keys, host).context(TokenSnafu)?;
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

////////////////////////////////////////////////////////////////////////////////////////////////////
//                           Verifying HTTP Signatures for ActivityPub                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Enforce items 2 & 3 in [cavage] section 2.3 (hence, the "_2_3_2_3").
///
/// [cavage]: https://datatracker.ietf.org/doc/html/draft-cavage-http-signatures-12
///
/// If the "(created)" and/or "(expires)" pseudo-headers are specified, then:
///
/// 1. the signature algorithm may not begin with "rsa", "hmac", or "ecdsa"
///
/// 2. the corresponding signature parameter must be present
///
/// Nb. It's unclear to me how to apply this if the signature algorithm is [hs2019], in which the
/// signature algorithm is "derived from metadata associated with [the key ID]". It's worth noting
/// that hs2019 is now the [recommended] algorithm for signature creation.
///
/// [hs2019]: https://datatracker.ietf.org/doc/html/draft-cavage-http-signatures#autoid-38
/// [recommended]: https://swicg.github.io/activitypub-http-signature/
pub fn enforce_cavage_2_3_2_3(http_signature: &picky::http::HttpSignature) -> Result<()> {
    let is_restricted_algorithm = match http_signature.algorithm {
        Some(ref algorithm) => match algorithm {
            picky::http::http_signature::HttpSigAlgorithm::Known(signature_algorithm) => {
                match signature_algorithm {
                    picky::signature::SignatureAlgorithm::RsaPkcs1v15(_) => true,
                    picky::signature::SignatureAlgorithm::Ecdsa(_) => true,
                    picky::signature::SignatureAlgorithm::Ed25519 => false,
                    _ => false, // `SignatureAlgorithm` doesn't implement `Display`, so not sure what to say, here.
                }
            }
            picky::http::http_signature::HttpSigAlgorithm::Custom(s) => {
                s.starts_with("rsa") || s.starts_with("hmac") || s.starts_with("ecdsa")
            }
        },
        None => false,
    };

    // From the draft: "If the header field name is `(created)` and the `algorithm` parameter starts
    // with `rsa`, `hmac`, or `ecdsa` an implementation MUST produce an error. If the `created`
    // Signature Parameter is not specified, or is not an integer, an implementation MUST produce an
    // error. Otherwise, the header field value is the integer expressed by the `created` signature
    // parameter."
    if http_signature
        .headers
        .contains(&picky::http::http_signature::Header::Created)
    {
        if is_restricted_algorithm {
            return Err(Error::CreatedWithRestrictedAlgo {
                algorithm: http_signature.algorithm.clone().unwrap(),
            });
        }
        if http_signature.created.is_none() {
            return Err(Error::CreatedHeaderWithNoParam);
        }
    }

    // And "If the header field name is `(expires)` and the `algorithm` parameter starts with
    // `rsa`, `hmac`, or `ecdsa` an implementation MUST produce an error. If the `expires`
    // Signature Parameter is not specified, or is not an integer, an implementation MUST
    // produce an error. Otherwise, the header field value is the integer expressed by the
    // `created` signature parameter."
    if http_signature
        .headers
        .contains(&picky::http::http_signature::Header::Expires)
    {
        if is_restricted_algorithm {
            return Err(Error::ExpiresWithRestrictedAlgo {
                algorithm: http_signature.algorithm.clone().unwrap(),
            });
        }
        if http_signature.expires.is_none() {
            return Err(Error::ExpiresHeaderWithNoParam);
        }
    }

    Ok(())
}

/// Check the SHA-256 digest of a request body
///
/// Compute the SHA-256 digest of `bytes` (presumably a request body), and compare it to the
/// `digest` header in `parts` (presumably the request parts corresponding to `bytes`). Fail if they
/// don't match (or if the digest header is missing entirely).
pub fn check_sha_256_content_digest(
    parts: &http::request::Parts,
    bytes: &bytes::Bytes,
) -> Result<()> {
    // Apparently <https://swicg.github.io/activitypub-http-signature/> the only permissible
    // digest algorithm is SHA-256 (!?), so there'd better be exactly one header with
    // a prefix of "sha-256=":
    let content_digest_header_value = parts
        .headers
        .get_all(http::HeaderName::from_static("digest"))
        .iter()
        .filter_map(|value| match value.to_str() {
            Ok(s) => {
                if s.to_lowercase().starts_with("sha-256=") {
                    Some(s.to_owned())
                } else {
                    None
                }
            }
            Err(_) => None,
        })
        .exactly_one()
        .map_err(|_| Error::OneContentDigest)?;

    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes.as_ref());
    let sha_256_result = format!(
        "sha-256={}",
        BASE64_STANDARD.encode(hasher.finalize().as_slice())
    );

    if sha_256_result.to_lowercase() != content_digest_header_value.to_lowercase() {
        return ContentDigestSnafu {
            computed: sha_256_result,
            reported: content_digest_header_value,
        }
        .fail();
    }

    Ok(())
}

pub fn ensure_sha_256(
    request: http::Request<reqwest::Body>,
) -> Result<http::Request<reqwest::Body>> {
    // Seems like a *lot* of work just to check for the presence of a SHA-256 digest... Ah: the joys
    // of HTTP.
    match request
        .headers()
        .get_all("digest")
        .iter()
        .filter_map(|h| {
            h.to_str()
                .map(|s| {
                    s.to_lowercase()
                        .starts_with("sha-256=")
                        .then_some(s.to_owned())
                })
                .unwrap_or(None)
        })
        .at_most_one()
        .map_err(|_| Error::MultipleContentDigests)?
    {
        Some(_) => Ok(request),
        None => {
            let (mut parts, body) = request.into_parts();
            let mut hasher = sha2::Sha256::new();
            hasher.update(body.as_bytes().context(StreamingBodySnafu)?);
            let sha_256_result = format!(
                "sha-256={}",
                BASE64_STANDARD.encode(hasher.finalize().as_slice())
            );
            parts.headers.append(
                "digest",
                HeaderValue::from_str(&sha_256_result).context(DigestToHeaderValueSnafu)?,
            );
            Ok(http::Request::from_parts(parts, body))
        }
    }
}

pub fn sign_request(
    request: http::Request<reqwest::Body>,
    key_id: &str,
    private_key: &picky::key::PrivateKey,
) -> Result<(http::Request<reqwest::Body>, HttpSignature)> {
    let (parts, body) = request.into_parts();

    if !parts.headers.contains_key("Date") {
        return Err(Error::NoDateHeader);
    }

    if !parts.headers.contains_key("Host") {
        return Err(Error::NoHostHeader);
    }

    if !parts.headers.contains_key("Content-Type") {
        return Err(Error::NoContentTypeHeader);
    }

    let http_signature = HttpSignatureBuilder::new()
        .key_id(key_id)
        .signature_method(
            private_key,
            SignatureAlgorithm::RsaPkcs1v15(HashAlgorithm::SHA2_256),
        )
        // `picky::http::http_request::HttpRequest` trait is implemented for
        // `http::request::Parts` for `http` crate with `http_trait_impl` feature gate
        .generate_signing_string_using_http_request(&parts)
        .request_target()
        .http_header(header::CONTENT_TYPE.as_str())
        .http_header(header::DATE.as_str())
        .http_header("digest")
        .http_header(header::HOST.as_str())
        .build()
        .context(SignatureSnafu)?;

    Ok((http::Request::from_parts(parts, body), http_signature))
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                      HTTP Signature Tests                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Right off the bat: indielinks supports the *draft* HTTP signatures [spec], AKA
/// "draft-cavage-http-signatures-12". There is a later RFC that, AFAICT, no one actually uses.
///
/// [spec]: https://datatracker.ietf.org/doc/html/draft-cavage-http-signatures-12
/// [RFC]: https://www.rfc-editor.org/rfc/rfc9421.html#signature-params
///
/// Next, getting the signature right is a
/// [commonly](https://socialhub.activitypub.rocks/t/http-signatures-and-mastodon-continue-to-confound/3801)
/// [noted](https://rknight.me/blog/building-an-activitypub-server/)
/// [pain](https://code.lag.net/robey/squidcity/src/branch/main/src/signatures.ts#L15)
/// [point](https://elvery.net/drzax/activitypub-on-a-mostly-static-website/). In order to
/// trouble-shoot the [picky] implementation, I wound-up re-implementing it for myself. Since I
/// suspect this is going to be a continuing pain point as I federate with more AP servers, I'm
/// vaguely planning on writing myself a tool. For now, I'm going to move this logic into this
/// module's unit tests.
#[cfg(test)]
mod http_signature_tests {
    use std::str::FromStr;

    use base64::prelude::{BASE64_STANDARD, Engine};
    use http::method::Method;
    use http::{HeaderValue, Request, Uri, header, request};
    use picky::http::HttpSignature;
    use picky::http::http_signature::HttpSignatureBuilder;
    use picky::key::{PrivateKey, PublicKey};
    use rsa::{RsaPrivateKey, RsaPublicKey, pkcs1v15};
    use sha2::Digest;

    use super::*;

    /// Fix [picky] test `http::http_signature::tests::legacy`
    #[test]
    fn pr_351_legacy() {
        let priv_key = PrivateKey::from_pem_str(
            "-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAnzyis1ZjfNB0bBgKFMSvvkTtwlvBsaJq7S5wA+kzeVOVpVWw
kWdVha4s38XM/pa/yr47av7+z3VTmvDRyAHcaT92whREFpLv9cj5lTeJSibyr/Mr
m/YtjCZVWgaOYIhwrXwKLqPr/11inWsAkfIytvHWTxZYEcXLgAXFuUuaS3uF9gEi
NQwzGTU1v0FqkqTBr4B8nW3HCN47XUu0t8Y0e+lf4s4OxQawWD79J9/5d3Ry0vbV
3Am1FtGJiJvOwRsIfVChDpYStTcHTCMqtvWbV6L11BWkpzGXSW4Hv43qa+GSYOD2
QU68Mb59oSk2OB+BtOLpJofmbGEGgvmwyCI9MwIDAQABAoIBACiARq2wkltjtcjs
kFvZ7w1JAORHbEufEO1Eu27zOIlqbgyAcAl7q+/1bip4Z/x1IVES84/yTaM8p0go
amMhvgry/mS8vNi1BN2SAZEnb/7xSxbflb70bX9RHLJqKnp5GZe2jexw+wyXlwaM
+bclUCrh9e1ltH7IvUrRrQnFJfh+is1fRon9Co9Li0GwoN0x0byrrngU8Ak3Y6D9
D8GjQA4Elm94ST3izJv8iCOLSDBmzsPsXfcCUZfmTfZ5DbUDMbMxRnSo3nQeoKGC
0Lj9FkWcfmLcpGlSXTO+Ww1L7EGq+PT3NtRae1FZPwjddQ1/4V905kyQFLamAA5Y
lSpE2wkCgYEAy1OPLQcZt4NQnQzPz2SBJqQN2P5u3vXl+zNVKP8w4eBv0vWuJJF+
hkGNnSxXQrTkvDOIUddSKOzHHgSg4nY6K02ecyT0PPm/UZvtRpWrnBjcEVtHEJNp
bU9pLD5iZ0J9sbzPU/LxPmuAP2Bs8JmTn6aFRspFrP7W0s1Nmk2jsm0CgYEAyH0X
+jpoqxj4efZfkUrg5GbSEhf+dZglf0tTOA5bVg8IYwtmNk/pniLG/zI7c+GlTc9B
BwfMr59EzBq/eFMI7+LgXaVUsM/sS4Ry+yeK6SJx/otIMWtDfqxsLD8CPMCRvecC
2Pip4uSgrl0MOebl9XKp57GoaUWRWRHqwV4Y6h8CgYAZhI4mh4qZtnhKjY4TKDjx
QYufXSdLAi9v3FxmvchDwOgn4L+PRVdMwDNms2bsL0m5uPn104EzM6w1vzz1zwKz
5pTpPI0OjgWN13Tq8+PKvm/4Ga2MjgOgPWQkslulO/oMcXbPwWC3hcRdr9tcQtn9
Imf9n2spL/6EDFId+Hp/7QKBgAqlWdiXsWckdE1Fn91/NGHsc8syKvjjk1onDcw0
NvVi5vcba9oGdElJX3e9mxqUKMrw7msJJv1MX8LWyMQC5L6YNYHDfbPF1q5L4i8j
8mRex97UVokJQRRA452V2vCO6S5ETgpnad36de3MUxHgCOX3qL382Qx9/THVmbma
3YfRAoGAUxL/Eu5yvMK8SAt/dJK6FedngcM3JEFNplmtLYVLWhkIlNRGDwkg3I5K
y18Ae9n7dHVueyslrb6weq7dTkYDi3iOYRW8HRkIQh06wEdbxt0shTzAJvvCQfrB
jg/3747WSsf/zBTcHihTRBdAv6OmdhV4/dD5YBfLAkLrd+mX7iE=
-----END RSA PRIVATE KEY-----",
        )
        .expect("Failed to build private key");

        let req = request::Builder::new()
            .method(Method::GET)
            .uri("/foo")
            .header(header::DATE, "Tue, 07 Jun 2014 20:51:35 GMT")
            .body(())
            .expect("couldn't build request");
        let (parts, _) = req.into_parts();

        // sign
        let http_signature = HttpSignatureBuilder::new()
            .key_id("my-rsa-key")
            .signature_method(
                &priv_key,
                SignatureAlgorithm::RsaPkcs1v15(HashAlgorithm::SHA2_256),
            )
            .request_target()
            .created(1402170695)
            .generate_signing_string_using_http_request(&parts)
            .http_header("Date")
            .legacy()
            .build()
            .expect("build http signature");

        const HTTP_SIGNATURE_LEGACY: &str = "Signature keyId=my-rsa-key,created=1402170695,\
        headers=(request-target) (created) date,\
        signature=bw579lDtTDsp7zif_F7Fy93KXrM6qUfCb43JMJtiL4-3nazIPlxcxVsRJEgZzK_QQPDoeUQp4B\
        YCzi2CbthYhHJMn_Wv008gNMcQQTuEw_KcnMrFWxqqUnVZQbCQvNai2y80WrBiOFZvN2VIdLUSO4SoIaOHvr\
        vEoQhl3sqpv1z7yCVbQtJHwnPOWoy_11p-SU3X2ARJXN555q5wSn-DykM0Ohq1cXD84MHXP5ulI0Fa84zQ5w\
        axoXsieex4FI-zXSlngGmchBPXMUC437u2wXA1zLA4KGUL_uNScL1MKrTMqgV0MK4o6sR0LHOqHmIiMJ7h--\
        UmOW_0Iw74CL2UGQ";

        assert_eq!(http_signature.to_string(), HTTP_SIGNATURE_LEGACY);
    }

    /// Fix [picky] test `http::http_signature::tests::sign`
    #[test]
    fn pr_351_sign() {
        let private_key = PrivateKey::from_pem_str(
            "-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAnzyis1ZjfNB0bBgKFMSvvkTtwlvBsaJq7S5wA+kzeVOVpVWw
kWdVha4s38XM/pa/yr47av7+z3VTmvDRyAHcaT92whREFpLv9cj5lTeJSibyr/Mr
m/YtjCZVWgaOYIhwrXwKLqPr/11inWsAkfIytvHWTxZYEcXLgAXFuUuaS3uF9gEi
NQwzGTU1v0FqkqTBr4B8nW3HCN47XUu0t8Y0e+lf4s4OxQawWD79J9/5d3Ry0vbV
3Am1FtGJiJvOwRsIfVChDpYStTcHTCMqtvWbV6L11BWkpzGXSW4Hv43qa+GSYOD2
QU68Mb59oSk2OB+BtOLpJofmbGEGgvmwyCI9MwIDAQABAoIBACiARq2wkltjtcjs
kFvZ7w1JAORHbEufEO1Eu27zOIlqbgyAcAl7q+/1bip4Z/x1IVES84/yTaM8p0go
amMhvgry/mS8vNi1BN2SAZEnb/7xSxbflb70bX9RHLJqKnp5GZe2jexw+wyXlwaM
+bclUCrh9e1ltH7IvUrRrQnFJfh+is1fRon9Co9Li0GwoN0x0byrrngU8Ak3Y6D9
D8GjQA4Elm94ST3izJv8iCOLSDBmzsPsXfcCUZfmTfZ5DbUDMbMxRnSo3nQeoKGC
0Lj9FkWcfmLcpGlSXTO+Ww1L7EGq+PT3NtRae1FZPwjddQ1/4V905kyQFLamAA5Y
lSpE2wkCgYEAy1OPLQcZt4NQnQzPz2SBJqQN2P5u3vXl+zNVKP8w4eBv0vWuJJF+
hkGNnSxXQrTkvDOIUddSKOzHHgSg4nY6K02ecyT0PPm/UZvtRpWrnBjcEVtHEJNp
bU9pLD5iZ0J9sbzPU/LxPmuAP2Bs8JmTn6aFRspFrP7W0s1Nmk2jsm0CgYEAyH0X
+jpoqxj4efZfkUrg5GbSEhf+dZglf0tTOA5bVg8IYwtmNk/pniLG/zI7c+GlTc9B
BwfMr59EzBq/eFMI7+LgXaVUsM/sS4Ry+yeK6SJx/otIMWtDfqxsLD8CPMCRvecC
2Pip4uSgrl0MOebl9XKp57GoaUWRWRHqwV4Y6h8CgYAZhI4mh4qZtnhKjY4TKDjx
QYufXSdLAi9v3FxmvchDwOgn4L+PRVdMwDNms2bsL0m5uPn104EzM6w1vzz1zwKz
5pTpPI0OjgWN13Tq8+PKvm/4Ga2MjgOgPWQkslulO/oMcXbPwWC3hcRdr9tcQtn9
Imf9n2spL/6EDFId+Hp/7QKBgAqlWdiXsWckdE1Fn91/NGHsc8syKvjjk1onDcw0
NvVi5vcba9oGdElJX3e9mxqUKMrw7msJJv1MX8LWyMQC5L6YNYHDfbPF1q5L4i8j
8mRex97UVokJQRRA452V2vCO6S5ETgpnad36de3MUxHgCOX3qL382Qx9/THVmbma
3YfRAoGAUxL/Eu5yvMK8SAt/dJK6FedngcM3JEFNplmtLYVLWhkIlNRGDwkg3I5K
y18Ae9n7dHVueyslrb6weq7dTkYDi3iOYRW8HRkIQh06wEdbxt0shTzAJvvCQfrB
jg/3747WSsf/zBTcHihTRBdAv6OmdhV4/dD5YBfLAkLrd+mX7iE=
-----END RSA PRIVATE KEY-----",
        )
        .expect("Failed to build private key");

        let http_signature_builder = HttpSignatureBuilder::new();
        http_signature_builder
            .key_id("my-rsa-key")
            .signature_method(
                &private_key,
                SignatureAlgorithm::RsaPkcs1v15(HashAlgorithm::SHA2_256),
            )
            .request_target()
            .created(1402170695)
            .http_header("Date");

        let req = request::Builder::new()
            .method(Method::GET)
            .uri("/foo")
            .header(header::DATE, "Tue, 07 Jun 2014 20:51:35 GMT")
            .header(header::CACHE_CONTROL, "max-age=60") // unused for signature
            .header(header::CACHE_CONTROL, "must-revalidate") // unused for signature
            .body(())
            .expect("couldn't build request");
        let (parts, _) = req.into_parts();

        let http_signature = http_signature_builder
            .clone()
            .generate_signing_string_using_http_request(&parts)
            .build()
            .expect("couldn't generate http signature");
        let http_signature_str = http_signature.to_string();

        const HTTP_SIGNATURE_EXAMPLE: &str = r#"Signature keyId="my-rsa-key",algorithm="rsa-sha256",created=1402170695,headers="(request-target) (created) date",signature="bw579lDtTDsp7zif/F7Fy93KXrM6qUfCb43JMJtiL4+3nazIPlxcxVsRJEgZzK/QQPDoeUQp4BYCzi2CbthYhHJMn/Wv008gNMcQQTuEw/KcnMrFWxqqUnVZQbCQvNai2y80WrBiOFZvN2VIdLUSO4SoIaOHvrvEoQhl3sqpv1z7yCVbQtJHwnPOWoy/11p+SU3X2ARJXN555q5wSn+DykM0Ohq1cXD84MHXP5ulI0Fa84zQ5waxoXsieex4FI+zXSlngGmchBPXMUC437u2wXA1zLA4KGUL/uNScL1MKrTMqgV0MK4o6sR0LHOqHmIiMJ7h++UmOW/0Iw74CL2UGQ==""#;

        assert_eq!(http_signature_str, HTTP_SIGNATURE_EXAMPLE);
    }

    #[test]
    #[allow(unstable_name_collisions)]
    fn test_sig_algo() {
        let http_signature = r#"keyId="http://localhost:3000/users/admin#main-key",algorithm="rsa-sha256",headers="host date digest content-type (request-target)",signature="Z8A9c1fqoC9lYbWHE98V4eVENKnMgQxnbNxkbN8zsQ6YTbrfCKJJA8bII+4GMmOX8v+macXhBviSyak/SeZTUuLvuvffL++4x/Sro3wSJCgFWUJrQzlBso1Qaly0U9AzRs6NrRQJRu+rBZIPvSq7tFtm1diVBWiAS5bR0/awrcBM9DLzjSGBN3WfT4PJbUFcmhdmx7TOGPuiF7cFhxMlsWc4My60gOiKqaDup6Aanqu8qxthOSiI2Tdnj846fyRFGNTBItQIMa2sMV336qbpqWscCKGJAT7r2kwAnq5gSy/7EbxMNK8S3fgm5bp07GgNUGfDkWfQws7FKWZL/egZ6A==""#;

        let parsed_http_signature = http_signature
            .parse::<HttpSignature>()
            .expect("Failed to parse http_signature");

        let request = Request::builder()
            .method(Method::POST)
            .uri("https://indiemark.local/users/sp1ff/inbox")
            .header(header::HOST, "indiemark.local")
            .header(header::DATE, "Sat, 01 Mar 2025 18:18:42 GMT")
            .header(
                http::HeaderName::from_str("digest").unwrap(),
                "SHA-256=cfmNH1ExF3RxxTnZW7sCRhHAFkVGuCY9gkX31bbmdb8=",
            )
            .header(header::CONTENT_TYPE, "application/activity+json")
            .header(
                header::USER_AGENT,
                "Mastodon/4.4.0-alpha.2 (http.rb/5.2.0; +http://localhost:3000/)",
            )
            .body(())
            .expect("Failed to build request");

        let (parts, _) = request.into_parts();

        let signing_string = parsed_http_signature
            .headers
            .iter()
            .map(|header| match header {
                picky::http::http_signature::Header::Name(name) => {
                    eprintln!("Retrieving header {}", name);
                    format!(
                        "{}: {}",
                        name.to_lowercase(),
                        parts.headers.get(name).unwrap().to_str().unwrap()
                    )
                }
                picky::http::http_signature::Header::RequestTarget => {
                    format!(
                        "(request-target): {} {}",
                        parts.method.as_str().to_lowercase(),
                        parts.uri.path()
                    )
                }
                _ => panic!("Should never be..."),
            })
            .intersperse("\n".to_owned())
            .collect::<Vec<String>>()
            .concat();
        eprintln!("foo signing_string: ``{}''", signing_string);

        let priv_key = PrivateKey::from_pem_str(
            r#"-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAsRil1toAIu3JaQXPFTL1zm9AeEsgi2lvgSDfUik2M2+cdWlR
hSxoj1FCHkE+nI+jsb9kaClsfXql+rxP9GRawnzfEWDTrvnZJm3b03vtzFYePQkp
txf6AFkq4PPMjva9s2FnBVOA9O7lwTaRODc6YuqmOjet9Ukow+buF4LIrT0qjJM/
g5BeBiyQPRN2oNC76/nevncAW5COl6iNkrs5oweF35E5LYRnQkZSSqniiGtWENVb
3v5YlLzaKOpizB/NIWgbkybAaDoacetD37XRX4pyyUQw9UQU64TXTMgXdTkqJQBh
fCLbXkws9OPD9ZJ6+yCr4GOhOjP9K+YmlTAqZQIDAQABAoIBAE+AniqW0Urc7vdL
2UxOBVV4Ujvulhyf56PTiw3KXH+40kdPUX50kjgMpZ8JcT89qKIKJtwwBHlMTykB
0u8HAkufXVDi5AUAOwDqfq4xS27ZFlgWPfjpNZ1kakB0kld342+dAW+9rf3HJbOY
eJcBDCAN8TPffSPJhRh1GlvJpadHNsnlSmC/oqCX1S0jGgqaD3cbC2q/53NA7ed0
Y7F88hXSNcMopeUi7U6rJhc/TbCx0CKnzJ6JFmnFztsHJyZpaw4kODzDCUogHoXJ
JIixF0ptVhwOZvkNzcPP6PrQCoOsyRTzeIcLZTIBr++46x82vVl9aGzVaEzShnlY
GKe3w70CgYEA75WvXUNerLQRYt3L98CibfMZUbJPF79Pk+WDwEK2OGWSgnsfp5Yc
/vJJoNyfsYZZX3CWdWEWyGiKJjQcm8dApnB1ceHMRMD4fZQWfFJLnn2n0OeD5C9e
oAUuezNe4p7AUYPrUIbc9w047XAf/cZKiCUCSQ3AFEpqvZWbc21OAjMCgYEAvTrq
fo9broRVptnRGsdmFnuVSgX+GyMjODXjGPHvlgZrQ0PhQfoIy3MTSJ6fz26dNhSJ
6V54K4SBW2D4HcHOWodK4/Kr5nxxSAetsMzOjPZLAhIpGz3PeFlK6SP5X/a3YmPW
hHIO/lR6eMT2xg2S06hjDqLM/laaEUnsrIbseQcCgYBlcFcDgdbAAK2r0oTdrS4Y
p2j88iYSw+mJkQ+rg5NrZXYW0NKiPiiguSz7cu4aV+vXQPAzWpwu1jRH4KCMRFzX
G55eTWATbDDJ2r16fc2OmV1IUf3By1yhHBCGEUYHZXfAC77CJZfA8lQ8E9E3vZEo
+6JwE+ZTsP5orsNWp9zziQKBgC2V/9+1UXTdVHT5jDJTTvijlPdcMjb/ZACqmqbr
wbf2m+h8dcubHHtGoaKg9AbYsu7QS9j4dSKmrTMCTUN96OROK3B2iYrg97lOgD1T
WX8D5lX9YgG6Bj0L5cv8apr/qHX/bzJA9/O1DjwB1yEnK/PpYNOpzJCI9Fyt3mJB
rr4JAoGBAKSlpzy98O9Wft877YtVExzFw/3rABbMH3k5IYjGxbXzZhi++QXjeoRb
m1qReygjchDqyaxLa2OIsj4hac68nbKfWi8HJIeZctIgoNlEPGM1gFU+44idiI9z
MUxws+swpG74UWQftsbgcsOtGA25OiU5mT9m1pYUqvQCMgayVKz4
-----END RSA PRIVATE KEY-----"#,
        )
        .expect("Failed to build signing key");

        let rsa_key =
            RsaPrivateKey::try_from(&priv_key).expect("Failed to build an RSA private key");

        use rsa::signature::SignatureEncoding;
        use rsa::signature::Signer;
        let parsed_http_signature = pkcs1v15::SigningKey::<sha2::Sha256>::new(rsa_key)
            .try_sign(signing_string.as_bytes())
            .expect("Failed to sign")
            .to_vec();
        let b64 = BASE64_STANDARD.encode(parsed_http_signature.clone());
        eprintln!("Base64 encoded signature: {}", b64);

        let pub_key = PublicKey::from_pem_str(
            "-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsRil1toAIu3JaQXPFTL1
zm9AeEsgi2lvgSDfUik2M2+cdWlRhSxoj1FCHkE+nI+jsb9kaClsfXql+rxP9GRa
wnzfEWDTrvnZJm3b03vtzFYePQkptxf6AFkq4PPMjva9s2FnBVOA9O7lwTaRODc6
YuqmOjet9Ukow+buF4LIrT0qjJM/g5BeBiyQPRN2oNC76/nevncAW5COl6iNkrs5
oweF35E5LYRnQkZSSqniiGtWENVb3v5YlLzaKOpizB/NIWgbkybAaDoacetD37XR
X4pyyUQw9UQU64TXTMgXdTkqJQBhfCLbXkws9OPD9ZJ6+yCr4GOhOjP9K+YmlTAq
ZQIDAQAB
-----END PUBLIC KEY-----
",
        )
        .expect("Failed to build public key");

        let rsa_key = RsaPublicKey::try_from(&pub_key).expect("Failed to build an RSA public key");
        use rsa::signature::Verifier;
        let signature = pkcs1v15::Signature::try_from(parsed_http_signature.as_slice())
            .expect("Failed to build an RSA signature");
        let result = pkcs1v15::VerifyingKey::<sha2::Sha256>::new(rsa_key)
            .verify(signing_string.as_bytes(), &signature);
        assert!(result.is_ok());
    }

    /// Validate `sign_request`
    #[tokio::test]
    #[allow(unstable_name_collisions)]
    async fn test_signature_generation() {
        // Let's build-up a test request:
        let body = "{\"@context\":[\"https://www.w3.org/ns/activitystreams\",\"https://w3id.org/security/v1\"],\"actor\":\"https://indiemark.local/users/sp1ff\",\"id\":\"https://indiemark.local/accepts/84b4ea535fb143dcba6cea35f258d03b\",\"object\":{\"actor\":\"http://localhost:3000/users/admin\",\"id\":\"http://localhost:3000/5480e6c4-66c4-472e-be04-3f2c6863d585\",\"object\":\"https://indiemark.local/users/sp1ff\"},\"type\":\"Accept\"}";
        // base64-encoded SHA-256 hash is: z5IYT+RvuPyi43aFGmvm0R/GpGYo8ItyZapm39loLfA=
        let mut hasher = sha2::Sha256::new();
        hasher.update(body.as_bytes());
        let digest_value = HeaderValue::from_str(&format!(
            "sha-256={}",
            BASE64_STANDARD.encode(hasher.finalize().as_slice())
        ))
        .unwrap();
        let body: reqwest::Body = body.into();
        let req = http::request::Request::builder()
            .method(Method::POST)
            .uri("http://localhost/users/admin/inbox".parse::<Uri>().unwrap())
            .header(header::CONTENT_TYPE, "application/activity+json")
            .header(header::DATE, "Mon, 03 Mar 2025 14:00:08 GMT")
            .header("digest", digest_value)
            .header(header::HOST, "localhost")
            .body(body)
            .unwrap();

        let priv_key = PrivateKey::from_pem_str("-----BEGIN RSA PRIVATE KEY-----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-----END RSA PRIVATE KEY-----").unwrap();

        // Now, sign it with my production code.
        let (request, http_signature) =
            sign_request(req, "https://indiemark.local/users/sp1ff", &priv_key).unwrap();

        eprintln!("{:#?}", http_signature);

        // Now, compute it "by hand"
        let signing_string = http_signature
            .headers
            .iter()
            .map(|header| match header {
                picky::http::http_signature::Header::Name(name) => {
                    format!(
                        "{}: {}",
                        name.to_lowercase(),
                        request.headers().get(name).unwrap().to_str().unwrap(),
                    )
                }
                picky::http::http_signature::Header::RequestTarget => {
                    format!(
                        "(request-target): {} {}",
                        request.method().as_str().to_lowercase(),
                        request.uri().path()
                    )
                }
                _ => panic!("Should never be..."),
            })
            .intersperse("\n".to_owned())
            .collect::<Vec<String>>()
            .concat();

        eprintln!("foo signing_string: ``{}''", signing_string);

        // (request-target): post /users/admin/inbox
        // content-type: application/activity+json
        // date: Mon, 03 Mar 2025 14:00:08 GMT
        // digest: sha-256=z5IYT+RvuPyi43aFGmvm0R/GpGYo8ItyZapm39loLfA=
        // host: localhost

        // Sending accept request: Request {
        //     method: POST,
        //     url: Url {
        //         scheme: "http",
        //         cannot_be_a_base: false,
        //         username: "",
        //         password: None,
        //         host: Some(
        //             Domain(
        //                 "localhost",
        //             ),
        //         ),
        //         port: Some(
        //             3000,
        //         ),
        //         path: "/users/admin/inbox",
        //         query: None,
        //         fragment: None,
        //     },
        //     headers: {
        //         "content-type": "application/activity+json",
        //         "date": "Mon, 03 Mar 2025 14:00:08 GMT",
        //         "host": "localhost",
        //         "signature": "keyId=\"https://indiemark.local/users/sp1ff\",algorithm=\"rsa-sha256\",headers=\"(request-target) content
        // -type date digest host\",signature=\"MLDDwQVbbV1Z51HZmfiNgjrfOgeb1WiGUkrRfXufHvyFVqqiiWs6zznn0coho9ddqLdeoKXHcMGz+ZgzCRVnTu3fXA
        // KdQoAl3QI5MfD35vU5gSS45TeWzH1QBZYu809pyqvxeuw4E82QjV6xkCGjl4OnBOlOgbGHqkmpIxMfi54OKyNsusEHGTrjXkIJF3MmY8uyq2pVW4ZAEmANmCOujipuL
        // yQXKjm+cLbAZrZXBCEx4g4trZ+8I8ZgrfG+KIadgQRNzlePQX2JuCNXpL9WsDwMsC3rqCpaMDgrCDHp1MigO4kRMzMQM4RLs6B9tg8V6DCb23VENTHbDXweiWjVn/yY
        // +QIGFih/ngBcqA6O1ck5QPSO9Lls9ZTy8LsKZ/8ADwXyCPlZY//g38Q8DzcxIzpboeadshOwiVYo2RXywxN/n5CAeCGHdfTqOYpXr7nTBfpMIitW5KyTaZ4dZu80TOf
        // lRxdPzr2H9SmCD6mbcm1a8FYYLzpQaKe9NI6l86CRgtscf6Z1gjxfCzB549zzkIZOoalKu16ChRdD9r9jzWFERALKXgU7+uF5wcF7CCwTEnVYYWFHmz8IyRkbouqMfn
        // fRu+9YuUOQFQnAkL1P2hkQLT6M1fdUOXwsd2LOghQBu8ZRpidP/pTLMuwF/ep2BiUL3zYfU8Ow8rW/4MGuG2k=\"",
        //         "digest": "sha-256=z5IYT+RvuPyi43aFGmvm0R/GpGYo8ItyZapm39loLfA=",
        //     },
        // }

        // "{\"error\":\"Verification failed for sp1ff@indiemark.local https://indiemark.local/users/sp1ff using rsa-sha256 (RSASSA-PKCS1-v1_5 with SHA-256)\",\"signed_string\":\"(request-target): post /users/admin/inbox\\ncontent-type: application/activity+json\\ndate: Mon, 03 Mar 2025 14:00:08 GMT\\ndigest: sha-256=z5IYT+RvuPyi43aFGmvm0R/GpGYo8ItyZapm39loLfA=\\nhost: localhost\",\"signature\":\"MLDDwQVbbV1Z51HZmfiNgjrfOgeb1WiGUkrRfXufHvyFVqqiiWs6zznn0coho9ddqLdeoKXHcMGz+ZgzCRVnTu3fXAKdQoAl3QI5MfD35vU5gSS45TeWzH1QBZYu809pyqvxeuw4E82QjV6xkCGjl4OnBOlOgbGHqkmpIxMfi54OKyNsusEHGTrjXkIJF3MmY8uyq2pVW4ZAEmANmCOujipuLyQXKjm+cLbAZrZXBCEx4g4trZ+8I8ZgrfG+KIadgQRNzlePQX2JuCNXpL9WsDwMsC3rqCpaMDgrCDHp1MigO4kRMzMQM4RLs6B9tg8V6DCb23VENTHbDXweiWjVn/yY+QIGFih/ngBcqA6O1ck5QPSO9Lls9ZTy8LsKZ/8ADwXyCPlZY//g38Q8DzcxIzpboeadshOwiVYo2RXywxN/n5CAeCGHdfTqOYpXr7nTBfpMIitW5KyTaZ4dZu80TOflRxdPzr2H9SmCD6mbcm1a8FYYLzpQaKe9NI6l86CRgtscf6Z1gjxfCzB549zzkIZOoalKu16ChRdD9r9jzWFERALKXgU7+uF5wcF7CCwTEnVYYWFHmz8IyRkbouqMfnfRu+9YuUOQFQnAkL1P2hkQLT6M1fdUOXwsd2LOghQBu8ZRpidP/pTLMuwF/ep2BiUL3zYfU8Ow8rW/4MGuG2k=\"}"

        // Signing string in the error message:
        // (request-target): post /users/admin/inbox
        // content-type: application/activity+json
        // ndate: Mon, 03 Mar 2025 14:00:08 GMT
        // digest: sha-256=z5IYT+RvuPyi43aFGmvm0R/GpGYo8ItyZapm39loLfA=
        // host: localhost
        let rsa_key =
            RsaPrivateKey::try_from(&priv_key).expect("Failed to build an RSA private key");

        use rsa::signature::SignatureEncoding;
        use rsa::signature::Signer;
        let signature = pkcs1v15::SigningKey::<sha2::Sha256>::new(rsa_key)
            .try_sign(signing_string.as_bytes())
            .expect("failed to sign")
            .to_vec();
        let b64 = BASE64_STANDARD.encode(signature.clone());

        assert_eq!(b64, http_signature.signature);
    }
}
