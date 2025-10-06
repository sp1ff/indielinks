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

//! # An Implementation of the `acct` URI
//!
//! This module provides an implementation of the [acct] URI scheme: "acct:user@host". The resource
//! identified by an [acct] URI "is a user account hosted at a service provider, where the service
//! provider is typically associated with a DNS domain name." The scheme is intended for
//! identification, not interaction, and so the protocol in which it is used ([WebFinger], in our
//! case) is responsible for specifying how it is used.
//!
//! [acct]: https://datatracker.ietf.org/doc/html/rfc7565
//! [WebFinger]: https://www.rfc-editor.org/rfc/rfc7033
//!
//! This implementation attempts to be a full & faithful implementation of the [RFC], independent of
//! indielinks. For instance, it will happily accept an [RFC]-compliant username that is not a valid
//! indielinks [Username] (the conditions for which are much more strict). If the username or host
//! are not legitimate by indielinks' lights, that needs to be detected & handled higher up the call
//! stack.
//!
//! [RFC]: https://datatracker.ietf.org/doc/html/rfc7565
//! [Username]: indielinks_shared::entities::Username
//!
//! # Syntax
//!
//! The [RFC] [provides](https://datatracker.ietf.org/doc/html/rfc7565#section-7) a grammar for
//! `acct` URIs in ABNF:
//!
//! ```text
//! acctURI     := "acct" ":" userpart "@" host
//! userpart    := (unreserved | sub-delims) *(unreserved | pct-encoded | sub-delims )
//! unreserved  := ALPHA | DIGIT | "-" | "." | "_" | "~"
//! sub-delims  := "!" | "$" | "&" | "'" | "(" | ")" | "*" | "+" | "," | ";" | "="
//! pct-encoded := "%" HEXDIG HEXDIG
//! host        := IP-literal | IPv4address | reg-name
//! IP-literal  := "[" ( IPv6address | IPvFuture  ) "]"
//! IPv6address :=                            6( h16 ":" ) ls32
//!              |                       "::" 5( h16 ":" ) ls32
//!              | [               h16 ] "::" 4( h16 ":" ) ls32
//!              | [ *1( h16 ":" ) h16 ] "::" 3( h16 ":" ) ls32
//!              | [ *2( h16 ":" ) h16 ] "::" 2( h16 ":" ) ls32
//!              | [ *3( h16 ":" ) h16 ] "::"    h16 ":"   ls32
//!              | [ *4( h16 ":" ) h16 ] "::"              ls32
//!              | [ *5( h16 ":" ) h16 ] "::"              h16
//!              | [ *6( h16 ":" ) h16 ] "::"
//! ls32        := ( h16 ":" h16 ) | IPv4address
//!                ; least-significant 32 bits of address
//! h16         := 1*4HEXDIG
//!                ; 16 bits of address represented in hexadecimal
//! IPvFuture   := "v" 1*HEXDIG "." 1*( unreserved | sub-delims | ":" )
//! IPv4address := dec-octet "." dec-octet "." dec-octet "." dec-octet
//! dec-octet   := DIGIT               ; 0-9
//!              | %x31-39 DIGIT       ; 10-99
//!              | "1" 2DIGIT          ; 100-199
//!              | "2" %x30-34 DIGIT   ; 200-249
//!              | "25" %x30-35        ; 250-255
//! reg-name    := *( unreserved | pct-encoded | sub-delims )
//! ```
//!
//! [RFC]: https://datatracker.ietf.org/doc/html/rfc7565
//!
//! The syntax used in the above augmented Backus-Naur form grammar is specified [here], but a few
//! notes:
//!
//! [here]: https://datatracker.ietf.org/doc/html/rfc2234
//!
//! - `n*m` is used before a grammar element to indicate "from `n` to `m` occurrences"; either
//!   may be omitted (`n` defaulting to 0 and `m` to infinity)
//! - `;` denotes a comment
//! - `%xnn` denotes the ASCII character whose encoding is `nn` in hex
//! - `ALPHA`, `DIGIT` & `HEXDIG` are taken to be understood
//!
//! # Discussion
//!
//! The reader will note that `acct` URIs generally take the form "acct:user@host", but that the
//! user portion may include %-encoded octets. This is discussed in [section
//! 4](https://datatracker.ietf.org/doc/html/rfc7565#page-3) of the RFC where the following
//! motivational example is given:
//!
//! Suppose that "a user with the email address 'juliet@capulet.example' registers with a commerce
//! website whose domain name is 'shoppingsite.example'.  In order to use her email address as the
//! localpart of the `acct` URI, the at-sign character (U+0040) needs to be percent-encoded as
//! described in [RFC3986].  Thus, the resulting `acct` URI would be
//! 'acct:juliet%40capulet.example@shoppingsite.example'."
//!
//! [RFC3986]: https://datatracker.ietf.org/doc/html/rfc3986
//!
//! ## indielinks-specific considerations
//!
//! indielinks uses the `acct` URI scheme in it's [WebFinger] implementation, where the `acct` URI
//! is used as a query parameter, and hence will presumably *again* be %-encoded, leading in the
//! case of the example above to an HTTP request of:
//!
//! ```text
//! GET /.well-known/webfinger?resource=acct%3Ajuliet%2540capulet.example%40shoppingsite.example
//! ```
//!
//! (notice that the '%' character in the %-encoding of '@' has been replaced with "%25").
//! Presumably, the web service framework in use by the server (axum, in our case) will carry-out
//! one round of %-decoding & present the application with a parameter of
//! "acct:juliet%40capulet.example@shoppingsite.example". This isn't discussed anywhere (that I can
//! find), but I surmise that it's up to the application to handle the subsequent round of
//! %-decoding.

use std::{collections::HashSet, fmt::Display, str::FromStr};

use lazy_static::lazy_static;
use pct_str::{Encoder, PctStr, PctString};
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use snafu::{prelude::*, Backtrace};

use crate::{origin::Host, util::exactly_two};

type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    BadUserAndHost {
        text: String,
        backtrace: Backtrace,
    },
    #[snafu(display("Empty hostnames are not permitted in acct URIs"))]
    EmptyHost,
    #[snafu(display("{host} could not be parsed as an acct host: {source}"))]
    Host {
        host: String,
        source: crate::origin::Error,
    },
    #[snafu(display("{text} is not a valid percent-encoded username"))]
    PctEncodedUsername {
        text: String,
        backtrace: Backtrace,
    },
    #[snafu(display("{text} is not correctly percent-encoded: {source}"))]
    PctStr {
        text: String,
        source: pct_str::InvalidPctString<String>,
    },
    #[snafu(display("{text} cannot be interpreted as an acct URI"))]
    Uri {
        text: String,
        backtrace: Backtrace,
    },
    #[snafu(display("{text} is not a valid username"))]
    Username {
        text: String,
        backtrace: Backtrace,
    },
}

type Result<T> = StdResult<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            Username                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

lazy_static! {
    // Regex matching a percent-encoded username portion of an `acct` URI
    static ref USER_PCT_ENCODED: Regex = Regex::new("^[-._~a-zA-Z0-9!$&'()*+.;=]([-._~a-zA-Z0-9!$&'()*+.;=]|%[0-9a-fA-F]{2})*$")
        .unwrap(/* known good */);
    // Regex matching the username portion of an `acct` URI
    static ref USER: Regex = Regex::new("^[-._~a-zA-Z0-9!$&'()*+.;=]+$")
        .unwrap(/* known good */);
    // The set of char's that are unreserved or sub-delims
    static ref UNRESERVED_OR_SUB_DELIMS: HashSet<char> = HashSet::from(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
                                                                        'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
                                                                        'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
                                                                        'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
                                                                        'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7',
                                                                        '8', '9', '-', '.', '_', '~', '!', '$', '&', '\'', '(', ')',
                                                                        '*', '+', ',', ';', '=',]);
}

struct UnreservedOrSubdelimsEncoder;

impl Encoder for UnreservedOrSubdelimsEncoder {
    fn encode(&self, c: char) -> bool {
        !UNRESERVED_OR_SUB_DELIMS.contains(&c)
    }
}

/// A refined type representing an [RFC-7565]-compliant username.
///
/// [RFC-7565]: https://datatracker.ietf.org/doc/html/rfc7565
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct Username(String);

impl Username {
    /// Validate that `s` complies with [RFC-7565]; percent-decode it if it does & return a
    /// [Username] instance.
    ///
    /// [RFC-7565]: https://datatracker.ietf.org/doc/html/rfc7565
    pub fn from_percent_encoded(s: &str) -> Result<Username> {
        if USER_PCT_ENCODED.find(s).is_none() {
            return PctEncodedUsernameSnafu { text: s.to_owned() }.fail();
        }
        Ok(Username(
            PctStr::new(s)
                .map_err(|err| err.into_owned())
                .context(PctStrSnafu { text: s.to_owned() })?
                .decode(),
        ))
    }
    /// Create a new [Username] instance from an arbitrary UTF-8 string encoding. Note that
    /// [RFC-7565] imposes constraints on the first character of the username, but after that
    /// non-permitted characters can be percent-encoded.
    ///
    /// [RFC-7565]: https://datatracker.ietf.org/doc/html/rfc7565
    pub fn new(s: &str) -> Result<Username> {
        if USER.find(s).is_none() {
            return UsernameSnafu { text: s.to_owned() }.fail();
        }
        Ok(Username(s.to_owned()))
    }
    /// Percent encode this username
    pub fn encode(&self) -> String {
        PctString::encode(self.0.chars(), UnreservedOrSubdelimsEncoder).into_string()
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

impl AsRef<str> for Username {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<Username> for String {
    fn from(value: Username) -> Self {
        value.0
    }
}

/// An [RFC-7565]-compliant account.
///
/// [RFC-7565]: https://datatracker.ietf.org/doc/html/rfc7565
///
/// In order for this type to work with the axum [Query] extractor, it must deserialize from strings
/// of the form "acct:{percent-encoded-username}@{host}". Therefore, this type hand- implements both
/// [Serialize] & [Deserialize] to work in this format. [Display] & [FromStr], while inverses, work
/// with the more natural "{percent-decoded-username}@{host}" format.
///
/// [Query]: axum::extract::Query
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Account {
    user: Username,
    host: Host,
}

impl Account {
    /// Given a string of the form "user@host", derive an [Account]
    pub fn new(s: &str) -> Result<Account> {
        let (user, host) = exactly_two(s.split('@')).unwrap();
        Ok(Self {
            user: Username::new(user)?,
            host: host.parse::<Host>().context(HostSnafu {
                host: host.to_owned(),
            })?,
        })
    }
    /// Given an `acct` URI of the form "acct:{user}@{host}", attempt to derive an [Account]
    pub fn from_uri(s: &str) -> Result<Account> {
        if !s.starts_with("acct:") {
            return UriSnafu { text: s.to_owned() }.fail();
        }
        let (user, host) = exactly_two(s[5..].split('@')).unwrap();
        Ok(Self {
            user: Username::from_percent_encoded(user)?,
            host: host.parse::<Host>().context(HostSnafu {
                host: host.to_owned(),
            })?,
        })
    }
    pub fn from_user_and_host(user: &str, host: &Host) -> Result<Account> {
        Ok(Account {
            user: Username::new(user)?,
            host: host.clone(),
        })
    }
    pub fn to_uri(&self) -> String {
        format!("acct:{}@{}", self.user.encode(), self.host)
    }
    pub fn host(&self) -> &Host {
        &self.host
    }
    pub fn user(&self) -> &Username {
        &self.user
    }
}

impl Display for Account {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.user, self.host)
    }
}

impl FromStr for Account {
    type Err = Error;

    /// Given a string in the form "user@host", derive an [Account]
    fn from_str(s: &str) -> Result<Self> {
        Account::new(s)
    }
}

impl<'de> Deserialize<'de> for Account {
    fn deserialize<D>(deserializer: D) -> StdResult<Account, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, Unexpected, Visitor};

        struct AccountVisitor;

        impl Visitor<'_> for AccountVisitor {
            type Value = Account;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string representing an Account")
            }

            fn visit_str<E>(self, s: &str) -> StdResult<Self::Value, E>
            where
                E: Error,
            {
                Account::from_uri(s).map_err(|err| {
                    let err_s = format!("{}", err);
                    Error::invalid_value(Unexpected::Str(s), &err_s.as_str())
                })
            }
        }

        deserializer.deserialize_str(AccountVisitor)
    }
}

impl Serialize for Account {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_uri())
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn smoke() {
        // OK
        assert!(Account::new("sp1ff@indiemark.sh").is_ok());
        // Not OK
        assert!(Account::new("juliet%40capulet.example@shoppingsite.example").is_err());
        // OK
        assert!(Account::from_uri("acct:juliet%40capulet.example@shoppingsite.example").is_ok());
        // Test the `Display` implementation:
        assert_eq!(
            format!("{}", Account::new("sp1ff@indiemark.sh").unwrap()),
            "sp1ff@indiemark.sh"
        );
    }

    #[test]
    fn account() {
        let acct = Account::new("sp1ff@indiemark.sh").unwrap();
        let ser = serde_json::to_string(&acct).unwrap();
        assert_eq!(ser, "\"acct:sp1ff@indiemark.sh\"");
        let acct: Account =
            serde_json::from_str("\"acct:juliet%40capulet.example@shoppingsite.example\"").unwrap();
        assert_eq!(acct.user().as_ref(), "juliet@capulet.example")
    }
}
