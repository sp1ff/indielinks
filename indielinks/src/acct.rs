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

//! # acct URI
//!
//! Support for the [acct] URI scheme as implemented in indielinks. The resource identified by an
//! [acct] URI "is a user account hosted at a service provider, where the service provider is
//! typically associated with a DNS domain name." The scheme is intended for identification, not
//! interaction, and so the protocol in which it used used ([WebFinger], in our case) is responsible
//! for specifying how it is used.
//!
//! [acct]: https://datatracker.ietf.org/doc/html/rfc7565
//! [WebFinger]: https://www.rfc-editor.org/rfc/rfc7033
//!
//! # Syntax per the RFC
//!
//! The [RFC] [provides](https://datatracker.ietf.org/doc/html/rfc7565#section-7) a grammer for
//! `acct` URIs in BNF:
//!
//! ```text
//! acctURI     = "acct" ":" userpart "@" host
//! userpart    = (unreserved | sub-delims) 0*(unreserved | pct-encoded | sub-delims )
//! unreserved  = ALPHA | DIGIT | "-" | "." | "_" | "~"
//! sub-delims  = "!" | "$" | "&" | "'" | "(" | ")" | "*" | "+" | "," | ";" | "="
//! pct-encoded = "%" HEXDIG HEXDIG
//! host        = IP-literal / IPv4address / reg-name
//! IP-literal  = "[" ( IPv6address / IPvFuture  ) "]"
//! IPvFuture   = "v" 1*HEXDIG "." 1*( unreserved / sub-delims / ":" )
//! IPv4address = dec-octet "." dec-octet "." dec-octet "." dec-octet
//! dec-octet   = DIGIT                 ; 0-9
//!               | %x31-39 DIGIT       ; 10-99
//!               | "1" 2DIGIT          ; 100-199
//!               | "2" %x30-34 DIGIT   ; 200-249
//!               | "25" %x30-35        ; 250-255
//! reg-name    = *( unreserved / pct-encoded / sub-delims )
//! ```
//!
//! [RFC]: https://datatracker.ietf.org/doc/html/rfc7565
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
//! # Syntax per indielinks
//!
//! In the [WebFinger] protocol, the `acct` URI is used as a query parameter, and hence will
//! presumably *again* be %-encoded, leading in the case of the example above to an HTTP request of:
//!
//! ```text
//! GET /.well-known/webfinger?resource=acct%3Ajuliet%2540capulet.example%40shoppingsite.example
//! ```
//!
//! (notice that the '%' character in the %-encoding of '@' has been replaced with "%25").
//! Presumably, the web service framework in use by the server would carry-out one round of
//! %-decoding & present the application with a parameter of
//! "acct:juliet%40capulet.example@shoppingsite.example". This isn't discussed anywhere (that I can
//! find), but I surmise that it's up to the application to handle the subsequent round of
//! %-decoding. Presumably in this case the application is aware that it's using e-mail addressess
//! to identify users and would be prepared for this.
//!
//! In the case of indielinks, we side-step this by identifying users with identifiers of the form
//! [a-z][-_a-z0-9]*, thereby simplifying parsing of the acct URI considerably.

//! # The acct error type
//!
//! Contra the indielinks app itself, I'm swinging back to my preferred approach of a hand-crafted
//! error enumeration with a few broad failure modes plus context.

use std::{fmt::Display, str::FromStr};

use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use url::Url;

type StdResult<T, E> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    BadUri { text: String },
    BadUserAndHost { text: String },
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::BadUri { text } => write!(f, "Bad URI: {}", text),
            Error::BadUserAndHost { text } => write!(f, "Bad user-and-host: {}", text),
        }
    }
}

type Result<T> = StdResult<T, Error>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Account {
    user: String,
    host: String,
}

impl Display for Account {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "acct:{}@{}", self.user, self.host)
    }
}

lazy_static! {
    static ref USER_AT_HOST: Regex = Regex::new("^([a-z][-_a-z0-9]*)@([-_.a-zA-Z0-9]+)$").unwrap(/* known good */);
    static ref URI_FORMAT: Regex = Regex::new("^acct:([a-z][-_a-z0-9]*)@([-_.a-zA-Z0-9]+)$").unwrap(/* known good */);
}

impl FromStr for Account {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let err = || -> Error {
            Error::BadUri {
                text: String::from(s),
            }
        };
        let caps = URI_FORMAT.captures(s).ok_or(err())?;
        let user = caps.get(1).ok_or(err())?;
        let host = caps.get(2).ok_or(err())?;
        Ok(Account {
            user: String::from(user.as_str()),
            host: String::from(host.as_str()),
        })
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
                Account::from_str(s).map_err(|err| {
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
        serializer.serialize_str(&format!("{}", self))
    }
}

impl Account {
    pub fn from_user_and_host(user: &str, host: &str) -> Account {
        Account {
            user: user.to_string(),
            host: host.to_string(),
        }
    }
    pub fn new(user_and_host: &str) -> Result<Account> {
        let err = || -> Error {
            Error::BadUserAndHost {
                text: String::from(user_and_host),
            }
        };
        let caps = USER_AT_HOST.captures(user_and_host).ok_or(err())?;
        let user = caps.get(1).ok_or(err())?;
        let host = caps.get(2).ok_or(err())?;
        Ok(Account {
            user: String::from(user.as_str()),
            host: String::from(host.as_str()),
        })
    }
    pub fn host(&self) -> &str {
        &self.host
    }
    pub fn user(&self) -> &str {
        &self.user
    }
    // Not sure this really belongs here...
    pub fn home(&self) -> Url {
        Url::parse(&format!("https://{}/~{}", self.host, self.user)).unwrap(/*known good*/)
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    pub fn smoke() {
        // OK
        assert!(Account::new("sp1ff@indiemark.sh").is_ok());
        // Not OK; I mean, it's OK per the RFC, but not for indiemark.
        assert!(Account::new("juliet%40capulet.example@shoppingsite.example").is_err());
        // Test the `Display` implementation:
        assert_eq!(
            format!("{}", Account::new("sp1ff@indiemark.sh").unwrap()),
            "acct:sp1ff@indiemark.sh"
        );
        // Test the `FromStr` implementation
        assert_eq!(
            Account::new("sp1ff@indiemark.sh").unwrap(),
            "acct:sp1ff@indiemark.sh".parse::<Account>().unwrap()
        );
    }
}
