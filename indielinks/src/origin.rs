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

//! indielinks-specific Type Representing an URL Origin
//!
//! # URLs
//!
//! ## Introduction
//!
//! Ah, the internet. The URL was first (as far as I know) standardized in [RFC1738] in 1994: "This
//! document describes the syntax and semantics for a compact string representation for a resource
//! available via the Internet. These strings are called 'Uniform Resource Locators' (URLs)." This
//! definition [saw](https://www.rfc-editor.org/rfc/rfc1808)
//! [several](https://www.rfc-editor.org/rfc/rfc2396)
//! [additions](https://www.rfc-editor.org/rfc/rfc2732) over the years. It was updated in 2005 by
//! [RFC3986] in which the venerable URL was generalized to "URI" (Uniform Resource Identifier); the
//! URL became a subset of URIs (alongside URNs, or Uniform Resource Names). [RFC3987] came out at
//! the same time, defining the IRI (Internationalized Resource Identifier) "as a complement to the
//! ...(URI)." The [URL Standard] appears to be a community-based standardization effort with the
//! goal of "align\[ing\] RFC 3986 and RFC 3987 with contemporary implementations and obsolete the
//! RFCs in the process" (among others). I was unaware of the [URL Standard] until now, but this is
//! what the [Url] crate cites as its specification.
//!
//! [RFC1738]: https://www.rfc-editor.org/rfc/rfc1738
//! [RFC3986]: https://www.rfc-editor.org/rfc/rfc3986
//! [RFC3987]: https://www.rfc-editor.org/rfc/rfc3987
//! [URL Standard]: https://url.spec.whatwg.org/
//!
//! ## URL Components
//!
//! This is a diagram describing the different components of a sample URL. Labels that are
//! capitalized are found in both [RFC3986] and the [URL Standard] (with the same definition in each
//! case). Labels that are not capitalized represent common usage; at any rate, they are the terms
//! used in [indielinks].
//!
//! [indielinks]: crate
//!
//! ```text
//! foo://user:pass@sub.domain.com:80/pa/th?query=value#frag
//! ---   ---- ---- --- ------ --- -- ----- ----------- ----
//!  ^     ^    ^    ^     ^    ^   ^   ^        ^        ^
//!  |     |    |    |     |    |   |   |        |        |
//! Scheme |    | subdomain|   TLD  |  Path    Query   Fragment
//!        |  password    root      |
//!    username          domain    Port
//!
//!       |       | |    Host    |
//!       +-------+ +------------+
//!       Userinfo  |    netloc     |
//!                 +---------------+
//!       |        Authority        |
//!       +-------------------------+
//! |            origin             |
//! +-------------------------------+
//! ```
//!
//! Of particular note is the `netloc` component, meaning the combination of the Host & Port
//! components (i.e. what one needs in order to connect a socket). The term `netloc` originated
//! in [RFC1808] (where it included optional credentials), but has since been deprecated, so
//! I'm going to appropriate it here.
//!
//! [RFC1808]: https://www.rfc-editor.org/rfc/rfc1808.html#section-2.1
//!
//! ## The Need for a Custom Origin Type
//!
//! [indielinks] frequently needs to form URLs naming entities such as [ActivityPub] [Actor]s,
//! public keys, user inboxes & so forth. For this, we need to express the "origin" portion of the
//! URL at which the [indielinks] instance can be reached from the public internet: scheme, host &
//! an optional port. I _could_ just use the [Url] class, which does offer an [Origin] abstraction.
//! That's less than ideal for a few reasons:
//!
//! [ActivityPub]: https://www.w3.org/TR/activitypub/
//! [Actor]: https://www.w3.org/TR/activitystreams-core/#actors
//! [Origin]: url::Origin
//!
//! - [Origin] represents "opaque" origins, which aren't salient here (i.e. Url's type is too broad)
//!
//! - [Origin] represents the scheme as an arbitrary [String], whereas [indielinks] is only
//!   concerned with http & https
//!
//! - [Origin] represents the [Host] in terms of [String], rather than a [refined type]
//!
//! [Host]: url::Host
//! [refined type]: https://en.wikipedia.org/wiki/Refinement_type

use std::{
    convert::{AsRef, TryFrom},
    fmt::Display,
    net::{Ipv4Addr, Ipv6Addr},
    ops::Deref,
    str::FromStr,
};

use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{prelude::*, Backtrace};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to parse {text} as a host: {source}"))]
    HostParse {
        text: String,
        source: url::ParseError,
        backtrace: Backtrace,
    },
    #[snafu(display("{text} results in an opaque origin"))]
    OpaqueOrigin { text: String, backtrace: Backtrace },
    #[snafu(display("Failed to parse {text} as an URL: {source}"))]
    OriginUrl {
        text: String,
        source: url::ParseError,
        backtrace: Backtrace,
    },
    #[snafu(display("{text} can't be interepreted as a protocol"))]
    Protocol { text: String, backtrace: Backtrace },
    #[snafu(display("{text} can't be interepreted as a reg-name"))]
    RegName { text: String, backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

pub type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            protocol                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum Protocol {
    #[serde(rename = "http")]
    Http,
    #[serde(rename = "https")]
    Https,
}

impl Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Protocol::Http => "http",
                Protocol::Https => "https",
            }
        )
    }
}

impl FromStr for Protocol {
    type Err = Error;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        let t = s.to_lowercase();
        if &t == "http" {
            Ok(Protocol::Http)
        } else if &t == "https" {
            Ok(Protocol::Https)
        } else {
            ProtocolSnafu { text: s.to_owned() }.fail()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                      indielinks Host type                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

lazy_static! {
    static ref REG_NAME: Regex = Regex::new("^([-._~a-zA-Z0-9!$&'()*+,;=]|%[0-9a-fA-F]{2})+$").unwrap(/* known good */);
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct RegName(String);

impl RegName {
    pub fn new(s: &str) -> Result<RegName> {
        if REG_NAME.find(s).is_none() {
            RegNameSnafu { text: s.to_owned() }.fail()
        } else {
            Ok(RegName(s.to_owned()))
        }
    }
}

impl Display for RegName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for RegName {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        RegName::new(s)
    }
}

impl AsRef<str> for RegName {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl Deref for RegName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<RegName> for String {
    fn from(value: RegName) -> Self {
        value.0
    }
}

/// A refined type representing an [RFC-7565]-compliant host. Well, mostly: we don't support
/// `IpvFuture`.
///
/// [RFC-7565]: https://datatracker.ietf.org/doc/html/rfc7565
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize)]
pub enum Host {
    IpLiteral(Ipv6Addr),
    Ipv4Address(Ipv4Addr),
    RegName(RegName),
}

impl Host {
    /// Validate that `s` complies with [RFC-7565]; return a [Host] instance.
    ///
    /// [RFC-7565]: https://datatracker.ietf.org/doc/html/rfc7565
    pub fn new(s: &str) -> Result<Host> {
        match url::Host::parse(s).context(HostParseSnafu { text: s.to_owned() })? {
            url::Host::Domain(s) => Ok(Host::RegName(RegName::new(&s)?)),
            url::Host::Ipv4(ipv4_addr) => Ok(Host::Ipv4Address(ipv4_addr)),
            url::Host::Ipv6(ipv6_addr) => Ok(Host::IpLiteral(ipv6_addr)),
        }
    }
}

impl Display for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Host::IpLiteral(ipv6) => write!(f, "{}", ipv6),
            Host::Ipv4Address(ipv4) => write!(f, "{}", ipv4),
            Host::RegName(reg_name) => write!(f, "{}", reg_name),
        }
    }
}

impl FromStr for Host {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Host::new(s)
    }
}

impl TryFrom<url::Host> for Host {
    type Error = Error;

    fn try_from(value: url::Host) -> std::result::Result<Self, Self::Error> {
        match value {
            url::Host::Domain(s) => Ok(Host::RegName(RegName::new(&s)?)),
            url::Host::Ipv4(ipv4) => Ok(Host::Ipv4Address(ipv4)),
            url::Host::Ipv6(ipv6) => Ok(Host::IpLiteral(ipv6)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             Origin                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// [indielinks] Origin
///
/// [indielinks]: crate
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Origin {
    scheme: Protocol,
    host: Host,
    port: Option<u16>,
}

impl Origin {
    pub fn host(&self) -> &Host {
        &self.host
    }
}

impl Display for Origin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.port {
            Some(port) => write!(f, "{}://{}:{}", self.scheme, self.host, port),
            None => write!(f, "{}://{}", self.scheme, self.host),
        }
    }
}

impl FromStr for Origin {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Url::parse(s)
            .context(OriginUrlSnafu { text: s.to_owned() })?
            .try_into()
    }
}

// This is a bit of an experiment for me. I wanted to write ctors that would express both "make an
// Origin out of these borrows" and "move these values into an Origin". It occured to me that `From`
// could handle both semantics.
impl From<(Protocol, Host, u16)> for Origin {
    fn from(value: (Protocol, Host, u16)) -> Self {
        Origin {
            scheme: value.0,
            host: value.1,
            port: Some(value.2),
        }
    }
}

impl From<(Protocol, Host)> for Origin {
    fn from(value: (Protocol, Host)) -> Self {
        Origin {
            scheme: value.0,
            host: value.1,
            port: None,
        }
    }
}

impl From<&(Protocol, Host, u16)> for Origin {
    fn from(value: &(Protocol, Host, u16)) -> Self {
        Origin {
            scheme: value.0,
            host: value.1.clone(),
            port: Some(value.2),
        }
    }
}

impl From<&(Protocol, Host)> for Origin {
    fn from(value: &(Protocol, Host)) -> Self {
        Origin {
            scheme: value.0,
            host: value.1.clone(),
            port: None,
        }
    }
}

impl TryFrom<Url> for Origin {
    type Error = Error;

    fn try_from(value: Url) -> std::result::Result<Self, Self::Error> {
        match value.origin() {
            url::Origin::Opaque(_) => OpaqueOriginSnafu {
                text: value.as_str().to_owned(),
            }
            .fail(),
            url::Origin::Tuple(scheme, host, port) => Ok(Origin {
                scheme: scheme.as_str().parse::<Protocol>().unwrap(),
                host: host.try_into()?,
                port: Some(port),
            }),
        }
    }
}

impl TryFrom<String> for Origin {
    type Error = Error;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Origin::from_str(&value)
    }
}
