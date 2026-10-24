// Copyright (C) 2026 Michael Herstine <sp1ff@pobox.com>
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

use std::{
    borrow::Borrow, collections::HashMap, net::SocketAddr, result::Result as StdResult, sync::Arc,
};

use async_trait::async_trait;
use scylla::{
    client::{session::Session as InnerSession, session_builder::SessionBuilder},
    errors::TranslationError,
    policies::address_translator::UntranslatedPeer,
};
use secrecy::ExposeSecret;
use snafu::{Backtrace, ResultExt, Snafu};
use tap::Pipe;

use crate::{credentials::Credentials, exactly_two::exactly_two};

#[derive(Clone, Debug)]
pub struct Translation(pub (SocketAddr, SocketAddr));

impl clap::builder::ValueParserFactory for Translation {
    type Parser = TranslationParser;

    fn value_parser() -> Self::Parser {
        TranslationParser
    }
}

#[derive(Clone, Debug)]
pub struct TranslationParser;

impl clap::builder::TypedValueParser for TranslationParser {
    type Value = Translation;

    fn parse_ref(
        &self,
        _cmd: &clap::Command,
        _arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> std::result::Result<Self::Value, clap::Error> {
        use clap::error::ErrorKind;
        value
            .to_str()
            .ok_or(clap::Error::new(ErrorKind::InvalidValue))?
            .split(',')
            .collect::<Vec<&str>>()
            .into_iter()
            .pipe(exactly_two)
            .map_err(|_| clap::Error::new(ErrorKind::WrongNumberOfValues))?
            .pipe(
                |(from, to)| match (from.parse::<SocketAddr>(), to.parse::<SocketAddr>()) {
                    (Ok(from), Ok(to)) => Ok(Translation((from, to))),
                    (_, _) => Err(clap::Error::new(ErrorKind::InvalidValue)),
                },
            )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       client construction                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Errors encountered while constructing a native ScyllaDB session.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No address translation rule for {address}"))]
    MissingTranslation {
        address: SocketAddr,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to create a ScyllaDB session: {source}"))]
    NewSession {
        #[snafu(source(from(scylla::errors::NewSessionError, Box::new)))]
        source: Box<scylla::errors::NewSessionError>,
        backtrace: Backtrace,
    },
}

pub type Result<T> = StdResult<T, Error>;

/// [Scylla] [AddressTranslator] implementation.
///
/// Scylla allows cluster members to identify one another by different addresses than clients. This
/// is intended to allow clients to work with clusters behind NAT (or similar), but it is also
/// handy for allowing integration tests to work with containerized clusters.
struct Translations {
    addresses: HashMap<SocketAddr, SocketAddr>,
}

impl Translations {
    pub fn new(addresses: impl IntoIterator<Item = (SocketAddr, SocketAddr)>) -> Self {
        Self {
            addresses: addresses.into_iter().collect(),
        }
    }
}

#[async_trait]
impl scylla::policies::address_translator::AddressTranslator for Translations {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> StdResult<SocketAddr, TranslationError> {
        self.addresses
            .get(&untranslated_peer.untranslated_address())
            .ok_or(TranslationError::NoRuleForAddress(
                untranslated_peer.untranslated_address(),
            ))
            .copied()
    }
}

/// Create an instance of the native ScyllaDB SDK client.
pub async fn create_client(
    hosts: impl IntoIterator<Item = impl Borrow<SocketAddr>>,
    credentials: Option<&Credentials>,
    translations: Option<impl IntoIterator<Item = (SocketAddr, SocketAddr)>>,
) -> Result<InnerSession> {
    let mut builder = SessionBuilder::new().known_nodes_addr(hosts);
    if let Some(Credentials((user, pass))) = credentials {
        builder = builder.user(user.expose_secret(), pass.expose_secret())
    }
    if let Some(translations) = translations {
        builder = builder.address_translator(Arc::new(Translations::new(translations)))
    }
    builder.build().await.context(NewSessionSnafu)
}
