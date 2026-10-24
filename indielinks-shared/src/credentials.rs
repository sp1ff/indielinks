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

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                      generic credentials                                       //
////////////////////////////////////////////////////////////////////////////////////////////////////

use secrecy::SecretString;
use serde::Deserialize;
use tap::Pipe;

use crate::exactly_two::exactly_two;

/// General-purpose credentials-- presumably username, password
// Not sure that the username should be secret, but why not?
#[derive(Clone, Debug, Deserialize)]
pub struct Credentials(pub (SecretString, SecretString));

impl clap::builder::ValueParserFactory for Credentials {
    type Parser = CredentialsParser;

    fn value_parser() -> Self::Parser {
        CredentialsParser
    }
}

#[derive(Clone, Debug)]
pub struct CredentialsParser;

impl clap::builder::TypedValueParser for CredentialsParser {
    type Value = Credentials;

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
            .pipe(|p| (p.0.into(), p.1.into())) // OMFG-- I can't map over a tuple
            .pipe(Credentials)
            .pipe(Ok)
    }
}
