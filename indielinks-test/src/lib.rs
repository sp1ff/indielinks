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
// You should have received a copy of the GNU General Public License along with mpdpopm.  If not,
// see <http://www.gnu.org/licenses/>.

use lazy_static::lazy_static;

use std::{ffi::OsString, path::PathBuf};

lazy_static! {
    static ref CONFIG_ARG: regex::bytes::Regex =
        regex::bytes::Regex::new("^--config(=.+)?$").unwrap(/* known good */);
}

/// Parse a sequence of [OsString]s (presumably command-line arguments) and parse out the `--config`
/// option, if present. Return both that option's value and the remaining arguments (as a vector).
pub fn parse_configuration_file_option<I>(args: I) -> (Option<PathBuf>, Vec<OsString>)
where
    I: Iterator,
    I::Item: Into<OsString> + Clone,
{
    enum State {
        Initial,
        SawBareConfig,
        GotConfig(PathBuf),
    }

    let mut state = State::Initial;

    let rest = args
        .filter_map(|s| {
            let s: OsString = s.into();
            match state {
                State::Initial => {
                    if let Some(what) = (*CONFIG_ARG).captures(s.as_encoded_bytes()) {
                        if let Some(val) = what.get(1) {
                            state = State::GotConfig(
                                String::from_utf8(val
                                              .as_bytes()
                                              .get(1..)
                                              .unwrap(/* known good */)
                                              .to_vec())
                                .unwrap(/* known good */)
                                .into(),
                            );
                            None
                        } else {
                            state = State::SawBareConfig;
                            None
                        }
                    } else {
                        Some(s)
                    }
                }
                State::SawBareConfig => {
                    state = State::GotConfig(s.into());
                    None
                }
                State::GotConfig { .. } => Some(s),
            }
        })
        .collect();

    if let State::GotConfig(cfg) = state {
        (Some(cfg), rest)
    } else {
        (None, rest)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_configuration_file_smoke() {
        let (arg, rest) = parse_configuration_file_option(
            vec!["foo", "bar", "splat"].into_iter().map(OsString::from),
        );
        assert!(arg.is_none());
        assert!(
            rest == vec!["foo", "bar", "splat"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<OsString>>()
        );

        let (arg, rest) = parse_configuration_file_option(
            vec!["foo", "--config=bar", "splat"]
                .into_iter()
                .map(OsString::from),
        );
        assert!(arg.unwrap() == PathBuf::from("bar"));
        assert!(
            rest == vec!["foo", "splat"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<OsString>>()
        );

        let (arg, rest) = parse_configuration_file_option(
            vec!["foo", "--config", "bar", "splat"]
                .into_iter()
                .map(OsString::from),
        );
        assert!(arg.unwrap() == PathBuf::from("bar"));
        assert!(
            rest == vec!["foo", "splat"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<OsString>>()
        );
    }
}
