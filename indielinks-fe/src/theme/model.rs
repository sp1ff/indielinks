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

//! # Theme & preference model
//!
//! ## Introduction
//!
//! This module holds the pure, browser-independent model behind the frontend's dark-theme
//! capability. It distinguishes three things that are easily conflated: whether the operator
//! compiled-in the capability ([Availability]), an explicit choice persisted in the browser
//! ([Preference]), and the palette actually being rendered ([Appearance]).
//!
//! The module is compiled on *all* targets (unlike the rest of this crate) so that its logic can
//! be unit-tested on the host; browser integration lives in `super::controller`.

use std::{result::Result as StdResult, str::FromStr};

use snafu::Snafu;

/// Errors arising while parsing theme configuration.
#[derive(Clone, Debug, Snafu)]
pub enum Error {
    /// The `INDIELINKS_FE_DARK_THEME` build-time variable held a value other than `true`/`false`.
    #[snafu(display(
        "invalid INDIELINKS_FE_DARK_THEME value `{value}`; expected `true` or `false`"
    ))]
    UnknownAvailability { value: String },
}

/// `Result` alias for this module's [`Error`].
pub type Result<T> = StdResult<T, Error>;

/// The browser storage key under which an explicit theme preference is persisted.
///
/// The key is versioned so that a future, incompatible representation can migrate by bumping the
/// suffix rather than contending with values written by older builds.
pub const STORAGE_KEY: &str = "indielinks.theme.v1";

/// Whether the operator compiled-in the dark-theme capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Availability {
    /// Dark-theme support is available: honor stored & system preferences and show the control.
    Enabled,
    /// Dark-theme support is compiled-out: always render light and omit the theme control.
    Disabled,
}

impl Availability {
    /// Interpret the compile-time value of `INDIELINKS_FE_DARK_THEME`.
    ///
    /// The capability is enabled unless the operator explicitly disabled it. `build.rs` rejects
    /// any other value at build time, so an `Err` here indicates that check was bypassed.
    pub fn from_compile_time(value: Option<&'static str>) -> Result<Availability> {
        value.map(str::parse).unwrap_or(Ok(Availability::Enabled))
    }
}

impl FromStr for Availability {
    type Err = Error;

    fn from_str(value: &str) -> Result<Availability> {
        match value {
            "true" => Ok(Availability::Enabled),
            "false" => Ok(Availability::Disabled),
            _ => UnknownAvailabilitySnafu { value }.fail(),
        }
    }
}

/// An explicit, browser-persisted choice between the light & dark palettes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Preference {
    Light,
    Dark,
}

impl Preference {
    /// Parse a persisted preference; any unrecognized value is treated as absent.
    pub fn from_stored(value: &str) -> Option<Preference> {
        match value {
            "light" => Some(Preference::Light),
            "dark" => Some(Preference::Dark),
            _ => None,
        }
    }

    /// The representation written to browser storage.
    pub fn as_stored(&self) -> &'static str {
        match self {
            Preference::Light => "light",
            Preference::Dark => "dark",
        }
    }
}

/// The palette actually being rendered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Appearance {
    Light,
    Dark,
}

impl Appearance {
    /// Resolve the effective appearance from capability, stored preference & system setting.
    ///
    /// A disabled capability always yields light; otherwise an explicit preference wins over the
    /// operating system's `prefers-color-scheme` setting.
    pub fn resolve(
        availability: Availability,
        preference: Option<Preference>,
        system_prefers_dark: bool,
    ) -> Appearance {
        match (availability, preference, system_prefers_dark) {
            (Availability::Disabled, _, _) => Appearance::Light,
            (Availability::Enabled, Some(preference), _) => Appearance::from(preference),
            (Availability::Enabled, None, system_prefers_dark) => match system_prefers_dark {
                true => Appearance::Dark,
                false => Appearance::Light,
            },
        }
    }

    /// The value used for the document element's `data-theme` attribute.
    pub fn as_data_attribute(&self) -> &'static str {
        match self {
            Appearance::Light => "light",
            Appearance::Dark => "dark",
        }
    }
}

impl From<Preference> for Appearance {
    fn from(preference: Preference) -> Appearance {
        match preference {
            Preference::Light => Appearance::Light,
            Preference::Dark => Appearance::Dark,
        }
    }
}

impl From<Appearance> for Preference {
    fn from(appearance: Appearance) -> Preference {
        match appearance {
            Appearance::Light => Preference::Light,
            Appearance::Dark => Preference::Dark,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn availability_defaults_to_enabled() {
        let availability = Availability::from_compile_time(None)
            .expect("an unset variable should parse as enabled");
        assert_eq!(Availability::Enabled, availability);
    }

    #[test]
    fn availability_parses_explicit_values() {
        for (raw, expected) in [
            (Some("true"), Availability::Enabled),
            (Some("false"), Availability::Disabled),
        ] {
            let availability =
                Availability::from_compile_time(raw).expect("a valid value should parse");
            assert_eq!(expected, availability);
        }
    }

    #[test]
    fn availability_rejects_unknown_values() {
        let error = Availability::from_compile_time(Some("yes"))
            .expect_err("an unrecognized value should be rejected");
        assert!(error.to_string().contains("`yes`"));
    }

    #[test]
    fn stored_preference_round_trips() {
        for preference in [Preference::Light, Preference::Dark] {
            assert_eq!(
                Some(preference),
                Preference::from_stored(preference.as_stored())
            );
        }
    }

    #[test]
    fn unknown_stored_values_are_absent() {
        for raw in ["", "dark ", "system", "DARK"] {
            assert_eq!(None, Preference::from_stored(raw));
        }
    }

    #[test]
    fn disabled_capability_always_renders_light() {
        for (preference, system_prefers_dark) in [
            (None, false),
            (None, true),
            (Some(Preference::Light), true),
            (Some(Preference::Dark), false),
        ] {
            assert_eq!(
                Appearance::Light,
                Appearance::resolve(Availability::Disabled, preference, system_prefers_dark)
            );
        }
    }

    #[test]
    fn explicit_preference_wins_over_system() {
        for (preference, expected) in [
            (Preference::Light, Appearance::Light),
            (Preference::Dark, Appearance::Dark),
        ] {
            for system_prefers_dark in [false, true] {
                assert_eq!(
                    expected,
                    Appearance::resolve(
                        Availability::Enabled,
                        Some(preference),
                        system_prefers_dark
                    )
                );
            }
        }
    }

    #[test]
    fn absent_preference_follows_system() {
        assert_eq!(
            Appearance::Dark,
            Appearance::resolve(Availability::Enabled, None, true)
        );
        assert_eq!(
            Appearance::Light,
            Appearance::resolve(Availability::Enabled, None, false)
        );
    }
}
