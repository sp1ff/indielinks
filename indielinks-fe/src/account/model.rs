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

//! Pure helpers for the account module. These types compile on both host and
//! WASM targets so they can be unit-tested without a browser.

use chrono::{DateTime, Utc};

/// Format a key's fallback label.
pub fn format_key_label(id: usize) -> String {
    format!("Key #{id}")
}

/// Check whether a key with the given expiry has passed its expiration.
pub fn is_expired(expiry: Option<DateTime<Utc>>) -> bool {
    expiry.map_or(false, |dt| dt < Utc::now())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn format_key_label_produces_expected_strings() {
        assert_eq!(format_key_label(0), "Key #0");
        assert_eq!(format_key_label(1), "Key #1");
        assert_eq!(format_key_label(99), "Key #99");
    }

    #[test]
    fn is_expired_none_is_false() {
        assert!(!is_expired(None));
    }

    #[test]
    fn is_expired_future_is_false() {
        let future = Utc::now() + Duration::hours(1);
        assert!(!is_expired(Some(future)));
    }

    #[test]
    fn is_expired_past_is_true() {
        let past = Utc::now() - Duration::hours(1);
        assert!(is_expired(Some(past)));
    }
}
