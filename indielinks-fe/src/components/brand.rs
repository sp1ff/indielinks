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

#![cfg(target_arch = "wasm32")]

//! The indielinks brand mark and wordmark.

use leptos::{either::Either, prelude::*};

/// Selects the compact mark or complete wordmark treatment.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Variant {
    /// The four-tile mark with a visually hidden accessible name.
    Mark,
    /// The four-tile mark followed by the visible wordmark.
    #[default]
    Full,
}

/// Render the indielinks logo.
///
/// The component's text supplies an accessible name when the logo is wrapped in a link. The SVG
/// tiles are decorative and therefore hidden from assistive technology.
#[component]
pub fn Logo(#[prop(optional)] variant: Variant) -> impl IntoView {
    let wordmark = match variant {
        Variant::Mark => Either::Left(view! { <span class="sr-only">"indielinks"</span> }),
        Variant::Full => Either::Right(view! {
            <span class="text-wordmark font-bold leading-none">"indielinks"</span>
        }),
    };

    view! {
        <span class="inline-flex items-center gap-2 text-ink">
            <svg
                aria-hidden="true"
                class="size-6 shrink-0 border border-subtle"
                focusable="false"
                viewBox="0 0 24 24"
                xmlns="http://www.w3.org/2000/svg"
            >
                <rect class="fill-surface" height="12" width="12" x="0" y="0" />
                <rect class="fill-heritage" height="12" width="12" x="12" y="0" />
                <rect class="fill-ink" height="12" width="12" x="0" y="12" />
                <rect class="fill-border-subtle" height="12" width="12" x="12" y="12" />
            </svg>
            {wordmark}
        </span>
    }
}
