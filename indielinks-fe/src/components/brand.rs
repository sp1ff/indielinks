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
    /// The linked-bookmarks mark with a visually hidden accessible name.
    Mark,
    /// The linked-bookmarks mark followed by the visible wordmark.
    #[default]
    Full,
}

/// Render the indielinks logo.
///
/// The component's text supplies an accessible name when the logo is wrapped in a link. The two
/// offset ribbons suggest both saved bookmarks and connected sites. The SVG is decorative and
/// therefore hidden from assistive technology.
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
                class="size-6 shrink-0"
                focusable="false"
                shape-rendering="crispEdges"
                viewBox="0 0 24 24"
                xmlns="http://www.w3.org/2000/svg"
            >
                <path class="fill-ink" d="M2 4H12V22L7 18.5L2 22V4Z" />
                <path class="fill-heritage" d="M9 2H22V20L15.5 16L9 20V2Z" />
                <rect class="fill-surface" height="4" width="4" x="9" y="8" />
            </svg>
            {wordmark}
        </span>
    }
}
