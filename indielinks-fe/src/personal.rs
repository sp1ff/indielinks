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

//! # The user's "personal" page
//!
//! # Introduction
//!
//! I had originally planned a "home" page for managing one's own links, as distinct from a "feeds"
//! page where you would see the typical "home", "local" and "federated" feeds. In the interests of
//! time, however, I've decided to combine them into a "personal" page for initial launch. That
//! said, with my newfound familiarity with Leptos (😂) I'm building it up in terms of components
//! that will hopefully be reusable.

use leptos::prelude::*;

use crate::{feeds::ItemFeedOuter, home::LinkFeed};

// A regrettable conflict in terminology has crept in: del.icio.us links and ActivityPub notes,
// replies & shares have both been termed "posts". For UI purposes, I'm going to refer to the former
// as "links" and the latter as "items".

/// The top-level [Personal] view.
#[component]
pub fn Personal() -> impl IntoView {
    view! {
        <div class="personal-layout">
            <h1 class="sr-only">"home"</h1>
            <section aria-labelledby="saved-links-heading" class="content-panel">
                <h2 class="content-panel__heading" id="saved-links-heading">"saved links"</h2>
                <div class="content-panel__body">
                    <LinkFeed />
                </div>
            </section>
            <section aria-labelledby="network-heading" class="content-panel">
                <h2 class="content-panel__heading" id="network-heading">"network"</h2>
                <div class="content-panel__body">
                    <ItemFeedOuter />
                </div>
            </section>
        </div>
    }
}
