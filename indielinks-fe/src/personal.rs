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
        <div class="flex w-full">
            <div class="flex-60 min-w-64 m-[8px] p-[4px] border border-solid border-sky-100 h-[80vh] overflow-y-auto">
              <LinkFeed />
            </div>
            <div class="flex-40 min-w-48 m-[8px] p-[4px] border border-solid border-sky-100 h-[80vh] overflow-y-auto">
              <ItemFeedOuter />
            </div>
        </div>
    }
}
