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

use leptos::prelude::*;

use crate::types::Base;

/// indielinks "instance" page
#[component]
pub fn Instance() -> impl IntoView {
    let base = use_context::<Base>().expect("No API base!?").0;
    view! {
        <div style="padding: 8px;">
        "This will be the \"instance\" page; it will be the only visible page unless you're "
        <a href={format!("{base}/s")}>"logged-in"</a>"."
        </div>
    }
}
