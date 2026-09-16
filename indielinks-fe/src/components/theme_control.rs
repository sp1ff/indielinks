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

//! The theme control: a single button toggling between the light & dark palettes.

use leptos::{either::Either, prelude::*};
use thaw::Icon;

use crate::theme::model::{Appearance, Availability};

/// A button that switches between the light & dark themes.
///
/// The control is purely presentational: it renders the read-only appearance signal it is given
/// and reports activations through `on_toggle`; it never touches browser storage or the document.
/// Its icon and accessible name describe the *resulting* action and update reactively. When the
/// operator has disabled the dark-theme capability, the control renders nothing at all.
#[component]
pub fn ThemeControl(
    availability: Availability,
    appearance: Signal<Appearance>,
    on_toggle: Callback<()>,
) -> impl IntoView {
    let label = move || match appearance.get() {
        Appearance::Light => "Switch to dark theme",
        Appearance::Dark => "Switch to light theme",
    };
    view! {
        <Show when=move || availability == Availability::Enabled>
            <button
                type="button"
                class="shell-theme-control"
                aria-label=label
                title=label
                on:click=move |_| on_toggle.run(())
            >
                <span aria-hidden="true" class="shell-theme-control__icon">
                    {move || match appearance.get() {
                        Appearance::Light => Either::Left(view! { <Icon icon=icondata::FiMoon /> }),
                        Appearance::Dark => Either::Right(view! { <Icon icon=icondata::FiSun /> }),
                    }}
                </span>
            </button>
        </Show>
    }
}
