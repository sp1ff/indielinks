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

//! # Theme controller & browser integration
//!
//! ## Introduction
//!
//! This module connects the pure [model](super::model) to the browser. [InitialTheme] captures
//! everything needed to choose an appearance *before* the application mounts, so that a saved
//! dark preference never paints the light interface first. [ThemeController] owns the reactive
//! state afterwards: it exposes read-only views of the capability & effective appearance plus the
//! toggle action used by the theme control, while writable signals and all browser access remain
//! private. This prevents child components from placing the palette, the document attribute and
//! the stored preference out of sync.
//!
//! Browser storage and `matchMedia` can be unavailable or denied in restricted browsing modes;
//! every interaction with them falls back to the light theme with a console diagnostic rather
//! than preventing the application from mounting.

use leptos::prelude::*;
use tracing::{error, warn};
use wasm_bindgen::{JsCast, closure::Closure};
use web_sys::{MediaQueryList, MediaQueryListEvent};

use crate::theme::model::{Appearance, Availability, Preference, STORAGE_KEY};

////////////////////////////////////////////////////////////////////////////////
//                            Private browser helpers                         //
////////////////////////////////////////////////////////////////////////////////

/// An active `prefers-color-scheme` change-listener registration.
///
/// Retaining the [MediaQueryList] & callback closure together lets the controller remove the
/// listener (and drop the closure) when an explicit preference supersedes it, rather than
/// leaking the closure for the remainder of the page lifetime.
struct ListenerRegistration {
    media_query_list: MediaQueryList,
    on_change: Closure<dyn FnMut(MediaQueryListEvent)>,
}

impl ListenerRegistration {
    /// Attach `on_change` to `media_query_list`'s `change` event.
    fn attach(
        media_query_list: &MediaQueryList,
        on_change: Closure<dyn FnMut(MediaQueryListEvent)>,
    ) -> ListenerRegistration {
        if let Err(error) = media_query_list
            .add_event_listener_with_callback("change", on_change.as_ref().unchecked_ref())
        {
            warn!("Couldn't subscribe to `prefers-color-scheme` changes: {error:?}");
        }
        ListenerRegistration {
            media_query_list: media_query_list.clone(),
            on_change,
        }
    }
}

impl Drop for ListenerRegistration {
    fn drop(&mut self) {
        let _ = self
            .media_query_list
            .remove_event_listener_with_callback("change", self.on_change.as_ref().unchecked_ref());
    }
}

/// Query the `prefers-color-scheme` media query list, if the browser allows it.
fn media_query_list() -> Option<MediaQueryList> {
    web_sys::window().and_then(|window| {
        window
            .match_media("(prefers-color-scheme: dark)")
            .ok()
            .flatten()
    })
}

/// Query the operating system's color-scheme setting, defaulting to light when unavailable.
fn system_prefers_dark() -> bool {
    media_query_list()
        .map(|media_query_list| media_query_list.matches())
        .unwrap_or_else(|| {
            warn!("Couldn't query `prefers-color-scheme`; defaulting to the light theme");
            false
        })
}

/// Read the persisted preference, treating storage failures & unknown values as absent.
fn stored_preference() -> Option<Preference> {
    match web_sys::window().and_then(|window| window.local_storage().ok().flatten()) {
        Some(storage) => match storage.get_item(STORAGE_KEY) {
            Ok(Some(raw)) => Preference::from_stored(&raw).or_else(|| {
                warn!("Ignoring unrecognized theme preference `{raw}` in browser storage");
                None
            }),
            Ok(None) => None,
            Err(error) => {
                warn!("Couldn't read the stored theme preference: {error:?}");
                None
            }
        },
        None => {
            warn!("Browser storage is unavailable; the theme preference will not persist");
            None
        }
    }
}

/// Persist an explicit preference, logging (rather than failing) when storage is denied.
fn persist(preference: Preference) {
    match web_sys::window().and_then(|window| window.local_storage().ok().flatten()) {
        Some(storage) => {
            if let Err(error) = storage.set_item(STORAGE_KEY, preference.as_stored()) {
                error!(
                    "Couldn't persist the theme preference; it will last for this page only: \
                     {error:?}"
                );
            }
        }
        None => error!(
            "Browser storage is unavailable; the theme preference will last for this page only"
        ),
    }
}

/// Set `data-theme` on the document element so that authored CSS can follow the rendered palette.
fn set_data_theme(appearance: Appearance) {
    if let Some(element) = document().document_element() {
        let _ = element.set_attribute("data-theme", appearance.as_data_attribute());
    }
}

////////////////////////////////////////////////////////////////////////////////
//                                InitialTheme                                //
////////////////////////////////////////////////////////////////////////////////

/// Everything needed to choose an appearance before the application mounts.
#[derive(Clone, Copy, Debug)]
pub struct InitialTheme {
    availability: Availability,
    preference: Option<Preference>,
    system_prefers_dark: bool,
}

impl InitialTheme {
    /// Resolve any stored preference & the system color scheme for the given capability.
    ///
    /// When the capability is disabled, browser state is ignored (though never deleted) and the
    /// result renders the light theme.
    pub fn detect(availability: Availability) -> InitialTheme {
        match availability {
            Availability::Disabled => InitialTheme {
                availability,
                preference: None,
                system_prefers_dark: false,
            },
            Availability::Enabled => InitialTheme {
                availability,
                preference: stored_preference(),
                system_prefers_dark: system_prefers_dark(),
            },
        }
    }

    /// The appearance the application should first render.
    pub fn appearance(&self) -> Appearance {
        Appearance::resolve(self.availability, self.preference, self.system_prefers_dark)
    }

    /// Tag the document element with the initial appearance, before any managed view is created.
    pub fn apply_to_document(&self) {
        set_data_theme(self.appearance());
    }
}

////////////////////////////////////////////////////////////////////////////////
//                               ThemeController                              //
////////////////////////////////////////////////////////////////////////////////

/// The application-wide theme controller.
///
/// Construct once, in the root component, and share via context. The controller follows live
/// operating-system color-scheme changes only while the capability is enabled and no explicit
/// preference exists, and keeps the document element's `data-theme` attribute in step with the
/// rendered palette.
#[derive(Clone, Copy)]
pub struct ThemeController {
    availability: Availability,
    preference: RwSignal<Option<Preference>>,
    appearance: Memo<Appearance>,
}

impl ThemeController {
    /// Create the controller from the pre-mount resolution & wire up browser integration.
    pub fn new(initial: InitialTheme) -> ThemeController {
        let availability = initial.availability;
        let preference = RwSignal::new(initial.preference);
        let system_prefers_dark = RwSignal::new(initial.system_prefers_dark);
        let appearance = Memo::new(move |_| {
            Appearance::resolve(availability, preference.get(), system_prefers_dark.get())
        });

        // Follow live operating-system changes only while the capability is enabled and the user
        // hasn't made an explicit choice. The listener is released (and its closure dropped) as
        // soon as a preference exists; `StoredValue` holds the registration because web-sys types
        // are not `Send`.
        if let Some(media_query_list) = (availability == Availability::Enabled)
            .then(media_query_list)
            .flatten()
        {
            let registration = StoredValue::new_local(None::<ListenerRegistration>);
            Effect::new(move |_| {
                // Dropping the previous run's registration detaches its listener; `StoredValue`
                // is non-reactive, so this neither triggers nor tracks anything.
                registration.set_value(None);
                if preference.read().is_none() {
                    registration.set_value(Some(ListenerRegistration::attach(
                        &media_query_list,
                        Closure::<dyn FnMut(MediaQueryListEvent)>::new(
                            move |event: MediaQueryListEvent| {
                                system_prefers_dark.set(event.matches());
                            },
                        ),
                    )));
                }
            });
        }

        // Keep the document attribute in step with the rendered palette.
        Effect::new(move |_| {
            set_data_theme(appearance.get());
        });

        ThemeController {
            availability,
            preference,
            appearance,
        }
    }

    /// Whether the operator compiled-in the dark-theme capability.
    pub fn availability(&self) -> Availability {
        self.availability
    }

    /// A read-only view of the palette currently being rendered.
    pub fn appearance(&self) -> Signal<Appearance> {
        self.appearance.into()
    }

    /// The action exposed by the theme control: switch to the opposite palette & persist it.
    pub fn toggle(&self) -> Callback<()> {
        let preference = self.preference;
        let appearance = self.appearance;
        Callback::new(move |()| {
            let choice = match appearance.get_untracked() {
                Appearance::Light => Preference::Dark,
                Appearance::Dark => Preference::Light,
            };
            persist(choice);
            preference.set(Some(choice));
        })
    }
}
