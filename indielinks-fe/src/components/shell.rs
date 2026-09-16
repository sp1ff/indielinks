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

//! # Application shell
//!
//! This module renders indielinks' route navigation, authentication controls, and responsive page
//! frame. Network and authentication state remain in the root application; the shell only receives
//! the state and actions needed to present them.

use leptos::prelude::*;
use leptos_router::hooks::use_location;
use thaw::Icon;

use crate::{
    components::{
        brand::{Logo, Variant},
        theme_control::ThemeControl,
    },
    theme::ThemeController,
};

/// URLs used by the application shell.
///
/// Keeping the configured base in one value prevents individual controls from constructing subtly
/// different links when the frontend is mounted below the domain root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Paths {
    add: String,
    home: String,
    popular: String,
    sign_in: String,
    sign_up: String,
}

impl Paths {
    /// Construct all shell URLs from the frontend's configured base path.
    pub fn new(base: &str) -> Self {
        Self {
            add: format!("{base}/a"),
            home: format!("{base}/h"),
            popular: format!("{base}/"),
            sign_in: format!("{base}/s"),
            sign_up: format!("{base}/u"),
        }
    }

    fn is_sign_in(&self, pathname: &str) -> bool {
        pathname == self.sign_in || pathname == "/s"
    }

    fn label(&self, pathname: &str) -> &'static str {
        Destination::from_path(pathname, self)
            .map(Destination::label)
            .unwrap_or_else(|| {
                if self.is_sign_in(pathname) {
                    "sign in"
                } else if pathname == self.sign_up || pathname == "/u" {
                    "sign up"
                } else {
                    "indielinks"
                }
            })
    }
}

/// A route presented in the primary application navigation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Destination {
    Popular,
    Home,
    Add,
}

impl Destination {
    const ROUTES: [Self; 2] = [Self::Popular, Self::Home];

    fn from_path(pathname: &str, paths: &Paths) -> Option<Self> {
        [Self::Popular, Self::Home, Self::Add]
            .into_iter()
            .find(|destination| destination.is_current(pathname, paths))
    }

    fn href<'a>(self, paths: &'a Paths) -> &'a str {
        match self {
            Self::Popular => &paths.popular,
            Self::Home => &paths.home,
            Self::Add => &paths.add,
        }
    }

    fn icon(self) -> icondata::Icon {
        match self {
            Self::Popular => icondata::FiGlobe,
            Self::Home => icondata::FiBookmark,
            Self::Add => icondata::FiPlus,
        }
    }

    fn is_current(self, pathname: &str, paths: &Paths) -> bool {
        pathname == self.href(paths) || pathname == self.relative_path()
    }

    fn label(self) -> &'static str {
        match self {
            Self::Popular => "popular",
            Self::Home => "home",
            Self::Add => "add link",
        }
    }

    fn relative_path(self) -> &'static str {
        match self {
            Self::Popular => "/",
            Self::Home => "/h",
            Self::Add => "/a",
        }
    }
}

/// Visual placement for a primary link.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Placement {
    Rail,
    Header,
    Action,
    Mobile,
}

impl Placement {
    fn class(self) -> &'static str {
        match self {
            Self::Rail => "shell-navigation__link--rail",
            Self::Header => "shell-navigation__link--header",
            Self::Action => "shell-navigation__link--action",
            Self::Mobile => "shell-navigation__link--mobile",
        }
    }
}

/// A route link with shared icon, label, and current-page behavior.
#[component]
fn PrimaryLink(destination: Destination, paths: Paths, placement: Placement) -> impl IntoView {
    let pathname = use_location().pathname;
    let href = destination.href(&paths).to_owned();
    let current = Memo::new(move |_| destination.is_current(&pathname.get(), &paths));

    view! {
        <a
            href=href
            aria-current=move || current.get().then_some("page")
            class=move || {
                format!(
                    "shell-navigation__link {}{}{}",
                    placement.class(),
                    if destination == Destination::Add {
                        " shell-navigation__link--add"
                    } else {
                        ""
                    },
                    if current.get() { " shell-navigation__link--current" } else { "" },
                )
            }
        >
            <span aria-hidden="true" class="shell-navigation__icon">
                <Icon icon=destination.icon() />
            </span>
            <span>{destination.label()}</span>
        </a>
    }
}

/// The brand link used by the rail and responsive header.
#[component]
fn BrandLink(paths: Paths, variant: Variant) -> impl IntoView {
    view! {
        <a class="shell-brand" href=paths.popular>
            <Logo variant />
        </a>
    }
}

/// Signed-in or guest controls for a navigation surface.
#[component]
fn AccountControls(
    paths: Paths,
    signed_in: Signal<bool>,
    on_sign_out: Callback<()>,
    class: &'static str,
) -> impl IntoView {
    let pathname = use_location().pathname;
    let is_sign_in = Memo::new({
        let paths = paths.clone();
        move |_| paths.is_sign_in(&pathname.get())
    });
    let sign_in = StoredValue::new(paths.sign_in);
    let sign_up = StoredValue::new(paths.sign_up);

    view! {
        <div class=format!("shell-account {class}")>
            <Show
                when=move || signed_in.get()
                fallback=move || {
                    view! {
                        <Show when=move || !is_sign_in.get()>
                            <a class="shell-account__link" href=sign_in.get_value()>
                                <span aria-hidden="true" class="shell-account__icon">
                                    <Icon icon=icondata::FiLogIn />
                                </span>
                                <span>"sign in"</span>
                            </a>
                        </Show>
                        <a
                            href=sign_up.get_value()
                            class="shell-account__link shell-account__link--primary"
                        >
                            "sign up"
                        </a>
                    }
                }
            >
                <button
                    class="shell-account__button"
                    type="button"
                    on:click=move |_| on_sign_out.run(())
                >
                    <span aria-hidden="true" class="shell-account__icon">
                        <Icon icon=icondata::FiLogOut />
                    </span>
                    <span class="shell-account__label">"sign out"</span>
                </button>
            </Show>
        </div>
    }
}

/// Persistent navigation shown to signed-in users at large widths.
#[component]
fn DesktopRail(paths: Paths, signed_in: Signal<bool>, on_sign_out: Callback<()>) -> impl IntoView {
    let theme = expect_context::<ThemeController>();
    view! {
        <aside class="shell-rail">
            <BrandLink paths=paths.clone() variant=Variant::Full />
            <nav aria-label="Primary" class="shell-navigation shell-navigation--rail">
                {Destination::ROUTES
                    .into_iter()
                    .map(|destination| {
                        view! {
                            <PrimaryLink
                                destination
                                paths=paths.clone()
                                placement=Placement::Rail
                            />
                        }
                    })
                    .collect_view()}
            </nav>
            <PrimaryLink
                destination=Destination::Add
                paths=paths.clone()
                placement=Placement::Action
            />
            <div class="shell-rail__footer">
                <ThemeControl
                    availability=theme.availability()
                    appearance=theme.appearance()
                    on_toggle=theme.toggle()
                />
                <AccountControls
                    paths
                    signed_in
                    on_sign_out
                    class="shell-account--rail"
                />
            </div>
        </aside>
    }
}

/// Header used for guest views and for signed-in users below the desktop breakpoint.
#[component]
fn SiteHeader(paths: Paths, signed_in: Signal<bool>, on_sign_out: Callback<()>) -> impl IntoView {
    let theme = expect_context::<ThemeController>();
    let pathname = use_location().pathname;
    let title_paths = paths.clone();
    let full_logo_paths = paths.clone();
    let compact_logo_paths = paths.clone();
    let navigation_paths = paths.clone();
    let action_paths = paths.clone();
    let account_paths = paths;

    view! {
        <header class="shell-header">
            <div class="shell-header__inner">
                <span class="shell-header__brand shell-header__brand--full">
                    <BrandLink paths=full_logo_paths variant=Variant::Full />
                </span>
                <span class="shell-header__brand shell-header__brand--compact">
                    <BrandLink paths=compact_logo_paths variant=Variant::Mark />
                </span>
                <span class="shell-header__title">
                    {move || title_paths.label(&pathname.get())}
                </span>
                <nav aria-label="Primary" class="shell-navigation shell-navigation--header">
                    {Destination::ROUTES
                        .into_iter()
                        .map(|destination| {
                            view! {
                                <PrimaryLink
                                    destination
                                    paths=navigation_paths.clone()
                                    placement=Placement::Header
                                />
                            }
                        })
                        .collect_view()}
                </nav>
                <div class="shell-header__actions">
                    <Show when=move || signed_in.get()>
                        <PrimaryLink
                            destination=Destination::Add
                            paths=action_paths.clone()
                            placement=Placement::Action
                        />
                    </Show>
                    <ThemeControl
                        availability=theme.availability()
                        appearance=theme.appearance()
                        on_toggle=theme.toggle()
                    />
                    <AccountControls
                        paths=account_paths
                        signed_in
                        on_sign_out
                        class="shell-account--header"
                    />
                </div>
            </div>
        </header>
    }
}

/// Fixed primary navigation shown to signed-in users on phones.
#[component]
fn MobileNavigation(paths: Paths) -> impl IntoView {
    view! {
        <nav aria-label="Primary" class="shell-mobile-navigation">
            {[Destination::Popular, Destination::Home, Destination::Add]
                .into_iter()
                .map(|destination| {
                    view! {
                        <PrimaryLink
                            destination
                            paths=paths.clone()
                            placement=Placement::Mobile
                        />
                    }
                })
                .collect_view()}
        </nav>
    }
}

/// Render the responsive application frame around routed content.
#[component]
pub fn Shell(
    paths: Paths,
    signed_in: Signal<bool>,
    on_sign_out: Callback<()>,
    children: Children,
) -> impl IntoView {
    view! {
        <div class=move || if signed_in.get() { "shell shell--signed-in" } else { "shell" }>
            <a class="shell-skip-link" href="#main-content">"Skip to main content"</a>
            <SiteHeader
                paths=paths.clone()
                signed_in
                on_sign_out
            />
            <div class="shell-frame">
                <DesktopRail
                    paths=paths.clone()
                    signed_in
                    on_sign_out
                />
                <main class="shell-main" id="main-content" tabindex="-1">
                    {children()}
                </main>
            </div>
            <MobileNavigation paths />
        </div>
    }
}
