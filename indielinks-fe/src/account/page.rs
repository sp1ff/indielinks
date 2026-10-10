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

//! Account page wrappers, section navigation, and route components.

use leptos::prelude::*;
use leptos_router::hooks::{use_location, use_navigate};

use crate::components::shell::Paths;

/// An account-management section.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Section {
    Profile,
    Password,
    ApiKeys,
}

impl Section {
    const ALL: [Self; 3] = [Self::Profile, Self::Password, Self::ApiKeys];

    fn href(self, paths: &Paths) -> String {
        match self {
            Self::Profile => paths.account_profile().to_owned(),
            Self::Password => paths.account_password().to_owned(),
            Self::ApiKeys => paths.account_api_keys().to_owned(),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Profile => "Profile",
            Self::Password => "Password",
            Self::ApiKeys => "API keys",
        }
    }

    fn relative_path(self) -> &'static str {
        match self {
            Self::Profile => "/m/profile",
            Self::Password => "/m/password",
            Self::ApiKeys => "/m/api-keys",
        }
    }
}

/// Responsive section navigation for the account area.
///
/// Desktop: compact left-column links.  Mobile: horizontally scrollable tab row.
#[component]
fn SectionNav() -> impl IntoView {
    let paths = expect_context::<Paths>();
    let pathname = use_location().pathname;

    view! {
        <nav aria-label="Account sections" class="account-section-nav">
            <ul class="account-section-nav__list" role="list">
                {Section::ALL.into_iter().map(|section| {
                    let href = section.href(&paths);
                    let relative = section.relative_path();
                    let href_for_current = href.clone();
                    let href_for_class = href.clone();
                    view! {
                        <li class="account-section-nav__item">
                            <a
                                href=href
                                aria-current=move || {
                                    let pathname = pathname.get();
                                    (pathname == href_for_current || pathname == relative)
                                        .then_some("page")
                                }
                                class=move || {
                                    let pathname = pathname.get();
                                    format!(
                                        "account-section-link{}",
                                        if pathname == href_for_class || pathname == relative {
                                            " account-section-link--current"
                                        } else {
                                            ""
                                        }
                                    )
                                }
                            >
                                {section.label()}
                            </a>
                        </li>
                    }
                }).collect_view()}
            </ul>
        </nav>
    }
}

/// Shared account page frame: heading, introduction, and responsive navigation.
#[component]
fn AccountPage(children: Children) -> impl IntoView {
    view! {
        <div class="account-layout">
            <div class="account-layout__nav">
                <SectionNav />
            </div>
            <div class="account-layout__panel">
                <h1 class="account-layout__heading">"Account"</h1>
                <p class="account-layout__introduction">
                    "Manage your password, API keys, and other account settings."
                </p>
                {children()}
            </div>
        </div>
    }
}

/// Landing route for `/m`; redirects to the profile section.
#[component]
pub fn AccountLanding() -> impl IntoView {
    let base = expect_context::<crate::types::Base>().0;
    let navigate = use_navigate();

    Effect::new(move |_| {
        navigate(&format!("{base}/m/profile"), Default::default());
    });

    view! {
        <div class="account-panel__loading">"Redirecting…"</div>
    }
}

/// `/m/profile` route view.
#[component]
pub fn AccountProfile() -> impl IntoView {
    view! {
        <AccountPage>
            <super::profile::ProfileSection />
        </AccountPage>
    }
}

/// `/m/password` route view.
#[component]
pub fn AccountPassword() -> impl IntoView {
    view! {
        <AccountPage>
            <super::password::PasswordSection />
        </AccountPage>
    }
}

/// `/m/api-keys` route view.
#[component]
pub fn AccountApiKeys() -> impl IntoView {
    view! {
        <AccountPage>
            <super::api_keys::ApiKeysSection />
        </AccountPage>
    }
}
