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

//! # Account requests
//!
//! This instance does not yet expose self-service registration. This route gives the shell's
//! sign-up destination an honest, useful account-request experience until that API exists.

use leptos::prelude::*;
use thaw::Icon;

use crate::{components::brand::Logo, types::Base};

/// Render the public account-request page.
#[component]
pub fn SignUp() -> impl IntoView {
    let sign_in = StoredValue::new(format!("{}/s", expect_context::<Base>().0));

    view! {
        <div class="authentication-page">
            <section aria-labelledby="account-request-heading" class="authentication-card">
                <div aria-hidden="true" class="authentication-card__mark">
                    <Logo />
                </div>
                <h1 class="authentication-card__heading" id="account-request-heading">
                    "Request an account"
                </h1>
                <p class="authentication-card__introduction">
                    "This indielinks instance currently creates accounts by request. Email the "
                    "administrator to get started."
                </p>
                <a
                    class="form-button form-button--primary authentication-card__primary-action"
                    href="mailto:sp1ff@pobox.com?subject=indielinks%20account%20request"
                >
                    <Icon icon=icondata::FiMail />
                    "Request an account"
                </a>
                <p class="authentication-card__secondary">
                    "Already have an account? "
                    <a href=sign_in.get_value()>"Sign in"</a>
                    "."
                </p>
            </section>
        </div>
    }
}
