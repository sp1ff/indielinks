// Copyright (C) 2025-2026 Michael Herstine <sp1ff@pobox.com>
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

//! # indielinks-fe "sign-in" page

use gloo_net::http::Request;
use leptos::{
    html::{self},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use thaw::{ToastIntent, ToasterInjection};
use tracing::{error, info};

use indielinks_shared::api::REFRESH_CSRF_COOKIE;

use crate::{
    components::feedback::show_toast,
    http::string_for_status,
    types::{Api, Base, Token, USER_AGENT},
};

// Need to move to the indielinks_shared implementations of these two:
#[derive(Clone, Debug, Serialize)]
struct LoginReq {
    username: String,
    password: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LoginRsp {
    token: String,
}

async fn login(
    api: &str,
    username: impl Into<String>,
    password: impl Into<String>,
) -> Result<String, String> {
    let rsp = Request::post(&format!("{api}/api/v1/users/login"))
        .header("User-Agent", USER_AGENT)
        .credentials(web_sys::RequestCredentials::Include)
        .json(&LoginReq {
            username: username.into(),
            password: password.into(),
        })
        .map_err(|err| format!("{err}"))?
        .send()
        .await
        .map_err(|err| err.to_string())
        .and_then(string_for_status)?
        .json::<LoginRsp>()
        .await
        .map_err(|err| err.to_string())?;

    info!("Login successful",);
    if wasm_cookies::get(REFRESH_CSRF_COOKIE).is_none() {
        error!("{REFRESH_CSRF_COOKIE} wasn't successfully set!");
    }

    Ok(rsp.token)
}

/// The indielinks login page
#[component]
pub fn SignIn() -> impl IntoView {
    // I think this is one of those things that "should never fail"; or where failure indicates a
    // coding error.
    let api = expect_context::<Api>().0;

    // TBH, I have *no* idea what this does:
    let username_element: NodeRef<html::Input> = NodeRef::new();
    let password_element: NodeRef<html::Input> = NodeRef::new();

    let token = expect_context::<Token>();

    let navigate = leptos_router::hooks::use_navigate();

    let on_submit = Action::new_local(move |_: &()| {
        let username = username_element
            .get()
            .expect("<username> should be mounted")
            .value();
        let password = password_element
            .get()
            .expect("<password> should be mounted")
            .value();
        let api_val = api.clone();
        async move { login(&api_val, username, password).await }
    });

    let base = expect_context::<Base>().0;
    let sign_up = StoredValue::new(format!("{base}/u"));

    Effect::new(move |_| {
        // Still figuring this out...
        let toaster = ToasterInjection::expect_context();
        match on_submit.value().get() {
            Some(Ok(new_token)) => {
                info!("My effect has been invoked with a new token");
                token.set(Some(new_token.into()));
                navigate(&format!("{}/h", base), Default::default())
            }
            Some(Err(err)) => {
                info!("My effect has been invoked with an error value of {err:?}");
                show_toast(toaster, ToastIntent::Error, "Sign in", err.to_string());
                if let Some(username) = username_element.get() {
                    let _ = username.focus();
                }
            }
            None => info!("Effect invoked with no value!?"),
        }
    });

    view! {
        <div class="authentication-page">
            <section aria-labelledby="sign-in-heading" class="authentication-card">
                <h1 class="authentication-card__heading" id="sign-in-heading">"Sign in"</h1>
                <p class="authentication-card__introduction">
                    "Sign in to manage your saved links and follow your network."
                </p>
                <form
                    class="indielinks-form"
                    on:submit=move |event| {
                        event.prevent_default();
                        if !on_submit.pending().get() {
                            on_submit.dispatch(());
                        }
                    }
                >
                    <div class="form-field">
                        <label class="form-field__label" for="username">"Username"</label>
                        <input
                            autofocus
                            autocomplete="username"
                            class="form-control"
                            id="username"
                            name="username"
                            node_ref=username_element
                            required
                            type="text"
                        />
                    </div>
                    <div class="form-field">
                        <label class="form-field__label" for="password">"Password"</label>
                        <input
                            autocomplete="current-password"
                            class="form-control"
                            id="password"
                            name="password"
                            node_ref=password_element
                            required
                            type="password"
                        />
                    </div>
                    <div class="form-actions">
                        <button
                            aria-busy=move || on_submit.pending().get().to_string()
                            class="form-button form-button--primary"
                            disabled=move || on_submit.pending().get()
                            type="submit"
                        >
                            {move || if on_submit.pending().get() { "Signing in…" } else { "Sign in" }}
                        </button>
                    </div>
                </form>
                <p class="authentication-card__secondary">
                    "Need an account? "
                    <a href=sign_up.get_value()>"Request one"</a>
                    "."
                </p>
            </section>
        </div>
    }
}
