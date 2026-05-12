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
use thaw::{Toast, ToastBody, ToastIntent, ToastOptions, ToastTitle, ToasterInjection};
use tracing::{error, info};

use indielinks_shared::api::REFRESH_CSRF_COOKIE;

use crate::{
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

    Effect::new(move |_| {
        // Still figuring this out...
        let toaster = ToasterInjection::expect_context();
        match on_submit.value().get() {
            Some(Ok(new_token)) => {
                info!("My effect has been invoked with a new token");
                token.set(Some(new_token));
                navigate(&format!("{}/h", base), Default::default())
            }
            Some(Err(err)) => {
                info!("My effect has been invoked with an error value of {err:?}");
                // set_error.set(Some(err))
                let message = format!("{err}");
                toaster.dispatch_toast(
                    move || {
                        view! {
                            <Toast>
                                <ToastTitle>"Login"</ToastTitle>
                                <ToastBody>{message}</ToastBody>
                            </Toast>
                        }
                    },
                    ToastOptions::default().with_intent(ToastIntent::Error),
                );
            }
            None => info!("Effect invoked with no value!?"),
        }
    });

    view! {
        <div class="flex items-center justify-around flex-col pt-[32px]" >
            <form
                class="grid grid-cols-[auto_1fr] gap-x-4 gap-y-3 items-center pt-4 border border-solid border-sky-100 p-8 text-gray-600"
                on:submit=move |ev| {
                  // If I don't say this, the damn page reloads before the HTTP call returns
                  ev.prevent_default();
                  on_submit.dispatch(());
            }>
                <label for="username">"Username:"</label>
                <input autofocus required
                    type="text"
                    id="username" name="username"
                    node_ref=username_element
                    class="bg-transparent border-0 border-b outline-none focus:border-sky-600"
                />
                <label for="password">"Password:"</label>
                <input required
                    type="password"
                    id="password" name="password"
                    node_ref=password_element
                    class="bg-transparent border-0 border-b outline-none focus:border-sky-600"
                />
                <input
                    type="submit"
                    value="Login"
                    class="bg-transparent border px-4 py-2 hover:bg-sky-300 hover:text-gray-900 transition-colors cursor-pointer col-span-2 mx-auto focus:bg-sky-300"
                />
            </form>
        </div>
    }
}
