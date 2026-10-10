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

//! Password-change form and session-rotation success flow.

use gloo_net::http::Request;
use leptos::{html, prelude::*};
use thaw::{ToastIntent, ToasterInjection};
use tracing::info;

use indielinks_shared::api::{ChangePasswordRequest, Password, SecretPassword};

use crate::{
    components::feedback::show_toast,
    http::send_with_retry,
    types::{Api, Token},
};

/// Render the password-change form.
#[component]
pub fn PasswordSection() -> impl IntoView {
    let api = expect_context::<Api>().0;
    let _token = expect_context::<Token>();

    let password_element: NodeRef<html::Input> = NodeRef::new();
    let confirm_element: NodeRef<html::Input> = NodeRef::new();

    let validation_error = RwSignal::new(None::<String>);

    let on_submit = Action::new_local(move |_: &()| {
        let api = api.clone();
        async move {
            let new_password = password_element
                .get()
                .expect("new-password input should be mounted")
                .value();
            let confirm = confirm_element
                .get()
                .expect("confirm-password input should be mounted")
                .value();

            if new_password.is_empty() {
                return Err("Password cannot be empty.".to_string());
            }
            if new_password != confirm {
                return Err("Passwords do not match.".to_string());
            }

            let body = ChangePasswordRequest {
                new_password: SecretPassword::new(Box::new(Password(new_password))),
            };

            let rsp = send_with_retry(
                move || {
                    Request::post(&format!("{api}/api/v1/users/change-password"))
                        .credentials(web_sys::RequestCredentials::Include)
                },
                body,
            )
            .await
            .map_err(|err| err.to_string())?;

            let status = rsp.status();
            if status >= 200 && status < 300 {
                info!("Password changed successfully (status {status})");
                Ok(())
            } else {
                Err(rsp.status_text())
            }
        }
    });

    let toaster = ToasterInjection::expect_context();

    Effect::new(move |_| match on_submit.value().get() {
        Some(Ok(())) => {
            password_element
                .get()
                .expect("new-password input should be mounted")
                .set_value("");
            confirm_element
                .get()
                .expect("confirm-password input should be mounted")
                .set_value("");
            validation_error.set(None);
            show_toast(
                toaster,
                ToastIntent::Success,
                "Password changed",
                "Your password has been updated. This browser remains signed in.",
            );
        }
        Some(Err(err)) => {
            show_toast(
                toaster,
                ToastIntent::Error,
                "Password change failed",
                err.clone(),
            );
        }
        None => {}
    });

    let pending = Signal::derive(move || on_submit.pending().get());

    view! {
        <section aria-labelledby="password-heading" class="account-section">
            <h2 class="account-section__heading" id="password-heading">
                "Password"
            </h2>
            <p class="account-section__help">
                "Choose a strong password. This browser will stay signed in after the change."
            </p>
            <form
                class="indielinks-form"
                on:submit=move |event| {
                    event.prevent_default();
                    if !pending.get() {
                        validation_error.set(None);
                        on_submit.dispatch(());
                    }
                }
            >
                <div class="form-field">
                    <label class="form-field__label" for="new-password">
                        "New password"
                    </label>
                    <input
                        id="new-password"
                        type="password"
                        autocomplete="new-password"
                        class="form-control"
                        node_ref=password_element
                    />
                </div>
                <div class="form-field">
                    <label class="form-field__label" for="confirm-password">
                        "Confirm new password"
                    </label>
                    <input
                        id="confirm-password"
                        type="password"
                        autocomplete="new-password"
                        class="form-control"
                        node_ref=confirm_element
                    />
                    {move || validation_error.get().map(|msg| {
                        view! {
                            <p class="form-field__error" role="alert">{msg}</p>
                        }
                    })}
                </div>
                <div class="form-actions">
                    <button
                        class="form-button form-button--primary"
                        type="submit"
                        aria-busy=move || if pending.get() { "true" } else { "false" }
                        disabled=move || pending.get()
                    >
                        {move || if pending.get() { "Changing…" } else { "Change password" }}
                    </button>
                </div>
            </form>
        </section>
    }
}
