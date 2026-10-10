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

//! Profile view and edit section.

use gloo_net::http::Request;
use leptos::{html, prelude::*};
use thaw::{ToastIntent, ToasterInjection};
use tracing::info;

use indielinks_shared::api::{UpdateProfileReq, UserProfile};

use crate::{
    components::feedback::{LoadingState, show_toast},
    http::{send_with_retry, send_with_retry_no_body},
    types::Api,
};

async fn load_profile(api: String) -> Result<UserProfile, String> {
    let rsp = send_with_retry_no_body(|| Request::get(&format!("{api}/api/v1/users/profile")))
        .await
        .map_err(|e| e.to_string())?;
    let status = rsp.status();
    if status >= 200 && status < 300 {
        rsp.json::<UserProfile>().await.map_err(|e| e.to_string())
    } else {
        Err(rsp.status_text())
    }
}

/// Render the profile view/edit section.
#[component]
pub fn ProfileSection() -> impl IntoView {
    let api = expect_context::<Api>().0;
    let rerender = ArcTrigger::new();

    let profile = LocalResource::new({
        let api = api.clone();
        let rerender = rerender.clone();
        move || {
            let api = api.clone();
            rerender.track();
            async move { load_profile(api).await }
        }
    });

    let display_name_element: NodeRef<html::Input> = NodeRef::new();
    let summary_element: NodeRef<html::Textarea> = NodeRef::new();

    let update_action = {
        let api = api.clone();
        Action::new_local(move |_: &()| {
            let api = api.clone();
            async move {
                let display_name = display_name_element
                    .get()
                    .expect("display-name input should be mounted")
                    .value();
                let summary = summary_element
                    .get()
                    .expect("summary textarea should be mounted")
                    .value();

                info!(
                    "Updating profile with display_name='{display_name}' summary_len={}",
                    summary.len()
                );

                let body = UpdateProfileReq {
                    display_name: Some(display_name),
                    summary: Some(summary),
                };

                let rsp = send_with_retry(
                    move || Request::post(&format!("{api}/api/v1/users/profile")),
                    body,
                )
                .await
                .map_err(|e| e.to_string())?;

                let status = rsp.status();
                if status >= 200 && status < 300 {
                    Ok(())
                } else {
                    Err(rsp.status_text())
                }
            }
        })
    };

    let toaster = ToasterInjection::expect_context();

    Effect::new({
        let rerender = rerender.clone();
        move |_| match update_action.value().get() {
            Some(Ok(())) => {
                rerender.notify();
                show_toast(
                    toaster,
                    ToastIntent::Success,
                    "Profile updated",
                    "Your profile changes have been saved.",
                );
            }
            Some(Err(err)) => {
                show_toast(
                    toaster,
                    ToastIntent::Error,
                    "Profile update failed",
                    format!("{err}"),
                );
            }
            None => {}
        }
    });

    view! {
        <section aria-labelledby="profile-heading" class="account-section">
            <h2 class="account-section__heading" id="profile-heading">"Profile"</h2>
            <p class="account-section__help">"View and edit your public profile."</p>
            {move || match profile.get() {
                None => view! {
                    <LoadingState label="Loading profile…" />
                }
                .into_any(),
                Some(Err(err)) => view! {
                    <div class="feedback-state feedback-state--error" role="alert">
                        <div class="feedback-state__content">
                            <h3 class="feedback-state__heading">"Profile could not be loaded"</h3>
                            <p class="feedback-state__message">"Try again."</p>
                            <details class="feedback-state__details">
                                <summary>"Technical details"</summary>
                                <p>{format!("{err}")}</p>
                            </details>
                        </div>
                    </div>
                }
                .into_any(),
                Some(Ok(data)) => {
                    let username = data.username.clone();
                    let display_name = data.display_name.clone();
                    let summary = data.summary.clone();

                    view! {
                        <form
                            class="profile-form"
                            on:submit=move |ev| {
                                ev.prevent_default();
                                update_action.dispatch(());
                            }
                        >
                            <div class="form-field">
                                <label class="form-field__label" for="profile-username">"Username"</label>
                                <input
                                    class="form-control"
                                    id="profile-username"
                                    readonly
                                    type="text"
                                    value=username
                                />
                                <p class="form-field__help">"This cannot be changed."</p>
                            </div>
                            <div class="form-field">
                                <label class="form-field__label" for="profile-display-name">"Display name"</label>
                                <input
                                    autocomplete="name"
                                    class="form-control"
                                    id="profile-display-name"
                                    node_ref=display_name_element
                                    type="text"
                                    value=display_name
                                />
                            </div>
                            <div class="form-field">
                                <label class="form-field__label" for="profile-summary">"Summary"</label>
                                <textarea
                                    class="form-control form-control--textarea"
                                    id="profile-summary"
                                    node_ref=summary_element
                                    rows="4"
                                >{summary}</textarea>
                            </div>
                            <div class="form-actions">
                                <button
                                    class="form-button form-button--primary"
                                    disabled=move || update_action.pending().get()
                                    type="submit"
                                >
                                    {move || if update_action.pending().get() { "Saving…" } else { "Save changes" }}
                                </button>
                            </div>
                        </form>
                    }
                    .into_any()
                }
            }}
        </section>
    }
}
