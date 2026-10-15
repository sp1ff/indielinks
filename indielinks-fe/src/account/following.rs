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

//! # Following section
//!
//! Provides the form for creating a new follow and the list of accounts the signed-in user is
//! following.

use gloo_net::http::Request;
use leptos::{ev::SubmitEvent, html, prelude::*};
use thaw::{ToastIntent, ToasterInjection};
use url::Url;

use indielinks_shared::{
    acct::Account,
    api::{FollowRequest, FollowTarget},
};

use crate::{
    account::collection::{CollectionKind, CollectionView},
    components::feedback::show_toast,
    http::send_with_retry,
    types::Api,
};

/// Parse a user-supplied string into a [FollowTarget].
///
/// HTTP and HTTPS URLs become [FollowTarget::ActivityPubId]; everything else is tried as an RFC
/// 7565 account handle via [Account::new].
fn parse_follow_target(input: &str) -> Result<FollowTarget, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("An ActivityPub address is required.".to_owned());
    }
    if let Ok(url) = Url::parse(input) {
        if url.scheme() == "http" || url.scheme() == "https" {
            return Ok(FollowTarget::ActivityPubId(url));
        }
    }
    Account::new(input)
        .map(FollowTarget::Account)
        .map_err(|_| format!("{input} is not a valid ActivityPub URL or account handle."))
}

async fn send_follow(api: String, target: FollowTarget) -> Result<(), String> {
    let body = FollowRequest { id: target };
    let rsp = send_with_retry(
        move || Request::post(&format!("{api}/api/v1/users/follow")),
        body,
    )
    .await
    .map_err(|err| err.to_string())?;

    if rsp.status() >= 200 && rsp.status() < 300 {
        Ok(())
    } else {
        Err(rsp.status_text())
    }
}

/// Render the Following section: a follow form plus the paginated following collection.
#[component]
pub fn FollowingSection() -> impl IntoView {
    let api = expect_context::<Api>().0;
    let toaster = ToasterInjection::expect_context();
    let input_element: NodeRef<html::Input> = NodeRef::new();
    let parse_error = RwSignal::new(None::<String>);
    let rerender = ArcTrigger::new();

    let follow_action = {
        let api = api.clone();
        Action::new_local(move |target: &FollowTarget| {
            let api = api.clone();
            let target = target.clone();
            async move { send_follow(api, target).await }
        })
    };

    Effect::new({
        let rerender = rerender.clone();
        move |_| match follow_action.value().get() {
            Some(Ok(())) => {
                input_element
                    .get()
                    .expect("follow input should be mounted")
                    .set_value("");
                parse_error.set(None);
                show_toast(
                    toaster,
                    ToastIntent::Success,
                    "Follow request sent",
                    "The remote server will need to accept it before they appear in your Following list.",
                );
                rerender.notify();
            }
            Some(Err(err)) => {
                show_toast(
                    toaster,
                    ToastIntent::Error,
                    "Follow request failed",
                    err.clone(),
                );
            }
            None => {}
        }
    });

    let on_submit = move |ev: SubmitEvent| {
        ev.prevent_default();
        let value = input_element
            .get()
            .expect("follow input should be mounted")
            .value();
        match parse_follow_target(&value) {
            Ok(target) => {
                parse_error.set(None);
                follow_action.dispatch(target);
            }
            Err(err) => {
                parse_error.set(Some(err));
                if let Some(input) = input_element.get() {
                    input.focus().ok();
                }
            }
        }
    };

    view! {
        <CollectionView
            title="Following"
            description="Accounts you follow across the Fediverse. New follows appear here once the remote server accepts the request."
            error_title="Following could not be loaded"
            empty_title="Not following anyone yet"
            empty_message="Enter an ActivityPub address above to follow someone."
            kind=CollectionKind::Following
            rerender=rerender
        >
            <form class="follow-form" on:submit=on_submit>
                <div class="form-field">
                    <label class="form-field__label" for="follow-target">
                        "ActivityPub address"
                    </label>
                    <input
                        class="form-control"
                        id="follow-target"
                        type="text"
                        placeholder="sp1ff@indieweb.social or https://indieweb.social/users/sp1ff"
                        node_ref=input_element
                        disabled=move || follow_action.pending().get()
                    />
                    <p class="form-field__help">
                        "A fediverse handle or an ActivityPub actor URL."
                    </p>
                    <Show when=move || parse_error.get().is_some()>
                        <p class="form-field__error" role="alert">
                            {move || parse_error.get().unwrap_or_default()}
                        </p>
                    </Show>
                </div>
                <div class="form-actions">
                    <button
                        class="form-button form-button--primary"
                        disabled=move || follow_action.pending().get()
                        type="submit"
                    >
                        {move || if follow_action.pending().get() { "Following…" } else { "Follow" }}
                    </button>
                </div>
            </form>
        </CollectionView>
    }
}
