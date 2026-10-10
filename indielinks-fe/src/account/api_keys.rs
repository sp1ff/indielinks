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

//! API-key list, mint, revocation, and one-time secret UI.

use std::{result::Result as StdResult, sync::Arc};

use chrono::{DateTime, Utc};
use gloo_net::http::Request;
use leptos::{either::Either, either::EitherOf3, prelude::*};
use snafu::prelude::*;
use thaw::Icon;
use thaw::{ToastIntent, ToasterInjection};
use tracing::info;

use indielinks_shared::api::{GetKeysResponse, MintKeyRsp, RevokeKeyRequest};

use crate::{
    account::model,
    components::feedback::{LoadingState, show_toast},
    http::{error_for_status1, send_with_retry, send_with_retry_no_body},
    types::Api,
};

#[derive(Clone, Debug, Snafu)]
enum Error {
    #[snafu(display("While loading API keys, {source}"))]
    Load { source: crate::http::Error },
    #[snafu(display("While parsing API keys, {source}"))]
    Parse {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
}

type Result<T> = StdResult<T, Error>;

async fn load_keys(api: String) -> Result<GetKeysResponse> {
    let url = format!("{api}/api/v1/users/keys");
    info!("Loading keys from {url}");
    let rsp = send_with_retry_no_body(|| Request::get(&url))
        .await
        .context(LoadSnafu)?;
    info!("Keys response status: {}", rsp.status());
    let rsp = error_for_status1(rsp).context(LoadSnafu)?;
    rsp.json::<GetKeysResponse>()
        .await
        .map_err(|e| Error::Parse {
            source: Arc::new(e),
        })
}

/// Render a localized date/time string using the browser's locale.
fn localized_date_time(dt: &DateTime<Utc>) -> String {
    let ts_ms = dt.timestamp_millis() as f64;
    let js_date = web_sys::js_sys::Date::new(&wasm_bindgen::JsValue::from_f64(ts_ms));
    let locale = web_sys::window()
        .and_then(|w| w.navigator().language())
        .filter(|l| !l.is_empty())
        .unwrap_or_else(|| "en-US".into());
    js_date
        .to_locale_string(&locale, &wasm_bindgen::JsValue::undefined())
        .into()
}

/// Human-readable expiry text for a key.
fn expiry_text(expiry: Option<DateTime<Utc>>) -> String {
    match expiry {
        Some(dt) if model::is_expired(Some(dt)) => {
            format!("Expired {}", localized_date_time(&dt))
        }
        Some(dt) => format!("Expires {}", localized_date_time(&dt)),
        None => "Never expires".to_string(),
    }
}

/// Elide a long secret string for display (e.g., `s1:24e4...5823`).
fn elide_secret(text: &str) -> String {
    if text.len() > 14 {
        let prefix = &text[..6];
        let suffix = &text[text.len() - 4..];
        format!("{}...{}", prefix, suffix)
    } else {
        text.to_string()
    }
}

/// One-time secret display panel with copy and dismiss actions.
#[component]
fn SecretPanel(secret: String, on_dismiss: Callback<()>) -> impl IntoView {
    let toaster = ToasterInjection::expect_context();
    let secret_display = secret.clone();
    let elided = elide_secret(&secret);

    view! {
        <div class="secret-panel" role="region" aria-label="New API key">
            <h3 class="secret-panel__heading">"New API key"</h3>
            <p class="secret-panel__help">"Copy this key now. It cannot be shown again."</p>
            <pre class="secret-panel__value">{secret_display.clone()}</pre>
            <div class="secret-panel__actions">
                <button
                    class="form-button form-button--primary"
                    type="button"
                    on:click=move |_| {
                        let window = web_sys::window().expect("window");
                        let navigator = window.navigator();
                        let clipboard = navigator.clipboard();
                        let _ = clipboard.write_text(&secret);
                        show_toast(
                            toaster,
                            ToastIntent::Success,
                            "Copied",
                            "Key copied to clipboard",
                        );
                    }
                >
                    "Copy key"
                </button>
                <button
                    class="form-button form-button--secondary"
                    type="button"
                    on:click=move |_| on_dismiss.run(())
                >
                    "Dismiss"
                </button>
            </div>
            <p class="secret-panel__usage">
                {format!("Use this value as the Authorization header: Bearer <username>:{elided}")}
            </p>
        </div>
    }
}

/// Render a single key row with optional inline revoke confirmation.
fn key_row_view(
    key: indielinks_shared::api::ApiKey,
    revoke_confirming: RwSignal<Option<usize>>,
    revoke_action: Action<usize, StdResult<(), String>>,
    any_pending: Signal<bool>,
) -> impl IntoView {
    let id = key.id;
    let label = model::format_key_label(id);
    let expiry = expiry_text(key.expiry);

    view! {
        <div class="api-key-row">
            <div class="api-key-row__info">
                <span class="api-key-row__label">{label.clone()}</span>
                <span class="api-key-row__expiry">{expiry.clone()}</span>
            </div>
            {move || if revoke_confirming.get() == Some(id) {
                Either::Left(view! {
                    <div class="api-key-row__confirm">
                        <p class="api-key-row__confirm-text">
                            {format!("Revoke {label}? {expiry}")}
                        </p>
                        <div class="api-key-row__confirm-actions">
                            <button
                                class="form-button form-button--secondary"
                                type="button"
                                on:click=move |_| revoke_confirming.set(None)
                            >"Cancel"</button>
                            <button
                                class="form-button form-button--danger"
                                type="button"
                                disabled=move || any_pending.get()
                                on:click=move |_| {
                                    revoke_confirming.set(None);
                                    revoke_action.dispatch(id);
                                }
                            >"Revoke"</button>
                        </div>
                    </div>
                })
            } else {
                Either::Right(view! {
                    <button
                        class="form-button form-button--danger"
                        type="button"
                        disabled=move || any_pending.get()
                        aria-label=format!("Revoke {label}")
                        on:click=move |_| revoke_confirming.set(Some(id))
                    >
                        "Revoke"
                    </button>
                })
            }}
        </div>
    }
}

/// The API keys account section.
#[component]
pub fn ApiKeysSection() -> impl IntoView {
    let api = expect_context::<Api>().0;
    let rerender = ArcTrigger::new();

    let keys = LocalResource::new({
        let api = api.clone();
        let rerender = rerender.clone();
        move || {
            let api = api.clone();
            rerender.track();
            async move { load_keys(api).await }
        }
    });

    let new_secret = RwSignal::new(None::<String>);

    let mint_action = {
        let api = api.clone();
        Action::new_local(move |_: &()| {
            let api = api.clone();
            async move {
                let rsp = send_with_retry_no_body(|| {
                    Request::get(&format!("{api}/api/v1/users/mint-key"))
                })
                .await
                .map_err(|e| e.to_string())?;
                let status = rsp.status();
                if status >= 200 && status < 300 {
                    let body = rsp.json::<MintKeyRsp>().await.map_err(|e| e.to_string())?;
                    Ok(body.key_text)
                } else {
                    Err(rsp.status_text())
                }
            }
        })
    };

    let revoke_action = {
        let api = api.clone();
        Action::new_local(move |id: &usize| {
            let id = *id;
            let api = api.clone();
            async move {
                let rsp = send_with_retry(
                    move || Request::post(&format!("{api}/api/v1/users/revoke-key")),
                    RevokeKeyRequest { id },
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
        move |_| match mint_action.value().get() {
            Some(Ok(key_text)) => {
                new_secret.set(Some(key_text));
                rerender.notify();
            }
            Some(Err(err)) => {
                show_toast(toaster, ToastIntent::Error, "Minting failed", err);
            }
            None => {}
        }
    });

    Effect::new({
        let rerender = rerender.clone();
        move |_| match revoke_action.value().get() {
            Some(Ok(())) => {
                new_secret.set(None);
                rerender.notify();
            }
            Some(Err(err)) => {
                show_toast(toaster, ToastIntent::Error, "Revocation failed", err);
            }
            None => {}
        }
    });

    let any_pending =
        Signal::derive(move || mint_action.pending().get() || revoke_action.pending().get());

    view! {
        <section aria-labelledby="api-keys-heading" class="account-section">
            <h2 class="account-section__heading" id="api-keys-heading">
                "API keys"
            </h2>

            // Persistent one-time secret panel
            {move || new_secret.get().map(|secret| {
                view! {
                    <SecretPanel
                        secret
                        on_dismiss=Callback::new(move |()| new_secret.set(None))
                    />
                }
            })}

            // Keys list: loading, error, or loaded
            {move || match keys.get() {
                None => view! {
                    <LoadingState label="Loading API keys…" />
                }
                .into_any(),
                Some(Err(err)) => view! {
                    <div class="feedback-state feedback-state--error" role="alert">
                        <span aria-hidden="true" class="feedback-state__icon">
                            <Icon icon=icondata::FiAlertCircle />
                        </span>
                        <div class="feedback-state__content">
                            <h3 class="feedback-state__heading">
                                "API keys could not be loaded"
                            </h3>
                            <p class="feedback-state__message">
                                "Try again. If the problem continues, the service may be unavailable."
                            </p>
                            <div class="feedback-state__actions">
                                <button
                                    class="form-button form-button--secondary"
                                    type="button"
                                    on:click={
                                        let r = rerender.clone();
                                        move |_| r.notify()
                                    }
                                >
                                    <Icon icon=icondata::IoRefresh />
                                    "Retry"
                                </button>
                            </div>
                            <details class="feedback-state__details">
                                <summary>"Technical details"</summary>
                                <p>{format!("{err}")}</p>
                            </details>
                        </div>
                    </div>
                }
                .into_any(),
                Some(Ok(keys)) => {
                    let show_replace = RwSignal::new(false);
                    let revoke_confirming = RwSignal::new(None::<usize>);

                    view! {
                        <div class="api-keys-list">
                            {match keys {
                                GetKeysResponse::NoKeys => EitherOf3::A(view! {
                                    <div class="api-keys-empty">
                                        <p class="api-keys-empty__text">
                                            "API keys let scripts and other clients access your account on your behalf."
                                        </p>
                                        <p class="api-keys-empty__text">
                                            "You don't have any keys yet. Mint one to get started."
                                        </p>
                                        <button
                                            class="form-button form-button--primary"
                                            type="button"
                                            disabled=move || any_pending.get()
                                            on:click=move |_| {
                                                new_secret.set(None);
                                                mint_action.dispatch(());
                                            }
                                        >
                                            "Mint API key"
                                        </button>
                                    </div>
                                }),
                                GetKeysResponse::OneKey(key) => {
                                    let row = key_row_view(
                                        key,
                                        revoke_confirming,
                                        revoke_action,
                                        any_pending,
                                    );
                                    EitherOf3::B(view! {
                                        <div class="api-keys-list__items">
                                            {row}
                                        </div>
                                        <div class="api-keys-list__actions">
                                            <button
                                                class="form-button form-button--primary"
                                                type="button"
                                                disabled=move || any_pending.get()
                                                on:click=move |_| {
                                                    new_secret.set(None);
                                                    mint_action.dispatch(());
                                                }
                                            >
                                                "Mint API key"
                                            </button>
                                        </div>
                                    })
                                }
                                GetKeysResponse::TwoKeys { junior, senior } => {
                                    let senior_id = senior.id;
                                    let senior_expiry_val = senior.expiry;
                                    let junior_row = key_row_view(
                                        junior,
                                        revoke_confirming,
                                        revoke_action,
                                        any_pending,
                                    );
                                    let senior_row = key_row_view(
                                        senior,
                                        revoke_confirming,
                                        revoke_action,
                                        any_pending,
                                    );
                                    EitherOf3::C(view! {
                                        <div class="api-keys-list__items">
                                            {junior_row}
                                            {senior_row}
                                        </div>
                                        <div class="api-keys-list__actions">
                                            {move || if !show_replace.get() {
                                                Either::Left(view! {
                                                    <button
                                                        class="form-button form-button--primary"
                                                        type="button"
                                                        disabled=move || any_pending.get()
                                                        on:click=move |_| {
                                                            new_secret.set(None);
                                                            show_replace.set(true);
                                                        }
                                                    >
                                                        "Replace oldest key"
                                                    </button>
                                                })
                                            } else {
                                                let senior_label = model::format_key_label(senior_id);
                                                let senior_expiry = expiry_text(senior_expiry_val);
                                                Either::Right(view! {
                                                    <div class="replace-confirm">
                                                        <p class="replace-confirm__text">
                                                            {format!("The oldest key ({senior_label}, {senior_expiry}) will stop working. A new key will take its place.")}
                                                        </p>
                                                        <div class="replace-confirm__actions">
                                                            <button
                                                                class="form-button form-button--secondary"
                                                                type="button"
                                                                on:click=move |_| show_replace.set(false)
                                                            >
                                                                "Cancel"
                                                            </button>
                                                            <button
                                                                class="form-button form-button--danger"
                                                                type="button"
                                                                disabled=move || any_pending.get()
                                                                on:click=move |_| {
                                                                    show_replace.set(false);
                                                                    mint_action.dispatch(());
                                                                }
                                                            >
                                                                "Replace oldest key"
                                                            </button>
                                                        </div>
                                                    </div>
                                                })
                                            }}
                                        </div>
                                    })
                                }
                            }}
                        </div>
                    }
                    .into_any()
                }
            }}
        </section>
    }
}
