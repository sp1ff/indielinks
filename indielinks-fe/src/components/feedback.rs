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

//! # Shared feedback presentation
//!
//! This module presents asynchronous loading, failure, empty, and toast states consistently while
//! leaving resource ownership and recovery behavior in each feature module.

use leptos::{either::Either, prelude::*};
use thaw::{
    Icon, Spinner, Toast, ToastBody, ToastIntent, ToastOptions, ToastTitle, ToasterInjection,
};

/// The amount of space a loading state occupies.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LoadingPlacement {
    /// A compact state rendered inside an existing panel.
    #[default]
    Panel,
    /// The branded state shown before the application router is mounted.
    Startup,
}

impl LoadingPlacement {
    fn class(self) -> &'static str {
        match self {
            Self::Panel => "feedback-state feedback-state--loading",
            Self::Startup => "feedback-state feedback-state--loading feedback-state--startup",
        }
    }
}

/// The recovery action offered by an empty state.
#[derive(Clone)]
pub enum EmptyAction {
    /// Navigate to another application route.
    Link { href: String, label: &'static str },
    /// Invoke a callback without navigating.
    Button {
        label: &'static str,
        callback: Callback<()>,
    },
}

/// Render an asynchronous loading status with a textual equivalent.
#[component]
pub fn LoadingState(
    label: &'static str,
    #[prop(optional)] placement: LoadingPlacement,
) -> impl IntoView {
    view! {
        <div aria-live="polite" class=placement.class() role="status">
            <Show when=move || placement == LoadingPlacement::Startup>
                <span aria-hidden="true" class="feedback-state__startup-mark">
                    <span class="feedback-state__startup-mark-prefix">"indie"</span>
                    <span class="feedback-state__startup-mark-suffix">"links"</span>
                </span>
            </Show>
            <Spinner />
            <span class="feedback-state__message">{label}</span>
        </div>
    }
}

/// Render a resource failure with optional recovery and preserved diagnostics.
#[component]
pub fn ErrorState(
    title: &'static str,
    errors: ArcRwSignal<Errors>,
    #[prop(optional)] retry: Option<Callback<()>>,
) -> impl IntoView {
    view! {
        <div class="feedback-state feedback-state--error" role="alert">
            <span aria-hidden="true" class="feedback-state__icon">
                <Icon icon=icondata::FiAlertCircle />
            </span>
            <div class="feedback-state__content">
                <h3 class="feedback-state__heading">{title}</h3>
                <p class="feedback-state__message">
                    "Try again. If the problem continues, the service may be unavailable."
                </p>
                <div class="feedback-state__actions">
                    {retry.map(|callback| view! {
                        <button
                            class="form-button form-button--secondary"
                            type="button"
                            on:click=move |_| callback.run(())
                        >
                            <Icon icon=icondata::IoRefresh />
                            "Retry"
                        </button>
                    })}
                </div>
                <details class="feedback-state__details">
                    <summary>"Technical details"</summary>
                    <ul>
                        {move || errors
                            .get()
                            .into_iter()
                            .map(|(_, error)| view! { <li>{error.to_string()}</li> })
                            .collect_view()}
                    </ul>
                </details>
            </div>
        </div>
    }
}

/// Render a quiet empty state and the action that can resolve it.
#[component]
pub fn EmptyState(
    title: &'static str,
    message: &'static str,
    #[prop(optional)] action: Option<EmptyAction>,
) -> impl IntoView {
    view! {
        <div class="feedback-state feedback-state--empty">
            <span aria-hidden="true" class="feedback-state__icon">
                <Icon icon=icondata::FiInbox />
            </span>
            <div class="feedback-state__content">
                <h3 class="feedback-state__heading">{title}</h3>
                <p class="feedback-state__message">{message}</p>
                <div class="feedback-state__actions">
                    {action.map(|action| match action {
                        EmptyAction::Link { href, label } => Either::Left(view! {
                            <a class="form-button form-button--primary" href=href>{label}</a>
                        }),
                        EmptyAction::Button { label, callback } => Either::Right(view! {
                            <button
                                class="form-button form-button--secondary"
                                type="button"
                                on:click=move |_| callback.run(())
                            >
                                <Icon icon=icondata::IoRefresh />
                                {label}
                            </button>
                        }),
                    })}
                </div>
            </div>
        </div>
    }
}

/// Dispatch a consistently structured application toast.
pub fn show_toast(
    toaster: ToasterInjection,
    intent: ToastIntent,
    title: impl Into<String>,
    message: impl Into<String>,
) {
    let title = title.into();
    let message = message.into();
    toaster.dispatch_toast(
        move || {
            view! {
                <Toast class="indielinks-toast">
                    <ToastTitle>{title}</ToastTitle>
                    <ToastBody>{message}</ToastBody>
                </Toast>
            }
        },
        ToastOptions::default().with_intent(intent),
    );
}
