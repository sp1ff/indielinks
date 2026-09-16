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

//! # Add Link
//!
//! This module owns the route form, query-string prefill, request, and post-save behavior used to
//! add a link to the signed-in user's collection.

use std::result::Result as StdResult;

use gloo_net::http::Request;
use leptos::{IntoView, component, html, prelude::*, view};
use leptos_router::{
    hooks::{use_location, use_navigate, use_query_map},
    params::ParamsMap,
};
use snafu::{ResultExt, Snafu};
use tap::Pipe;
use thaw::{ToastIntent, ToasterInjection};
use tracing::debug;
use url::Url;

use indielinks_shared::api::PostAddReq;

use crate::{
    components::feedback::show_toast,
    http::{error_for_status1, send_with_retry_no_body},
    types::{Api, Base},
};

#[derive(Clone, Debug, Snafu)]
pub enum Error {
    #[snafu(display("HTTP error {source}"))]
    Http { source: crate::http::Error },
    #[snafu(display("While serializing the request, {source}"))]
    RequestSer {
        source: serde_urlencoded::ser::Error,
    },
    #[snafu(display("The title must be non-empty"))]
    Title {
        source: indielinks_shared::nonempty_string::Empty,
    },
    #[snafu(display("While parsing the URL, {source}"))]
    Url { source: url::ParseError },
}

type Result<T> = StdResult<T, Error>;

#[derive(Clone, Copy, Debug)]
struct Form {
    pub url: RwSignal<String>,
    pub title: RwSignal<String>,
    pub notes: RwSignal<String>,
    pub tags: RwSignal<String>,
    pub private: RwSignal<bool>,
    pub unread: RwSignal<bool>,
    pub another: RwSignal<bool>,
}

impl Form {
    pub fn from_query(params: ParamsMap) -> Self {
        debug!("another: {:?}", params.get("another"));
        Self {
            url: RwSignal::new(params.get("url").unwrap_or_default()),
            title: RwSignal::new(params.get("title").unwrap_or_default()),
            notes: RwSignal::new(params.get("notes").unwrap_or_default()),
            tags: RwSignal::new(params.get("tags").unwrap_or_default()),
            // If a query string key has no value (e.g. "splat" in "?foo=bar&splat"), `get()` will
            // return Some("").
            private: RwSignal::new(params.get("private").is_some()),
            unread: RwSignal::new(params.get("unread").is_some()),
            another: RwSignal::new(params.get("another").is_some()),
        }
    }
    pub fn reset(&self) {
        self.url.set(Default::default());
        self.title.set(Default::default());
        self.notes.set(Default::default());
        self.tags.set(Default::default());
        self.private.set(false);
        self.unread.set(false);
    }
}

impl TryInto<PostAddReq> for Form {
    type Error = Error;
    fn try_into(self) -> Result<PostAddReq> {
        Ok(PostAddReq {
            url: Url::parse(&self.url.get()).context(UrlSnafu)?,
            title: self.title.get().try_into().context(TitleSnafu)?,
            notes: self.notes.with(|notes| notes.as_str().try_into().ok()),
            tags: self.tags.with(|tags| tags.as_str().try_into().ok()),
            dt: None,
            replace: Some(true),
            shared: Some(!self.private.get()),
            to_read: Some(self.unread.get()),
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct FormElements {
    pub url: NodeRef<html::Input>,
    pub title: NodeRef<html::Input>,
    pub tags: NodeRef<html::Input>,
}

impl Default for FormElements {
    fn default() -> Self {
        Self {
            url: NodeRef::new(),
            title: NodeRef::new(),
            tags: NodeRef::new(),
        }
    }
}

async fn submit(form: Form) -> Result<()> {
    let api = expect_context::<Api>().0;

    let request: PostAddReq = form.try_into()?;
    let qs = serde_urlencoded::to_string(&request).context(RequestSerSnafu)?;
    let url = format!("{api}/api/v1/posts/add?{qs}");
    send_with_retry_no_body(|| Request::post(&url))
        .await
        .context(HttpSnafu)?
        .pipe(error_for_status1)
        .context(HttpSnafu)
        .map(|_| ())
}

/// Hook setting-up the [AddLink] ocmponent
fn use_add_link() -> (Form, FormElements, Action<(), ()>) {
    // I *think* this is ok (the `get_untracked()`)? It seems to work in manual testing, at any
    // rate.
    let form = Form::from_query(use_query_map().get_untracked());
    let elements: FormElements = Default::default();

    let base = expect_context::<Base>().0;
    let toaster = ToasterInjection::expect_context();
    let navigate = use_navigate();

    let loc = use_location();
    let from = move || {
        let st = loc.state.get().to_js_value(); // JsValue
        web_sys::js_sys::Reflect::get(&st, &"from".into())
            .ok()
            .and_then(|v| v.as_string())
    };

    let on_submit = Action::new_local(move |_: &()| {
        // Coding tentatively, here...
        let base = base.clone();
        let from = from.clone();
        let navigate = navigate.clone();
        async move {
            match submit(form).await {
                Ok(_) => {
                    if form.another.get() {
                        form.reset();
                        show_toast(
                            toaster,
                            ToastIntent::Success,
                            "Link saved",
                            "The link was saved. Add another when you're ready.",
                        );
                        elements
                            .url
                            .get()
                            .expect("title should be mounted")
                            .focus()
                            .expect("url should be focusable");
                    } else {
                        let home = format!("{}/h", base);
                        navigate(
                            from().as_deref().unwrap_or(home.as_str()),
                            Default::default(),
                        )
                    }
                }
                Err(err @ Error::Url { .. }) => {
                    show_toast(toaster, ToastIntent::Error, "Add link", format!("{err}"));
                    elements
                        .url
                        .get()
                        .expect("title should be mounted")
                        .focus()
                        .expect("url should be focusable");
                }
                Err(err @ Error::Title { .. }) => {
                    show_toast(toaster, ToastIntent::Error, "Add link", format!("{err}"));
                    elements
                        .title
                        .get()
                        .expect("title should be mounted")
                        .focus()
                        .expect("title should be focusable");
                }
                Err(err) => show_toast(toaster, ToastIntent::Error, "Add link", format!("{err}")),
            }
        }
    });

    (form, elements, on_submit)
}

/// Renders an "add link" form. Pre-populate the form from the current query string.
// I'm still not sure how I want to handle the query string so as to make this maximally reusable.
#[component]
pub fn AddLink() -> impl IntoView {
    let (form, elements, on_submit) = use_add_link();

    view! {
        <div class="form-page">
            <section aria-labelledby="add-link-heading" class="form-card">
                <h1 class="form-card__heading" id="add-link-heading">"Add link"</h1>
                <p class="form-card__introduction">
                    "Save a page to your collection and choose how it should appear."
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
                        <label class="form-field__label" for="url">"URL"</label>
                        <input
                            aria-describedby="url-help"
                            autofocus
                            class="form-control"
                            id="url"
                            inputmode="url"
                            name="url"
                            node_ref=elements.url
                            required
                            type="url"
                            bind:value=form.url
                        />
                        <p class="form-field__help" id="url-help">
                            "Enter the complete address of the page you want to save."
                        </p>
                    </div>
                    <div class="form-field">
                        <label class="form-field__label" for="title">"Title"</label>
                        <input
                            class="form-control"
                            id="title"
                            name="title"
                            node_ref=elements.title
                            required
                            type="text"
                            bind:value=form.title
                        />
                    </div>
                    <div class="form-field">
                        <label class="form-field__label" for="notes">"Notes"</label>
                        <textarea
                            class="form-control form-control--textarea"
                            id="notes"
                            name="notes"
                            placeholder="Optional notes about this link"
                            rows="4"
                            bind:value=form.notes
                        ></textarea>
                    </div>
                    <div class="form-field">
                        <label class="form-field__label" for="tags">"Tags"</label>
                        <input
                            aria-describedby="tags-help"
                            autocomplete="off"
                            class="form-control"
                            id="tags"
                            name="tags"
                            node_ref=elements.tags
                            type="text"
                            bind:value=form.tags
                        />
                        <p class="form-field__help" id="tags-help">
                            "Separate multiple tags with commas."
                        </p>
                    </div>
                    <fieldset class="form-options">
                        <legend class="form-options__legend">"Link options"</legend>
                        <label class="form-check" for="private">
                            <input id="private" type="checkbox" bind:checked=form.private />
                            <span>
                                <strong>"Private"</strong>
                                <small>"Only you can see this link."</small>
                            </span>
                        </label>
                        <label class="form-check" for="unread">
                            <input id="unread" type="checkbox" bind:checked=form.unread />
                            <span>
                                <strong>"Unread"</strong>
                                <small>"Keep this link in your reading queue."</small>
                            </span>
                        </label>
                    </fieldset>
                    <fieldset class="form-options form-options--compact">
                        <legend class="form-options__legend">"After saving"</legend>
                        <label class="form-check" for="another">
                            <input
                                id="another"
                                name="another"
                                type="checkbox"
                                bind:checked=form.another
                            />
                            <span>"Keep this form open to add another link"</span>
                        </label>
                    </fieldset>
                    <div class="form-actions">
                        <button
                            aria-busy=move || on_submit.pending().get().to_string()
                            class="form-button form-button--primary"
                            disabled=move || on_submit.pending().get()
                            type="submit"
                        >
                            {move || if on_submit.pending().get() { "Saving…" } else { "Save link" }}
                        </button>
                    </div>
                </form>
            </section>
        </div>
    }
}
