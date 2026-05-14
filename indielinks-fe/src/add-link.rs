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

use std::result::Result as StdResult;

use gloo_net::http::Request;
use leptos::{IntoView, component, html, prelude::*, view};
use leptos_router::{
    hooks::{use_location, use_navigate, use_query_map},
    params::ParamsMap,
};
use snafu::{ResultExt, Snafu};
use tap::Pipe;
use thaw::{Toast, ToastBody, ToastIntent, ToastOptions, ToastTitle, ToasterInjection};
use tracing::debug;
use url::Url;

use indielinks_shared::api::PostAddReq;

use crate::{
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

fn do_toast(toaster: ToasterInjection, message: String) {
    toaster.dispatch_toast(
        move || {
            view! {
                <Toast>
                    <ToastTitle>"Add Post"</ToastTitle>
                    <ToastBody>
                        {message}
                    </ToastBody>
                </Toast>
            }
        },
        ToastOptions::default().with_intent(ToastIntent::Error),
    );
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
                    do_toast(toaster, format!("{err}"));
                    elements
                        .url
                        .get()
                        .expect("title should be mounted")
                        .focus()
                        .expect("url should be focusable");
                }
                Err(err @ Error::Title { .. }) => {
                    do_toast(toaster, format!("{err}"));
                    elements
                        .title
                        .get()
                        .expect("title should be mounted")
                        .focus()
                        .expect("title should be focusable");
                }
                Err(err) => do_toast(toaster, format!("{err}")),
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
        <div class="pt-[32px]">

            <form
                class="grid grid-cols-[auto_1fr] gap-x-4 gap-y-3 items-center w-full max-w-lg min-w-64 mx-auto border border-solid border-sky-100 p-8 text-gray-600"
                on:submit = move |ev| {
                    debug!("Got an {ev:?}");
                    ev.prevent_default();
                    debug!("default prevented");
                    on_submit.dispatch(());
                    debug!("dispatched");
                }>

                ////////////////////////////////////////////////////////////////////////////////////
                // URL
                ////////////////////////////////////////////////////////////////////////////////////
                <label for="url" class="">"Url:"</label>
                <input autofocus required
                       class="bg-transparent border-0 border-b outline-none focus:border-sky-600"
                       type="text" id="url" name="url"
                       placeholder="URL to be saved"
                       node_ref=elements.url
                       bind:value=form.url />

                ////////////////////////////////////////////////////////////////////////////////////
                // Title
                ////////////////////////////////////////////////////////////////////////////////////
                <label for="title" class="">"Title:"</label>
                <input required
                       class="bg-transparent border-0 border-b outline-none focus:border-sky-600"
                       type="text" id="title" name="title"
                       placeholder="Page title"
                       node_ref=elements.title
                       bind:value=form.title />

                ////////////////////////////////////////////////////////////////////////////////////
                // Notes
                ////////////////////////////////////////////////////////////////////////////////////
                <label for="notes" class="self-start">"Notes:"</label>
                <textarea
                    rows="4"
                    class="bg-transparent border-0 border-b border-r outline-none focus:border-sky-600"
                    placeholder="Optional free-form notes..."
                    id="notes" name="notes"
                    bind:value=form.notes >
                </textarea>

                ////////////////////////////////////////////////////////////////////////////////////
                // Tags
                ////////////////////////////////////////////////////////////////////////////////////
                <label for="tags" class="">"Tags:"</label>
                <input class="bg-transparent border-0 border-b outline-none focus:border-sky-600"
                       type="text"
                       id="tags" name="tags"
                       placeholder="Comma-delimited tags..."
                       node_ref=elements.tags
                       bind:value=form.tags />

                ////////////////////////////////////////////////////////////////////////////////////
                // Private, Unread
                ////////////////////////////////////////////////////////////////////////////////////
                <div class="col-span-full items-center flex gap-x-4">
                    <label class="flex gap-x-1">
                        <input type="checkbox" bind:checked=form.private/> private
                    </label>
                    <label class="flex gap-x-1">
                        <input type="checkbox" bind:checked=form.unread/> unread
                    </label>
                </div>

                ////////////////////////////////////////////////////////////////////////////////////
                // Add another, Submit
                ////////////////////////////////////////////////////////////////////////////////////
                <div class="col-span-full items-center flex gap-x-4">
                    <label class="flex gap-x-1">
                        <input type="checkbox"
                               id="another" name="another"
                               bind:checked=form.another />
                        "add another"
                    </label>
                    <input
                      class="bg-transparent border px-4 py-2 hover:bg-sky-300 hover:text-gray-900 transition-colors cursor-pointer focus:bg-sky-300"
                      type="submit"
                      value="save"/>
                </div>

            </form>
        </div>
    }
}
