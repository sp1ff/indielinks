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

//! # indielinks-fe Home component
//!
//! This module exports a [Leptos] component rendering the indielinks "home"; a pagable collection
//! of links the user has saved.
//!
//! [Leptos]: https://book.leptos.dev
//!
//! ## Implementation Notes
//!
//! ### State & Query Parameters
//!
//! This component keeps its state in the URL query parameters. I like the idea of the the more
//! keyboard-oriented user being able to drive the component by editing the query string. See
//! [QueryParams] for details.

use std::{collections::HashSet, result::Result as StdResult, str::FromStr, sync::Arc};

use gloo_net::http::Request;
use itertools::Itertools;
use leptos::{
    either::{Either, EitherOf3},
    html,
    prelude::*,
};
use leptos_router::{
    hooks::{use_navigate, use_query},
    params::Params,
};
use nonempty_collections::{set::NESet, vector::NEVec};
use serde::{Serialize, Serializer};
use snafu::prelude::*;
use tap::Pipe;
use thaw::{
    Button, ButtonAppearance, Icon, InfoLabel, InfoLabelInfo, Spinner, Toast, ToastBody,
    ToastIntent, ToastOptions, ToastTitle, ToasterInjection,
};
use tracing::{debug, error};
use url::Url;

use indielinks_shared::{
    api::{PostAddReq, PostsAllRsp},
    entities::{Post, StorUrl, Tagname},
};

use crate::{
    http::{error_for_status1, send_with_retry_no_body},
    types::{Api, Base},
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Snafu)]
pub enum Error {
    #[snafu(display("The 'tags' parameter has no value"))]
    EmptyTags,
    #[snafu(display("Error sending an HTTP request"))]
    Http { source: crate::http::Error },
    #[snafu(display("{source}"))]
    Params {
        source: leptos_router::params::ParamsError,
    },
    #[snafu(display("While re-encoding the query parameters, {source}"))]
    ParamsSer {
        source: serde_urlencoded::ser::Error,
    },
    #[snafu(display("While deserializing a posts response, {source}"))]
    PostsDe {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    #[snafu(display("'{value} cannot be interepreted as a tag"))]
    Tagname {
        value: String,
        #[snafu(source(from(indielinks_shared::entities::Error, Arc::new)))]
        source: Arc<indielinks_shared::entities::Error>,
    },
    #[snafu(display("The title must be non-empty"))]
    Title {
        source: indielinks_shared::nonempty_string::Empty,
    },
    #[snafu(display("'{value}' cannot be interpreted as an 'unread' setting"))]
    Unread { value: String },
    #[snafu(display("while serializing {request:?} to a query string, {source}"))]
    UrlEncode {
        request: PostAddReq,
        source: serde_urlencoded::ser::Error,
    },
    #[snafu(display("while parsing an URL, {source}"))]
    UrlParse { source: url::ParseError },
}

pub type Result<T> = StdResult<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        query parameters                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

// `use_query()` returns a (memoized) `Result`, so I'm going to parse strictly here at the boundry
// Newtype on which we can implement `FromStr`

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(transparent)]
struct Unread(bool);

impl FromStr for Unread {
    type Err = Error;
    fn from_str(s: &str) -> Result<Unread> {
        // If the query string just says "?unread&...", then we'll be invoked with the empty string.

        match s.to_ascii_lowercase().as_str() {
            "" | "true" | "yes" => Ok(Unread(true)),
            "false" | "no" => Ok(Unread(false)),
            _ => UnreadSnafu {
                value: s.to_owned(),
            }
            .fail(),
        }
    }
}

// Newtype on which we can implement `FromStr`
#[derive(Clone, Debug, PartialEq)]
struct Tags(NESet<Tagname>);

impl Serialize for Tags {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = self.0.iter().map(|tagname| tagname.to_string()).join(",");
        serializer.serialize_str(&s)
    }
}

impl FromStr for Tags {
    type Err = Error;
    fn from_str(s: &str) -> Result<Tags> {
        Ok(Tags(
            NESet::try_from_set(
                // We'll handle the tags being ',' or '+' delimited
                s.split(|c: char| c == ',' || c == '+')
                    .map(|s| {
                        Tagname::new(s).context(TagnameSnafu {
                            value: s.to_owned(),
                        })
                    })
                    .collect::<Result<HashSet<Tagname>>>()?,
            )
            .context(EmptyTagsSnafu)?,
        ))
    }
}

/// Typed form of the [LinkFeed] query parameters
#[derive(Clone, Debug, Default, Params, PartialEq, Serialize)]
struct QueryParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    tag: Option<Tags>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unread: Option<Unread>,
}

impl QueryParams {
    /// Check to see if the page can be decremented. So indicate if it cannot. If it can, return the
    /// query string representing the current state with the current page decremented.
    ///
    /// The utility of this admittedly odd contract is to test the condition of the page not being
    /// (implicitly or explicitly) zero, and if it isn't, return a type that can be used to
    /// infallibly replace the current query string. This way, we don't carry-out the test,
    /// conditionally carry-out the operation & find ourselves ignoring an error case we know will
    /// never occur.
    ///
    /// `Err` means failure. `Ok(None)` means the page was already at zero. `Ok(Some(...))` means
    /// we're still at zero, but we're returning the query string with the page decremented.
    fn qs_for_decremented_page(&self) -> Result<Option<String>> {
        match self.page {
            None | Some(0) => Ok(None),
            Some(n) => Ok(Some(
                serde_urlencoded::to_string(&Self {
                    tag: self.tag.clone(),
                    page: Some(n - 1),
                    unread: self.unread,
                })
                // I guess I just don't see how this can happen... but still. I just can't leave an
                // `unwrap()`
                .context(ParamsSerSnafu)?,
            )),
        }
    }
    fn qs_for_incremented_page(&self) -> Result<String> {
        serde_urlencoded::to_string(&Self {
            tag: self.tag.clone(),
            page: Some(self.page.unwrap_or(0) + 1),
            unread: self.unread,
        })
        // I guess I just don't see how this can happen... but still. I just can't leave an
        // `unwrap()`
        .context(ParamsSerSnafu)
    }
    /// Return the negated status of the `unread` field, along with what the query string would look
    /// like for that opreation
    fn toggle_unread(&self) -> Result<(bool, String)> {
        Ok((
            self.unread.map(|x| !x.0).unwrap_or(true),
            serde_urlencoded::to_string(&Self {
                tag: self.tag.clone(),
                page: self.page,
                unread: self.unread.map(|x| Unread(!x.0)).or(Some(Unread(true))),
            })
            // I guess I just don't see how this can happen... but still. I just can't leave an
            // `unwrap()`
            .context(ParamsSerSnafu)?,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         navigation bar                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
fn BackButton() -> Result<impl IntoView> {
    let navigate = use_navigate();
    let query_params = use_query::<QueryParams>()
        .get_untracked()
        .unwrap_or_default();
    match query_params.qs_for_decremented_page()? {
        None => Ok(view! { <Icon icon=icondata::VsChevronLeft class="text-gray-400"/> }),
        Some(qs) => Ok(view! {
            <Icon icon=icondata::VsChevronLeft
                class="text-gray-800"
                on_click=move |_| {
                    navigate(&format!("/h?{qs}"), Default::default())
                }/>
        }),
    }
}

#[component]
fn ToggleButton() -> Result<impl IntoView> {
    let navigate = use_navigate();
    let query_params = use_query::<QueryParams>()
        .get_untracked()
        .unwrap_or_default();

    let (toggled, qs) = query_params.toggle_unread()?;
    if toggled {
        Ok(view! {
            <Button
                class="!font-normal !text-gray-600"
                appearance=ButtonAppearance::Transparent
                on_click=move |_| { navigate(&format!("/h?{qs}"), Default::default()) }
            >
                "unread links"
            </Button>
        })
    } else {
        Ok(view! {
            <Button
                class="!font-normal !text-gray-600"
                appearance=ButtonAppearance::Transparent
                on_click=move |_| { navigate(&format!("/h?{qs}"), Default::default()) }
            >
                "all links"
            </Button>
        })
    }
}

#[component]
fn ForwardButton(last: bool) -> Result<impl IntoView> {
    let navigate = use_navigate();
    let query_params = use_query::<QueryParams>()
        .get_untracked()
        .unwrap_or_default();
    if last {
        Ok(view! { <Icon icon=icondata::VsChevronRight class="text-gray-400"/> })
    } else {
        let qs = query_params.qs_for_incremented_page()?;
        Ok(view! { <Icon icon=icondata::VsChevronRight
        class="text-gray-800"
        on_click=move |_| {
            navigate(&format!("/h?{qs}"), Default::default())
        } />})
    }
}

/// A component representing the navigation bar at the top & bottom of the link list. The caller
/// shall indicate whether this is the last page or not.
#[component]
fn Nav(last: bool) -> Result<impl IntoView> {
    let query_params = use_query::<QueryParams>();
    Ok(view! {
        <div class="mx-auto flex">
            // Alright, our "nav bar" consists of the following:
            <div class="mx-auto flex items-center">
            // - a back button :: disabled on page=0, otherwise action is to decrement the page
            <BackButton />
            // - page indicator :: always present, not "live"; simple enough to just code-up "inline"
            {
                let query_params = query_params.get_untracked().unwrap_or_default();
                view! {
                    <div class="inline-block px-[12px] py-[5px] font-normal text-gray-600">
                        "page "{query_params.page.unwrap_or(0)}
                    </div>
                }
            }
            // - "all posts"/"unread posts" :: reacts to the "unread" parameter, action is to toggle
            <ToggleButton />
            // - forward button :: disabled on last page, otherwise action is to increment the page
            <ForwardButton last />
            </div>
        </div>
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                     the `Links` component                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

// Our event handler will ultimately invoke this Action with a call to the `dispatch()` method,
// which can only take a single parameter, so we need a little utility struct to package up the
// multiple parameters we actually need:
#[derive(Clone, Debug)]
struct ToggleReadLaterParams {
    pub api: Api,
    pub post: Post,
}

// Given the amount of moving/cloning in Leptos apps, I'd really prefer to *move* the `Post` into
// the new `PostAddReq`, but since all the members are private, and since module
// indielinks_shared::entities knows nothing about `PostAddReq`, I'm not sure how to do that.
fn copy_post_to_add_req(post: &Post) -> PostAddReq {
    PostAddReq {
        url: post.url().clone().into(),
        // This is awful-- decded whether `Post::title` can be empty or not
        title: post
            .title()
            .try_into()
            .unwrap_or("<untitled>".try_into().unwrap(/* known good */)),
        notes: match post.notes() {
            Some(s) => {
                if s.is_empty() {
                    None
                } else {
                    Some(s.try_into().unwrap(/* known good */))
                }
            }
            None => None,
        },
        tags: Some(post.tags().join(",")),
        dt: Some(post.posted()),
        replace: Some(true),
        shared: Some(post.public()),
        to_read: Some(post.unread()),
    }
}

fn use_toggle(rerender: ArcTrigger) -> Action<ToggleReadLaterParams, Result<()>> {
    let on_toggle = Action::<ToggleReadLaterParams, Result<()>>::new_unsync(
        move |params: &ToggleReadLaterParams| {
            let mut request = copy_post_to_add_req(&params.post);
            request.to_read = request.to_read.map(|x| !x);
            let params = params.clone();
            async move {
                let qs =
                    serde_urlencoded::to_string(&request).context(UrlEncodeSnafu { request })?;
                let url = format!("{}/api/v1/posts/add?{}", params.api.0, qs);
                send_with_retry_no_body(move || Request::post(&url))
                    .await
                    .and_then(error_for_status1)
                    .context(HttpSnafu)
                    .map(|_| ())
            }
        },
    );

    let toaster = ToasterInjection::expect_context();

    Effect::new({
        let rerender = rerender.clone();
        move || {
            // This is really weird: the docs all indicate that `on_toggle.value().get()` should
            // return `Option<thing>` where "thing" is the return type of my Action. Turns out,
            // that's only true when "thing" is Clone! And it was _really_ hard to figure that out:
            // the `get()` method is on the `Get` trait, which is not directly implemented by
            // `MappedSignal` (the type returned from `value()`)-- rather, it gets it from a blanket
            // implementation of `Get`. But that blanket implementation is conditional on "thing"
            // being Clone. This seems to remove it from the list of blanket implementations on the
            // documentation page for `MappedSignal`, meaning that you have to "just know" that
            // `get()` is from `Get` and navigate from there.
            match on_toggle.value().get() {
                Some(Ok(_)) => rerender.notify(),
                Some(Err(err)) => {
                    // May want to factor this out
                    error!("While toggling read-only: {err:?}");
                    toaster.dispatch_toast(
                        move || {
                            view! {
                                <Toast>
                                    <ToastTitle>"Read-only"</ToastTitle>
                                    <ToastBody>{format!("{err}")}</ToastBody>
                                </Toast>
                            }
                        },
                        ToastOptions::default().with_intent(ToastIntent::Error),
                    )
                }
                None => (),
            }
        }
    });

    on_toggle
}

#[derive(Clone, Debug)]
pub struct DeleteParams {
    pub api: Api,
    pub url: StorUrl,
}

fn use_delete(rerender: ArcTrigger) -> Action<DeleteParams, Result<()>> {
    let on_delete = Action::<DeleteParams, Result<()>>::new_unsync(move |params: &DeleteParams| {
        let params = params.clone();
        async move {
            let mut full_url = url::Url::parse(&format!("{}/api/v1/posts/delete", params.api.0))
                .context(UrlParseSnafu)?;
            full_url.query_pairs_mut().append_pair("url", &params.url);
            let url = format!("{full_url}");
            send_with_retry_no_body(|| Request::post(&url))
                .await
                .and_then(error_for_status1)
                .context(HttpSnafu)
                .map(|_| ())
        }
    });

    let toaster = ToasterInjection::expect_context();

    Effect::new({
        let rerender = rerender.clone();
        move || {
            match on_delete.value().get() {
                Some(Ok(_)) => rerender.notify(),
                Some(Err(err)) => {
                    // May want to factor this out
                    error!("While deleting: {err:?}");
                    toaster.dispatch_toast(
                        move || {
                            view! {
                                <Toast>
                                    <ToastTitle>"Delete"</ToastTitle>
                                    <ToastBody>{format!("{err}")}</ToastBody>
                                </Toast>
                            }
                        },
                        ToastOptions::default().with_intent(ToastIntent::Error),
                    )
                }
                None => (),
            }
        }
    });

    on_delete
}

/// Render a [Post] for viewing (as opposed to editing)
#[component]
fn ViewPost(
    /// The [Post] to be rendered
    post: Post,
    /// [WriteSignal] for setting the [Post] currently being edited
    set_editing: WriteSignal<Option<StorUrl>>,
    /// Trigger a re-render
    rerender: ArcTrigger,
) -> impl IntoView {
    let base = expect_context::<Base>().0;
    let api = expect_context::<Api>();

    // Seems inefficient-- pass the query parameters as a property?
    let query_params = use_query::<QueryParams>()
        .get_untracked()
        .unwrap_or_default();

    let on_toggle = use_toggle(rerender.clone());
    let on_delete = use_delete(rerender.clone());

    let url = post.url().clone();
    let title = post.title().to_owned();
    let posted = post.posted().format("%Y-%m-%d %H:%M:%S").to_string();

    let mut tag_base = format!("{base}/h?");
    let unread = query_params.unread.unwrap_or(Unread(false)).0;
    if unread {
        tag_base += "unread&"
    }

    view! {
        // The link itself (larger, more prominent)
        <div class="text-lg">
            <a href={ url.to_string() } class="text-blue-600 underline hover:text-blue-800"> { title }</a>
        </div>
        // The post time & tags (smaller, gray text)
        <div class="flex">
            <div class="flex-[0 0 auto] text-gray-400"> { posted } </div>
            <div class="px-2">
            {
                post
                    .tags()
                    .cloned()
                    .map(|tag| view! {
                        <a href={ format!("{tag_base}tag={tag}") }>{ format!("{tag}") }</a> " "
                    })
                    .collect::<Vec<_>>()
            }
            </div>
        </div>
        // Row of controls (right-aligned)
        <div class="self-end text-sm">
            // Should just ditch the `<Button>` component entirely? I spend a lot of effort just
            // overriding the styles it sets unconditionally.
            <Button
              appearance=ButtonAppearance::Transparent
              class="!text-sm !text-gray-600 !px-1 !py-0 !min-w-0 !font-normal">
                "conversation"
            </Button>
            <Button
              appearance=ButtonAppearance::Transparent
              class="!text-sm !text-gray-600 !px-1 !py-0 !min-w-0 !font-normal"
              on_click={
                    let api = api.clone();
                    let post = post.clone();
                    move |_| {
                        on_toggle.dispatch(ToggleReadLaterParams {
                            api: api.clone(), post: post.clone()
                        });
                }}>
                "mark as "{ if post.unread() { "read" } else { "unread"} }
            </Button>
            <Button
              appearance=ButtonAppearance::Transparent
              class="!text-sm !text-gray-600 !px-1 !py-0 !min-w-0 !font-normal"
              // `on_edit` is going to be *moved* out into the Leptos runtime, if not into the DOM
              // itself. As such, needs to implement `Fn` (i.e. not `FnOnce` or `FnMut`).
              on_click={
                  let url=url.clone();
                  move |_| {
                      // and, since on each invocation, we're moving `url` into the `set_editing`
                      // signal, we need to clone it on each invocation, so that we can be called
                      // repeatedly.
                      set_editing.set(Some(url.clone()));
                  }
              } >
                "edit"
            </Button>
            <Button
              appearance=ButtonAppearance::Transparent
              class="!text-sm !text-gray-600 !px-1 !py-0 !min-w-0 !font-normal"
              on_click={
                  let api = api.clone();
                  let url = url.clone();
                  move |_| {
                      on_delete.dispatch(DeleteParams {
                          api: api.clone(), url: url.clone()
                      });}
              } >
                "delete"
            </Button>
        </div>
    }
}

#[derive(Clone, Copy, Debug)]
struct Form {
    pub url: RwSignal<String>,
    pub title: RwSignal<String>,
    pub notes: RwSignal<String>,
    pub tags: RwSignal<String>,
    pub private: RwSignal<bool>,
    pub unread: RwSignal<bool>,
}

impl From<Post> for Form {
    fn from(value: Post) -> Self {
        Self {
            url: RwSignal::new(value.url().to_string()),
            title: RwSignal::new(value.title().to_string()),
            notes: RwSignal::new(value.notes().map(|s| s.to_owned()).unwrap_or(String::new())),
            tags: RwSignal::new(
                value
                    .tags()
                    .map(|n| n.to_string())
                    .collect::<Vec<String>>()
                    .join(","),
            ),
            private: RwSignal::new(!value.public()),
            unread: RwSignal::new(value.unread()),
        }
    }
}

impl TryInto<PostAddReq> for Form {
    type Error = Error;
    fn try_into(self) -> Result<PostAddReq> {
        Ok(PostAddReq {
            url: Url::parse(&self.url.get()).context(UrlParseSnafu)?,
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
    let qs = serde_urlencoded::to_string(&request).context(ParamsSerSnafu)?;
    debug!("submit(): {qs}");
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

/// Hook setting-up the [EditPost] component
// Similar to, but not quite the same as, the "Add Link" page. Not sure it's worth it to factor-out
// the commonalities?
fn use_edit_post(
    post: Post,
    set_editing: WriteSignal<Option<StorUrl>>,
) -> (Form, FormElements, Action<(), ()>) {
    let form: Form = post.into();
    let elements: FormElements = Default::default();
    let toaster = ToasterInjection::expect_context();

    let on_submit = Action::new_local(move |_: &()| async move {
        match submit(form).await {
            Ok(_) => set_editing.set(None),
            Err(err @ Error::UrlParse { .. }) => {
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
    });

    (form, elements, on_submit)
}

/// Render a [Post] for editing (as opposed to viewing)
#[component]
fn EditPost(
    /// The [Post] to be edited
    post: Post,
    /// [WriteSignal] for setting the [Post] currently being edited
    set_editing: WriteSignal<Option<StorUrl>>,
) -> impl IntoView {
    let (form, elements, on_submit) = use_edit_post(post, set_editing);

    view! {
        <div>
            <form class="grid grid-cols-[auto_1fr] gap-x-3 gap-y-2 items-center w-full text-gray-600"
                  on:submit=move |ev| {
                      ev.prevent_default();
                      on_submit.dispatch(());
                  } >

                ////////////////////////////////////////////////////////////////////////////////////
                // URL
                ////////////////////////////////////////////////////////////////////////////////////
                <label for="url">"Url:"</label>
                <input required
                       class="bg-transparent border-0 border-b outline-none focus:borkder-sky-600"
                       type="text" id="url" name="url"
                       node_ref=elements.url
                       bind:value=form.url />

                ////////////////////////////////////////////////////////////////////////////////////
                // Title
                ////////////////////////////////////////////////////////////////////////////////////
                <label for="title">"Title:"</label>
                <input required
                       class="bg-transparent border-0 border-b outline-none focus:borkder-sky-600"
                       type="text" id="title" name="title"
                       node_ref=elements.title
                       bind:value=form.title />

                ////////////////////////////////////////////////////////////////////////////////////
                // Notes
                ////////////////////////////////////////////////////////////////////////////////////
                <label for="notes" class="self-start">"Notes:"</label>
                <textarea
                    class="bg-transparent border-0 border-b border-r outline-none focus:borkder-sky-600"
                    rows="4"
                    placeholder="Optional free-form notes..."
                    id="notes" name="notes"
                    bind:value=form.notes >
                </textarea>

                ////////////////////////////////////////////////////////////////////////////////////
                // Tags
                ////////////////////////////////////////////////////////////////////////////////////
                <label for="tags" class="">"Tags:"</label>
                <input type="text"
                       class="bg-transparent border-0 border-b outline-none focus:borkder-sky-600"
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

                <div class="col-span-full items-center flex gap-x-4">
                    <input class="bg-transparent cursor-pointer focus:bg-sky-300"
                           type="submit" value="save"/>
                    <input class="bg-transparent cursor-pointer focus:bg-sky-300"
                           type="button" value="cancel"
                           on:click=move |_| { set_editing.set(None); } />
                </div>
            </form>
        </div>
    }
}

/// Render one page's worth of indielinks posts
#[component]
fn Links(posts: Vec<Post>, rerender: ArcTrigger) -> impl IntoView {
    // We may edit zero or one posts at a time.
    let (editing, set_editing): (ReadSignal<Option<StorUrl>>, WriteSignal<Option<StorUrl>>) =
        signal(None);

    posts
        .into_iter()
        .map(|post: Post| {
            view! {
                // This will need to be made much more complex, to handle viewing posts, editing them
                // and viewing the conversation associated with each. For now, while getting basic
                // pagination up & working, let's just show a div and the title.
                <div class="flex flex-col border border-solid border-sky-100 m-2 p-2 text-gray-600">
                {
                    let rerender = rerender.clone();
                    move || {
                        if Some(post.url()) == editing.get().as_ref() {
                            Either::Left(view! {
                                <EditPost post=post.clone() set_editing />
                            } )
                        } else {
                            Either::Right(view!{
                                <ViewPost post=post.clone() set_editing rerender=rerender.clone() />
                            })
                        }
                    }
                }
                </div>
            }
        })
        .collect::<Vec<_>>()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                the `LinkFeed` public component                                 //
////////////////////////////////////////////////////////////////////////////////////////////////////

async fn load_data(
    api: String,
    page: usize,
    page_size: usize,
    tags: Option<NESet<Tagname>>,
    unread_only: bool,
) -> Result<(Option<NEVec<Post>>, bool)> {
    let mut url = format!(
        "{api}/api/v1/posts/all?start={}&results={page_size}&unread={unread_only}",
        page * page_size,
    );
    if let Some(tags) = &tags {
        url += "&tag=";
        url += &tags
            .into_iter()
            .map(|tagname| tagname.to_string())
            .join(",");
    }
    let posts = send_with_retry_no_body(|| Request::post(&url))
        .await
        .context(HttpSnafu)?
        .pipe(error_for_status1)
        .context(HttpSnafu)?
        .json::<PostsAllRsp>()
        .await
        .context(PostsDeSnafu)?
        .posts
        .pipe(NEVec::try_from_vec);

    Ok((posts, tags.is_none() && (!unread_only)))
}

// This should really be configurable, ideally by the user.
const PAGE_SIZE: usize = 20;

/// A component for displaying a paged "feed" of saved links.
///
/// Expects to have the `Api` available via context.
#[component]
pub fn LinkFeed() -> impl IntoView {
    let api = expect_context::<Api>().0;

    // Setup a mechanism by which we can force this view to be re-rendered. A signal won't really do
    // it because there are places (say, after a delete) where we want to *force* a re-render
    // programmatically. "A trigger is a data-less signal with the sole purpose of notifying other
    // reactive code of a change."
    let rerender = ArcTrigger::new();

    // Setup a local resource yielding a `Result<Option<NEVec<Post>>>`. This will reactively track
    // the query parameters, re-running and yielding a new `Result` every time they are changed
    // (either interactively by the user, or programmatically through us using `navigate()`). Any
    // code that reactively tracks this resource's `Result` with then also be re-run.
    let posts = LocalResource::new({
        let rerender = rerender.clone();
        move || {
            let api = api.clone();
            rerender.track();
            async move {
                let query_params = use_query::<QueryParams>().get().unwrap_or_default();
                load_data(
                    api,
                    query_params.page.unwrap_or(0),
                    PAGE_SIZE,
                    query_params.tag.map(|tags| tags.0),
                    query_params.unread.unwrap_or(Unread(false)).0,
                )
                .await
            }
        }
    });

    view! {
        <ErrorBoundary
            // In the event of an unrecoverable error in any of our child components, we'll end-up
            // here, rendering a little "Oops!" label on which the usewr can click to get more
            // information. This mirrors the fallback for the user's home feed.
            fallback=|errors| view! {
                <InfoLabel>
                    <InfoLabelInfo slot>
                        <ul>
                        { move || errors
                          .get()
                          .into_iter()
                          .map(|(_, err)| view!{ <li>{err.to_string()}</li>})
                          .collect::<Vec<_>>() }
                        </ul>
                    </InfoLabelInfo>
                    "Ooops!"
                </InfoLabel>
            } >
            <Transition fallback=move || view! { <Spinner /> } >
            {
                // The body of the `Transition` is a lambda yielding a `Result`; that means we can
                // use the `?` sigil naturally below in cases where we want to invoke our fallback,
                // above.
                move || -> Result<_> {
                    // Our resource can yield no posts for two reasons that we want to distinguish
                    // visually:
                    //     - no posts because the user hasn't saved any, in which we just display a
                    //       little message inviting them to start saving some.
                    //     - no posts because their filter ruled-out all their posts, in which case
                    //       we just show an empty list
                    let posts : Option<(Option<NEVec<Post>>, bool)> = posts.get().transpose()?;
                    // Technically, we're rendering an `Option<EitherOf3<...>>` here, but we *know*
                    // we'll never yield the `None` case because we're in a transition, so just work
                    // "inside" `posts` via `.map()` to avoid having to explicitly handle the `None`
                    // case.
                    Ok(posts.map(|(maybe_posts, none_means_no_posts)| {
                        match (maybe_posts, none_means_no_posts) {
                            (Some(posts), _) => {
                                let last = posts.len().get() != PAGE_SIZE;
                                EitherOf3::A(view! {
                                    // Repeating the navigation widgets at the top and bottom.
                                    <Nav last />
                                    <Links posts=posts.into() rerender=rerender.clone() />
                                    <Nav last />
                                })},
                            (None, false) => {
                                EitherOf3::B(view! {
                                    <Nav last=true />
                                    <Links posts=Vec::new() rerender=rerender.clone() />
                                })
                            },
                            (None, true) => EitherOf3::C(view! {
                                <div class="mx-auto max-w-md m-8 text-gray-600">
                                    <p>"You don't have any saved links, yet. Click "<a href="/a" class="text-blue-600 underline hover:text-blue-800 visited:text-purple-600">"here"</a>" to start adding some."</p>
                                </div>
                            })
                        }
                    }))
                }
            }
            </Transition>
        </ErrorBoundary>
    }
}
