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
use leptos_router::{hooks::use_query, params::Params};
use nonempty_collections::{Singleton, set::NESet, vector::NEVec};
use serde::{Serialize, Serializer};
use snafu::prelude::*;
use tap::Pipe;
use thaw::{
    Icon, InfoLabel, InfoLabelInfo, Spinner, Toast, ToastBody, ToastIntent, ToastOptions,
    ToastTitle, ToasterInjection,
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

    /// Return the query string for filtering on one tag while retaining the unread setting.
    fn qs_for_tag(&self, tag: Tagname) -> Result<String> {
        serde_urlencoded::to_string(&Self {
            tag: Some(Tags(NESet::singleton(tag))),
            page: None,
            unread: self.unread,
        })
        .context(ParamsSerSnafu)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         navigation bar                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

fn home_href(base: &str, query: &str) -> String {
    if query.is_empty() {
        format!("{base}/h")
    } else {
        format!("{base}/h?{query}")
    }
}

/// Previous and next links for the saved-link feed.
#[component]
fn Pager(last: Signal<bool>) -> impl IntoView {
    let base = expect_context::<Base>().0;
    let query_params = use_query::<QueryParams>();

    move || -> Result<_> {
        let query_params = query_params.get().unwrap_or_default();
        let previous = query_params
            .qs_for_decremented_page()?
            .map(|query| home_href(&base, &query));
        let next = if last.get() {
            None
        } else {
            Some(home_href(&base, &query_params.qs_for_incremented_page()?))
        };
        let page = query_params.page.unwrap_or(0) + 1;

        Ok(view! {
            <nav aria-label="Saved links pages" class="saved-links-pager">
                {match previous {
                    Some(href) => Either::Left(view! {
                        <a class="saved-links-pager__control" href=href>
                            <span aria-hidden="true" class="saved-links-pager__icon">
                                <Icon icon=icondata::VsChevronLeft />
                            </span>
                            <span class="saved-links-pager__label">"previous"</span>
                        </a>
                    }),
                    None => Either::Right(view! {
                        <span aria-disabled="true" class="saved-links-pager__control saved-links-pager__control--disabled">
                            <span aria-hidden="true" class="saved-links-pager__icon">
                                <Icon icon=icondata::VsChevronLeft />
                            </span>
                            <span class="saved-links-pager__label">"previous"</span>
                        </span>
                    }),
                }}
                <span class="saved-links-pager__status">"page "{page}</span>
                {match next {
                    Some(href) => Either::Left(view! {
                        <a class="saved-links-pager__control" href=href>
                            <span class="saved-links-pager__label">"next"</span>
                            <span aria-hidden="true" class="saved-links-pager__icon">
                                <Icon icon=icondata::VsChevronRight />
                            </span>
                        </a>
                    }),
                    None => Either::Right(view! {
                        <span aria-disabled="true" class="saved-links-pager__control saved-links-pager__control--disabled">
                            <span class="saved-links-pager__label">"next"</span>
                            <span aria-hidden="true" class="saved-links-pager__icon">
                                <Icon icon=icondata::VsChevronRight />
                            </span>
                        </span>
                    }),
                }}
            </nav>
        })
    }
}

/// Filter and pagination controls shown above the saved-link feed.
#[component]
fn FeedControls(last: Signal<bool>) -> impl IntoView {
    let base = expect_context::<Base>().0;
    let query_params = use_query::<QueryParams>();

    view! {
        <div class="saved-links-controls">
            {move || -> Result<_> {
                let query_params = query_params.get().unwrap_or_default();
                let (show_unread, query) = query_params.toggle_unread()?;
                Ok(view! {
                    <a class="saved-links-controls__filter" href=home_href(&base, &query)>
                        {if show_unread { "show unread" } else { "show all" }}
                    </a>
                })
            }}
            <Pager last />
        </div>
    }
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
struct DeleteParams {
    api: Api,
    url: StorUrl,
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

fn host_label(url: &StorUrl) -> String {
    let url: &Url = url.as_ref();
    url.host_str()
        .map(str::to_owned)
        .unwrap_or_else(|| url.scheme().to_owned())
}

/// Origin, saved time, and applicable state for a saved link.
#[component]
fn LinkMetadata(
    host: String,
    datetime: String,
    posted: String,
    unread: bool,
    private: bool,
) -> impl IntoView {
    view! {
        <div class="saved-link__metadata">
            <span class="saved-link__host">{host}</span>
            <span aria-hidden="true" class="saved-link__separator">"·"</span>
            <time datetime=datetime>{posted}</time>
            {unread.then(|| view! { <span class="saved-link__badge saved-link__badge--unread">"unread"</span> })}
            {private.then(|| view! {
                <span class="saved-link__badge">
                    <span aria-hidden="true" class="saved-link__badge-icon">
                        <Icon icon=icondata::FiLock />
                    </span>
                    "private"
                </span>
            })}
        </div>
    }
}

/// Alphabetized tag filters for a saved link.
#[component]
fn LinkTags(tags: Vec<Tagname>) -> impl IntoView {
    let base = expect_context::<Base>().0;
    let query_params = use_query::<QueryParams>();
    let tags = StoredValue::new(tags);

    move || -> Result<_> {
        let query_params = query_params.get().unwrap_or_default();
        let tags = tags
            .get_value()
            .into_iter()
            .sorted()
            .map(|tag| {
                let label = tag.to_string();
                let query = query_params.qs_for_tag(tag)?;
                Ok(view! {
                    <li>
                        <a class="saved-link__tag" href=home_href(&base, &query)>{label}</a>
                    </li>
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((!tags.is_empty()).then(|| {
            view! {
                <ul aria-label="Tags" class="saved-link__tags" role="list">{tags}</ul>
            }
        }))
    }
}

/// Commands that act on one saved link.
#[component]
fn LinkActions(
    post: Post,
    title: String,
    /// [WriteSignal] for setting the saved link currently being edited
    set_editing: WriteSignal<Option<StorUrl>>,
    /// Trigger a re-render
    rerender: ArcTrigger,
) -> impl IntoView {
    let api = expect_context::<Api>();
    let on_toggle = use_toggle(rerender.clone());
    let on_delete = use_delete(rerender.clone());
    let url = post.url().clone();
    let action_label = format!("Actions for {title}");
    let read_label = if post.unread() {
        "mark read"
    } else {
        "mark unread"
    };

    view! {
        <div aria-label=action_label class="saved-link__actions" role="group">
            <button
                class="saved-link__action"
                type="button"
                on:click={
                    let api = api.clone();
                    let post = post.clone();
                    move |_| {
                        on_toggle.dispatch(ToggleReadLaterParams {
                            api: api.clone(), post: post.clone()
                        });
                    }
                }
            >
                <span aria-hidden="true" class="saved-link__action-icon">
                    <Icon icon=icondata::FiBookOpen />
                </span>
                {read_label}
            </button>
            <button
                class="saved-link__action"
                type="button"
                on:click={
                    let url = url.clone();
                    move |_| set_editing.set(Some(url.clone()))
                }
            >
                <span aria-hidden="true" class="saved-link__action-icon">
                    <Icon icon=icondata::FiEdit2 />
                </span>
                "edit"
            </button>
            <button
                class="saved-link__action saved-link__action--danger"
                type="button"
                on:click={
                    let api = api.clone();
                    let url = url.clone();
                    move |_| {
                        on_delete.dispatch(DeleteParams {
                            api: api.clone(), url: url.clone()
                        });
                    }
                }
            >
                <span aria-hidden="true" class="saved-link__action-icon">
                    <Icon icon=icondata::FiTrash2 />
                </span>
                "delete"
            </button>
        </div>
    }
}

/// Render a saved link for reading.
#[component]
fn SavedLink(
    post: Post,
    set_editing: WriteSignal<Option<StorUrl>>,
    rerender: ArcTrigger,
) -> impl IntoView {
    let url = post.url().clone();
    let title = post.title().to_owned();
    let host = host_label(&url);
    let datetime = post.posted().to_rfc3339();
    let posted = post.posted().format("%Y-%m-%d %H:%M UTC").to_string();
    let unread = post.unread();
    let private = !post.public();
    let notes = post.notes().map(str::to_owned);
    let tags = post.tags().cloned().collect::<Vec<_>>();

    view! {
        <article class=if unread { "saved-link saved-link--unread" } else { "saved-link" }>
            <h3 class="saved-link__title">
                <a href=url.to_string()>{title.clone()}</a>
            </h3>
            <LinkMetadata host datetime posted unread private />
            {notes.map(|notes| view! { <p class="saved-link__notes">{notes}</p> })}
            <footer class="saved-link__footer">
                <LinkTags tags />
                <LinkActions post title set_editing rerender />
            </footer>
        </article>
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

/// Hook setting-up the [EditLink] component
// Similar to, but not quite the same as, the "Add Link" page. Not sure it's worth it to factor-out
// the commonalities?
fn use_edit_link(
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

/// Render a saved link for editing.
#[component]
fn EditLink(
    /// The saved link to be edited.
    post: Post,
    /// [WriteSignal] for setting the saved link currently being edited.
    set_editing: WriteSignal<Option<StorUrl>>,
) -> impl IntoView {
    let (form, elements, on_submit) = use_edit_link(post, set_editing);

    view! {
        <article class="saved-link saved-link--editing">
            <h3 class="saved-link__edit-heading">"edit saved link"</h3>
            <form class="saved-link__editor grid grid-cols-[auto_1fr] gap-x-3 gap-y-2 items-center w-full text-muted"
                  on:submit=move |ev| {
                      ev.prevent_default();
                      on_submit.dispatch(());
                  } >

                ////////////////////////////////////////////////////////////////////////////////////
                // URL
                ////////////////////////////////////////////////////////////////////////////////////
                <label for="url">"Url:"</label>
                <input required
                       class="bg-transparent border-0 border-b outline-none focus:border-focus"
                       type="text" id="url" name="url"
                       node_ref=elements.url
                       bind:value=form.url />

                ////////////////////////////////////////////////////////////////////////////////////
                // Title
                ////////////////////////////////////////////////////////////////////////////////////
                <label for="title">"Title:"</label>
                <input required
                       class="bg-transparent border-0 border-b outline-none focus:border-focus"
                       type="text" id="title" name="title"
                       node_ref=elements.title
                       bind:value=form.title />

                ////////////////////////////////////////////////////////////////////////////////////
                // Notes
                ////////////////////////////////////////////////////////////////////////////////////
                <label for="notes" class="self-start">"Notes:"</label>
                <textarea
                    class="bg-transparent border-0 border-b border-r outline-none focus:border-focus"
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
                       class="bg-transparent border-0 border-b outline-none focus:border-focus"
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
                    <input class="bg-transparent cursor-pointer focus:bg-brand-subtle"
                           type="submit" value="save"/>
                    <input class="bg-transparent cursor-pointer focus:bg-brand-subtle"
                           type="button" value="cancel"
                           on:click=move |_| { set_editing.set(None); } />
                </div>
            </form>
        </article>
    }
}

/// Render one page's worth of indielinks posts
#[component]
fn Links(posts: Vec<Post>, rerender: ArcTrigger) -> impl IntoView {
    // We may edit zero or one posts at a time.
    let (editing, set_editing): (ReadSignal<Option<StorUrl>>, WriteSignal<Option<StorUrl>>) =
        signal(None);

    view! {
        <ol class="saved-links-list" role="list">
            {posts
                .into_iter()
                .map(|post: Post| {
                    view! {
                        <li class="saved-links-list__item">
                            {
                                let rerender = rerender.clone();
                                move || {
                                    if Some(post.url()) == editing.get().as_ref() {
                                        Either::Left(view! {
                                            <EditLink post=post.clone() set_editing />
                                        })
                                    } else {
                                        Either::Right(view! {
                                            <SavedLink
                                                post=post.clone()
                                                set_editing
                                                rerender=rerender.clone()
                                            />
                                        })
                                    }
                                }
                            }
                        </li>
                    }
                })
                .collect_view()}
        </ol>
    }
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
    let add_href = StoredValue::new(format!("{}/a", expect_context::<Base>().0));

    // Setup a mechanism by which we can force this view to be re-rendered. A signal won't really do
    // it because there are places (say, after a delete) where we want to *force* a re-render
    // programmatically. "A trigger is a data-less signal with the sole purpose of notifying other
    // reactive code of a change."
    let rerender = ArcTrigger::new();
    let last_page = RwSignal::new(true);

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
                                last_page.set(last);
                                EitherOf3::A(view! {
                                    <FeedControls last=last_page.into() />
                                    <Links
                                        posts=posts.into()
                                        rerender=rerender.clone()
                                    />
                                    <div class="saved-links-footer-pager">
                                        <Pager last=last_page.into() />
                                    </div>
                                })},
                            (None, false) => {
                                last_page.set(true);
                                EitherOf3::B(view! {
                                    <FeedControls last=last_page.into() />
                                })
                            },
                            (None, true) => EitherOf3::C(view! {
                                <div class="mx-auto max-w-md m-8 text-muted">
                                    <p>"You don't have any saved links, yet. Click "<a href=add_href.get_value() class="text-link underline hover:text-link-hover visited:text-link-visited">"here"</a>" to start adding some."</p>
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
