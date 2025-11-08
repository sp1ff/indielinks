// Copyright (C) 2025 Michael Herstine <sp1ff@pobox.com>
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
//! This module exports a [Leptos] component rendering the indielinks Home page.
//!
//! [Leptos]: https://book.leptos.dev

use gloo_net::http::Request;
use indielinks_shared::{
    api::{PostAddReq, PostsAllRsp},
    entities::{Post, StorUrl},
};
use itertools::Itertools;
use leptos::{
    either::Either,
    html::{self},
    prelude::*,
};
use leptos_router::hooks::use_query_map;
use snafu::{prelude::*, Backtrace};
use tap::Pipe;
use tracing::{debug, error};
use web_sys::MouseEvent;

use crate::{
    http::{error_for_status, send_with_retry, string_for_status},
    types::{Api, Base, PageSize, Token, USER_AGENT},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("While deserializing the response body as JSON, {source}"))]
    Json { source: gloo_net::Error },
    #[snafu(display("While sending an HTTP request, {source}"))]
    Request { source: gloo_net::Error },
    #[snafu(display("while serializing {request:?} to a query string, {source}"))]
    UrlEncode {
        request: PostAddReq,
        source: serde_urlencoded::ser::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("while parsing an URL, {source}"))]
    UrlFrom {
        source: indielinks_shared::entities::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("while parsing an URL, {source}"))]
    UrlParse {
        source: url::ParseError,
        backtrace: Backtrace,
    },
}

// Given the amount of moving/cloning in Leptos apps, I'd really prefer to *move* the `Post` into
// the new `PostAddReq`, but since all the members are private, and since module
// indielinks_shared::entities knows nothing about `PostAddReq`, I'm not sure how to do that.
fn copy_post_to_add_req(post: &Post) -> PostAddReq {
    PostAddReq {
        url: post.url().clone(),
        title: post.title().to_owned(),
        notes: post.notes().map(str::to_owned),
        tags: Some(post.tags().join(",")),
        dt: Some(post.posted()),
        replace: Some(true),
        shared: Some(post.public()),
        to_read: Some(post.unread()),
    }
}

/// Render a [Post] for viewing (as opposed to editing)
///
/// We expect the HTTP client, indielinks API location, and the login token to be available in the context.
#[component]
fn ViewPost(
    /// The [Post] to be rendered
    post: Post,
    /// [WriteSignal] for setting the [Post] currently being edited
    set_editing: WriteSignal<Option<StorUrl>>,
    /// Trigger a re-render
    rerender: ArcTrigger,
) -> impl IntoView {
    debug!("Post invoked.");

    let api = expect_context::<Api>();
    let base = expect_context::<Base>().0;
    let token = expect_context::<Token>().get_untracked().expect("While rendering a Post, the authorization \
                                                                  token was found to be empty. This is a bug \
                                                                  and should be reported.");
    // Unlike `on_edit`, which merely updates reactive state within our app, `on_toggle` needs to
    // call back to the indielinks API. TBH, I'm not sure this is the right primitive, but, per
    // <https://book.leptos.dev/async/13_actions.html>: "If you’re trying to occasionally run an
    // async function in response to something like a user clicking a button, you probably want to
    // use an Action."

    // Now, our event handler will ultimately invoke this Action with a call to the `dispatch()`
    // method, which can only take a single parameter, so we need a little utility struct to package
    // up the multiple parameters we actually need:
    #[derive(Clone, Debug)]
    struct ToggleReadLaterParams {
        pub api: Api,
        pub token: String,
        pub post: Post,
    }

    let on_toggle = Action::<ToggleReadLaterParams, Result<(), Error>>::new_unsync(
        move |params: &ToggleReadLaterParams| {
            let mut request = copy_post_to_add_req(&params.post);
            request.to_read = request.to_read.map(|x| !x);
            let params = params.clone();
            async move {
                let qs =
                    serde_urlencoded::to_string(&request).context(UrlEncodeSnafu { request })?;
                let url = url::Url::parse(&format!("{}/api/v1/posts/add?{}", params.api.0, qs))
                    .context(UrlParseSnafu)?;
                send_with_retry(|| {
                    Request::post(url.as_str())
                        .header("User-Agent", USER_AGENT)
                        .header("Authorization", &format!("Bearer {}", params.token))
                        .send()
                })
                .await
                .and_then(error_for_status)
                .context(RequestSnafu)
                .map(|_| ())
            }
        },
    );

    // Now, when we invoke `on_toggle`, an API call back to indielinks will take place in the
    // background. The more I work with it, the more I think `Action` *is* the right thing to use,
    // here, because it's reactive: I can `get()` its result in a view, or use its `pending()`
    // status in a view, and the view will re-render when the API call returns and the `Action`
    // yields its result. By returning a `Result`, I had even thought to use it with ErrorBoundry.
    //
    // In this particular case, however, there's nothing to render: on success we want to trigger a
    // re-render via `rerender`, and on failure we just log to the console. The thing to do here
    // seems to be an `Effect`:
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
            //
            // Rather than try 'n make `Error` Clone, I'll just go with the "with" form:
            on_toggle.value().with(|result| {
                if let Some(result) = result {
                    match result {
                        Ok(_) => rerender.notify(),
                        Err(err) => error!("Failed to toggle 'unread': {err}"),
                    }
                }
            })
        }
    });
    let url = post.url().clone();

    // `on_edit` is going to be *moved* out into the Leptos runtime, if not into the DOM itself. As
    // such, needs to implement `Fn` (i.e. not `FnOnce` or `FnMut`).
    let on_edit = {
        // Therefore, we need to move any values it needs into the closure...
        let url = url.clone();
        move |_: MouseEvent| {
            // and, since on each invocation, we're moving `url` into the `set_editing` signal, we
            // need to clone it on each invocation, so that we can be called repeatedly.
            set_editing.set(Some(url.clone()));
        }
    };

    #[derive(Clone, Debug)]
    pub struct DeleteParams {
        pub api: Api,
        pub token: String,
        pub url: StorUrl,
    }

    let on_delete =
        Action::<DeleteParams, Result<(), Error>>::new_unsync(move |params: &DeleteParams| {
            let params = params.clone();
            async move {
                let mut full_url =
                    url::Url::parse(&format!("{}/api/v1/posts/delete", params.api.0))
                        .context(UrlParseSnafu)?;
                full_url.query_pairs_mut().append_pair("url", &params.url);
                send_with_retry(|| {
                    Request::post(full_url.as_str())
                        .header("User-Agent", USER_AGENT)
                        .header("Authorization", &format!("Bearer {}", params.token))
                        .send()
                })
                .await
                .and_then(error_for_status)
                .context(RequestSnafu)
                .map(|_| ())
            }
        });

    Effect::new({
        let rerender = rerender.clone();
        move || {
            on_delete.value().with(|result| {
                if let Some(result) = result {
                    match result {
                        Ok(_) => rerender.notify(),
                        Err(err) => error!("Failed to delete post: {err}"),
                    }
                }
            })
        }
    });

    // Alright, with that bit of reactivity setup, now I'm ready to lay-out the actual UI.
    let posted = post.posted().format("%Y-%m-%d %H:%M:%S").to_string();
    let title = post.title().to_owned();

    let query = use_query_map(); // Memo<...>
    let unread = query.get().get("unread").map(|s| {
        let t = s.to_ascii_lowercase();
        t == "true" || t == "yes" || t == "" || t == "please"
    });
    let mut link_base = format!("{base}/h?page=0");
    if unread.is_some_and(|b| b) {
        link_base += "&unread";
    }

    view! {
        <div class="post">
            <div class="post-title"><a href={ url.to_string() }> { title } </a></div>
            <div class="post-info">
                <div class="post-info-left"> { posted } </div>
                <div class="post-info-right"> {
                    post
                        .tags()
                        .cloned()
                        .map(|tag| {
                            view! {
                                <a href={ format!("{link_base}&tag={tag}") }>{ format!("{tag}") } </a> " "
                            }
                        })
                        .collect::<Vec<_>>()
                } </div>
            </div>
            <div class="post-controls">
                <a href="#" on:click={
                    let api = api.clone();
                    let token = token.clone();
                    let post = post.clone();
                    move |_| {
                        on_toggle.dispatch(ToggleReadLaterParams {
                            api: api.clone(), token: token.clone(), post: post.clone()
                        });
                }}>"mark as "{ if post.unread() { "read" } else { "unread" } }</a>
                " "
                <a href="#" on:click=on_edit>edit</a>
                " "
                <a href="#" on:click={
                    let api = api.clone();
                    let token = token.clone();
                    let url = url.clone();
                    move |_| {
                        on_delete.dispatch(DeleteParams {
                            api: api.clone(), token: token.clone(), url: url.clone(),});
                    }}>delete</a>
            </div>
        </div>
    }
}

/// Render a [Post] for editing (as opposed to viewing)
///
/// We expect the HTTP client, indielinks API location, and the login token to be available in the context.
#[component]
fn EditPost(
    /// The [Post] to be edited
    post: Post,
    /// [WriteSignal] for setting the [Post] currently being edited
    set_editing: WriteSignal<Option<StorUrl>>,
) -> impl IntoView {
    debug!("EditPost invoked.");

    let api = expect_context::<Api>();
    let token = expect_context::<Token>().get_untracked().expect("While editing a Post, the authorization \
                                                                  token was found to be empty. This is a bug \
                                                                  and should be reported.");
    // This view really just amounts to a form, with uncontrolled elements
    // <https://book.leptos.dev/view/05_forms.html#uncontrolled-inputs>: the inputs are under the
    // control of the browser, and we don't do anything with 'em (other than initialize them) until
    // submit time.
    //
    // "NodeRef is a kind of reactive smart pointer: we can use it to access the underlying DOM node."
    let url_element: NodeRef<html::Input> = NodeRef::new();
    let title_element: NodeRef<html::Input> = NodeRef::new();
    let notes_element: NodeRef<html::Input> = NodeRef::new();
    let tags_element: NodeRef<html::Input> = NodeRef::new();
    let private_element: NodeRef<html::Input> = NodeRef::new();
    let unread_element: NodeRef<html::Input> = NodeRef::new();

    // I'm still not familiar with my options in working with forms in Leptos. This, however, seems
    // *really* painful... is there no way to make this more ergonomic?
    fn string_for_node_ref(node: &NodeRef<html::Input>) -> String {
        node
            // More trait black magic. Here's how it works. We have `NodeRef: Track + ReadUntracked`. These two traits are
            // explicitly implemented on type `NodeRef`.
            //
            // This means `NodeRef` picks-up, for free, a `Read` implementation, due to a blanket
            // implementation defined by `Read`:
            //
            //     impl<T> Read for T
            //     where
            //         T: Track + ReadUntracked
            //
            // This, in turn, gives it a `With` implementation, which then gives it a `Get` implementation, so long as
            // the associated type `Value` (`html::Input`) is Clone (it is).
            //
            // The associated type `Value` is `<<NodeRef<html::Input> as Read>::Value as Deref>::Target`-- that's
            // `<NodeRef<html::Input>::Value as Deref>::Target` :=> (oh, man) =>
            //
            // ```
            // <ReadGuard<
            //     Option<<html::Input as ElementType>::Output>,
            //     Derefable<Option<<html::Input as ElementType>::Output>>> as Deref>::Target
            // ```
            //
            // ```
            // Option<<html::Input as ElementType>::Output>
            // ```
            //
            // ```
            // web_sys::HtmlInputElement
            // ```
            .get() // returns an Option<web_sys::HtmlInputElement>
            .expect("string_for_node_ref() invoked before the node was mounted into the DOM; this is a bug and should be reported.")
            // So at this point, we have a plain ol' `web_sys::HtmlInputElement` instance, so we can just call `value()`
            .value()
    }
    fn bool_for_node_ref(node: &NodeRef<html::Input>) -> bool {
        node
            .get() // returns an Option<web_sys::HtmlInputElement>
            .expect("string_for_node_ref() invoked before the node was mounted into the DOM; this is a bug and should be reported.")
            .checked()
    }

    let on_submit = Action::<(), Result<(), Error>>::new_local(move |_: &()| {
        let api = api.0.clone();
        let token = token.clone();
        async move {
            let notes = string_for_node_ref(&notes_element);
            let tags = string_for_node_ref(&tags_element)
                .split(' ')
                .map(|s| s.to_owned())
                .collect::<Vec<String>>()
                .join(",");
            let request = PostAddReq {
                url: string_for_node_ref(&url_element)
                    .try_into()
                    .context(UrlFromSnafu)?,
                title: string_for_node_ref(&title_element),
                notes: if notes.is_empty() { None } else { Some(notes) },
                tags: if tags.is_empty() { None } else { Some(tags) },
                dt: None,
                replace: Some(true),
                shared: Some(!bool_for_node_ref(&private_element)),
                to_read: Some(bool_for_node_ref(&unread_element)),
            };

            let url = format!(
                "{}/api/v1/posts/add?{}",
                api,
                serde_urlencoded::to_string(&request).context(UrlEncodeSnafu { request })?
            );
            send_with_retry(|| {
                Request::post(url.as_str())
                    .header("User-Agent", USER_AGENT)
                    .header("Authorization", &format!("Bearer {}", token))
                    .send()
            })
            .await
            .and_then(error_for_status)
            .context(RequestSnafu)
            .map(|_| ())
        }
    });

    Effect::new({
        move || {
            on_submit.value().with(|result| {
                if let Some(result) = result {
                    match result {
                        Ok(_) => set_editing.set(None),
                        Err(err) => error!("Failed to edit post: {err}"),
                    }
                }
            })
        }
    });

    // Alright-- let's setup the values we'll be moving into the View:
    let url = post.url().to_string();
    let title = post.title().to_owned();
    let notes = post.notes().map(|s| s.to_owned());
    let tags = post
        .tags()
        .cloned()
        .map(|tag_name| tag_name.into())
        .collect::<Vec<String>>()
        .join(" ");
    let private = !post.public();
    let unread = post.unread();

    view! {
        <div class="post">
            <form style="padding; 1em;" on:submit = move |ev| {
                ev.prevent_default();
                on_submit.dispatch(());
            }>
            <div style="margin-bottom: 8px;">
                <input type="text" id="url" name="url" value={ url } node_ref=url_element required/>
            </div>
            <div style="margin-bottom: 8px;">
                <input type="text" id="title" name="title" value={ title } node_ref=title_element required/>
            </div>
            <div style="margin-bottom: 3px;">
                <label for="notes" style="font-size: smaller; color: #276173">notes</label>
            </div>
            <div style="margin-bottom: 8px;">
                <input type="textarea" id="notes" name="notes" value={ notes } node_ref=notes_element/>
            </div>
            <div style="margin-bottom: 3px;">
                <label for="tags" style="font-size: smaller; color: #276173">tags</label>
            </div>
            <div style="margin-bottom: 8px;">
                <input type="text" id="tags" name="tags" value={ tags } node_ref=tags_element/>
            </div>
            <div style="margin-bottom: 8px; font-size: smaller;">
                <label>
                    <input type="checkbox" id="private" name="private" checked={ private } node_ref=private_element/> private
                </label>
                " "
                <label>
                    <input type="checkbox" id="unread" name="unread" checked={ unread } node_ref=unread_element/> unread
                </label>
            </div>
            <div>
                <input type="submit" value="save" disabled=move || on_submit.pending().get()/>
                <button type="button" on:click=move |_| {
                    set_editing.set(None);
                }>"cancel"</button>
            </div>
            </form>
        </div>
    }
}

/// The indielinks' user "home" page
///
/// We expect the HTTP client & the indielinks API location to be available as context.
#[component]
pub fn Home() -> impl IntoView {
    debug!("Home invoked.");

    // RE posts: we can be in one of three situations:
    // 1. zero posts-- the user just signed-up & hasn't posted anything; don't display the
    // pagination controls & show an explanatory message in the list div
    // 2. up to one page's worth of posts: don't display the pagination controls, display the
    // posts in the list div
    // 3. more than one page's worth of posts: display the pagnination controls, display
    // the appropriate page in the list div

    // This is an interesting problem: we have a collection of posts on the server, and we need to
    // display one page's worth. That collection can change-out from beneath us. We have three
    // operations at our disposal:

    // - /posts/date : date of the most recent change
    // - /posts/recent : request the n most recent posts
    // - /posts/all : request a range of posts (offset + # to return)

    // I think my initial approach is going to be to simply maintain the "current page" as a signal
    // and manipulate it via an Effect/Action (still not sure when to use which). The idea then will
    // be to render the posts div according to that state on demand. This means we'll only re-render
    // when the page is changed, not when we discover that the server-side state has changed-out
    // beneath us. I'm not sure how to force a render; for now I'm planning on just adding a
    // "refresh" button.

    let api = use_context::<Api>()
        .expect("Failed to rerieve the API net location")
        .0;

    let page_size = use_context::<PageSize>()
        .expect("Failed to retrieve the page size configuration item")
        .0;

    let base = use_context::<Base>().expect("No base!?").0;

    // I'm still confused on how & when these `View` constructing functions are invoked, but it
    // seems that this function won't be invoked until after sign-in, so we can extract the token &
    // provide it to all our subordinate components via context rather than prop drilling.
    let token = use_context::<Token>()
        .expect("No token Cell!?")
        .get_untracked()
        .expect("No token!?");

    // Create a bit of reactive state for the current page: whether & which `Post` is currently
    // being edited.
    let (editing, set_editing): (ReadSignal<Option<StorUrl>>, WriteSignal<Option<StorUrl>>) =
        signal(None);
    // Setup a mechanism by which we can force this view to be re-rendered. A signal won't really do
    // it because there are places (say, after a delete) where we want to *force* a re-render
    // programmatically. "A trigger is a data-less signal with the sole purpose of notifying other
    // reactive code of a change."
    let rerender = ArcTrigger::new();

    // Not sure how to best handle this; if I use a *typed* query map, the `get()` method can fail,
    // and I'm not sure how to handle that. This way, any malformed query parameters will just
    // fallback to their defaults.
    let query = use_query_map(); // Memo<...>

    let filters = Memo::new(move |_| {
        let q = query.read();
        let tag = q.get("tag");
        let page = q
            .get("page")
            .map(|s| s.parse::<usize>()) // Option<Result<usize, Error>>
            .and_then(Result::ok);
        let unread = q.get("unread").map(|s| {
            let t = s.to_ascii_lowercase();
            t == "true" || t == "yes" || t == ""
        });
        (tag, page, unread) // (Option<String>, Option<usize>, Option<bool>)
    });

    // I'd really prefer to return a `Result<Vec<Post>>`, but I can't use this as a `LocalResource`
    // unless the return type is `Clone`, which errors generally are not. I *could* change my error
    // type to box all source errors and so be `Clone`, but before I go to that length, I want to be
    // sure I'm actually going to *use* the returned error. The hackernews example, for instance,
    // returns an `Option`, simply logging any errors it encounters before returning `None`.
    async fn load_data(
        api: String,
        token: String,
        page: usize,
        page_size: usize,
        tag: Option<String>,
        unread_only: bool,
    ) -> Option<Vec<Post>> {
        let mut url = format!(
            "{api}/api/v1/posts/all?start={}&results={page_size}&unread={unread_only}",
            page * page_size,
        );
        if let Some(tag) = tag {
            url += &format!("&tag={tag}");
        }
        send_with_retry(|| {
            Request::post(&url)
                .header("Authorization", &format!("Bearer {token}"))
                .send()
        })
        .await
        .map_err(|err| err.to_string())
        .and_then(string_for_status)
        .map_err(|err| error!("Requesting a page's worth of posts: {err:?}"))
        .ok()?
        .json::<PostsAllRsp>()
        .await
        .map_err(|err| error!("Deserializing a page's worth of posts; {err:?}"))
        .ok()?
        .pipe(|rsp| {
            debug!("Loaded {} posts.", rsp.posts.len());
            Some(rsp.posts)
        })
    }

    let tracker = rerender.clone();

    let page_data = LocalResource::new(move || {
        debug!("page_data invoked!");
        tracker.track();
        let q = filters.get();
        load_data(
            api.clone(),
            token.clone(),
            q.1.unwrap_or(0),
            page_size,
            q.0,
            q.2.unwrap_or(false),
        )
    });

    let on_click = {
        let trig = rerender.clone();
        move |_| trig.notify()
    };

    view! {
        <div class="user-view" style="display: flex;">
            <Await future=page_data.into_future() let:_data>
                // User's posts
                <div class="posts-view">
                    <div class="posts-nav" style="display: flex; font-size: smaller; color: #888">
                        <span style="padding: 2px 6px;">
                            {
                                let base = base.clone();
                                move || {
                                    let (tag, page, unread) = filters.get();
                                    if page.is_some_and(|n| n > 0) {
                                        let mut back = format!("{}/h?{}", base.clone(), match (page, tag) {
                                            (Some(p), None) => format!("page={}", p - 1),
                                            (Some(p), Some(tag)) => format!("?page={}&tag={tag}", p - 1),
                                            _ => unimplemented!("impossible"),
                                        });
                                        if unread.is_some_and(|b|b) {
                                            back = back + "&unread";
                                        }
                                        Either::Left(view!{
                                            <a href={ back }>"< prev"</a>
                                        })
                                    } else {
                                        Either::Right(view!{"< prev"})
                                    }
                                }
                            }
                        </span>
                        <span style="padding: 2px 6px;">"page " {move || { let (_, page, _) = filters.get(); page.unwrap_or(0) }} " " <a href="#" on:click=on_click >"    refresh"</a></span>
                        <span style="padding: 2px 6px;">
                            {
                                let base = base.clone();
                                move || {
                                    let (tag, page, unread) = filters.get();
                                    let query = match (tag, page) {
                                        (None, None) => "".to_owned(),
                                        (Some(t), None) => format!("tag={t}"),
                                        (None, Some(p)) => format!("page={p}"),
                                        (Some(t), Some(p)) => format!("tag={t}&page={p}")
                                    };
                                    let (url, text) = if unread.unwrap_or(false) {
                                        (format!("{}/h?{query}", base.clone()), "all posts".to_owned())
                                    } else {
                                        (format!("{base}/h?{query}&unread"), "unread posts".to_owned())
                                    };
                                    view!{<a href={ url } >{ text }</a>}
                            }}
                        </span>
                        <span style="padding: 2px 6px;">
                            {
                                move || {
                                    match page_data.get() {
                                        Some(Some(posts)) if posts.len() == page_size =>
                                            {
                                                let (tag, page, unread) = filters.get();
                                                let mut forward = format!("{base}/h?{}", match (page, tag) {
                                                    (Some(p), None) => format!("page={}", p + 1),
                                                    (Some(p), Some(tag)) => format!("page={}&tag={tag}", p + 1),
                                                    (None, None) => "page=1".to_owned(),
                                                    (None, Some(tag)) => format!("page=1&tag={tag}")
                                                });
                                                if unread.is_some_and(|b|b) {
                                                    forward = forward + "&unread";
                                                }
                                                Either::Left(view!{<a href={ forward }>"> next"</a>})
                                            },
                                        _ => Either::Right(view!{"next >"})
                                    }}
                            }
                        </span>
                    </div>
                    <div class="post-list">
                        <For each=move || page_data.get().unwrap_or_default().unwrap_or_default()
                             key=|post| post.id()
                             let:post>
                            {
                                let r1 = rerender.clone();
                                move || {
                                    if Some(post.url()) == editing.get().as_ref() {
                                        Either::Left(view!{<EditPost post=post.clone() set_editing/>})
                                    } else {
                                        Either::Right(view!{<ViewPost post=post.clone() set_editing rerender=r1.clone()/>})
                                    }
                            }}
                        </For>
                    </div>
                </div>
                // User's tags
                <div style="flex: 1; margin: 6px; border: 1px solid #ccc; display: flex; align-items: center; justify-content: space-around;">
                    <span>"Tags will go here!"</span>
                </div>
            </Await>
        </div>
    }
}
