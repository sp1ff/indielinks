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

//! # indielinks frontend
//!
//! ## First thoughts
//!
//! I haven't done any sort of frontend/UI development in twenty years. It seems the fundamentals of
//! web frontends haven't changed all that much: you lay-out the UI in terms of HTML elements and
//! make them responsive by modifying the DOM. And things _have_ gotten better: back in the day, we
//! all had to use the `<table>` element for layout whereas HTML 5 provides a number of elements
//! specifically designed for this purpose.
//!
//! I've chosen the [Leptos] Rust framework to build this app in the hopes of flattening my learning
//! curve. [Leptos] uses the Rust macro system to define a DSL for expressing reactive UI
//! components. Thing is, the syntax of this DSL is barely documented, and the semantics not at all.
//! As I figure both out, I'll be documenting the code copiously.
//!
//! [Leptos]: https://book.leptos.dev

use leptos::{
    either::Either,
    html::{self},
    prelude::*,
    reactive::spawn_local,
};
use leptos_router::{
    components::{A, ProtectedRoute, Route, Router, Routes},
    hooks::use_location,
    path,
};
use serde::{Deserialize, Serialize};
use tap::prelude::*;
use thaw::{Layout, LayoutHeader, Link, Tab, TabList};
use tracing::{debug, error, info};
use tracing_subscriber::fmt;
use tracing_subscriber_wasm::MakeConsoleWriter;

use indielinks_shared::{Post, PostAddReq, PostsAllRsp, StorUrl};

// This file is long overdue for a re-factor!

#[derive(Clone, Debug)]
pub struct Api(pub String);
#[derive(Clone, Debug)]
pub struct Token(pub String);
#[derive(Clone, Debug)]
pub struct Base(pub String);

/// indielinks "instance" page
#[component]
fn Instance() -> impl IntoView {
    let base = use_context::<Base>().expect("No API base!?").0;
    view! {
        <div style="padding: 8px;">
        "This will be the \"instance\" page; it will be the only visible page unless you're "
        <a href={format!("{base}/s")}>"logged-in"</a>"."
        </div>
    }
}

#[component]
/// Render a `Post` in "view" mode (as opposed to "edit" mode)
///
/// We expect the HTTP client, indielinks API location, and the login token to be available in the context.
fn Post(
    post: Post,
    set_tag: WriteSignal<Option<String>>,
    set_editing: WriteSignal<Option<StorUrl>>,
    rerender: ArcTrigger,
) -> impl IntoView {
    debug!("Post invoked.");

    let client = use_context::<reqwest::Client>().expect("Failed to retrieve the HTTP client");
    let api = use_context::<Api>().expect("No context for the API location!?");
    let token = use_context::<Token>().expect("No token!");

    let title = post.title().to_owned();
    let posted = post.posted().format("%Y-%m-%d %H:%M:%S").to_string();
    let url = post.url().clone();

    #[derive(Clone, Debug)]
    pub struct EditParams {
        pub url: StorUrl,
        pub set_editing: WriteSignal<Option<StorUrl>>,
        pub rerender: ArcTrigger,
    }

    let edit_url = url.clone();
    let on_edit = move |_| {
        info!("Now editing {edit_url}");
        set_editing.set(Some(edit_url.clone()));
    };

    #[derive(Clone, Debug)]
    pub struct DeleteParams {
        pub client: reqwest::Client,
        pub api: Api,
        pub token: Token,
        pub rerender: ArcTrigger,
        pub url: StorUrl,
    }

    let on_delete = Action::new_unsync(move |params: &DeleteParams| {
        let params = params.clone();
        let mut full_url =
            url::Url::parse(&format!("{}/api/v1/posts/delete", params.api.0)).expect("Bad URL!?");
        full_url.query_pairs_mut().append_pair("url", &params.url);
        info!("Sending: {full_url}");
        async move {
            let _ = params
                .client
                .post(full_url)
                .bearer_auth(params.token.0)
                .send()
                .await
                .and_then(|rsp| rsp.error_for_status())
                .map_err(|err| error!("Deleting post: {err:?}"));
            params.rerender.notify();
        }
    });

    let base = use_context::<Base>().expect("No base!?").0;
    // These clones are getting out of hand... I understand I need to move them into the `View`, but
    // why do they need to be cloned *again* in closures inside the `View`?
    let c1 = client.clone();
    let a1 = api.clone();
    let t1 = token.clone();
    let r1 = rerender.clone();
    let u0 = url.clone().to_string();
    let t = format!("{base}/t");
    view! {
        <div class="post">
            <div class="post-title"><a href={ u0.clone() }> { title } </a></div>
            <div class="post-info">
                <div class="post-info-left"> { posted }</div>
                <div class="post-info-right">
                    {
                        post.tags().map(|tag| {
                            // Man, this involves a *lot* of cloning-- needed?
                            let tag: String = tag.clone().into();
                            let tag0 = tag.clone();
                            view! {
                                <A href={ t.clone() } on:click=move |_| set_tag.set(Some(tag0.clone()))> { tag } </A> " "
                            }
                        }).collect::<Vec<_>>()
                    }
                </div>
            </div>
            <div class="post-controls">
            <a href="#" on:click=on_edit>edit</a>
            " "
            <a href="#" on:click=move |_| {
                on_delete.dispatch(DeleteParams {
                    client: c1.clone(), api: a1.clone(), token: t1.clone(), rerender: r1.clone(), url: url.clone(),
                });
            }>delete</a>
            </div>
        </div>
    }
}

#[component]
fn EditPost(
    post: Post,
    set_editing: WriteSignal<Option<StorUrl>>,
    rerender: ArcTrigger,
) -> impl IntoView {
    debug!("EditPost invoked.");

    let client = use_context::<reqwest::Client>().expect("Failed to retrieve the HTTP client");
    let api = use_context::<Api>().expect("No context for the API location!?");
    let token = use_context::<Token>().expect("No token!");

    // TBH, I have *no* idea what this does:
    let url_element: NodeRef<html::Input> = NodeRef::new();
    let title_element: NodeRef<html::Input> = NodeRef::new();
    let notes_element: NodeRef<html::Input> = NodeRef::new();
    let tags_element: NodeRef<html::Input> = NodeRef::new();
    let private_element: NodeRef<html::Input> = NodeRef::new();
    let unread_element: NodeRef<html::Input> = NodeRef::new();

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

    let on_submit = Action::new_local(move |_: &()| {
        let api = api.0.clone();
        let token = token.0.clone();
        let client = client.clone();
        let rerender = rerender.clone();
        async move {
            // This seems *really* painful... is there no way to make this more elegant?
            let url: StorUrl = match url_element
                .get()
                .expect("url should be mounted")
                .value()
                .try_into()
            {
                Ok(url) => url,
                Err(err) => {
                    error!("{err:?}");
                    window().alert_with_message("Invalid URL").unwrap();
                    return;
                }
            };
            let title = title_element
                .get()
                .expect("title should be mounted")
                .value();
            let notes = notes_element
                .get()
                .expect("notes should be mounted")
                .value();
            let tags = tags_element
                .get()
                .expect("tags should be mounted")
                .value()
                .split(' ')
                .map(|s| s.to_owned())
                .collect::<Vec<String>>()
                .join(",");
            let shared = !private_element
                .get()
                .expect("shared should be mounted")
                .checked();
            let to_read = unread_element
                .get()
                .expect("unread should be mounted")
                .checked();

            let url = reqwest::Url::parse(&format!(
                "{}/api/v1/posts/add?{}",
                api,
                serde_urlencoded::to_string(&PostAddReq {
                    url,
                    title,
                    notes: if notes.is_empty() { None } else { Some(notes) },
                    tags: if tags.is_empty() { None } else { Some(tags) },
                    dt: None,
                    replace: Some(true),
                    shared: Some(shared),
                    to_read: Some(to_read),
                })
                .expect("Bad query!?")
            ))
            .expect("Bad URL!?");
            info!("Sending {url}");
            match client
                .post(url)
                .bearer_auth(token)
                .send()
                .await
                .and_then(|rsp| rsp.error_for_status())
            {
                Ok(_) => {
                    info!("Updated post successfully!");
                    set_editing.set(None);
                    rerender.notify();
                }
                Err(err) => {
                    error!("{err:?}");
                }
            }
        }
    });

    use leptos::prelude::window;

    view! {
        <div class="post">
            <form style="padding; 1em;" on:submit = move |ev| { // leptos::tachys::html::event::Event
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
                    <input type="checkbox" id="private" name="private" value={ private } node_ref=private_element/> private
                </label>
                " "
                <label>
                    <input type="checkbox" id="unread" name="unread" value={ unread } node_ref=unread_element/> unread
                </label>
            </div>
            <div>
                <input type="submit" value="save"/>
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
fn Home(token: ReadSignal<Option<String>>, set_tag: WriteSignal<Option<String>>) -> impl IntoView {
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

    // Pull a few standard items from context:
    let client = use_context::<reqwest::Client>().expect("Failed to retrieve the HTTP client");
    let api = use_context::<Api>()
        .expect("Failed to rerieve the API net location")
        .0;

    // I'm still confused on how & when these `View` constructing functions are invoked, but it
    // seems that this function won't be invoked until after sign-in, so we can extract the token &
    // provide it to all our subordinate components via context rather than prop drilling.
    let token = token.get_untracked().expect("No token in Home!");
    provide_context(Token(token.clone()));

    // Later: recognize some basic query parameters <https://book.leptos.dev/router/18_params_and_queries.html>

    // Create a bit of reactive state for the current page...
    let (page, set_page) = signal(0usize);
    // and whether & which `Post` is currently being edited.
    let (editing, set_editing): (ReadSignal<Option<StorUrl>>, WriteSignal<Option<StorUrl>>) =
        signal(None);
    // Finally, setup a mechanism by which we can force this view to be re-rendered. A signal won't
    // really do it because there are places (say, after a delete) where we want to *force* a
    // re-render programmatically. "A trigger is a data-less signal with the sole purpose of
    // notifying other reactive code of a change."
    let rerender = ArcTrigger::new();

    // I'd really prefer to return a `Result<Vec<Post>>`, but I can't use this as a `LocalResource`
    // unless the return type is `Clone`, which errors generally are not. I *could* change my error
    // type to box all source errors and so be `Clone`, but before I go to that length, I want to be
    // sure I'm actually going to *use* the returned error. The hackernews example, for instance,
    // returns an `Option`, simply logging any errors it encounters before returning `None`.
    async fn load_data(
        client: reqwest::Client,
        api: String,
        token: String,
        page: usize,
    ) -> Option<Vec<Post>> {
        client
            .post(format!(
                "{api}/api/v1/posts/all?start={}&results=6",
                page * 6
            ))
            .bearer_auth(token)
            .send()
            .await
            .and_then(|rsp| rsp.error_for_status())
            .map_err(|err| error!("Requesting a page's worth of posts: {err:?}"))
            .ok()?
            .json::<PostsAllRsp>()
            .await
            .map_err(|err| error!("Deserializing a page's worth of posts; {err:?}"))
            .ok()?
            .pipe(|rsp| Some(rsp.posts))
    }

    let tracker = rerender.clone();
    let page_data = LocalResource::new(move || {
        tracker.track();
        load_data(client.clone(), api.clone(), token.clone(), page.get())
    });

    let on_click = {
        let trig = rerender.clone();
        move |_| trig.notify()
    };

    view! {
        <div class="user-view" style="display: flex;">
            // User's posts
            <div class="posts-view">
            <div class="posts-nav" style="display: flex; font-size: smaller; color: #888">
            <span style="padding: 2px 6px;">
                {
                    if page.get() > 0 {
                        Either::Left(view!{
                            <a href="#" on:click=move |_| set_page.update(|n| *n -= 1)>"< prev"</a>
                        })
                    } else {
                        Either::Right(view!{"< prev"})
                    }
                }
            </span>
            <span style="padding: 2px 6px;">"page " {page} " " <a href="#" on:click=on_click >"    refresh"</a></span>
            <span style="padding: 2px 6px;">
                {
                    match page_data.get() {
                        Some(Some(posts)) if posts.len() == 6 => Either::Left(view!{
                            <a href="#" on:click=move |_| set_page.update(|n| *n += 1)>"> next"</a>
                        }),
                        _ => Either::Right(view!{"next >"})
                    }}
            </span>
        </div>
        <div class="post-list">
            <Transition fallback=move || view!{ <p>"Loading..."</p> }>
            <For each=move || page_data.get().unwrap_or_default().unwrap_or_default()
                 key=|post| post.id()
                 let:post>
                {
                    let r0 = rerender.clone();
                    let r1 = rerender.clone();
                    move || {
                        info!("{} ==? {:?}", post.url(), editing.get());
                        if Some(post.url()) == editing.get().as_ref() {
                            Either::Left(view!{<EditPost post=post.clone() set_editing rerender=r0.clone()/>})
                        } else {
                            Either::Right(view!{<Post post=post.clone() set_tag set_editing rerender=r1.clone()/>})
                        }
                }}
            </For>
            </Transition>
        </div>
        </div>
        // User's tags
        <div style="flex: 1; margin: 6px; border: 1px solid #ccc; display: flex; align-items: center; justify-content: space-around;">
        <span>"Tags will go here!"</span>
        </div>
        </div>
    }
}

/// The "tags view" variant of Home
// This duplicates a lot of the code in `Home`-- should probably be refactored. However, I want to
// wait until I implement query parameters in the URL to do that.
#[component]
fn Tags(
    token: ReadSignal<Option<String>>,
    tag: ReadSignal<Option<String>>,
    set_tag: WriteSignal<Option<String>>,
) -> impl IntoView {
    debug!("Tags invoked.");

    let client = use_context::<reqwest::Client>().expect("Failed to retrieve the HTTP client");
    let api = use_context::<Api>()
        .expect("Failed to rerieve the API net location")
        .0;

    // It seems that this function won't be invoked until after sign-in, so we can extract the token
    // & provide it to all our subordinate components via context rather than prop drilling.
    let token = token.get().expect("No token in Home!");
    provide_context(Token(token.clone()));

    let (page, set_page) = signal(0usize);
    let (editing, set_editing): (ReadSignal<Option<StorUrl>>, WriteSignal<Option<StorUrl>>) =
        signal(None);

    // Finally, setup a mechanism by which we can force this view to be re-rendered. A signal won't
    // really do it because there are places (say, after a delete) where we want to *force* a
    // re-render programmatically. "A trigger is a data-less signal with the sole purpose of
    // notifying other reactive code of a change."
    let rerender = ArcTrigger::new();

    // Again, I'd really prefer to return a `Result<Vec<Post>>`, but I can't use this as a
    // `LocalResource` unless the return type is `Clone`, which errors generally are not.
    async fn load_data(
        client: reqwest::Client,
        api: String,
        token: String,
        tag: String,
        page: usize,
    ) -> Option<Vec<Post>> {
        client
            .post(format!(
                "{api}/api/v1/posts/all?tag={tag}&start={}&results=6",
                page * 6
            ))
            .bearer_auth(token)
            .send()
            .await
            .and_then(|rsp| rsp.error_for_status())
            .map_err(|err| error!("Requesting a page's worth of posts: {err:?}"))
            .ok()?
            .json::<PostsAllRsp>()
            .await
            .map_err(|err| error!("Deserializing a page's worth of posts; {err:?}"))
            .ok()?
            .pipe(|rsp| Some(rsp.posts))
    }

    let tracker = rerender.clone();
    let page_data = LocalResource::new(move || {
        tracker.track();
        load_data(
            client.clone(),
            api.clone(),
            token.clone(),
            tag.get().expect("Trying to show tags with no tag!?"),
            page.get(),
        )
    });

    let on_click = {
        let trig = rerender.clone();
        move |_| trig.notify()
    };

    view! {
        <p> "Posts tagged " {tag.get()} ":" </p>
        <div class="posts-view">
            <div class="posts-nav" style="display: flex; font-size: smaller; color: #888">
                <span style="padding: 2px 6px;">
                    {
                        if page.get() > 0 {
                            Either::Left(view!{
                                <a href="#" on:click=move |_| set_page.update(|n| *n -= 1)>"< prev"</a>
                            })
                        } else {
                            Either::Right(view!{"< prev"})
                        }
                    }
                </span>
                <span style="padding: 2px 6px;">"page " {page} " " <a href="#" on:click=on_click>"    refresh"</a></span>
                <span style="padding: 2px 6px;">
                    {
                      match page_data.get() {
                          Some(Some(posts)) if posts.len() == 6 => Either::Left(view!{
                              <a href="#" on:click=move |_| set_page.update(|n| *n += 1)>"> next"</a>
                          }),
                          _ => Either::Right(view!{"next >"})
                      }
                    }
                </span>
        </div>
        <div class="post-list">
            <Transition fallback=move || view!{ <p>"Loading..."</p> }>
                <For each=move || page_data.get().unwrap_or_default().unwrap_or_default()
                     key=|post| post.id()
                     let:post>
                    {
                        let r0 = rerender.clone();
                        let r1 = rerender.clone();
                        move || {
                            if Some(post.url()) == editing.get().as_ref() {
                                Either::Left(view!{<EditPost post=post.clone() set_editing rerender=r0.clone()/>})
                            } else {
                                Either::Right(view!{<Post post=post.clone() set_tag set_editing rerender=r1.clone()/>})
                            }
                        }
                    }
                </For>
            </Transition>
        </div>
        </div>

    }
}

/// The indielinks' user "feeds" page
#[component]
fn Feeds() -> impl IntoView {
    view! {
        <div style="padding: 8px;">
        "This will be the user's \"feeds\" page; it should only be visible page when you're signed-in"
        </div>
    }
}

// Ugh-- need to move to the indielinks_shared implementations of these two:
#[derive(Clone, Debug, Serialize)]
struct LoginReq {
    username: String,
    password: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LoginRsp {
    token: String,
}

async fn login(
    client: &reqwest::Client,
    api: &str,
    username: impl Into<String>,
    password: impl Into<String>,
) -> Result<String, String> {
    match client
        .post(format!("{api}/api/v1/users/login"))
        .json(&LoginReq {
            username: username.into(),
            password: password.into(),
        })
        .send()
        .await
        .and_then(|rsp| rsp.error_for_status())
    {
        Ok(rsp) => match rsp.json::<LoginRsp>().await {
            Ok(rsp) => {
                info!("Login successful: {rsp:?}");
                Ok(rsp.token)
            }
            Err(err) => {
                error!("Error deserializing a successful response: {err:?}");
                Err(format!("{err}"))
            }
        },
        Err(err) => {
            error!("Error response: {err:?}");
            Err(format!("{err}"))
        }
    }
}

/// The indielinks login page
#[component]
fn SignIn(
    _token: ReadSignal<Option<String>>,
    set_token: WriteSignal<Option<String>>,
) -> impl IntoView {
    debug!("SignIn invoked.");
    // I think this is one of those things that "should never fail"; or where failure indicates a
    // coding error.
    let client = use_context::<reqwest::Client>().expect("Failed to retrieve the HTTP client");
    let api = use_context::<Api>()
        .expect("No context for the API location!?")
        .0;

    // TBH, I have *no* idea what this does:
    let username_element: NodeRef<html::Input> = NodeRef::new();
    let password_element: NodeRef<html::Input> = NodeRef::new();

    // I want to display an error message below the form in the case of error. I think this is the
    // way to do it:
    let (error, set_error): (ReadSignal<Option<String>>, WriteSignal<Option<String>>) =
        signal(None);

    let navigate = leptos_router::hooks::use_navigate();

    let on_submit = Action::new_local(move |_: &()| {
        let username = username_element
            .get()
            .expect("<username> should be mounted")
            .value();
        let password = password_element
            .get()
            .expect("<password> should be mounted")
            .value();
        let req = LoginReq { username, password };
        let api_val = api.clone();
        let client = client.clone();
        async move {
            // This still feels prolix to me...
            match client
                .post(format!("{api_val}/api/v1/users/login"))
                .json(&req)
                .send()
                .await
                .and_then(|rsp| rsp.error_for_status())
            {
                Ok(rsp) => match rsp.json::<LoginRsp>().await {
                    Ok(rsp) => {
                        info!("Login successful: {rsp:?}");
                        set_token.set(Some(rsp.token));
                        Ok(())
                    }
                    Err(err) => {
                        error!("Error deserializing a successful response: {err:?}");
                        Err(format!("{err}"))
                    }
                },
                Err(err) => {
                    error!("Error response: {err:?}");
                    Err(format!("{err}"))
                }
            }
        }
    });

    let base = use_context::<Base>().expect("No API base!?").0;
    Effect::new(move |_| {
        // Still figuring this out...
        match on_submit.value().get() {
            Some(Ok(_)) => navigate(&format!("{}/h", base), Default::default()),
            Some(Err(err)) => {
                info!("My effect has been invoked with an error value of {err:?}");
                set_error.set(Some(err))
            }
            None => info!("Effect invoked with no value!?"),
        }
    });

    // Ugh: I really need to move this stuff to CSS:
    view! {
        <div style="display: flex; align-items: center; justify-content: space-around; flex-direction: column;">
            <form style="padding: 1em;" on:submit=move |ev| {
                // If I don't say this, the damn page reloads before the HTTP call returns
                ev.prevent_default();
                on_submit.dispatch(());
            }>
                // ChatGPT claims that stacking three divs on top of one another is idiomatic, here
                <div style="margin-bottom: 8px;">
                    <label for="username" style="width: 100px; display: inline-block;">"Username:"</label>
                    <input type="text" id="username" name="username" node_ref=username_element required />
                </div>
                <div style="margin-bottom: 12px;">
                    <label for="password" style="width: 100px; display: inline-block;">"Password:"</label>
                    <input type="password" id="password" name="password" node_ref=password_element required />
                </div>
                <div style="display: flex; align-items: center; justify-content: space-around;">
                    <input type="submit" value="Login" />
                </div>
            </form>
            <Show when=move || error.get().is_some()>
                <div style="color: red;">
                { move || error.get().unwrap() }
                </div>
            </Show>
        </div>
    }
}

/// [indielinks-fe](crate) root component
// Despite (naive?) appearances, this function isn't called every time we need to render this
// component; rather it is called *once* and it returns a thing that can be converted into a `View`,
// which I gather is the reactive entity that gets rendered & re-rendered as needed.
#[component]
fn App() -> impl IntoView {
    debug!("App invoked.");
    // We setup our HTTP client here, and make it avaiable throughout the app through context. Not
    // really sure how to handle failure; if we can't construct the `Client` then there's not much
    // we can really do-- I guess I need a fallback page of some sort, but I want to see some other
    // failure opportunities before I set this up.
    //
    // In particular, I'm interested in the `ErrorBoundary` component as a means of error handling.
    //
    // Make the client available to any of our sub-components (avoiding "prop drilling"-- making
    // this a parameter into every single `View` setup function below us):
    provide_context(
        reqwest::Client::builder()
            .user_agent("indielinks-fe/0.0.1 (+sp1ff@pobox.com)")
            .build()
            .expect("Failed to build an HTTP client!"),
    );

    // Next, I'm going to setup the network location at which we can reach indielinks. In my world,
    // this would be a configuration item, typically provided by something as sophisticated as a
    // parameter management system, or as simple as a configuration file. In this world
    // (frontend/CSR/SPA) there are a few options, including fetching, at runtime a configuration
    // file from the same endpoint at which this WASM bundle is being served.
    //
    // For now, I'm going to go with the simplest option, which is to read environment variables;
    // the read happens at *compile* time, meaning that this can be configured as part of the build.
    provide_context(Api(option_env!("INDIELINKS_FE_API")
        .unwrap_or("http://127.0.0.1:20679")
        .to_owned()));

    // Finally, this is where we "mount" this frontend. When developing against Trunk, this will
    // just be "", but as of the time of this writing, it is served by the backend at "/fe"
    provide_context(Base(
        option_env!("INDIELINKS_BASE").unwrap_or("").to_owned(),
    ));

    // OK-- we store the access token here. Perhaps make this into a context, as well? At this
    // level, we need R/W access, but in subordinate `View`s, perhaps the accessor could be provided
    // via context.
    let (token, set_token): (ReadSignal<Option<String>>, WriteSignal<Option<String>>) =
        signal(None);
    let (tag, set_tag): (ReadSignal<Option<String>>, WriteSignal<Option<String>>) = signal(None);

    // Not sure if this is required?
    let selected_value = RwSignal::new("home".to_owned());

    // This is a dev-time optimization-- log my test user in automatically on load:
    let client = use_context::<reqwest::Client>().expect("No context for the client?");
    let api = use_context::<Api>()
        .expect("No context for the API location!?")
        .0;
    spawn_local(async move {
        match login(&client, &api, "sp1ff", "f00-b@r-sp1at").await {
            Ok(token) => set_token.set(Some(token)),
            Err(err) => error!("Automatic login failed; {err}"),
        }
    });

    let base = use_context::<Base>().expect("No base for the API!?").0;
    let s = format!("{base}/s");
    let u = format!("{base}/u");
    // I have *no idea* why I need to use these, here.
    let i = StoredValue::new(format!("{base}/"));
    let h = StoredValue::new(format!("{base}/h"));
    let f = StoredValue::new(format!("{base}/f"));
    view! {
        // I'm using a `thaw` component here, `Layout` (https://thawui.vercel.app/components/layout)
        <Layout>
            <LayoutHeader class="banner">
                <h1 class="logo">indielinks</h1>
                <Show when=move || token.get().is_some() >
                    <TabList selected_value class="tab-list">
                        // Should I really be using `Link` here?
                        <Tab value="instance" class="tab"><Link href={ i.read_value().clone() }>"Instance"</Link></Tab>
                        <Tab value="home" class="tab"><Link href={ h.read_value().clone() }>"Home"</Link></Tab>
                        <Tab value="feeds" class="tab"><Link href={ f.read_value().clone() }>"Feeds"</Link></Tab>
                    </TabList>
                </Show>
                <Show when = move || token.get().is_none() && use_location().pathname.get() != "/s" >
                    <div class="uath-actions">
                        <ul style="list-style-type: none; font-size: smaller;">
                        <li><Link href={ s.clone() }>"sign-in"</Link></li>
                        <li><Link href={ u.clone() }>"sign-up"</Link></li>
                        </ul>
                    </div>
                </Show>
                <Show when = move || token.get().is_some() && use_location().pathname.get() != "/s" >
                    <div class="uath-actions">
                        <ul style="list-style-type: none; font-size: smaller;">
                        // To be implemented...
                        <li><Link href="#">"sign-out"</Link></li>
                        </ul>
                    </div>
                </Show>
            </LayoutHeader>
            <Layout>
                <main>
                    <Router base>
                        <Routes fallback=Instance>
                            <Route path=path!("/") view=Instance />
                            <Route path=path!("/s") view=move || view!{ <SignIn _token=token set_token /> } />
                            <ProtectedRoute
                                path=path!("/h")
                                // Some(true) means display, Some(false) means do *not* display, and
                                // None means that this information is still loading
                                condition = move || Some(token.get().is_some())
                                redirect_path = || "/"
                                view=move || view! { <Home token=token set_tag=set_tag/>}
                            />
                            <ProtectedRoute
                                path=path!("/t")
                                // Some(true) means display, Some(false) means do *not* display, and
                                // None means that this information is still loading
                                condition = move || Some(token.get().is_some())
                                redirect_path = || "/"
                                view=move || view! { <Tags token=token tag=tag set_tag=set_tag/>}
                            />
                            <ProtectedRoute
                                path=path!("/f")
                                // Some(true) means display, Some(false) means do *not* display, and
                                // None means that this information is still loading
                                condition = move || Some(token.get().is_some())
                                redirect_path = || "/"
                                view=Feeds
                            />
                        </Routes>
                    </Router>
                </main>
            </Layout>
        </Layout>
    }
}

fn main() {
    // This is actually pretty cool: we can setup a bog standard tracing-subscriber `Subscriber`,
    // configured to output to the browser console:
    fmt()
        .with_writer(MakeConsoleWriter::default().map_trace_level_to(tracing::Level::DEBUG))
        .without_time()
        .with_ansi(false)
        .init();
    // By default, panics that happen while running WASM code in the browser just throw an error in
    // the browser with an unhelpful message like "Unreachable executed" and a stack trace that
    // points into your WASM binary. With `console_error_panic_hook`, we'll get an actual Rust stack
    // trace that includes a line in the Rust source code
    // (https://book.leptos.dev/getting_started/leptos_dx.html).
    console_error_panic_hook::set_once();
    // Run `App()`, mount the result to the document <body>:
    leptos::mount::mount_to_body(App);
}
