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

use chrono::{DateTime, Utc};
use leptos::{either::Either, html, prelude::*, reactive::spawn_local};
use leptos_router::{
    components::{A, ProtectedRoute, Route, Router, Routes},
    hooks::use_location,
    path,
};
use serde::{Deserialize, Serialize};
use tap::prelude::*;
use thaw::{Layout, LayoutHeader, Link, Tab, TabList};
use tracing::{error, info};
use tracing_subscriber::fmt;
use tracing_subscriber_wasm::MakeConsoleWriter;

use indielinks_shared::Post;

/// indielinks "instance" page
#[component]
fn Instance() -> impl IntoView {
    view! {
        <div style="padding: 8px;">
        "This will be the \"instance\" page; it will be the only visible page unless you're "
        <a href="/s">"logged-in"</a>"."
        </div>
    }
}

#[component]
fn Post(post: Post, set_tag: WriteSignal<Option<String>>) -> impl IntoView {
    let title = post.title().to_owned();
    let posted = post.posted().format("%Y-%m-%d %H:%M:%S").to_string();
    let url = post.url().to_string();
    view! {
        <div class="post">
            <div class="post-title"><a href={ url }> { title } </a></div>
            <div class="post-info">
                <div class="post-info-left"> { posted }</div>
                // I want to make these links, eventually, but let's start with plain text
                <div class="post-info-right">
                    {
                        post.tags().map(|tag| {
                            let setter = set_tag;
                            let s0: String = tag.clone().into();
                            let s1 = s0.clone();
                            view! {
                                <A href="/t" on:click=move |_| setter.set(Some(s1.clone()))> { s0 } </A> " "
                            }
                        }).collect::<Vec<_>>()
                    }
                </div>
            </div>
            <div class="post-controls">
                <a href="#">edit</a>" "<a href="#">delete</a>
            </div>
        </div>
    }
}

/// The indielinks' user "home" page
#[component]
fn Home(token: ReadSignal<Option<String>>, set_tag: WriteSignal<Option<String>>) -> impl IntoView {
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
    let client =
        use_context::<ReadSignal<reqwest::Client>>().expect("Failed to retrieve the HTTP client");

    let (page, set_page) = signal(0usize);

    // Ugh-- copied from `indielinks`; if I take a dependency on the indielinks crate, I get a ton
    // of error messages when I try to build *this* one-- I suspect because `indielinks` has
    // dependencies that don't support the wasm32 target (?) I may need to factor types like this
    // out into their own crate that can be shared by the front-end & back-end.
    #[derive(Clone, Debug, Deserialize, Serialize)]
    pub struct PostsAllRsp {
        pub user: String, /*Username*/
        pub tag: String,
        pub posts: Vec<Post>,
    }

    // I'd really prefer to return a `Result<Vec<Post>>`, but I can't use this as a `LocalResource`
    // unless the return type is `Clone`, which errors generally are not. I *could* change my error
    // type to box all source errors and so be `Clone`, but before I go to that length, I want to be
    // sure I'm actually going to *use* the returned error. The hackernews example, for instance,
    // returns an `Option`, simply logging any errors it encounters before returning `None`.
    async fn load_data(client: reqwest::Client, token: String, page: usize) -> Option<Vec<Post>> {
        client
            .post(format!(
                "http://127.0.0.1:20673/api/v1/posts/all?start={}&results=6",
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

    let page_data = LocalResource::new(move || {
        load_data(
            client.get().clone(),
            token.get().clone().expect("Missing token"),
            page.get(),
        )
    });

    view! {
        <div class="user-view" style="display: flex;">
            // User's posts
            <div class="posts-view">
                <div class="posts-nav" style="display: flex; font-size: smaller; color: #888">
                    <span style="padding: 2px 6px;">
                        {move || {
                            if page.get() > 0 {
                                Either::Left(view!{
                                    <a href="#" on:click=move |_| set_page.update(|n| *n -= 1)>"< prev"</a>
                                })
                            } else {
                                Either::Right(view!{"< prev"})
                            }
                        }}
                    </span>
                    <span style="padding: 2px 6px;">"page " {page} " " <a href="#" on:click=move |_| {
                        let p = page.get();
                        set_page.set(p); // Touch, don't change value-- seems to work
                    }>"    refresh"</a></span>
                    <span style="padding: 2px 6px;">
                        {move || {
                          match page_data.get() {
                              Some(Some(posts)) if posts.len() == 6 => Either::Left(view!{
                                  <a href="#" on:click=move |_| set_page.update(|n| *n += 1)>"> next"</a>
                              }),
                              _ => Either::Right(view!{"next >"})
                          }}
                        }
                    </span>
            </div>
            {move || {
                view! {
                    <div class="post-list">
                        <Transition fallback=move || view!{ <p>"Loading..."</p> }>
                            <For each=move || page_data.get().unwrap_or_default().unwrap_or_default()
                                 key=|post| post.id()
                                 let:post>
                              <Post post set_tag/>
                            </For>
                        </Transition>
                    </div>
                }
            }}
            </div>
            // User's tags
            <div style="flex: 1; margin: 6px; border: 1px solid #ccc; display: flex; align-items: center; justify-content: space-around;">
              <span>"Tags will go here!"</span>
            </div>
        </div>
    }
}

/// The "tags view" variant of Home
#[component]
fn Tags(
    token: ReadSignal<Option<String>>,
    tag: ReadSignal<Option<String>>,
    set_tag: WriteSignal<Option<String>>,
) -> impl IntoView {
    let client =
        use_context::<ReadSignal<reqwest::Client>>().expect("Failed to retrieve the HTTP client");

    let (page, set_page) = signal(0usize);

    // Ugh-- next on my list is to factor-out a crate that just contains these sorts of entities and
    // builds for all targets.
    #[derive(Clone, Debug, Deserialize, Serialize)]
    pub struct PostsAllReq {
        tag: Option<String>,
        start: Option<usize>,
        results: Option<usize>,
        fromdt: Option<DateTime<Utc>>,
        todt: Option<DateTime<Utc>>,
        #[serde(rename = "meta")]
        _meta: Option<bool>,
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    pub struct PostsAllRsp {
        pub user: String, /*Username*/
        pub tag: String,
        pub posts: Vec<Post>,
    }

    // Again, I'd really prefer to return a `Result<Vec<Post>>`, but I can't use this as a
    // `LocalResource` unless the return type is `Clone`, which errors generally are not.
    async fn load_data(
        client: reqwest::Client,
        token: String,
        tag: String,
        page: usize,
    ) -> Option<Vec<Post>> {
        client
            .post(format!(
                "http://127.0.0.1:20673/api/v1/posts/all?tag={tag}&start={}&results=6",
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

    let page_data = LocalResource::new(move || {
        load_data(
            client.get().clone(),
            token.get().clone().expect("Missing token"),
            tag.get().expect("Trying to show tags with no tag!?"),
            page.get(),
        )
    });

    view! {
        <p> "Posts tagged " {tag.get()} ":" </p>
        <div class="posts-view">
            <div class="posts-nav" style="display: flex; font-size: smaller; color: #888">
                <span style="padding: 2px 6px;">
                    {move || {
                        if page.get() > 0 {
                            Either::Left(view!{
                                <a href="#" on:click=move |_| set_page.update(|n| *n -= 1)>"< prev"</a>
                            })
                        } else {
                            Either::Right(view!{"< prev"})
                        }
                    }}
                </span>
                <span style="padding: 2px 6px;">"page " {page} " " <a href="#" on:click=move |_| {
                    let p = page.get();
                    set_page.set(p); // Touch, don't change value-- seems to work
                }>"    refresh"</a></span>
                <span style="padding: 2px 6px;">
                    {move || {
                      match page_data.get() {
                          Some(Some(posts)) if posts.len() == 6 => Either::Left(view!{
                              <a href="#" on:click=move |_| set_page.update(|n| *n += 1)>"> next"</a>
                          }),
                          _ => Either::Right(view!{"next >"})
                      }}
                    }
                </span>
        </div>
        {move || {
            view! {
                <div class="post-list">
                    <Transition fallback=move || view!{ <p>"Loading..."</p> }>
                        <For each=move || page_data.get().unwrap_or_default().unwrap_or_default()
                             key=|post| post.id()
                             let:post>
                          <Post post set_tag/>
                        </For>
                    </Transition>
                </div>
            }
        }}
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
    username: impl Into<String>,
    password: impl Into<String>,
) -> Result<String, String> {
    match client
        .post("http://127.0.0.1:20673/api/v1/users/login")
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
    // I think this is one of those things that "should never fail"; or where failure indicates a
    // coding error.
    let client =
        use_context::<ReadSignal<reqwest::Client>>().expect("Failed to retrieve the HTTP client");

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
        async move {
            // This still feels prolix to me...
            match client
                .get()
                .post("http://127.0.0.1:20673/api/v1/users/login")
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

    Effect::new(move |_| {
        // Still figuring this out...
        match on_submit.value().get() {
            Some(Ok(_)) => navigate("/h", Default::default()),
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
    // We setup our HTTP client here, and make it avaiable throughout the app through context. Not
    // really sure how to handle failure; if we can't construct the `Client` then there's not much
    // we can really do-- I guess I need a fallback page of some sort, but I want to see some other
    // failure opportunities before I set this up.
    //
    // In particular, I'm interseted in the `ErrorBoundary` component as a means of error handling.
    let (client, _) = signal(
        reqwest::Client::builder()
            .user_agent("indielinks-fe/0.0.1 (+sp1ff@pobox.com)")
            .build()
            .expect("Failed to build an HTTP client!"),
    );
    // Make the client available to any of our sub-components (avoiding "prop drilling"-- making
    // this a parameter into every single `View` setup function below us):
    provide_context(client);

    // OK-- we store the access token here. Perhaps make this into a context, as well? Yeah... on
    // further reflection, this ("token") and "tag" should really be provided by a context.
    let (token, set_token): (ReadSignal<Option<String>>, WriteSignal<Option<String>>) =
        signal(None);

    let (tag, set_tag): (ReadSignal<Option<String>>, WriteSignal<Option<String>>) = signal(None);

    // Not sure if this is required?
    let selected_value = RwSignal::new("home".to_owned());

    // This is a dev-time optimization-- log my test user in automatically on load:
    spawn_local(async move {
        match login(&client.get(), "sp1ff", "f00-b@r-sp1at").await {
            Ok(token) => set_token.set(Some(token)),
            Err(err) => error!("Automatic login failed; {err}"),
        }
    });

    view! {
        // I'm using a thaw component here, `Layout` (https://thawui.vercel.app/components/layout)
        <Layout>
            <LayoutHeader class="banner">
                <h1 class="logo">indielinks</h1>
                <Show when=move || token.get().is_some() >
                    <TabList selected_value class="tab-list">
                        <Tab value="instance" class="tab"><Link href="/">"Instance"</Link></Tab>
                        <Tab value="home" class="tab"><Link href="/h">"Home"</Link></Tab>
                        <Tab value="feeds" class="tab"><Link href="/f">"Feeds"</Link></Tab>
                    </TabList>
                </Show>
                <Show when = move || token.get().is_none() && use_location().pathname.get() != "/s" >
                    <div class="uath-actions">
                        <ul style="list-style-type: none; font-size: smaller;">
                        <li><Link href="/s">"sign-in"</Link></li>
                        <li><Link href="/u">"sign-up"</Link></li>
                        </ul>
                    </div>
                </Show>
                <Show when = move || token.get().is_some() && use_location().pathname.get() != "/s" >
                    <div class="uath-actions">
                        <ul style="list-style-type: none; font-size: smaller;">
                        <li><Link href="/s">"sign-out"</Link></li>
                        </ul>
                    </div>
                </Show>
            </LayoutHeader>
            <Layout>
                <main>
                    <Router>
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
