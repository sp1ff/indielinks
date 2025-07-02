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
//! all had to use the `<table>` element whereas HTML 5 provides a number of elements specifically
//! designed for this purpose.
//!
//! I've chosen the [Leptos] Rust framework to build this app in the hopes of flattening my learning
//! curve. [Leptos] uses the Rust macro system to define a DSL for expressing reactive UI
//! components. Thing is, the syntax of this DSL is barely documented, and the semantics not at all.
//! As I figure both out, I'll be documenting the code copiously.

use leptos::{html, prelude::*};
use leptos_router::{
    components::{ProtectedRoute, Route, Router, Routes},
    hooks::use_location,
    path,
};
use serde::{Deserialize, Serialize};
use thaw::{Layout, LayoutHeader, Link, Tab, TabList};
use tracing::{error, info};
use tracing_subscriber::fmt;
use tracing_subscriber_wasm::MakeConsoleWriter;

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

/// The indielinks' user "home" page
#[component]
fn Home() -> impl IntoView {
    view! {
        <div style="padding: 8px;">
        "This will be the user's \"home\" page; it should only be visible page when you're signed-in"
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

    // TBH, I have *no* head what this does:
    let username_element: NodeRef<html::Input> = NodeRef::new();
    let password_element: NodeRef<html::Input> = NodeRef::new();

    // I want to display an error message below the form in the case of error. I think this is the
    // way to do it:
    let (error, set_error): (ReadSignal<Option<String>>, WriteSignal<Option<String>>) =
        signal(None);

    #[derive(Clone, Debug, Serialize)]
    struct LoginReq {
        username: String,
        password: String,
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct LoginRsp {
        token: String,
    }

    let navigate = leptos_router::hooks::use_navigate();

    // let try_login = create_action(|()|)

    // If I don't say this, the damn page reloads before the HTTP call returns
    // ev.prevent_default();
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

    view! {
        <div style="display: flex; align-items: center; justify-content: space-around; flex-direction: column;">
            <form style="padding: 1em;" on:submit=move |ev| {
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
    // failure opportunities before I set thi sup.
    let (client, _) = signal(
        reqwest::Client::builder()
            .user_agent("indielinks-fe/0.0.1 (+sp1ff@pobox.com)")
            .build()
            .expect("Failed to build an HTTP client!"),
    );
    provide_context(client);

    // OK-- we store the access token here.
    let (token, set_token): (ReadSignal<Option<String>>, WriteSignal<Option<String>>) =
        signal(None);

    // Not sure if this is required?
    let selected_value = RwSignal::new("instance".to_owned());

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
                                view=Home
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
