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

#![cfg(target_arch = "wasm32")]

//! # The [indielinks] Front End
//!
//! ## Introduction
//!
//! This crate implements the [indielinks] front end, a client-side rendered, single-page
//! application.
//!
//! I haven't done any sort of GUI development in almost twenty years. It seems the fundamentals of
//! web front ends haven't changed all that much: you lay-out the UI in terms of HTML elements and
//! make them responsive by modifying the DOM from Javascript. Things _have_ gotten better during
//! that time: back in the day, we all had to use the `<table>` element for layout whereas HTML 5
//! provides a number of elements specifically designed for this purpose.
//!
//! That said, I'm completely new to reactive programming. I've chosen the [Leptos] framework to
//! build this app in the hopes of flattening my learning curve. I'll be documenting the code
//! copiously as a learning tool while I figure all this out.
//!
//! [Leptos]: https://book.leptos.dev
//!
//! ## Overview of a Reactive Rust Front End
//!
//! I think I'm finally getting my head around how this all works, and I wanted to write it down to
//! be sure. The [Leptos] documentation is regrettably focused on examples & "howto"s, and at no
//! point lays out a simple "theory of operations". Oddly, this sort of information _can_ be found
//! in a pair of videos made by [Leptos]' creator, Greg Johnston:
//!
//! - [Let's Build a Frontend Web Framework in Rust](https://www.youtube.com/watch?v=cp3tnlTZ9IU)
//! - [Learning Leptos: Build a fine-grained reactive system in 100 lines of code](https://www.youtube.com/watch?v=GWB3vTWeLd4&t=1659s)
//!
//! Each of these helped me tremendously.
//!
//! To understand how a [Leptos] front end (or, at least, a CSR, SPA) works, the first step is to
//! shift your mental model of the program's execution. No longer are we implementing a Unix process
//! whose lifetime is conicident with the execution of `main()`. Rather, we're building a
//! WebAssembly bundle whose post-load *initialization* corresponds to the execution of `main()`.
//! After `main()` returns, the `.wasm` lives on, making functions & data available to the the page
//! from Javascript. The data structures allocated by `main()` are only truly cleaned-up when the
//! user navigates away from the page and the `.wasm` bundle is unloaded.
//!
//! As an aside, this explains why data has to be _moved_ into the numerous lambdas handed off to
//! the [Leptos] runtime: they must have `'static` lifetime.
//!
//! The `view!` macro is just syntactic sugar around a typical fluent interface for building-up the
//! DOM you'd like your app to manage. The "builders" that make-up this fluent interface use
//! [wasm-bindgen] to call out to the Javascript world, creating the corresponding elements in the
//! "real" (i.e. page) DOM.
//!
//! [wasm-bindgen]: https://wasm-bindgen.github.io/wasm-bindgen/
//!
//! What makes this all reactive is the interplay between signals & effects. By tracking a "stack"
//! of currently-executing effects, [Leptos] can automatically detect which effects depend on which
//! signals (by looking at the top of the stack when a signal's value is accessed), and build-up a
//! dependency graph. Those signals & effects reside in the "runtime", which AFAICT is a collection
//! of data structures to track this graph maintained by [Leptos], inside the .wasm bundle, driven
//! by the user's interactions with the page via Javascript event handlers.

use gloo_net::http::Request;
use leptos::prelude::*;
use leptos_router::{
    components::{ProtectedRoute, Route, Router, Routes},
    hooks::{use_location, use_navigate},
    location::State,
    path, NavigateOptions,
};
use thaw::{Layout, LayoutHeader, Link, Tab, TabList};
use tracing::{debug, error, info, Level};
use tracing_subscriber::fmt;
use tracing_subscriber_wasm::MakeConsoleWriter;
use wasm_bindgen::JsValue;

use indielinks_shared::api::REFRESH_CSRF_COOKIE;

use indielinks_fe::{
    add_link::AddLink,
    feeds::Feeds,
    home::Home,
    http::{refresh_token, string_for_status},
    instance::Instance,
    signin::SignIn,
    types::{Api, Base, PageSize, Token, USER_AGENT},
};

/// [indielinks-fe](crate) root component
#[component]
fn App() -> impl IntoView {
    debug!("App invoked.");

    // Next, I'm going to setup the network location at which we can reach indielinks. In my world,
    // this would be a configuration item, typically provided by something as sophisticated as a
    // parameter management system, or as simple as a configuration file. In this world
    // (frontend/CSR/SPA) there are a few options, including fetching, at runtime a configuration
    // file from the same endpoint at which this WASM bundle is being served.
    //
    // For now, I'm going to go with the simplest option, which is to read environment variables;
    // the read happens at *compile* time, meaning that this can be configured as part of the build.
    provide_context(Api(option_env!("INDIELINKS_FE_API")
        .unwrap_or("http://localhost:20679")
        .to_owned()));

    provide_context(PageSize(
        option_env!("INDIELINKS_PAGE_SIZE")
            .unwrap_or("4")
            .parse::<usize>()
            .expect("Couldn't parse INDIELINKS_PAGE_SIZE"),
    ));

    // Finally, this is where we "mount" this frontend. When developing against Trunk, this will
    // just be "", but as of the time of this writing, it is served by the backend at "/fe"
    provide_context(Base(
        option_env!("INDIELINKS_BASE").unwrap_or("").to_owned(),
    ));

    // OK-- we store the access token here. Perhaps make this into a context, as well? At this
    // level, we need R/W access, but in subordinate `View`s, perhaps the accessor could be provided
    // via context.

    provide_context(RwSignal::<Option<String>>::new(None));

    // Now, on a fresh load, we *may* have a refresh token laying around-- let's try it:

    // Not sure if this is required?
    let selected_value = RwSignal::new("home".to_owned());

    let token = use_context::<Token>().expect("No token Cell!?");

    let base = use_context::<Base>().expect("No base for the API!?").0;
    let s = format!("{base}/s");
    let u = format!("{base}/u");
    // I have *no idea* why I need to use these, here.
    let i = StoredValue::new(format!("{base}/"));
    let h = StoredValue::new(format!("{base}/h"));
    let f = StoredValue::new(format!("{base}/f"));

    // I'm interested in the `ErrorBoundary` component as a means of error handling.

    // The constant cloning is getting *really* tiresome:
    let t0 = token.clone();
    let t1 = token.clone();
    // let t2 = token.clone();
    // let t3 = token.clone();
    // let t4 = token.clone();

    // Seems prolix...
    async fn refresh() -> bool {
        match refresh_token().await {
            Ok(_) => {
                info!("Successfully obtained an access token!");
                true
            }
            Err(err) => {
                error!("While refreshing token: {err:?}");
                false
            }
        }
    }
    // If I don't use a `LocalResource`; if I, say, just call `refresh_token()` directly in the
    // `Await` below, I get pages of warnings about the future not being Send (?)
    let fetcher = LocalResource::new(|| refresh());

    let t5 = token.clone();
    let api = use_context::<Api>().expect("No API!?").0;
    let on_sign_out = Action::new_unsync(move |_: &()| {
        let api = api.clone();
        let token = token.get().expect("No token!?");
        t5.set(None);
        wasm_cookies::delete(REFRESH_CSRF_COOKIE);
        async move {
            let rsp = Request::post(&format!("{api}/api/v1/users/logout"))
                .credentials(web_sys::RequestCredentials::Include)
                .header("User-Agent", USER_AGENT)
                .header("Authorization", &format!("Bearer {}", token))
                .send()
                .await
                .map_err(|err| err.to_string())
                .and_then(string_for_status);
            info!("Logged-out: {rsp:?}");
        }
    });

    view! {
        <Await
            future=fetcher.into_future()
            let:_data>
            <Router base>
                // Should factor this out into it's own component
                {
                    let location = use_location(); // -> Location (which is Clone, but that's it)
                    let navigate = use_navigate(); // -> impl Fn(&str, NavigateOptions) + Clone
                    let add_link =
                        move |_| {
                            let state = web_sys::js_sys::Object::new();
                            let _ = web_sys::js_sys::Reflect::set(
                                &state,
                                &"from".into(),
                                &JsValue::from(location.pathname.get()),
                            );
                            navigate(
                                "/a",
                                NavigateOptions {
                                    state: State::new(Some(state.into())),
                                    ..Default::default()
                                },
                            );
                    };

                    view! {
                        // I'm using a `thaw` component here, `Layout` (https://thawui.vercel.app/components/layout)
                        <LayoutHeader class="banner">
                            <h1 class="logo">indielinks</h1>
                            <Show when=move || t0.get().is_some() >
                                <TabList selected_value class="tab-list">
                                    // Should I really be using `Link` here?
                                    <Tab value="instance" class="tab"><Link href={ i.read_value().clone() }>"Instance"</Link></Tab>
                                    <Tab value="home" class="tab"><Link href={ h.read_value().clone() }>"Home"</Link></Tab>
                                    <Tab value="feeds" class="tab"><Link href={ f.read_value().clone() }>"Feeds"</Link></Tab>
                                </TabList>
                            </Show>
                            <Show when = move || t1.get().is_none() && use_location().pathname.get() != "/s" >
                                <div class="auth-actions">
                                    <ul style="list-style-type: none; font-size: smaller;">
                                    <li><a href={ s.clone() }>"sign-in"</a></li>
                                    <li><a href={ u.clone() }>"sign-up"</a></li>
                                    </ul>
                                </div>
                            </Show>
                            <Show when = move || token.get().is_some() && use_location().pathname.get() != "/s" >
                                <div class="auth-actions">
                                    <ul style="list-style-type: none; font-size: smaller;">
                                        // Note the `clone()` in the on:click handler-- this is
                                        // essential! TBH, I'm a bit hazy on why, but it has to do
                                        // with the fact that we're inside a `<Show>` component,
                                        // here.
                                        <li><a href="#" on:click=add_link.clone()>"add link"</a></li>
                                        <li><a href="#" on:click=move |_| {
                                            on_sign_out.dispatch(());
                                        }>"sign-out"</a></li>
                                    </ul>
                                </div>
                            </Show>
                        </LayoutHeader>
                    }
                }
                <Layout>
                    <main>
                        <Routes fallback=Instance>
                            <Route path=path!("/") view=Instance />
                            <Route path=path!("/s") view=move || view!{ <SignIn /> } />
                            <ProtectedRoute
                                path=path!("/h")
                                // Some(true) means display, Some(false) means do *not* display, and
                                // None means that this information is still loading
                                condition = move || Some(token.get().is_some())
                                redirect_path = || "/"
                                view=move || view! { <Home />}
                            />
                            <ProtectedRoute
                                path=path!("/f")
                                // Some(true) means display, Some(false) means do *not* display, and
                                // None means that this information is still loading
                                condition = move || Some(token.get().is_some())
                                redirect_path = || "/"
                                view=Feeds
                            />
                            <ProtectedRoute
                                path=path!("/a")
                                // Some(true) means display, Some(false) means do *not* display, and
                                // None means that this information is still loading
                                condition = move || Some(token.get().is_some())
                                redirect_path = || "/"
                                view=AddLink
                            />
                        </Routes>
                    </main>
                </Layout>
            </Router>
        </Await>
    }
}

fn main() {
    // This is actually pretty cool: we can setup a standard tracing-subscriber `Subscriber`,
    // configured to output to the browser console:
    fmt()
        .with_writer(MakeConsoleWriter::default().map_trace_level_to(tracing::Level::DEBUG))
        .with_max_level(Level::DEBUG)
        .without_time()
        .with_ansi(false)
        .init();
    // By default, panics that happen while running WASM code in the browser just throw an error in
    // the browser with an unhelpful message like "Unreachable executed" and a stack trace that
    // points into your WASM binary. With `console_error_panic_hook`, we'll get an actual Rust stack
    // trace that includes a line in the Rust source code
    // (https://book.leptos.dev/getting_started/leptos_dx.html).
    console_error_panic_hook::set_once();
    // Mount the `App` component as the document <body>:
    leptos::mount::mount_to_body(App);
}
