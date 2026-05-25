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
//! point lays out what used to be called a "theory of operations"; an architectural overview of how
//! the system works. Oddly, this sort of information _can_ be found in a pair of videos made by
//! [Leptos]' creator, Greg Johnston:
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
//! [wasm-bindgen] to call *out* to the Javascript world, creating the corresponding elements in the
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
    components::{A, ProtectedRoute, Route, Router, Routes},
    hooks::use_location,
    path,
};
use secrecy::ExposeSecret;
use thaw::{ConfigProvider, Layout, LayoutHeader, Tab, TabList, ToasterProvider};
use tracing::{Level, info};
use tracing_subscriber::fmt;
use tracing_subscriber_wasm::MakeConsoleWriter;
// use wasm_bindgen::JsValue;

use indielinks_shared::api::REFRESH_CSRF_COOKIE;

use indielinks_fe::{
    add_link::AddLink,
    http::{refresh_token, string_for_status},
    instance::Instance,
    personal::Personal,
    signin::SignIn,
    types::{Api, Base, PageSize, Token, USER_AGENT},
};

/// [App] is [indielinks-fe](crate) root component; `main()` will mount it as the DOM body.
#[component]
fn App() -> impl IntoView {
    // Setup the network location at which we can reach indielinks. In my world (the backend), this
    // would be a configuration item, typically provided by something as sophisticated as a
    // parameter management system, or as simple as a configuration file. In this world
    // (frontend/CSR/SPA) there are a few options, including fetching, at runtime, a configuration
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

    let base = expect_context::<Base>().0;
    // These don't need to be reactive, but signals are `Copy`, so we don't have to do all the work
    // of cloning these things multiple times. `StoredValue` seems to be the thing to use to get a
    // `Copy` handle to a non-reactive thing, but I never figured-out how to get it working.
    let (a, _) = signal(format!("{base}/a"));
    let (p, _) = signal(format!("{base}/"));
    let (h, _) = signal(format!("{base}/h"));
    // Weirdly, these two get moved into a subsidiary view, so they do need to be cloned into it.
    let s = format!("{base}/s");
    let u = format!("{base}/u");

    // OK-- we store the access token here, but I'm probably going to revisit, since it needs to be
    // refreshed periodically. Something else to consider at that time: should this be a
    // `SecretString`, instead? Also, why am I making this an `RwSignal`?
    provide_context(Token::new(None));
    let token = expect_context::<Token>();

    let selected_value = RwSignal::new(String::new());

    // If I don't use a `LocalResource`; if I, say, just call `refresh_token()` directly in an
    // `Await` below, I get pages of warnings about the future not being Send (?)
    let try_token_refresh = LocalResource::new(|| async move { refresh_token().await.is_ok() });
    // So... `try_token_refresh.get()` will return an `Option<bool>`.

    let on_sign_out = Action::new_unsync(move |_: &()| {
        wasm_cookies::delete(REFRESH_CSRF_COOKIE);
        async move {
            let api = expect_context::<Api>().0;
            let tokens = token.get().expect("No token!?");
            let rsp = Request::post(&format!("{api}/api/v1/users/logout"))
                .credentials(web_sys::RequestCredentials::Include)
                .header("User-Agent", USER_AGENT)
                .header(
                    "Authorization",
                    &format!("Bearer {}", tokens.expose_secret()),
                )
                .send()
                .await
                .map_err(|err| err.to_string())
                .and_then(string_for_status);
            info!("Logged-out: {rsp:?}");
            token.set(None);
        }
    });

    view! {
        <ConfigProvider> // Required by `ToasterProvider`
            <ToasterProvider> // Needed to show toast in child components.
                // I thought to use the `Await` component while waiting for the token refresh to
                // complete, but the future it demands must be send, and ultimately, the future
                // returned by `refresh_token()` is not. Not sure what I want to use for a fallback,
                // however.
                <Suspense fallback=move || view! { <p>"Attempting a token refresh"</p> }>
                    {
                        // This closure returns an `Option<impl IntoView>`, but because we're inside
                        // a `<Suspense>` component, the `None` case will never be returned.
                        move || try_token_refresh.get().map(|_| {
                            let s = s.clone();
                            let u = u.clone();
                            view! {
                                <Router base=base.clone()>
                                    {
                                        move || {
                                            selected_value.set(if use_location().pathname.get() == "/h" {
                                                "home"
                                            } else {
                                                "popular"
                                            }.to_owned());
                                        }
                                    }
                                    <LayoutHeader class="w-full text-sky-100 flex bg-sky-600 items-baseline">

                                        <div class="text-sky-100 font-header font-bold text-6xl pt-[6px] pb-[6px] pl-[12px] pr-[28px]">indielinks</div>

                                        // Tab list running across the top when logged in:
                                        <Show when=move || token.get().is_some() >
                                            <TabList selected_value>
                                                <Tab value="popular" >
                                                    // Regrettably, we have to style the text here, because Thaw
                                                    // sets these properties (the `ConfigProvider` component,
                                                    // specifically).
                                                    <A href=p.get()  attr:class="font-header font-medium text-2xl text-sky-100">"popular"</A>
                                                </Tab>
                                                <Tab value="home" >
                                                    <A href=h.get() attr:class="font-header font-medium text-2xl text-sky-100">"home"</A>
                                                </Tab>
                                            </TabList>
                                        </Show>

                                        // Links on the top RHS; what's shown depends on whether or not the user
                                        // is logged-in
                                        <Show when=move || use_location().pathname.get() != "/s">
                                            <Show when=move || token.get().is_some()
                                                  // Worth it to factor this out into its own component?
                                                  fallback={
                                                      let s = s.clone();
                                                      let u = u.clone();
                                                      move || {
                                                          view! {
                                                              <div class="mr-[8px] ml-auto font-header font-medium text-lg self-center">
                                                                  <ul class="list-none">
                                                                    <li class="leading-tight"><A href=s.clone()>"sign-in"</A></li>
                                                                    <li class="leading-tight"><A href=u.clone()>"sign-up"</A></li>
                                                                  </ul>
                                                              </div>
                                                          }
                                                      }
                                                  } >
                                                // Same here-- make this its own component?
                                                <div class="mr-[8px] ml-auto font-header font-medium text-lg self-center">
                                                    <ul class="list-none">
                                                        // Should these be <buttons>?
                                                        <li class="leading-tight"><A href=a.get()>"add link"</A></li>
                                                        <li class="leading-tight"><a href="#" on:click=move |_| { on_sign_out.dispatch(()); }>
                                                            "sign-out"</a>
                                                        </li>
                                                    </ul>
                                                </div>
                                            </Show>
                                        </Show>
                                    </LayoutHeader>
                                    <Layout>
                                        <Routes fallback=Instance>
                                            <Route path=path!("/") view=Instance />
                                            <Route path=path!("/s") view=SignIn />
                                            <ProtectedRoute
                                                path=path!("/h")
                                                // Some(true) means display, Some(false) means do *not* display, and
                                                // None means that this information is still loading
                                                condition = move || Some(token.get().is_some())
                                                redirect_path = || "/"
                                                view=Personal
                                            />
                                            <ProtectedRoute
                                                path=path!("/a")
                                                // Some(true) means display, Some(false) means do *not* display, and
                                                // None means that this information is still loading
                                                condition = move || {
                                                    Some(token.get().is_some())
                                                }
                                                redirect_path = || "/"
                                                view=AddLink
                                            />
                                        </Routes>
                                    </Layout>
                                </Router>
                        }})
                    }
                </Suspense>
            </ToasterProvider>
        </ConfigProvider>
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
    // <https://book.leptos.dev/getting_started/leptos_dx.html>.
    console_error_panic_hook::set_once();
    // Mount the `App` component as the document <body>:
    leptos::mount::mount_to_body(App);
}
