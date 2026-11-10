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
//!
//! ## Operator Configuration
//!
//! The frontend is configured at *compile* time via environment variables; changing any of them
//! requires rebuilding the WASM bundle:
//!
//! - `INDIELINKS_FE_API`: the public API base URL; if unset, the app derives the API location from
//!   the browser's current origin as `${window.location.origin}`. Set this when developing with
//!   `trunk` serving the UI from a different origin than the indielinks API.
//! - `INDIELINKS_BASE`: the path at which the frontend is mounted; defaults to the empty string
//! - `INDIELINKS_FE_DARK_THEME`: `true` (or unset) enables the dark-theme capability: the
//!   frontend follows the operating system's `prefers-color-scheme` setting until the user makes
//!   an explicit choice via the theme control, which is then persisted in that browser. `false`
//!   forces the light theme and omits the theme control. Any other value fails the build.
//!
//! For example, to build a light-only bundle:
//!
//! ```bash
//! INDIELINKS_FE_DARK_THEME=false trunk build --release
//! ```
//!

use gloo_net::http::Request;
use leptos::either::Either;
use leptos::prelude::*;
use leptos_router::{
    components::{ProtectedRoute, Route, Router, Routes},
    path,
};
use secrecy::ExposeSecret;
use thaw::{ConfigProvider, Icon, ToasterProvider};
use tracing::{Level, info};
use tracing_subscriber::fmt;
use tracing_subscriber_wasm::MakeConsoleWriter;
// use wasm_bindgen::JsValue;

use indielinks_shared::api::{InstanceResponse, REFRESH_CSRF_COOKIE};

use indielinks_fe::{
    account::{
        AccountApiKeys, AccountFollowers, AccountFollowing, AccountLanding, AccountPassword,
        AccountProfile,
    },
    add_link::AddLink,
    components::{
        feedback::{LoadingPlacement, LoadingState},
        shell::{Paths, Shell},
    },
    http::{fetch_instance, refresh_token, string_for_status},
    instance::{AboutPage, DocumentationPage, Instance, PrivacyPage, TermsPage},
    personal::Personal,
    signin::SignIn,
    signup::SignUp,
    theme::model::{Appearance, Availability},
    theme::{self, InitialTheme, ThemeController},
    types::{Api, Base, InstanceConfiguration, Token, USER_AGENT},
};

/// A retryable full-page error shown when instance configuration cannot be loaded at startup.
#[component]
fn StartupError(#[prop(into)] error: String, retry: Callback<(), ()>) -> impl IntoView {
    view! {
        <div class="feedback-state feedback-state--error feedback-state--startup" role="alert">
            <span aria-hidden="true" class="feedback-state__startup-mark">
                <span class="feedback-state__startup-mark-prefix">"indie"</span>
                <span class="feedback-state__startup-mark-suffix">"links"</span>
            </span>
            <span aria-hidden="true" class="feedback-state__icon">
                <Icon icon=icondata::FiAlertCircle />
            </span>
            <div class="feedback-state__content">
                <h3 class="feedback-state__heading">"Could not load instance configuration"</h3>
                <p class="feedback-state__message">{error}</p>
                <div class="feedback-state__actions">
                    <button
                        class="form-button form-button--secondary"
                        type="button"
                        on:click=move |_| retry.run(())
                    >
                        <Icon icon=icondata::IoRefresh />
                        "Retry"
                    </button>
                </div>
            </div>
        </div>
    }
}

/// [App] is [indielinks-fe](crate) root component; `main()` will mount it as the DOM body.
///
/// `initial_theme` carries the pre-mount theme resolution performed by `main()`, so that the
/// palette first rendered here is the same one already advertised by the document element's
/// `data-theme` attribute.
#[component]
fn App(initial_theme: InitialTheme, api_base: String) -> impl IntoView {
    // The public API base URL is normally derived from the browser's current origin, since
    // indielinks serves the API at `/api/v1` on the same origin that serves this frontend. For
    // development builds where `trunk` serves the UI from a different origin, `main()` accepts an
    // explicit value (typically supplied via the `INDIELINKS_FE_API` compile-time environment
    // variable) and passes it here as a prop.
    provide_context(Api(api_base));

    // Finally, this is where we "mount" this frontend. When developing against Trunk, this will
    // just be ""; the backend now serves the front end at the origin root, so production builds
    // use "" as well.
    provide_context(Base(
        option_env!("INDIELINKS_BASE").unwrap_or("").to_owned(),
    ));

    let base = expect_context::<Base>().0;
    let paths = Paths::new(&base);
    provide_context(paths.clone());

    // OK-- we store the access token here, but I'm probably going to revisit, since it needs to be
    // refreshed periodically. Something else to consider at that time: should this be a
    // `SecretString`, instead? Also, why am I making this an `RwSignal`?
    provide_context(Token::new(None));
    let token = expect_context::<Token>();

    // The application-wide theme controller: owns the stored preference, follows the operating
    // system's color-scheme setting while no preference exists, and keeps the document element's
    // `data-theme` attribute in step with the rendered palette.
    let theme_controller = ThemeController::new(initial_theme);
    provide_context(theme_controller);

    // `ConfigProvider` consumes an `RwSignal<Theme>`; keep it in step with the controller's
    // effective appearance.
    let visual_theme = RwSignal::new(match initial_theme.appearance() {
        Appearance::Light => theme::light(),
        Appearance::Dark => theme::dark(),
    });
    Effect::new(move |_| {
        visual_theme.set(match theme_controller.appearance().get() {
            Appearance::Light => theme::light(),
            Appearance::Dark => theme::dark(),
        });
    });

    // Fetch instance configuration in parallel with token refresh. Both must complete before the
    // application shell is rendered. Token refresh is idempotent; the result is stored as a side
    // effect in the `Token` signal. The instance configuration is provided via context.
    let startup_retry = ArcTrigger::new();
    let startup = LocalResource::new({
        let api = expect_context::<Api>().0.clone();
        let startup_retry = startup_retry.clone();
        move || {
            let api = api.clone();
            startup_retry.track();
            async move {
                let (token_res, instance_res) =
                    futures::future::join(refresh_token(), fetch_instance(&api)).await;
                // Token refresh sets the signal by side effect; ignore its result here.
                let _ = token_res;
                instance_res.map_err(|e| e.to_string())
            }
        }
    });

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
    let signed_in = Signal::derive(move || token.get().is_some());
    let dispatch_sign_out = Callback::new(move |()| {
        on_sign_out.dispatch(());
    });

    Effect::new(move |_| {
        document().set_title("Indielinks");
    });

    view! {
        <ConfigProvider theme=visual_theme class="indielinks-app"> // Required by `ToasterProvider`
            <ToasterProvider> // Needed to show toast in child components.
                // I thought to use the `Await` component while waiting for the token refresh to
                // complete, but the future it demands must be send, and ultimately, the future
                // returned by `refresh_token()` is not. Not sure what I want to use for a fallback,
                // however.
                <Suspense fallback=move || view! {
                    <LoadingState
                        label="Loading indielinks…"
                        placement=LoadingPlacement::Startup
                    />
                }>
                    {
                        // This closure returns an `Option<Either<View, View>>`, but because we're
                        // inside a `<Suspense>` component, the `None` case will never be returned.
                        move || {
                            let base = base.clone();
                            let paths = paths.clone();
                            let startup_retry = startup_retry.clone();
                            startup.get().map(move |result: Result<InstanceResponse, String>| {
                                match result {
                                    Err(e) => Either::Left(view! {
                                        <StartupError
                                            error=e
                                            retry=Callback::new({
                                                let startup_retry = startup_retry.clone();
                                                move |_| startup_retry.notify()
                                            })
                                        />
                                    }),
                                    Ok(instance) => {
                                        provide_context(InstanceConfiguration(instance));
                                        Either::Right(view! {
                                            <Router base=base.clone()>
                                                <Shell
                                                    paths=paths.clone()
                                                    signed_in
                                                    on_sign_out=dispatch_sign_out
                                                >
                                                    <Routes fallback=Instance>
                                                        <Route path=path!("/") view=Instance />
                                                        <Route path=path!("/about") view=AboutPage />
                                                        <Route path=path!("/docs") view=DocumentationPage />
                                                        <Route path=path!("/privacy") view=PrivacyPage />
                                                        <Route path=path!("/terms") view=TermsPage />
                                                        <Route path=path!("/s") view=SignIn />
                                                        <Route path=path!("/u") view=SignUp />
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
                                                        <ProtectedRoute
                                                            path=path!("/m")
                                                            condition = move || Some(token.get().is_some())
                                                            redirect_path = || "/"
                                                            view=AccountLanding
                                                        />
                                                        <ProtectedRoute
                                                            path=path!("/m/profile")
                                                            condition = move || Some(token.get().is_some())
                                                            redirect_path = || "/"
                                                            view=AccountProfile
                                                        />
                                                        <ProtectedRoute
                                                            path=path!("/m/password")
                                                            condition = move || Some(token.get().is_some())
                                                            redirect_path = || "/"
                                                            view=AccountPassword
                                                        />
                                                        <ProtectedRoute
                                                            path=path!("/m/api-keys")
                                                            condition = move || Some(token.get().is_some())
                                                            redirect_path = || "/"
                                                            view=AccountApiKeys
                                                        />
                                                        <ProtectedRoute
                                                            path=path!("/m/following")
                                                            condition = move || Some(token.get().is_some())
                                                            redirect_path = || "/"
                                                            view=AccountFollowing
                                                        />
                                                        <ProtectedRoute
                                                            path=path!("/m/followers")
                                                            condition = move || Some(token.get().is_some())
                                                            redirect_path = || "/"
                                                            view=AccountFollowers
                                                        />
                                                    </Routes>
                                                </Shell>
                                            </Router>
                                        })
                                    }
                                }
                            })
                        }
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
    // Resolve the theme capability, any stored preference & the system color scheme *before*
    // mounting, and tag the document element with the result, so that a saved dark preference
    // never paints the complete light interface first. `build.rs` rejects invalid values of
    // `INDIELINKS_FE_DARK_THEME` at build time, hence `expect` here.
    let availability = Availability::from_compile_time(option_env!("INDIELINKS_FE_DARK_THEME"))
        .expect("INDIELINKS_FE_DARK_THEME should have been validated at build time");
    let initial_theme = InitialTheme::detect(availability);
    initial_theme.apply_to_document();
    // Determine the public API base URL. A compile-time override supports development builds
    // where `trunk` serves the frontend from an origin other than the indielinks API; in
    // production, the API lives at `/api/v1` on the same origin as the frontend.
    let api_base = option_env!("INDIELINKS_FE_API")
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            let origin = web_sys::window()
                .expect("frontend should run in a browser")
                .location()
                .origin()
                .expect("window.location.origin should be available");
            format!("{origin}")
        });

    // Mount the `App` component as the document <body>:
    leptos::mount::mount_to_body(move || view! { <App initial_theme api_base/> });
}
