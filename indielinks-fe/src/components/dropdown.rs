// Copyright (C) 2026 Michael Herstine <sp1ff@pobox.com>
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

//! # A simple dropdown menu component for Leptos
//!
//! Use the component like so:
//!
//! ```ignore
//! // `MenuId` can be anything so long as it's Clone, Send, Sync, & 'static, and
//! // implementats PartialEq.
//! let open_menu = use_dropdown::<MenuId>();
//! // ...
//! <Drowndown open_menu>
//!     <DropdownTrigger text"menu".to_string() menu_id=/*...*/ />
//!     <DropdownMenuItems menu_id=/*...*/>
//!         <DropdownMenuItem
//!             text="Item 1".to_string()
//!             handler=Callback::new(|()| debug!("Item 1 selected")) />
//!         <DropdownMenuItem
//!             text="Item 2".to_string()
//!             handler=Callback::new(|()| debug!("Item 2 selected")) />
//!     </DropdownMenuItems menu_id=/*...*/>
//! </Dropdown>
//! ```
//!
//! The "trigger" is the static portion of the element & the "menu" is the what drops down when the
//! trigger is clicked.
//!
//! This is my first Leptos component, so there are no doubt things that I've missed or that could
//! be improved. It's un-styled & not at all accessible.
//!
//! ## Discussion
//!
//! When authoring Leptos components, the implementation code can easily become long and difficult
//! to read. I think this is because the component will in general consist of presentation ("Put a
//! div here, a link there..."), state ("I'm displaying a list of T..."), and logic ("When the link
//! is clicked, toggle this boolean..."). Conflating the three makes for prolix, bug-prone
//! implementations.
//!
//! As I understand it, Leptos is structured similarly to SolidJS in the Javascript world, so a
//! little research turned-up the notion of "[hooks]" there: the idea being to pull together the
//! state & logic (typically in the form of signals & callbacks) in a function (conventionally named
//! `use_...`) and returning it in one shot to the caller, who then destructures the (possibly
//! large) return value and uses it in a `view!` invocation that is, as a result, terse and focused
//! on presentation only.
//!
//! [hook]: https://derkuba.de/posts/en/0625/hooks-in-solidjs/
//!
//! Fowler, as is his wont, formalized this even further into the idea of [headless components]:
//! state & logic that, stripped of UI, can be unit tested independently.
//!
//! [headless components]: https://martinfowler.com/articles/headless-component.html
//!
//! I haven't gotten that far yet, but apparently wasm-bindgen does offer some [support] for this.
//!
//! [support]: https://wasm-bindgen.github.io/wasm-bindgen/wasm-bindgen-test/browsers.html

// In addition, as an experiment, I co-wrote this module with Claude Sonnet 4.6. I was able to
// describe, in detail, the implementation I wanted to Claude who did a pretty-good job at realizing
// it. As such, architectural flaws are on me, while odd wording or other quirks should be blamed on
// the 'bot ;)

use leptos::prelude::*;
use send_wrapper::SendWrapper;
use tracing::debug;
use wasm_bindgen::prelude::*;
use web_sys::MouseEvent;

/// A type-erased callback for closing the currently open menu.
///
/// Why a type alias?  `DropdownMenuItems` must provide a way for
/// `DropdownMenuItem` to close the menu without exposing the generic `MenuId`.
/// Wrapping the operation in `Callback<()>` and storing it in Leptos context
/// under this concrete type lets `DropdownMenuItem` retrieve it without being
/// generic itself.
type CloseMenuCallback = Callback<()>;

/// `use_dropdown` is a "hook" in the SolidJS sense: a plain function (not a
/// `#[component]`) that encapsulates the reactive state and side-effects for
/// the dropdown menu. Callers invoke it once during their own setup, and it
/// hands back everything they need to drive the UI.
///
/// Why a hook rather than putting this logic directly in the component?
/// Keeping state and effects together in one function makes the component
/// body a thin presentation layer and makes the logic easy to test or reuse
/// independently.
///
/// `MenuId` identifies which menu instance is currently open.  It is left
/// generic so the caller can use whatever discriminant makes sense in their
/// context (an enum variant, a `usize` index, a `String` key, etc.).
pub fn use_dropdown<MenuId>() -> RwSignal<Option<MenuId>>
where
    // `Send + Sync` are required because Leptos signals may be shared across
    // threads in some configurations (even in CSR the bounds are enforced at
    // compile time).
    MenuId: Clone + Send + Sync + 'static,
{
    debug!("use_dropdown: hook invoked");

    // The signal tracking which menu (if any) is currently open.
    // `None` means all menus are closed. We use `RwSignal` (read-write signal)
    // rather than a split `(ReadSignal, WriteSignal)` pair so that a single
    // value can be passed around and used for both reading and writing.
    let open_menu: RwSignal<Option<MenuId>> = RwSignal::new(None);

    // Obtain the document so we can attach a global click listener to it.
    // We call `expect()` here because, in a CSR app, `window` and `document`
    // are guaranteed to exist; a panic would indicate a fundamentally broken
    // environment, not a recoverable error.
    let document = web_sys::window()
        .expect("use_dropdown: no global `window`")
        .document()
        .expect("use_dropdown: no `document` on window");

    // Build the closure that will fire on every click anywhere in the document.
    //
    // Why `Closure::<dyn Fn(MouseEvent)>`?  wasm-bindgen requires that any
    // Rust function passed to JS as a callback be wrapped in a `Closure` so
    // that it can manage the underlying heap allocation.  We hold on to this
    // `Closure` value so that:
    //   (a) the JS function object stays valid for as long as the listener is
    //       registered, and
    //   (b) we can pass the exact same function pointer to `remove_event_listener`
    //       later — JS event listener removal is identity-based.
    //
    // `open_menu` is `Copy`, so moving it into the closure does not consume
    // it; we can still return it at the end of the function.
    let closure = Closure::<dyn Fn(MouseEvent)>::new(move |_event: MouseEvent| {
        debug!("use_dropdown: global click detected — closing open menu");
        // Any click that reaches the document was not stopped by a trigger's
        // `stop_propagation()` call, which means the click landed outside an
        // open menu. Close it.
        open_menu.set(None);
    });

    document
        .add_event_listener_with_callback("click", closure.as_ref().unchecked_ref())
        .expect("use_dropdown: failed to attach click listener to document");

    // `on_cleanup` requires its argument to be `Send + Sync + 'static`, but
    // `wasm_bindgen::Closure` and `web_sys::Document` are JS-backed types that
    // can never implement `Send` or `Sync`.  `SendWrapper` resolves this: it
    // is a zero-cost wrapper that satisfies the bounds by asserting the value
    // will only ever be accessed on the thread it was created on.  In WASM
    // that assertion is always true (WASM is single-threaded), so this is safe;
    // `SendWrapper` would panic at runtime if somehow called from a different
    // thread.
    let closure = SendWrapper::new(closure);
    let document = SendWrapper::new(document);

    // `on_cleanup` registers a function that Leptos calls when the reactive
    // owner that invoked `use_dropdown` is destroyed — typically when the
    // component unmounts.  We use it to:
    //   1. Remove the event listener, preventing stale handlers from firing
    //      if the component is ever re-mounted.
    //   2. Drop `closure`, which frees the underlying JS function object and
    //      avoids a memory leak.
    //
    // Moving both `SendWrapper`s into the cleanup closure transfers ownership:
    // the JS objects stay alive until cleanup runs, then are freed together.
    on_cleanup(move || {
        debug!("use_dropdown: cleaning up — removing global click listener");

        document
            .remove_event_listener_with_callback("click", closure.as_ref().unchecked_ref())
            .expect("use_dropdown: failed to remove click listener from document");

        // `closure` and `document` are dropped here, freeing both.
    });

    open_menu
}

/// `Dropdown` is the top-level wrapper for a dropdown menu.
///
/// Why a separate wrapper component rather than embedding the hook call inside
/// `DropdownTrigger`?  The signal created by `use_dropdown` must be shared
/// between the trigger (which writes to it) and the menu panel (which will
/// read it to decide whether to render).  Providing the signal from a common
/// ancestor via Leptos context is the idiomatic way to share reactive state
/// between siblings without prop-drilling.
///
/// `Dropdown` itself contributes no DOM element; it is a pure logical boundary
/// that exposes the reactive state to its descendants.
///
/// # Why accept `open_menu` as a prop rather than calling `use_dropdown` here?
/// Leptos `#[component]` only knows a generic type parameter exists if it
/// appears in the function's *parameter list*.  A type used only inside the
/// function body is "unused" from the compiler's perspective and triggers
/// `E0392`.  Accepting `open_menu: RwSignal<Option<MenuId>>` as a prop makes
/// `MenuId` appear in the signature, so Rust can infer it at the call site.
/// The caller (e.g. a page component) invokes `use_dropdown::<usize>()` and
/// passes the result in; `Dropdown` then publishes it via context.
#[component]
pub fn Dropdown<MenuId>(open_menu: RwSignal<Option<MenuId>>, children: Children) -> impl IntoView
where
    MenuId: Clone + Send + Sync + 'static,
{
    // Expose the signal to all descendants via Leptos context.  Any child that
    // calls `use_context::<RwSignal<Option<MenuId>>>()` will receive this value
    // without needing it threaded through every intermediate component as a prop.
    provide_context(open_menu);

    // Wrap children in a relatively-positioned container.
    //
    // Why `position: relative`?  `DropdownMenuItems` renders its panel with
    // `position: absolute`, which offsets relative to the nearest *positioned*
    // ancestor (one with `position` other than `static`).  Without this
    // wrapper, the panel would be positioned relative to some distant ancestor
    // and land in the wrong place.  `display: inline-block` keeps the wrapper
    // as small as the trigger button so the panel is anchored tightly to it.
    view! {
        <div style="position: relative; display: inline-block;">
            {children()}
        </div>
    }
}

/// `DropdownTrigger` renders the button that opens the dropdown menu.
///
/// Why stop click propagation?  The `use_dropdown` hook registers a global
/// document click listener that sets `open_menu` to `None` on every click.
/// Without `stop_propagation`, the sequence on a trigger click would be:
///   1. click fires on `<button>` → we set `open_menu = Some(id)` ✓
///   2. same event bubbles to `document` → listener sets `open_menu = None` ✗
/// Calling `stop_propagation` in the button handler breaks the bubble at
/// step 1, so the document listener never sees this particular click.
///
/// # Props
/// * `text`    — label displayed on the button
/// * `menu_id` — the `MenuId` value that identifies this menu instance;
///               clicking sets `open_menu` to `Some(menu_id)`
#[component]
pub fn DropdownTrigger<MenuId>(text: String, menu_id: MenuId) -> impl IntoView
where
    // `Debug` is added here (beyond the base bounds on `use_dropdown`) so we
    // can include the id in log messages.  The trigger is the only place we
    // need to print a `MenuId` value, so we don't widen the bound on
    // `use_dropdown` or `Dropdown` — keeping those APIs as permissive as
    // possible.
    MenuId: Clone + Send + Sync + 'static + std::fmt::Debug,
{
    // Retrieve the signal provided by the nearest ancestor `<Dropdown>`.
    // We panic rather than silently no-oping because a `DropdownTrigger`
    // outside a `Dropdown` is a programming error, not a recoverable condition.
    let open_menu = use_context::<RwSignal<Option<MenuId>>>()
        .expect("DropdownTrigger must be used inside a Dropdown");

    view! {
        <button
            on:click=move |event| {
                // Stop propagation before updating the signal.  See the
                // doc-comment on this component for a full explanation.
                event.stop_propagation();
                debug!("DropdownTrigger: clicked — opening menu {:?}", menu_id);
                open_menu.set(Some(menu_id.clone()));
            }
        >
            {text}
        </button>
    }
}

/// `DropdownMenuItems` is the floating panel that displays menu items when
/// its associated menu instance is the currently open one.
///
/// # Why `menu_id` on both trigger and items?
/// The `open_menu` signal stores *which* menu is open by ID.  The trigger
/// writes `Some(menu_id)` on click; the items panel reads it and compares
/// against its own `menu_id` to decide whether to render.  Without this
/// prop the panel could not distinguish "I am open" from "a sibling is open".
///
/// # Why `ChildrenFn` instead of `Children`?
/// `Children` (`Box<dyn FnOnce()>`) can only be called once.  Leptos's
/// `<Show>` component may call its children's render function multiple times
/// as the `when` condition toggles.  `ChildrenFn` (`Box<dyn Fn()>`) supports
/// repeated calls.
///
/// # Why provide `CloseMenuCallback` via context?
/// `DropdownMenuItem` needs to close the menu when clicked, but closing
/// requires writing to `RwSignal<Option<MenuId>>` — a generic type.  Rather
/// than making every `<DropdownMenuItem>` generic (which would require type
/// annotations at every call site), `DropdownMenuItems` wraps the operation
/// in the concrete `CloseMenuCallback` type alias and publishes it via
/// context.  `DropdownMenuItem` retrieves it without ever knowing `MenuId`.
#[component]
pub fn DropdownMenuItems<MenuId>(menu_id: MenuId, children: ChildrenFn) -> impl IntoView
where
    MenuId: Clone + Send + Sync + PartialEq + std::fmt::Debug + 'static,
{
    let open_menu = use_context::<RwSignal<Option<MenuId>>>()
        .expect("DropdownMenuItems must be used inside a <Dropdown>");

    // Wrap the close operation in a non-generic callback so DropdownMenuItem
    // can call it without being generic over MenuId.
    let close_menu: CloseMenuCallback = Callback::new(move |()| {
        debug!("DropdownMenuItems: close_menu invoked");
        open_menu.set(None);
    });
    provide_context(close_menu);

    view! {
        // `<Show>` renders its children only while `when` is true, removing
        // them from the DOM otherwise.  This avoids items accumulating
        // hidden event listeners.
        <Show when=move || open_menu.get() == Some(menu_id.clone())>
            // Stop propagation on the panel so that bare clicks on its
            // background (padding, borders, whitespace) don't bubble to the
            // global document listener and prematurely close the menu.
            // Clicks on individual items stop propagation themselves.
            <div
                style="position: absolute; \
                       top: calc(100% + 4px); \
                       left: -4px; \
                       z-index: 10; \
                       background: white; \
                       border: 1px solid #ccc; \
                       border-radius: 4px; \
                       padding: 4px 0; \
                       display: flex; \
                       flex-direction: column; \
                       min-width: 120px;"
                on:click=|event| event.stop_propagation()
            >
                {children()}
            </div>
        </Show>
    }
}

/// `DropdownMenuItem` renders a single selectable button inside a
/// `DropdownMenuItems` panel.
///
/// # Why is this component not generic over `MenuId`?
/// It does not need to touch `open_menu` directly.  Closing the menu is
/// delegated to the `CloseMenuCallback` that `DropdownMenuItems` placed in
/// context.  This keeps the call-site API simple: callers never annotate a
/// type parameter on individual items.
///
/// # Props
/// * `text`    — label shown on the button
/// * `handler` — called when the user selects this item; receives `()` because
///               the raw `MouseEvent` is rarely needed by item handlers
#[component]
pub fn DropdownMenuItem(text: String, handler: Callback<()>) -> impl IntoView {
    let close_menu = use_context::<CloseMenuCallback>()
        .expect("DropdownMenuItem must be used inside a <DropdownMenuItems>");

    // Clone `text` so we can both log it in the click handler and render it
    // as the button label.  The view! macro moves the original into the DOM;
    // the closure captures the clone.
    let text_for_log = text.clone();

    view! {
        <button
            style="display: block; \
                   width: 100%; \
                   padding: 6px 12px; \
                   text-align: left; \
                   background: none; \
                   border: none; \
                   cursor: pointer;"
            on:click=move |event| {
                // Stop propagation so neither the panel's handler nor the
                // global document listener sees this click.  Without this,
                // the menu would close, but via two separate signal writes
                // (one here, one in the global listener) rather than one.
                event.stop_propagation();
                debug!("DropdownMenuItem: '{}' selected", text_for_log);
                handler.run(());
                close_menu.run(());
            }
        >
            {text}
        </button>
    }
}
