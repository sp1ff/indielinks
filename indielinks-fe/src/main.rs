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

use leptos::prelude::*;
use tracing_subscriber::fmt;
use tracing_subscriber_wasm::MakeConsoleWriter;

#[component]
fn App() -> impl IntoView {
    view! {
        <h1>indielinks</h1>
    }
}

fn main() {
    fmt()
        .with_writer(MakeConsoleWriter::default().map_trace_level_to(tracing::Level::DEBUG))
        .without_time()
        .with_ansi(false)
        .init();
    console_error_panic_hook::set_once();
    // I'd *like* to setup the State here, but somehow the `App` component doesn't receive it.
    leptos::mount::mount_to_body(App);
}
