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

//! Route modules, shared components, and client-side services for the indielinks frontend.
//!
//! Almost everything in this crate only makes sense in the browser, so all modules except
//! [theme] are compiled only for `wasm32`. The theme module's pure [model](theme::model) is
//! compiled on every target so that its logic can be unit-tested on the host via
//! `cargo test -p indielinks-fe --lib`.

#[cfg(target_arch = "wasm32")]
#[path = "add-link.rs"]
pub mod add_link;
#[cfg(target_arch = "wasm32")]
pub mod components;
#[cfg(target_arch = "wasm32")]
pub mod feeds;
#[cfg(target_arch = "wasm32")]
pub mod home;
#[cfg(target_arch = "wasm32")]
pub mod http;
#[cfg(target_arch = "wasm32")]
pub mod instance;
#[cfg(target_arch = "wasm32")]
pub mod personal;
#[cfg(target_arch = "wasm32")]
pub mod signin;
#[cfg(target_arch = "wasm32")]
pub mod signup;
pub mod theme;
#[cfg(target_arch = "wasm32")]
pub mod types;
