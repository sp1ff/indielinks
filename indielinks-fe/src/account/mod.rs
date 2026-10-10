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

//! Account management page frame and section navigation.
//!
//! The account area lives under `/m` and is structured so that future sections (Profile, Blocks,
//! Follows, Followers) can be added without redesigning the route prefix or responsive layout.
//!
//! The [page] submodule (browser builds only) contains the Leptos components and route wrappers.
//! Pure helpers live in [model], which compiles on all targets so they can be unit-tested.

pub mod model;

#[cfg(target_arch = "wasm32")]
mod api_keys;
#[cfg(target_arch = "wasm32")]
mod page;
#[cfg(target_arch = "wasm32")]
mod password;
#[cfg(target_arch = "wasm32")]
mod profile;

#[cfg(target_arch = "wasm32")]
pub use page::*;
