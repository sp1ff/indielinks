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

//! # The indielinks visual theme
//!
//! ## Introduction
//!
//! Thaw components and application-authored Tailwind classes share the CSS variables emitted by
//! this theme. Keeping the palette here prevents the two styling systems from drifting apart.
//!
//! The frontend offers an optional dark appearance alongside the canonical light one. The
//! capability is controlled at compile time by `INDIELINKS_FE_DARK_THEME` (see the crate-level
//! documentation); when enabled, the rendered palette follows the operating system's
//! `prefers-color-scheme` setting until the user makes an explicit choice via the theme control,
//! which is then persisted in browser storage.
//!
//! ## Module layout
//!
//! - [model] holds the pure configuration/preference/appearance types. It is compiled on all
//!   targets so that its logic can be unit-tested on the host.
//! - `palettes` constructs the light & dark Thaw themes (browser builds only).
//! - `controller` integrates the model with the browser: storage, media queries and the document
//!   element's `data-theme` attribute (browser builds only).

pub mod model;

#[cfg(target_arch = "wasm32")]
mod controller;
#[cfg(target_arch = "wasm32")]
mod palettes;

#[cfg(target_arch = "wasm32")]
pub use controller::{InitialTheme, ThemeController};
#[cfg(target_arch = "wasm32")]
pub use palettes::{dark, light};
