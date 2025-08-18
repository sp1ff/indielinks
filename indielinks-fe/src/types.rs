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

//! # indielinks-fe Types & Constants
//!
//! I normally loathe these sorts of "types" or "entities" modules, but again, there really is a set
//! of shared types for this crate.

use leptos::prelude::RwSignal;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                   indielinks-fe common types                                   //
////////////////////////////////////////////////////////////////////////////////////////////////////

// A few new types for `use_context()`
#[derive(Clone, Debug)]
pub struct Api(pub String); // Make this a proper `Url`
#[derive(Clone, Debug)]
pub struct Base(pub String); // How to represent the path portion of an `Url` here?

// and a type alias for obvious reasons:
pub type Token = RwSignal<Option<String>>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                    indielinks-fe constants                                     //
////////////////////////////////////////////////////////////////////////////////////////////////////

pub static USER_AGENT: &str = "indielinks-fe/0.0.1 (+sp1ff@pobox.com)";
