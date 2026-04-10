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

//! # [indielinks] Integration Test Support
//!
//! ## Introduction
//!
//! The Rust testing framework is oriented toward testing library crates. Even "integration tests"
//! are intended to test a Rust library crate from the perspective of a consumer of that library
//! crate (rather than a privileged insider, as with unit tests). These are, of course, common
//! testing needs, and [indielinks] eagerly makes use of them. Testing a binary crate, or a crate
//! that requires additional services at runtime (a database or a cache, for instance), requires the
//! test author to implement that support on their own.
//!
//! [indielinks] has a number of cases like this. Initially, I handled each case individually &
//! separately, writing whatever support I needed. I wrote about the process [here].
//!
//! [here]: https://www.unwoundstack.com/blog/integration-testing-rust-binaries.html
