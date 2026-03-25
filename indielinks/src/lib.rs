// Copyright (C) 2024-2025 Michael Herstine <sp1ff@pobox.com>
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

//! # indielinks
//!
//! Right now, the library crate has the same name as the binary, meaning that `rustdoc` will
//! ignore the binary crate. You can find general documentation [here].
//!
//! [here]: crate::_docs
pub mod _docs;
pub mod acct;
#[path = "activity-pub.rs"]
pub mod activity_pub;
pub mod actor;
#[path = "ap-entities.rs"]
pub mod ap_entities;
#[path = "ap-resolution.rs"]
pub mod ap_resolution;
#[path = "app-logic.rs"]
pub mod app_logic;
pub mod authn;
#[path = "background-tasks.rs"]
pub mod background_tasks;
pub mod bookmarklets;
pub mod cache;
pub mod client;
#[path = "client-types.rs"]
pub mod client_types;
pub mod delicious;
pub mod dynamodb;
#[path = "dynamodb-schemas.rs"]
pub mod dynamodb_schemas;
pub mod entities;
pub mod grpc;
#[path = "home-timeline.rs"]
pub mod home_timeline;
pub mod http;
pub mod indielinks;
pub mod metrics;
#[path = "metrics-task.rs"]
pub mod metrics_task;
pub mod ops;
pub mod peppers;
#[path = "protobuf-interop.rs"]
pub mod protobuf_interop;
#[path = "sanitized-html.rs"]
pub mod sanitized_html;
pub mod scylla;
#[path = "signing-keys.rs"]
pub mod signing_keys;
pub mod storage;
pub mod token;
pub mod users;
pub mod util;
pub mod webfinger;
