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

//! # General indielinks Documentation
//!
//! General (i.e. not documenting a particular struct or a method) documentation goes here. It's
//! really just a grab bag at this point; I'll polish it after collecting some content.
//!
//! ## The Data Store
//!
//! Some might be surprised at my choice of ScyllaDB and DynamoDB. I admit, I didn't think it
//! through very deeply; it's just that in 2024 I think we're past the point where a relational
//! database should be the default answer when you need to write-down some state. A NoSQL
//! wide-column store is pretty-much my default solution, and it's only when & if I discover during
//! design that I truly can't wriggle-out of joins, or referential integrity, or full-blow ACID
//! transactions that I change to a RDBMS.
//!
//! indielink's data isn't particularly relational in nature, so DynamoDB was my "go to" answer.
//! That said, a design goal for me is to enable indielinks to be run in a wide variety of
//! configurations. If you want to run it as a daemon on bare metal in your house, I'll support
//! that. If you want to run a containerized cluster in the cloud, I'll support that, too. I added
//! ScyllaDB to support the local case: you can install & run ScyllaDB locally. The choice of
//! ScyllaDB was also motivated by their "Alternator" interface, which allows me to talk to it via
//! the DynamoDB API. That said, I still went through the hassle of supporting the DDB interface as
//! well as the native Cql interface. I may at some point just standardize on the former (and
//! simplify the code considerably).
//!
//! ## Observability
//!
//! Indielinks at this time provides for logs & metrics. Traces are less interesting given my design
//! goal of keeping the system as operationally simple as possible; in particular, there's only one
//! microservice other than the data store, making tracing rather uninteresting.
//!
//! ### Metrics
//!
//! I chose to use [OpenTelemetry] (AKA OTel) for for indielinks metrics, both because my sense is
//! that its adoption is growing and just because I like the idea of vendor-agnostic support. You
//! can still scrapte [Prometheus]-formatted metrics at `/metrics`, just because I wouldn't feel
//! comfortable without it, but you can also configure indielinks with the address of an OTLP
//! collector to have it push metrics in OTLP format (http/protobuf only) to that endpoint.
//!
//! [OpenTelemetry]: https://opentelemetry.io/docs/what-is-opentelemetry/
//!
//! #### OTel
//!
//! OTel distinguishes between the "API", which is the set of data types & operations that
//! indielinks (or any OTel caller) invokes to create spans/metrics/log records, and the "SDK" which
//! is the *implementation* of the API that actually makes telemetry flow. One could, in principle,
//! define a "NOP" implementation that does nothing.
//!
//! The OTel SDK is implemented by the [opentelemetry] crate, and the SDK by [opentelemetry-sdk]. Among
//! the things the implementation provides is an *exporter*. [opentelemetry-otlp] exports metrics
//! (as well as logs & traces, for that matter) in the OTLP format to either an OTel collector or an
//! OTLP-compatible backend. At the time of this writing, indielinks supports exporting OTLP over
//! HTTP with protobuf payloads. One compatible backend is [Prometheus].
//!
//! [opentelemetry]: https://docs.rs/opentelemetry/latest/opentelemetry/index.html
//! [opentelemetry-sdk]: https://docs.rs/opentelemetry_sdk/latest/opentelemetry_sdk/index.html
//! [opentelemetry-otlp]: https://docs.rs/opentelemetry-otlp/latest/opentelemetry_otlp/index.html
//! [Prometheus]: https://prometheus.io/docs/guides/opentelemetry/#enable-the-otlp-receiver
//!
//! Nb. the [opentelemetry-prometheus] exporter is deprecated.
//!
//! [opentelemetry-prometheus]: https://docs.rs/opentelemetry-prometheus/latest/opentelemetry_prometheus/
//!
//! #### Instance IDs
//!
//! When running your service in a cluster, unless you never ship logs off each host, it's important
//! to have your log messages tagged with some kind of identifier that can identify _which instance_
//! produced it. The operator can specify this on the command line or in the environment (indielinks
//! uses a UUID).
//!
//! Maybe I'm missing something, but I found it surprisingly difficult to get that into the
//! [tracing] infrastructure. Some suggest creating a "root" span in `main()` & entering it before
//! doing anything else. The idea is to guarantee that every subsequent [Span] & [Event] is a child
//! of that root. That seems workable to me in a synchronous program, but not in async: [Span]s are
//! not propogated across async tasks.
//!
//! [Span]: tracing::Span
//! [Event]: tracing::Event
//!
//! I considered a custom [FormatEvent] implementation (as was suggested
//! [here](https://users.rust-lang.org/t/global-fields-to-always-log-on-any-event-in-tracing-subscriber/81649/4)),
//! but I couldn't see how to add the instance ID to the [Event]'s fields, meaning that I'd have to
//! replicate the code implementing my chosen formatter, and just write-out the instance ID inline,
//! which I found unappealing.
//!
//! [FormatEvent]: tracing_subscriber::fmt::FormatEvent
//!
//! For now, I'm just relying on the [instrument] macro, which is unfortunate, since any code
//! executed on a code path I failed to instrument will lack the attribute. If it becomes a problem
//! in practice, I'll have to fall-back to a custom [FormatEvent] implementation.
//!
//! [instrument]: https://docs.rs/tracing/latest/tracing/attr.instrument.html
//!
//! ## Background Task Processing
//!
//! Surprisingly (to me), [axum] makes no provision for compute outside request handlers. There's
//! nothing like the old [OnIdle] mechanism, back in the day. So, I wrote one. The framework is
//! documented [here], but, briefly, if you need to carry-out some background processing
//! "near-line", do the following:
//!
//! [OnIdle]: https://stackoverflow.com/questions/8349677/how-cwinthreadonidle-is-used
//! [here]: crate::background_tasks
//!
//! 1. define a struct containing all the information needed to carry-out your background task; the
//!    struct must implement [Serialize] and [Deserialize]
//!
//! [Serialize]: serde::Serialize
//! [Deserialize]: serde::Deserialize
//!
//! 2. implement [TaggedTask] on your struct; in particular, the [exec()] method will actually
//!    do the task's work
//!
//! [TaggedTask]: crate::background_tasks::TaggedTask
//!
//! [exec()]: crate::background_tasks::Task::exec
//!
//! 3. register the task with the system:
//!
//! ```ignore
//! inventory::submit! {
//!     BackgroundTask {
//!         id: /* a UUID identify your task type */,
//!         de: |buf| { Ok(Box::new(rmp_serde::from_slice::<MyTask>(buf)?)) }
//!     }
//! }
//! ```
//!
//! 4. the [indielinks] state instance has a `task_sender` member that can "send" tasks to the
//!    background processing system; instantiate your struct and invoke `send()`.
//!
//! [indielinks]: crate
//!
//! ## Refined Types in [indielinks]
//!
//! Nb this may turn into a blog post at some point.
//!
//! ### What Is a Refinement Type?
//!
//! A [Refinement Type] is a sub-type that restricts the parent type in some way. For instance, if
//! the `string` type contains all valid UTF-8-encoded bytestrings, the type of all such strings
//! whose byte values are valid *ASCII* encodings (which is more strict) could be termed a *refined
//! type*.
//!
//! [Refinement Type]: https://en.wikipedia.org/wiki/Refinement_type
//!
//! In type systems that support dependent types, refinement types can be implemented as a pair
//! whose first element is of the parent type & whose second is a predicate asserting the additional
//! condition. In such type systems, one can express "the type of all unsigned ints less than five",
//! for example. And if one tries to instantiate that type with six, the compiler will complain.
//!
//! ### Refinement Types in Rust
//!
//! Rust doesn't (fully) support dependent types, but it does at least support
//! correct-by-construction types. The approach is typically this: create a newtype struct with a
//! private interior, and provide a fallible constructor or constructors taking the parent type that
//! will enforce the additional constraint and only return an instance of the newtype if it's
//! satisfied. For instance:
//!
//! ```
//! use std::result::Result;
//!
//! pub struct AsciiString(String);
//!
//! impl AsciiString {
//!     pub fn new(s: &str) -> Result<AsciiString, String> {
//!         if !s.is_ascii() {
//!             Err("Not ASCII".to_owned())
//!         } else {
//!             Ok(AsciiString(s.to_owned()))
//!         }
//!     }
//! }
//! ```
//!
//! Even this, however, brings up some questions:
//!
//! - Does the refinement type have a natural implementation of the [Default] trait?
//!
//! - Does the refinement type have a natural implementation of the [Display] trait? Nb that
//!   implementing [Display] will automatically implement [ToString]
//!
//!   [Display]: std::fmt::Display
//!
//! - Does the refinement type have natural implementations of the following (commonly derived)
//!   traits?
//!
//!   - [Clone]
//!
//!   - [Copy]
//!
//!   - [Debug]
//!
//!   - [Eq]
//!
//!   - [Hash]
//!
//!   - [Ord]
//!
//!   - [PartialEq]
//!
//!   - [PartialOrd]
//!
//!   - [ToOwned]: generally implemented for !Sized !Clone; i.e. `&str`, `Box<str>` and so on
//!
//!   - [Borrow]: not sure about this one, either: "Types express that they can be borrowed as some
//!     type T by implementing `Borrow<T>`.... A type is free to borrow as several different types."
//!
//!     [Borrow]: std::borrow::Borrow
//!
//! - Should we dispense with the constructor and implement [TryFrom]? The difference here is that
//!   [TryFrom] consumes its argument; I suppose we could implement for `&str` (or "reference to
//!   whatever") If the refined type is [String], then [TryFrom] isn't needed-- just implement
//!   [FromStr] and have users call `parse()`
//!
//!   [FromStr]: std::str::FromStr
//!
//! - If the type being refined can naturally be represented as a [String], should [FromStr] be
//!   implemented? This will allow us to read instances via [str::parse].
//!
//! - The refined type should be usable wherever the parent type is required. This is generally done
//!   by implementing [AsRef] and [Deref] (the former is typically implemented in terms of the
//!   latter). You'll also likely want to implement [From] on the parent type for the refined type.
//!   [TryFrom] isn't needed, since the conversion from refined type to parent type is infallible.
//!
//!   [Deref]: std::ops::Deref
//!
//! - Depending on the application's needs there may be additional traits that need to be
//!   implemented on the newytpe: [Serialize] and/or [Deserialize], for instance
//!
//! Unfortunately, I don't see an obvious way to generalize this. Maybe a proc macro that takes
//! assorted options?
//!
//! ### Attempts to Express Refinement Types at the Type Level in Rust
//!
//! - [nutype](https://github.com/greyblake/nutype) "is a proc macro that allows adding extra
//!   constraints like sanitization and validation to the regular newtype pattern." This is, I
//!   suppose, the sort of procedural macro I was imagining, except that they express the
//!   constraints in a DSL rather than free-form code
//!
//! - [refined-type](https://github.com/tomoikey/refined_type) takes the approach of requiring
//!   you to implement a trait for each rule, but uses proc macros to combine them
//!
//! - [refined]: the most promising approach I've found; it
//!   promises no macros as well as composibility. Unfortunately, this crate is young and (perhaps
//!   understandably) seems to have made a choice to depend on the nightly toolchain rather than
//!   stable.
//!
//! [refined]: https://github.com/jkaye2012/refined
//!
//! ### Refined Types in [indielinks]
//!
//! At this point, and at the risk of indulging my tendency to "just code it up", I think I'm going
//! to go with just coding-up my refined types by hand, while both keeping an eye on the [refined]
//! crate and looking for patterns in my own refined types that might be amenable to abstraction,
//! even through macros.
//!
//! ## Integration Tests
//!
//! I've begun building-out integration tests in the [indielinks-test] crate. While these tests fit
//! into the standard Rust integration test framework, I've created them with custom harnesses that
//! will stand-up ScyllaDB, spin-up an indielinks instance, & then allow the tests to interact with
//! indielinks as HTTP clients.
//!
//! [indielinks-test]: ../../indielinks_test/index.html
//!
//! ## indielinks as Client
//!
//! Middleware turns-out to be as useful when writing an HTTP client as when writing the server.
//! Unfortunately, the "go to" Rust HTTP Client, [reqwest] is incompatible with the standard
//! middleware library ([tower]). [Aleksey Sidorov](https://github.com/alekseysidorov)'s
//! [tower-reqwest](https://github.com/alekseysidorov/tower-http-client) crate inspired me to write
//! a translation layer that would allow me to stack tower middleware on top of a reqwest client. I
//! wrote more about that [here](https://www.unwoundstack.com/blog/rust-client-middleware.html).
//!
//! [tower]: https://docs.rs/tower/0.5.2/tower/
//!
//! # Developers' Documentation
//!
//! I spent the best part of a year (on & off) "picking at" the problem of implementing an
//! ActivityPub server. That initial prototyping can be found on branch `prototypes`, but `master`
//! (and its offshoots) contains the production code.
