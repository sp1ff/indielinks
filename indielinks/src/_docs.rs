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
//! a bit of a grab bag at this point.
//!
//! ## The Data Store
//!
//! Some might be surprised at my choice of ScyllaDB and DynamoDB. I admit, I didn't think it
//! through very deeply; it's just that in 2024 I thought we were past the point where a relational
//! database should be the default answer when you need to write down some state. A NoSQL
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
//! can still scrape [Prometheus]-formatted metrics at `/metrics`, just because I wouldn't feel
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
//! I've begun building-out integration tests in dedicated crates (e.g. [indielinks-test]). While
//! these tests fit into the standard Rust integration test framework, I've created them with custom
//! harnesses that will stand-up ScyllaDB, spin-up an indielinks instance & then allow the tests to
//! interact with indielinks as HTTP clients. I wrote about this in detail [here].
//!
//! [indielinks-test]: ../../indielinks_test/index.html
//! [here]: https://www.unwoundstack.com/blog/integration-testing-rust-binaries.html
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
//! ## Request Signing, Instance Actors, Authorized Fetch, and All That There
//!
//! ActivityPub communicates server-to-server through `POST` [requests] that are [authenticated] by
//! HTTP signatures. The situation with respect to HTTP signatures in ActivityPub is regrettable.
//! Right off the bat: indielinks supports the *draft* HTTP signatures [spec], AKA
//! "draft-cavage-http-signatures-12". There is a later RFC that, AFAICT, no one actually uses
//! (update: since I wrote that, Mastodon has added support for RFC 9421 signatures [beginning] in version 4.4.0).
//!
//! [requests]: https://www.w3.org/TR/activitypub/#delivery
//! [authenticated]: https://www.w3.org/wiki/ActivityPub/Primer/Authentication_Authorization#Server_to_Server
//! [spec]: https://datatracker.ietf.org/doc/html/draft-cavage-http-signatures-12
//! [RFC]: https://www.rfc-editor.org/rfc/rfc9421.html#signature-params
//! [beginning]: https://docs.joinmastodon.org/spec/security/#http-message-signatures
//!
//! It was my understanding that `GET` ActivityPub requests need *not* be signed, but it turns out
//! that Mastodon has introduced something called [secure mode]: "Secure mode is the foundation upon
//! which 'limited federation mode' is built. A Mastodon server in limited federation mode will only
//! federate with servers its admin has explicitly allowed, and reject all other requests." A
//! Mastodon instance in secure mode will, among other things, enable what is known as [authorized
//! fetch] "which requires all HTTP GET requests for ActivityPub objects to include HTTP
//! Signatures... Servers with authorized fetch enabled generally don't enforce any fine grained
//! access control over the actors whose signatures they require to fetch data. They usually only
//! reject requests from actors on domains that they've blocked at the server level."
//!
//! [secure mode]: https://docs.joinmastodon.org/spec/activitypub/#secure-mode
//! [authorized fetch]: https://www.w3.org/wiki/ActivityPub/Primer/Authentication_Authorization#Authorized_fetch
//!
//! This creates problems for the other party in communication with an ActivityPub server
//! that enforces authorized fetch:
//!
//! - on receipt of a `POST` from such as server, the other party will need to authenticate the
//!   message signature; in general, this will mean resolving the public key ID in the signature
//!   to the actual public key material, which means a GET request _back_ to the secure server. Now
//!   that the other party needs to *sign* that request, we'll see a request from the secure server
//!   *back* to the other party to resolve the key ID. Not only is this rather "chatty", it leads
//!   to an infinite loop when the other party *also* enforces authorized fetch!
//! - if the `POST` request from a secure server lands in the other party's shared inbox, it
//!   is unclear whose private key should be used to sign the outgoing `GET` request to resolve
//!   the `POST` request's key ID
//!
//! [Terrence Eden](https://shkspr.mobi/blog/) notes this problem
//! [here](https://shkspr.mobi/blog/2024/02/http-signature-infinite-loop/). The answer generally in
//! use seems to be to have an "instance actor": an actor that represents the entire instance of the
//! ActivityPub app and whose public key may be fetched *without* authentication.
//!
//! Typically, Sebastian Jambor [explains] the solution well: "Most Fediverse instances have an
//! actor that is not tied to any user, but instead to the instance itself. On Mastodon, it has its
//! own username, which is the domain name itself. For example, for the Academy, the instance actor
//! is @activitypub.academy@activitypub.academy... This actor is special in many ways. For example,
//! its actor ID does not follow the usual Mastodon naming schema, it is simply
//! <https://activitypub.academy/actor>, and if you check out this profile in the ActivityPub
//! Explorer, you see that the actor is of type Application."
//!
//! [explains]: https://seb.jambor.dev/posts/understanding-activitypub-part-4-threads/#the-instance-actor
//!
//! TBH, the whole thing feels ad hoc and "bolted on" to me. Steve Bate has a nice
//! [write-up](https://socialhub.activitypub.rocks/t/authorized-fetch-and-the-instance-actor/3868)
//! on how this situation came to be. See also
//! [here](https://fedify.dev/manual/access-control#instance-actor)
//!
//! # Developers' Documentation
//!
//! I spent the best part of a year (on & off) "picking at" the problem of implementing an
//! ActivityPub server. That initial prototyping can be found on branch `prototypes`, but `master`
//! (and its offshoots) contains the production code.
//!
//! ## Project Structure
//!
//! ### Module Dependencies
//!
//! I try to maintain a layered structure to the modules making-up [indielinks]. In particular, if
//! we visualize their dependencies as a graph, it should be acyclic. I'd love to find some tooling
//! to analyze this & even make part of the CI pipeline, but at the time of this writing, here it is
//! (constructed manually):
//!
//! 1) actor, webfinger, users, delicious, webfinger
//!    - all the public endpoints
//! 2) indielinks, activity-pub, dynamodb, scylla, client
//!    - implementations of lower-level abstractions
//! 3) ap-entities, background_tasks, cache
//!    - internal subsystems
//! 4) client-types (authn, http)
//! 5) authn
//! 6) storage, token, entities, acct
//! 7) peppers, signing-keys, http
//! 8) util, metrics
//!    - depend on nothing
//!
//! ## The ChangeLog (or the lack thereof)
//!
//! First, let's fix terminology. In this section, I'm using the GNU term "ChangeLog", as
//! [discussed] in the [GNU Coding
//! Standards](https://www.gnu.org/prep/standards/standards.html#Top): "Keep a change log to
//! describe all the changes made to program source files. The purpose of this is so that people
//! investigating bugs in the future will know about the changes that might have introduced the
//! bug." This seems superfluous to me in the age of widespread SCM. In general, a user who's
//! obtained your source code without cloning the git repo (as an Autotools-style source code
//! distribution, say) won't immediately have the commit history available to them, so providing the
//! file seems helpful in that case. However, indielinks at this time doesn't provide any such
//! thing.
//!
//! [discussed]: https://www.gnu.org/prep/standards/standards.html#Change-Logs
//!
//! It _ought_ to be possible to generate a GNU style ChangeLog from a git repo, but there doesn't
//! seem to be many tools out there to do that (which reinforces my sense that ChangeLogs just
//! aren't that useful). I've used the [gnulib] tool [gitlog-to-changelog] to produce them from the
//! git history on other projects, but it's otuput is not terribly good: it mostly just reformats
//! the git commit messages; it doesn't filter-out, say, merge commits, nor does it display the list
//! of modified files (let alone which functions were modified in those files).
//!
//! [gnulib]: https://www.gnu.org/software/gnulib/manual/gnulib.html#Top
//! [gitlog-to-changelog]: https://www.gnu.org/software/gnulib/manual/gnulib.html#gitlog_002dto_002dchangelog
//!
//! As a ressult, I'm simply not maintaining a ChangeLog for the indielinks project.
//!
//! Now, [keep a changelog] seems to mean something different by the workd "changelog", declaring
//! the "GNU changelog style guide, \[and\] the two-paragraph-long GNU NEWS file 'guideline'...
//! inadequate or insufficient" which would be interesting if they only said in what way they were
//! deficient. They don't, and the project's "[changelog]" looks to me like... a GNU [NEWS] file: "a
//! list of user-visible changes worth mentioning." As a user myself I've found this useful when
//! trouble-shooting, so I distribute them along with all my projects. I really don't do much with
//! it until I roll a release, where my release checklist always includes updating the file. I _do_
//! keep a section at the top of the file titled "UNRELEASED" where I note major changes during
//! development, usually when merging a feature branch.
//!
//! [keep a changelog]: https://keepachangelog.com/en/1.1.0/
//! [changelog]: https://github.com/olivierlacan/keep-a-changelog/blob/main/CHANGELOG.md
//! [NEWS]: https://www.gnu.org/prep/standards/standards.html#NEWS-File
//!
//! ## Packaging
//!
//! Although at the time of this writing, I haven't stood-up a public indielinks instance, I'd like
//! to make it as easy as possible for people to download & install it locally to play with. My
//! first step has been to provide Debian & Arch binary packages, as those are the two distributions
//! I currently use.
//!
//! ### Arch
//!
//! The obvious move here is to write a `PKGBUILD` for a "git-style" package, and maybe even place
//! it on the AUR itself. Now, there *is* a Cargo plugin for this: [cargo-aur], but I'm not using it
//! for a few reasons:
//!
//! [cargo-aur]: https://github.com/fosskers/cargo-aur
//!
//! - I have non-Rust projects I'd like to package in this way, so I'd like to familiarize myself
//!   with carrying-out the process "by hand"
//! - IME, this approach requires to the plugin author to make certain assumptions about your
//!   project, that may not necessarily be applicable to any given project
//! - and, indeed, it [turns out] that cargo-aur doesn't support workspaces
//!
//! [turns out]: https://github.com/fosskers/cargo-aur/issues/17
//!
//! I've implemented a "git" package for the AUR in the `indielinks-git` repo. I took a look at
//! doing builds in a chroot using `aurutils`, but the process seemed complex & poorly documented,
//! so I just fell back to using an Arch Docker container for building binary packages, linting and
//! testing them.
//!
//! ### Debian
//!
//! The same situation presents itself with respect to [cargo-deb], though in this case the
//! question is more complex, because both options are even less appealing: Debian packaging is
//! [notoriously](https://optimizedbyotto.com/post/debcraft-easy-debian-packaging/)
//! [difficult](https://www.reddit.com/r/rust/comments/ursdqx/have_you_guys_tried_cargodeb_amazing/),
//! and [cargo-deb] seems to have adapted by not even trying to produce a compliant Debian source
//! package.
//!
//! [cargo-deb]: https://github.com/kornelski/cargo-deb
//!
//! I spent a little time reading-up on Debian packaging in the hopes of building a proper source
//! package (for the same reasons as with Arch), but quickly found myself bewildered by the
//! [welter](https://manpages.debian.org/bullseye/debhelper/debhelper.7.en.html)
//! [of](https://optimizedbyotto.com/post/debcraft-easy-debian-packaging/)
//! [tools](https://people.debian.org/~nthykier/blog/2023/a-new-debian-package-helper-debputy.html)
//! out there for the job.
//!
//! The situation is particularly knotty [with respect to
//! Rust](https://wiki.debian.org/Teams/RustPackaging/Policy): the debhelper
//! build system that I'd _thought_ was salient, [dh-cargo], apparently doesn't support workspaces.
//! Now, there's a _new_ debhelper build system, [dh-rust] that *does*, but that's only available in
//! Debian 13, whereas my distro is based on Debian 12.
//!
//! [dh-cargo]: https://packages.debian.org/sid/dh-cargo
//! [dh-rust]: https://packages.debian.org/search?keywords=dh-rust&searchon=names&suite=stable&section=all
//!
//! At this point, I threw up my hands & went with [cargo-deb].
//!
//! ## ActivityPub Signatures
//!
//! The logic for computing & verifying ActivityPub signatures (see above) resides in a few places.
//! Module [authn](crate::authn) contains AP signature-related utilities & tests. The actual
//! signature is added to outgoing requests in the [client](crate::client) module, and then only if
//! the request extensions contain either a [User](crate::entities::User) or a
//! [UserPrivateKey](indielinks_shared::entities::UserPrivateKey) (in a middleware layer).
//!
//! Incoming AP requests have their sigantures checked in [actor](crate::actor), again in a (route)
//! layer. I think this makes sense, since at validation time, we're making API-level decisions
//! (whether or not to bounce the request, e.g.), so it probably belongs where it is.
//!
//! [send_activity_pub](crate::activity_pub::send_activity_pub) & friends is a higher-level
//! "convenience" function; it takes care to:
//!
//! - actually build the request (given the URL, method, generic body)
//! - converts the body to JLD
//! - takes care to set the property extensions so the client will sign the request
//! - send the request through the client
//! - deserialize the response body, if any, to an indielinks AP entity
//!
//! At the time of this writing, however, the API is unfortunate in that it generally requires type
//! hints from the caller.
