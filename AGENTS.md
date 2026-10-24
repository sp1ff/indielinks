# AGENTS.md

This file provides guidance to coding agents when working with code in this repository.

## What This Is

indielinks brings del.icio.us-style social bookmarking to the
Fediverse: a Rust web service that speaks the old del.icio.us API, a
user-management API, and ActivityPub (so public bookmarks federate
into Mastodon timelines). GPL-3.0-or-later. Backed by a wide-column
store — ScyllaDB (native CQL or Alternator/DynamoDB protocol) or AWS
DynamoDB — never an RDBMS.

**Read `indielinks/src/_docs.rs` first.** It is a documentation-only
module holding the project's design rationale: storage choice,
observability, background tasks, refinement types, ActivityPub
signatures / instance actors, error design, module layering, stacks,
and Nickel configuration. `tests-support/src/lib.rs` and
`indielinks-cache/src/_docs.rs` play the same role for their areas.
`HACKING.org` has the operational quick-start; `INSTALL.org` covers
prerequisites.

## Workspace Layout

| Crate                                        | Role                                                                                     |
|----------------------------------------------|------------------------------------------------------------------------------------------|
| `indielinks`                                 | the server; bins `indielinksd` (daemon) and `indielinks-schemas` (schema create/migrate) |
| `indielinks-shared`                          | types shared between server and clients                                                  |
| `indielinks-cache`                           | turns a cluster of indielinks nodes into a distributed cache over Raft (openraft)        |
| `indielinks-fe`                              | Leptos/WASM front end (not in `default-members`; built for `wasm32-unknown-unknown`)     |
| `indielinks-client`                          | CLI, binary name `indic`                                                                 |
| `tests-support`                              | shared integration-test harness (fixtures/domains/backends)                              |
| `tests-indielinks`, `tests-indielinks-cache` | integration tests, custom `harness = false`                                              |

Non-crate dirs: `admin` (project scripts), `conf` (Nickel + TOML config), `infra` (docker compose,
cluster up/down scripts), `scripts` (install/admin), `assets` (GitHub page).

## Build, Lint, Test

```bash
cargo build
cargo doc --no-deps --open          # rustdocs, incl. the _docs modules
admin/run-linters                   # rg for TODOs, clippy -Dwarnings, fmt --check, rustdoc -Dwarnings
admin/cargo-build-test-smoke        # build + wasm build + full test suite
admin/signoff                       # run before every push; refuses a dirty tree
```

Integration tests need `127.0.0.1 indiemark.local` in `/etc/hosts`, Docker, and haproxy.

```bash
cargo test                                                  # whole suite, default stack
cargo test -p tests-indielinks --test=smoke-tests healthcheck
cargo test -p tests-indielinks --test=smoke-tests dynamodb-single-node/000test_healthcheck
```

Fixture names are part of the libtest-mimic test name; `INDIELINKS_TEST_FIXTURES` (comma-separated)
narrows the set. Fixtures for the web service: `{scylla,dynamodb}-single-node`,
`{scylla,dynamodb}-single-node-pre-charged`, `{scylla,dynamodb}-cluster`.

`admin/run-linters` fails the build on any `TODO`, `LATER`, `todo!`, or `dbg!` in Rust/proto/CSS/HTML,
and on `rmp_serde::to_vec()` — use `to_vec_named()` (see the MessagePack section of `_docs.rs`).

## Stacks

Each integration-test fixture owns a set of ports, docker project names, and state dirs, so only one
instance can run at a time. To allow parallel test runs across git worktrees, the project defines
**stacks** — disjoint parameter sets, declared in `conf/stacks.ncl` via `conf/mk-stack.ncl`
(`master`, `bugfix`, `front-end`, `pre-alpha`, `cloud`, …). Each has an `admin/cargo-test-<stack>`
wrapper that sets `INDIELINKS_{SMOKE,CACHE,CACHE_CLUSTER,BACKGROUND_TASK}_TESTS_CONFIG` to files
under `target/conf/<stack>/`, and an `infra/<stack>.env` for docker compose.

This worktree is the **cloud** stack — use `admin/cargo-test-cloud`, not bare `cargo test`, when
running the integration suite here.

## Configuration

Config lives in Nickel (`*.ncl`) under `conf/` and each test crate's `conf/`. Server configs and the
paired integration-test configs are generated from the *same* Nickel source by the crates' `build.rs`
(`indielinks`, `tests-indielinks`, `tests-indielinks-cache`) into `target/conf/<stack>/`, so they
stay in sync and get cleaned by `cargo clean`. **Edit the `.ncl`, not the generated TOML.** Some
hand-written `.toml` and `.env` files remain. Requires the `nickel` CLI on hacking config.

## Runtime Shape

`indielinksd` serves three listeners (defaults for the master stack):

- **public** (20679) — `make_world_router`: ActivityPub actor routes merged at root, del.icio.us API
  and user API nested at `/api/v1`, bookmarklets at `/bookmarklets`, webfinger, healthcheck, `/metrics`
- **local** (20680) — `make_local_router`: `/ops` admin API and `/ops/cache` Raft/cache control,
  localhost-only
- **gRPC** (20681) — intra-cluster distributed-cache traffic

Per-module `make_router(state: Arc<Indielinks>)` functions are composed in
`indielinks/src/bin/indielinksd.rs`. `Indielinks` (in `indielinks/src/indielinks.rs`) is the shared
axum state: storage backend, caches, clients, signing keys, peppers, background-task sender, Raft
cache node.

Cluster bring-up is not automatic: a fresh node must be told its Raft membership via
`POST /ops/cache/init-cluster` before `/healthcheck` returns `GOOD`.

Storage goes through the `Backend` trait (`indielinks/src/storage.rs`), implemented over ScyllaDB
native CQL (`scylla.rs`) and DynamoDB/Alternator (`dynamodb.rs`).

## Module Layering

`indielinks`'s modules are kept in an acyclic, layered dependency graph (the layer list is in
`_docs.rs`; regenerate with `admin/check-dependencies`). Keep new code within that discipline —
roughly: `util` → `peppers` → `entities`/`acct` → `storage`/`token` → `http`/`authn`/`cache` →
`app_logic` → routers.

## Local Development

```bash
infra/scylla-down && infra/scylla-up            # 3-node containerized ScyllaDB + schema
cd indielinks && RUST_BACKTRACE=1 RUST_LOG=debug,... \
  cargo run --bin indielinksd -- -D -p -c ../conf/indielinksd-scylla.toml -F
```

(The full `RUST_LOG` incantation that quiets the AWS/scylla/openraft crates is in `HACKING.org`.)
Front end: `npx @tailwindcss/cli -i style.css -o tailwind.css --watch` in one shell and
`INDIELINKS_FE_API="http://localhost:20679" trunk serve --port=18080 index.html` in another, both
from `indielinks-fe`.

## Commit Procedure

Branch, hack, commit, repeat. Before pushing, squash so each commit is an individually revertible
unit of functionality; commit messages start with a one-line summary followed by a paragraph or two
on functionality and salient implementation decisions. Run `admin/signoff` before pushing (CI mostly
replicates it, but the local machine is faster). Then cut a PR. There is no ChangeLog; note
user-visible changes in the `UNRELEASED` section of `NEWS`.

**Do not run `git commit` without first presenting the commit message and staged files for the
user's explicit approval.** Stage with `git add`, show the diff with `git diff --cached`, present
the proposed commit message, and wait for the user to say "commit" or "yes" before invoking `git
commit`.
