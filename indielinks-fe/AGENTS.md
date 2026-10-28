# AGENTS.md

This file applies to the `indielinks-fe` crate. The repository-root
`AGENTS.md` also applies; this file adds frontend-specific guidance
and takes precedence where the two conflict.

## What This Crate Is

`indielinks-fe` is the client-side rendered Leptos single-page application. It compiles to
WebAssembly, communicates with the indielinks public API through `gloo-net`, and uses Trunk for
development and bundling. Thaw supplies UI components, and Tailwind supplies most styling.

The library and binary are restricted to `wasm32`, with one exception: the pure theme model in
`src/theme/model.rs` compiles on every target so its logic can be unit-tested on the host. Always
specify the `wasm32-unknown-unknown` target when checking or building this crate.

## Configuration

The following environment variables are read at compile time in `src/main.rs`; changing one
requires rebuilding the frontend:

- `INDIELINKS_FE_API`: public API base URL; defaults to `http://localhost:20679`.
- `INDIELINKS_PAGE_SIZE`: number of items requested per page; defaults to `4`.
- `INDIELINKS_BASE`: path at which the frontend is mounted; defaults to the empty string.
- `INDIELINKS_FE_DARK_THEME`: `true` (or unset) enables the dark-theme capability; `false` forces
  the light theme and omits the theme control. Any other value fails the build (see `build.rs`).

## Build and Check

From the repository root, check or build the crate with:

```bash
cargo check -p indielinks-fe --target wasm32-unknown-unknown
cargo build -p indielinks-fe --target wasm32-unknown-unknown
```

Run the repository linters from the repository root before signoff:

```bash
admin/run-linters
```

The pure theme model (`src/theme/model.rs`) carries host-side unit tests; run them with:

```bash
cargo test -p indielinks-fe --lib
```

Add tests only when they exercise meaningful logic; use `wasm-bindgen-test` when the behavior
requires a browser or WebAssembly environment.

## Development Server

Run Tailwind in one shell:

```bash
cd indielinks-fe
npx @tailwindcss/cli -i style.css -o tailwind.css --watch
```

Run Trunk in another shell:

```bash
cd indielinks-fe
INDIELINKS_FE_API="http://localhost:20679" trunk serve --port=18080 index.html
```

Open `http://localhost:18080`. The server API must be available at the configured URL for sign-in
and data-backed views to work.

Do not commit `dist/` or `tailwind.css`; both are generated and ignored. Edit `style.css` for
global CSS and Tailwind input. Most component styling lives in Leptos `view!` class attributes.

## Rust and Leptos Conventions

- Begin every Rust source file with the repository copyright and GPL notice, followed immediately
  by a `//!` module documentation block.
- Put `use` statements after module documentation. Group them in this order: `std`, third-party
  crates, other workspace crates, then this crate. Consolidate imports by their top-level path.
- Order definitions by dependency so each dependency appears before the item that uses it. Put a
  module's SNAFU `Error` near the top, followed by `pub type Result<T> = StdResult<T, Error>`.
- Define types that make invalid state unrepresentable. Parse external values into refined or sum
  types at the boundary, and return `Result` for operations that can fail.
- Prefer expression-based iterator, future, and `Result` chains over imperative sequencing.
  Attach call-site-specific SNAFU context with `context` or `with_context`; do not include a
  variant's `source` in its display text.
- Use `futures::StreamExt::then` for asynchronous stream closures and `map` for synchronous ones.
- Keep reactive state as local as practical. Use Leptos context only for application-wide values,
  and preserve URL query parameters as the source of truth for views that already use them.
- Keep user-facing errors suitable for toast or page display while retaining structured error
  context for logging and diagnosis.

When changing a component, verify its loading, empty, success, and error states. For authentication
or HTTP changes, also verify missing-token, refresh, unauthorized, and retry behavior as applicable.
