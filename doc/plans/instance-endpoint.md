# Instance customization endpoint

## Goal

Add an authenticated `GET /api/v1/users/instance` endpoint that gives the
frontend the operator-controlled values needed to present an indielinks
instance:

- the default frontend page size;
- the instance origin;
- the operator's contact email address; and
- the instance's About, privacy-policy, terms-of-service, and optional message
  of the day documents as Markdown.

The endpoint reads and expands the Markdown templates for every request. An
operator can therefore update the documents without rebuilding the frontend or
restarting `indielinksd`.

This plan ends at making the customization data available through the shared
API. Frontend routes and Markdown rendering can consume that contract in a
follow-up change once their placement and interaction design have been settled.

## API contract

The public router nests the users router below `/api/v1`, so the route added to
`users::make_router` is `/users/instance` and its externally visible URL is:

```text
GET /api/v1/users/instance
```

The route uses the existing `authenticate` middleware and accepts the same
Bearer access token, Bearer API key, and Basic authentication schemes as the
other authenticated users endpoints. A request with no valid authenticated
user returns `401 Unauthorized`.

Add the response types to `indielinks-shared/src/api.rs` so the server and a
future frontend consumer share one wire contract. Use kebab-case JSON field
names, as follows:

```json
{
  "default-page-size": 16,
  "origin": {
    "scheme": "https",
    "host": { "RegName": "example.com" },
    "port": null
  },
  "contact-email": "admin@indiemark.local",
  "documents": {
    "about": "# About https://example.com\n",
    "privacy-policy": "# Privacy policy\n",
    "terms-of-service": "# Terms of service\n",
    "motd": null
  }
}
```

Define an `InstanceResponse` containing:

- `default_page_size: NonZero<usize>`;
- `origin: Origin`;
- `contact_email: UserEmail`; and
- `documents: InstanceDocuments`.

Define `InstanceDocuments` with `String` fields for `about`, `privacy_policy`,
and `terms_of_service`, plus `motd: Option<String>`. Serialize an absent MotD
as JSON `null` so the response shape remains stable. The `Origin` JSON
representation intentionally matches the existing shared `Origin` type; use
its `Display` implementation when substituting `%{origin}` into Markdown.

A successful request returns `200 OK` and JSON. A template read, UTF-8 decode,
or expansion failure returns `500 Internal Server Error` through the existing
users error-response mechanism, with detailed context logged on the server.

## Configuration

Add these top-level version-one configuration fields in
`indielinks/src/configuration.rs`:

```toml
default-page-size = 16
contact-email = "admin@indiemark.local"
templates = "/usr/share/indielinks/templates"
```

`default-page-size` is a `NonZero<usize>` with a Serde default of `16`. This
provides a usable frontend default when the field is omitted and rejects zero
during configuration parsing.

`contact-email` is a required `UserEmail`. Do not give it a Serde default: a
configuration that omits it or supplies an invalid address must fail to parse,
and `indielinksd` must fail during startup. The daemon currently falls back to
`ConfigV1::default()` when the implicit `/etc/indielinks.toml` cannot be read.
Remove that fallback (and the now-unused `Default for ConfigV1` implementation)
so running without a configuration file cannot bypass the required contact
email. Startup parsing remains fatal; a bad configuration encountered during a
SIGHUP reload continues to leave the daemon on its last known-good
configuration.

`templates` is a `PathBuf` whose Serde default is
`/usr/share/indielinks/templates`. Keeping the production location as the
default satisfies the installation contract while allowing the unprivileged
local installer and tests to select a temporary directory. Carry these three
values from `ConfigV1` into immutable fields on `Indielinks` in
`indielinks/src/indielinks.rs`.

Update every checked-in configuration source rather than generated TOML:

- set the default page size to `16` and the contact email to
  `admin@indiemark.local` in `conf/indielinksd-common.ncl`, which feeds every
  generated stack and the AWS configuration;
- update the hand-maintained `conf/indielinks-deb.toml` and
  `conf/indielinks-local.toml`; and
- point the local configuration's `templates` field at
  `$HOME/share/indielinks/templates`, using the concrete home path already
  written by the local-install workflow rather than relying on shell expansion
  inside TOML.

Extend the configuration tests to prove that the page-size default is applied,
an explicit nonzero value is accepted, zero is rejected, and a missing or
invalid contact email is rejected.

## Template files and expansion

Track the package source templates under `indielinks/templates/` with these
fixed names:

```text
about.md
privacy-policy.md
terms-of-service.md
motd.md
```

Ship usable defaults for the first three documents. Do not ship `motd.md` by
default; its absence is the normal representation of no current announcement.
Operators may create it later.

Each document is UTF-8 Markdown and may contain placeholders of the form
`%{parameter-name}`. Implement expansion as a small, separately testable helper
that receives the template text and a parameter map. Initially the map contains
only:

```text
origin -> state.origin.to_string()
```

Replace every occurrence, including repeated occurrences. Treat an unknown or
unterminated placeholder as an expansion error instead of returning an
unexpanded operator mistake to the frontend. Percent signs that do not begin
`%{` remain ordinary Markdown text.

Implement a document-loading helper that accepts the template directory and
origin so it can be tested with a temporary directory. Read all four files on
each endpoint request. The three required files fail the whole request on any
read error. Only `ErrorKind::NotFound` for `motd.md` becomes `None`; permission,
I/O, invalid UTF-8, and expansion failures for that file remain server errors.
An existing empty `motd.md` is returned as `Some("")`.

Add call-site-specific SNAFU variants for required-template reads, MotD reads,
and template expansion. Include the path or document kind as context, keep the
source out of display text, and map these failures to `500 Internal Server
Error` in `Error::status`.

## Handler and routing

In `indielinks/src/users.rs`:

1. Add the template expansion and loading helpers near the new endpoint's
   handler, ordered before the items that use them.
2. Have the handler extract
   `Extension<Result<User, ExtensionRejection>>`, matching existing users
   handlers: return `401` when authentication supplied no user, otherwise load
   the current documents and return `Json<InstanceResponse>`.
3. Register `get(instance)` at `/users/instance`, apply the existing
   authentication route layer, and use the same non-credentialed GET CORS
   policy and allowed headers as the other read-only users endpoints.
4. Continue relying on the users router's JSON content-type response layer.

The handler must not cache the processed documents in `Indielinks`; reading on
each request is what makes operator edits visible immediately.

## Installation and packaging

Extend both installation paths:

- Add the three required Markdown files to the base and AWS `cargo-deb` asset
  lists in `indielinks/Cargo.toml`, targeting
  `/usr/share/indielinks/templates/` with mode `0644`. The asset entries cause
  the package to create the directory. Leave `motd.md` unmanaged so package
  upgrades do not create or overwrite an operator announcement.
- Update `admin/local-install` to create
  `$HOME/share/indielinks/templates` and install the same three defaults there,
  matching the path emitted in `conf/indielinks-local.toml`.

Extend the Debian base and AWS package harnesses to assert that the directory
and three required files exist with package ownership and mode `0644`, and that
`motd.md` is absent from a fresh install. Where the harness starts the daemon,
also ensure the installed configuration contains an explicit valid
`contact-email`.

## Tests

Add focused tests before broad validation:

1. Shared-API JSON tests pin the kebab-case response shape, the structural
   `Origin`, and `motd: null`.
2. Configuration tests cover the default and explicit page sizes, zero, the
   required valid contact email, the templates default, and an explicit
   templates directory.
3. Users-module tests use `tempfile::TempDir` to cover all four documents,
   repeated `%{origin}` substitutions, an absent MotD, an empty MotD, a missing
   required document, invalid UTF-8, unknown parameters, and unterminated
   placeholders.
4. Add a backend-agnostic users integration test that first obtains a login
   token, confirms that the endpoint is `401` without it, then requests the
   endpoint with the token and asserts the configured page size, origin,
   contact email, expanded documents, and optional MotD. Point that fixture's
   `templates` configuration at fixture-owned temporary files so the test does
   not write under `/usr/share`.
5. Run the Debian package harnesses after package construction to cover the
   installed files.

## Validation

Run, in order:

```bash
cargo test -p indielinks-shared
cargo test -p indielinks --lib
cargo test -p indielinks --bin indielinksd
admin/cargo-test-front-end
cargo check -p indielinks-fe --target wasm32-unknown-unknown
admin/run-linters
```

Regenerate the stack configurations through the normal `indielinks` build and
inspect representative generated TOML under `target/conf/front-end/` to verify
the new values. Build both Debian variants with the existing packaging scripts
before running their harnesses. Finish with `git diff --check` and a search for
configuration instances that still omit `contact-email`.

## Acceptance criteria

- `indielinksd` refuses to start without a readable configuration containing a
  valid contact email.
- Omitting `default-page-size` yields `16`; configuring zero fails parsing.
- An authenticated GET returns the exact shared response contract and an
  unauthenticated GET returns `401`.
- The endpoint reads and expands the current Markdown files on every request.
- `%{origin}` expands to the configured public origin in every document.
- Missing required documents fail the request; a missing MotD becomes JSON
  `null`.
- Base, AWS, and local installs include the required default templates in their
  configured directories without installing a MotD.
- The frontend WASM check, targeted tests, package checks, and repository
  linters pass.
