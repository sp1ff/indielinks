# Instance-aware frontend plan

Status: **planning only**. This document is an implementation handoff; it does
not authorize feature implementation or a Git commit.

## Goal

Make `indielinks-fe` describe the instance it is connected to instead of
presenting deployment-specific text compiled into the application. At startup,
the frontend will fetch `GET /api/v1/users/instance`, make the resulting
`InstanceResponse` available throughout the component tree, and use it as the
single source of truth for the instance origin, contact address, default page
size, and operator-authored documents.

The visible result is:

- the shell wordmark displays the instance origin;
- the Popular page says `Welcome to <origin>`;
- every contact link uses the instance contact email;
- the introductory contact sentence on Popular becomes `Read more here.
  Documentation here. Want an account? Contact the sysop.`, with the two
  occurrences of `here` and `Contact the sysop` linked;
- About, Documentation, Privacy, and Terms are available as dedicated,
  readable pages inside the existing application shell; and
- an optional message of the day appears near the Popular-page introduction.

This plan follows the compact, responsive, del.icio.us-inspired visual system
described in [`ui-visual-system-overview.md`](ui-visual-system-overview.md).

## Current-code basis

The backend and shared contract already exist:

- `indielinks-shared/src/api.rs` defines `InstanceResponse` with
  `default_page_size`, `origin`, `contact_email`, and `documents`.
- `InstanceDocuments` contains `about`, `documentation`, `privacy_policy`,
  `terms_of_service`, and optional `motd` Markdown strings.
- `indielinks/src/users.rs` publicly serves the response at
  `/api/v1/users/instance`; the route does not require an access token.

The frontend currently provides `Api`, `PageSize`, `Base`, token, theme, and
shell state from `src/main.rs`. It obtains page size from
`INDIELINKS_PAGE_SIZE`, refreshes the access token before rendering the router,
and hard-codes `Indielinks` as the document title. The shell logo hard-codes
`indielinks`; the Popular and sign-up pages contain the three current
`mailto:` links. The Popular page already has a responsive introduction,
content panels, and an `About this instance` panel suitable for the document
navigation proposed below.

Treat the current shared types and public route as authoritative. The earlier
backend plan in `instance-endpoint.md` predates the implemented
`documentation` field and public route and must not override current source.

## Decisions

### Startup and application context

Fetch the instance response immediately when the root `App` component starts.
Start this public request and the existing token-refresh resource independently
so one network round trip does not wait for the other. Do not attach an
authorization header or invoke the authenticated retry helper for the public
instance endpoint.

Introduce a context newtype with a descriptive name such as
`InstanceConfiguration(Arc<InstanceResponse>)`. Provide it only after the
response has been successfully decoded, before mounting the router and shell.
Components consume that context rather than receiving individual instance
fields through long prop chains.

The response is required application configuration. Keep the startup loading
view while it is pending. If the request, HTTP status check, or JSON decoding
fails, replace the startup loading view with a full-page error state that
explains that instance information could not be loaded and offers Retry. Retry
only the instance request; token refresh may continue or complete
independently. Do not silently substitute compiled branding or a contact
address, because that could direct a user to the wrong operator.

Use `InstanceResponse::default_page_size` wherever pagination currently reads
the `PageSize` context. Remove the duplicate `PageSize` context and the
`INDIELINKS_PAGE_SIZE` compile-time setting from `main.rs`, `build.rs`, and
crate documentation once all consumers have moved. Keep `INDIELINKS_FE_API`
as the development-origin override and construct the request URL consistently
with the frontend's existing API-base convention.

The initial fetch is once per application load. The documents are immutable in
the frontend context for that load; an operator update becomes visible after a
page reload, which obtains a fresh endpoint response.

### Instance identity and contact links

Display `InstanceResponse::origin` with its `Display` implementation. In the
shell's full brand treatment, replace the visible `indielinks` wordmark with
that origin. In the compact mark, retain the graphical mark and use the origin
as its visually hidden accessible name. Keep the mark itself and existing
brand styles.

Change the Popular-page eyebrow to `Welcome to <origin>`. Preserve the current
heading and lead text below it. Product references that describe indielinks as
software are outside this text substitution and remain unchanged.

Replace the introductory contact paragraph with this exact visible copy:

> Read more here. Documentation here. Want an account? Contact the sysop.

Link the first `here` to About, the second `here` to Documentation, and
`Contact the sysop` to the configured email address. Because `here` is
ambiguous outside its surrounding sentence, give those links accessible names
such as `Read more about <origin>` and `Read documentation for <origin>` while
preserving the requested visible copy.

Create one helper for contact links so the address and query parameters cannot
drift between pages. It accepts the refined `UserEmail` and an optional subject
and produces a correctly percent-encoded `mailto:` URL. Use it for:

- `Contact the sysop` in the Popular introduction;
- the visible contact email in the Popular-page instance panel; and
- the account-request link on the sign-up page.

Use a subject such as `Account request for <origin>` for account requests and
no subject for the plain contact-email link. Keep visible email text where the
current design displays the address. After conversion, no hard-coded
`mailto:` address remains in `indielinks-fe`.

### Document navigation and routes

Add four public, base-aware application routes:

| Route | Label | Response field |
|---|---|---|
| `/about` | About | `documents.about` |
| `/docs` | Documentation | `documents.documentation` |
| `/privacy` | Privacy | `documents.privacy_policy` |
| `/terms` | Terms | `documents.terms_of_service` |

Represent this mapping once with a small closed enum such as `Document` that
provides the route, navigation label, page heading, and a method selecting the
corresponding Markdown from `InstanceDocuments`. This keeps route registration,
navigation, and field selection synchronized.

At the bottom of the existing `About this instance` panel, add a compact
`Instance resources` navigation row containing About, Documentation, Privacy,
and Terms. Let it wrap naturally on narrow screens and use ordinary links with
visible focus treatment. The introductory About and Documentation links target
the same routes. Do not add these resources to primary navigation: they are
secondary instance information and should not compete with Popular, Home, or
Add Link.

Each route renders within the normal shell. Use a centered content panel with:

1. a page heading;
2. the same compact four-link navigation, marking the active link with
   `aria-current="page"`; and
3. the selected document in a semantic `<article>`.

Give each page a stable heading id and connect the article or containing
section with `aria-labelledby`. On route changes, preserve the router's normal
history behavior and ensure keyboard users reach a useful heading through the
existing skip-link/main-content flow.

### Reusable Markdown component

Add a reusable component, for example `MarkdownDocument`, whose public input is
Markdown text and whose output is a consistently styled semantic article. It
must be independent of the instance response and document kind so later
features can reuse it for other operator-authored Markdown.

Use `pulldown-cmark` as a direct frontend dependency and keep Markdown-to-HTML
conversion in a narrow helper. Treat the endpoint response as display content,
not trusted DOM:

- render headings, paragraphs, emphasis, strong text, lists, block quotes,
  code spans, fenced code blocks, thematic breaks, and tables;
- render raw and inline HTML as escaped text rather than passing it through;
- allow fragment links, relative application links, and `http`, `https`, and
  `mailto` destinations;
- prevent unsafe schemes such as `javascript` and `data` from becoming active
  links; preserve their label as plain text;
- avoid scriptable image content; either omit images initially or subject them
  to an equally explicit URL policy; and
- never insert unfiltered Markdown directly with Leptos `inner_html`.

The conversion helper may return the sanitized HTML consumed by one tightly
scoped `inner_html` site. Document the invariant at that site. If filtering an
event stream becomes difficult to audit, use a sanitizer after Markdown
conversion and pin its allowlist in tests; do not rely on the fact that the
documents are operator-authored.

Add a `.markdown-document` style block using existing Thaw and indielinks CSS
variables. Keep body text at a readable measure, preserve the existing compact
spacing rhythm, distinguish heading levels without oversized display type,
give links the existing normal/visited/focus treatment, allow wide tables and
code blocks to scroll within the panel, and preserve contrast in both themes.
Do not introduce document-specific literal colors.

### Message of the day

When `documents.motd` is `Some` and non-empty, render it through the same
Markdown component in a compact informational callout immediately below the
Popular-page introduction and before the recent-post/popular-tag panels. Give
the callout an `Instance notice` heading for assistive technology. Do not add a
dismiss control in this change because no persistence behavior has been
defined.

Treat `None` and an empty or whitespace-only string as no visible notice. This
prevents an empty operator template from creating an unexplained blank panel.
The four long-form document routes remain available regardless of MotD state.

## Implementation checklist

### Instance loading and context

- [ ] Add a frontend error variant for sending, status-checking, and decoding
      the public instance response with call-site-specific SNAFU context.
- [ ] Implement a focused loader for `GET /api/v1/users/instance` that returns
      the shared `InstanceResponse`.
- [ ] Start the instance loader at root-component startup in parallel with the
      existing token refresh.
- [ ] Add a retryable full-page startup error state and retain the existing
      startup loading state while instance configuration is pending.
- [ ] Wrap the successful response in `InstanceConfiguration` (or an
      equivalently named context newtype) and provide it above the router and
      shell.
- [ ] Replace `PageSize` context reads with the response's
      `default_page_size` and remove the redundant context type.
- [ ] Remove `INDIELINKS_PAGE_SIZE` handling from `main.rs`, `build.rs`, and
      frontend configuration documentation.
- [ ] Verify development API overrides and same-origin production builds both
      request exactly `/api/v1/users/instance` without duplicating `/api/v1`.

### Branding and contact information

- [ ] Let the shell brand component receive or consume the formatted origin
      for both its full visible label and compact accessible name.
- [ ] Replace the Popular-page eyebrow with `Welcome to <origin>`.
- [ ] Add one percent-encoding `mailto:` helper based on `UserEmail` and an
      optional subject.
- [ ] Render the exact three-part introductory sentence and link About,
      Documentation, and Contact the sysop to their respective targets.
- [ ] Give both visible `here` links descriptive accessible names.
- [ ] Replace the Popular instance-panel email and sign-up account-request
      address with context-derived links.
- [ ] Search all frontend Rust, HTML, and CSS sources for remaining hard-coded
      contact addresses and `mailto:` URLs.

### Document model, routes, and navigation

- [ ] Add the closed document enum and define each route, label, heading, and
      response-field selection once.
- [ ] Extend base-aware `Paths` and router registration for About,
      Documentation, Privacy, and Terms.
- [ ] Add the compact `Instance resources` navigation to the bottom of the
      existing instance-information panel.
- [ ] Implement the shared document-page layout with heading, resource
      navigation, active-page semantics, and article content.
- [ ] Confirm direct navigation, browser back/forward, configured base paths,
      and the unknown-route fallback continue to work.

### Markdown presentation and MotD

- [ ] Add `pulldown-cmark` as a direct dependency for the WASM frontend.
- [ ] Implement and document the Markdown conversion policy, including escaped
      raw HTML and blocked unsafe links.
- [ ] Implement the reusable `MarkdownDocument` component with semantic article
      markup.
- [ ] Add responsive, theme-aware prose, heading, list, quote, table, link, and
      code styles using existing CSS variables.
- [ ] Render a non-empty MotD in an `Instance notice` callout on Popular and
      omit the callout for missing or blank content.
- [ ] Check long origins, long unbroken document text, wide tables, and code
      blocks for overflow at desktop and phone widths.

### Tests and validation

- [ ] Add focused Markdown tests for every supported construct, raw HTML,
      unsafe schemes, allowed links, escaping, and blank input.
- [ ] Add focused tests for document-enum field selection and route metadata so
      each response field is connected to the intended route exactly once.
- [ ] Test mail URL generation with a plain address, origin-derived subject,
      spaces, `+`, and other characters requiring percent encoding.
- [ ] Add browser/WASM component tests for startup loading, successful context
      provision, failed loading with retry, all four document pages, active
      navigation state, exact Popular-page copy, and MotD present/absent states.
- [ ] Test narrow and wide layouts, keyboard navigation, focus indicators,
      accessible link names, heading structure, light/dark contrast, and
      horizontal overflow.
- [ ] Run `cargo test -p indielinks-fe --lib` for existing host-side tests.
- [ ] Run the configured `wasm-bindgen-test` browser suite for the new WASM
      behavior.
- [ ] Run
      `cargo check -p indielinks-fe --target wasm32-unknown-unknown`.
- [ ] Run
      `cargo build -p indielinks-fe --target wasm32-unknown-unknown`.
- [ ] Run `admin/run-linters` from the repository root.
- [ ] Run `git diff --check` and inspect the final diff for generated
      `tailwind.css` or `dist/` files; neither belongs in the change.

## Acceptance criteria

- The frontend requests `/api/v1/users/instance` as it starts and does not
  mount the normal application shell until a valid response is available.
- A startup failure has a useful error message and working retry action; it
  never displays a fabricated origin or contact address.
- The shared instance response is available to all routed components through
  one application context and is the sole frontend source of instance origin,
  contact email, and default page size.
- The shell's full wordmark and compact accessible name use the instance
  origin, and Popular says `Welcome to <origin>`.
- Popular displays the exact requested sentence, with working About,
  Documentation, and sysop links and descriptive accessible names.
- Every frontend `mailto:` URL uses the configured contact email and encodes
  its optional subject correctly; no hard-coded operator address remains.
- About, Documentation, Privacy, and Terms have dedicated, base-aware routes
  using one reusable document component and one consistent resource
  navigation idiom.
- Raw Markdown HTML and unsafe URL schemes cannot create active DOM content.
- A non-empty MotD appears as an accessible instance notice; missing or blank
  MotD produces no empty panel.
- Document content remains readable and usable with keyboard navigation, both
  themes, and phone and desktop layouts.
- Focused tests, the WASM check and build, repository linters, and whitespace
  validation pass.
