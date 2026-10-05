# Account Management Frontend Plan

Status: draft for review. This document plans the feature; it does not authorize implementation.

## Purpose

Add a protected account-management area to `indielinks-fe` where a
signed-in user can change their password and inspect, mint, and revoke
API keys. Structure the area so that profile editing, blocks, follows,
followers, and other account features can be added without replacing
its route or page shell.

This work follows the visual direction in
[`ui-visual-system-overview.md`](ui-visual-system-overview.md): compact
navigation, reusable visual tokens, responsive layout, consistent forms and feedback, and full
keyboard and accessibility validation.

The current-code basis for this plan is the [application router](../../indielinks-fe/src/main.rs),
[shell account controls](../../indielinks-fe/src/components/shell.rs), [authenticated HTTP
helpers](../../indielinks-fe/src/http.rs), [API-key model](../../indielinks/src/entities.rs), [mint
endpoint](../../indielinks/src/users.rs), and [Authorization-header
parser](../../indielinks/src/authn.rs). The password flow also follows OWASP's
[authentication](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
and [session-management](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html)
guidance for protecting password changes and browser sessions.

## Decisions

- Use `/m` ("me") for the protected account area. `/a` remains the existing Add Link route.
- Give the account area its own section navigation. The first sections are Password and API keys.
- Reserve the structure, without rendering placeholders, for later Profile, Blocks, Follows, and Followers sections. A future Profile section is expected to edit email address, display name, and summary/bio.
- A successful password change rotates the current browser's refresh and CSRF cookies to a newly
  identified session while keeping the user signed in. Sessions in other browsers remain valid
  until their stateless tokens expire.
- Existing API-key secrets cannot be recovered. List only a key's zero-based index and expiry.
- A newly minted secret is shown once and is never persisted by the frontend.
- Preserve the server's zero/one/two-key model. When two keys exist, minting explicitly warns that key `#0`, the oldest key, will be replaced.
- Keep revocation index-based for now. The presentation should allow a future name to become the primary label without changing the page structure or action flow.
- New password-change and key-revocation mutations return `202 Accepted`. Treat that response as meaning the requested state change has completed sufficiently for the UI to update; it is not a signal to poll a background job.
- The existing mint endpoint continues to return `key_text` as `v1:<hex>`. The frontend must not claim that this is a complete Authorization-header value; the value sent on the wire is `<username>:<key_text>`.

## Scope

### Included

- A signed-in account entry point in the application shell.
- Responsive account section navigation.
- Password-change form and current-browser session rotation.
- API-key loading, empty, error, and populated states.
- Optional expiry selection when minting.
- Explicit oldest-key replacement confirmation.
- One-time presentation and copying of newly minted key material.
- Per-key revocation with confirmation.
- Accessible pending, success, and failure feedback.

### Excluded

- Password recovery or forgotten-password flows.
- Current-password entry; the approved request contains only `new_password`.
- Listing or invalidating sessions on other devices; the backend stores no session state.
- API-key names, scopes, permissions, or more than two simultaneous keys.
- Recovery of existing key material.
- Profile, block, follow, and follower management.
- Backend implementation.

Changing a password does not revoke API keys. Those are separate credentials with their own
explicit revocation controls.

## Backend prerequisites and contracts

The frontend implementation begins after the backend and shared request/response types are
available. All endpoints require the normal bearer access token and use the existing
`send_with_retry` helpers so an expired access token gets one refresh-and-retry attempt. The
password request must also set `RequestCredentials::Include`, and its CORS policy must allow the
configured frontend origin and credentials, so the browser accepts replacement cookies.

### Change password

```http
POST /api/v1/users/change-password
Content-Type: application/json

{"new_password":"clear text password"}
```

- Success: `202 Accepted`, with no response document required.
- The backend validates and hashes the new password according to its existing password policy.
- The response replaces the current browser's HttpOnly refresh cookie and signed CSRF cookie. Both
  name the same newly generated session UUID and retain the configured lifetime, path, `SameSite`,
  `Secure`, and `Partitioned` attributes.
- The backend stores no session state. Existing access tokens and refresh/CSRF pairs in other
  browsers remain valid until their normal expirations.
- The current access token remains usable until it expires; its next refresh uses the replacement
  cookie pair.
- Validation failures return an error response suitable for display without exposing the password.

Suggested shared type: `ChangePasswordReq { new_password: SecretPassword }`.

### List API keys

```http
GET /api/v1/users/keys
```

Success: `200 OK`. Model the server's zero/one/two-key invariant directly:

```rust
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApiKeySummary {
    pub index: usize,
    pub expiry: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ListKeysRsp {
    NoKeys,
    OneKey(ApiKeySummary),
    TwoKeys {
        senior: ApiKeySummary,
        junior: ApiKeySummary,
    },
}
```

Use Serde's default externally tagged representation. The possible response documents are:

```json
"NoKeys"
```

```json
{"OneKey":{"index":0,"expiry":null}}
```

```json
{
  "TwoKeys": {
    "senior": {"index":0,"expiry":"2027-10-04T19:30:00Z"},
    "junior": {"index":1,"expiry":null}
  }
}
```

`senior` is the older key and `junior` is the newer key. `index` is the value used by revocation;
it identifies a slot in this response, not a permanent key ID. `expiry: null` means that the key
does not expire. A later `name` field may be added to `ApiKeySummary`, but the initial frontend
neither requests nor expects it.

The backend guarantees index `0` for `OneKey`, and senior index `0` plus junior index `1` for
`TwoKeys`. Parse those invariants at the HTTP boundary so the rest of the account UI can rely on
them.

### Mint API key

The existing endpoint and response remain authoritative:

```http
POST /api/v1/users/mint-key
```

- With no request body, mint a key without an expiry.
- With a JSON `MintKeyReq` body, mint a key with its RFC 3339 expiry.
- Success remains the endpoint's current `201 Created` response:

  ```json
  { "key_text": "v1:<hex>" }
  ```

- The returned text is secret and is available only in this response.
- Authentication uses `Authorization: Bearer <username>:<key_text>`.

### Revoke API key

```http
POST /api/v1/users/revoke-key
Content-Type: application/json

{"index":0}
```

- Success: `202 Accepted`, with no response document required.
- An index not present in the caller's current key list fails without revoking another key.
- After success, the frontend reloads the list because the remaining key may receive a new index.

Suggested shared type: `RevokeKeyReq { index: usize }`.

## Information architecture and navigation

Add an Account link to the signed-in controls rendered by
`components::shell::AccountControls`. Keep Sign out available beside
it. The account link appears in the desktop rail and responsive
header; it does not displace Popular, Home, or Add Link in primary
navigation.

Use these routes:

- `/m` — protected account landing route; redirect to `/m/password`.
- `/m/password` — password section.
- `/m/api-keys` — API-key section.

The account page owns a heading, short introduction, and section
navigation. On wider screens, render the section navigation as a
compact left column and the active section as the main panel. At
narrower widths, render the same links as a horizontally scrollable
tab row above the panel. Use normal links, `aria-current="page"`,
visible focus indicators, and meaningful section headings.

Represent account sections as one small route/label/icon definition so
adding Profile or social management later updates desktop and mobile
navigation together. Do not show disabled links for features that do
not exist.

## Password section

Render a focused Security panel containing:

- New password input with `autocomplete="new-password"`.
- Confirm new password input with `autocomplete="new-password"`.
- Help text stating that the current browser stays signed in after success.
- A primary `Change password` action with pending text and disabled repeated submission.

The frontend checks only that both fields are non-empty and match.
Password-strength rules remain server-owned so the frontend cannot
drift from backend policy. Submit the password using the shared
secret-bearing request type; do not log it, include it in diagnostics,
or retain it after the request completes.

On validation failure, keep focus in the relevant field and render an
associated inline error. On backend failure, keep the form available
and show the existing error-toast pattern. On `202`:

1. Clear both password fields.
2. Retain the in-memory access token; the browser stores the replacement refresh and CSRF cookies.
3. Remain on `/m/password`.
4. Show `Password changed.` in an accessible success toast.

The frontend neither reads the HttpOnly refresh cookie nor copies cookie values from the response.
The browser applies both `Set-Cookie` headers because the request includes credentials. The next
refresh reads the replacement CSRF cookie and proves possession in the existing way.

## API-key section

### Loading and listing

Load keys when the section mounts. Use the shared `LoadingState`,
`ErrorState`, and retry patterns. For zero keys, explain that API keys
authenticate scripts and clients and offer the mint action.

For each key, render a compact row with:

- `Key #0` or `Key #1` as the current fallback label.
- `Expires <localized date and time>` or `Never expires`.
- `Expired` status when the expiry is in the past.
- A Revoke action whose accessible name includes the key index.

Preserve the RFC 3339 UTC value for machine handling and expose the
exact value in a tooltip or secondary text while presenting a
localized date to the user. A future optional name can replace `Key
#N` as the heading while retaining the index as secondary text.

### Minting

When fewer than two keys exist, label the action `Mint API key`. If
expiry input is supported, offer `Never` as the default and a
date/time input as the alternative. Convert local input to UTC once at
the request boundary and let the server enforce minimum-future and
other validity rules.

When two keys exist, change the action to `Replace oldest key`. Before
sending the request, show an inline confirmation naming key `#0` and
its expiry and explaining that it will stop working. A cancel action
returns to the unchanged list. This confirmation makes the existing
rolling two-key behavior explicit without changing it.

After a successful mint:

1. Replace any previously displayed new-key secret in memory.
2. Reload the key list so indices and replacement are current.
3. Show a prominent one-time-secret panel with the returned `v1:<hex>` value in a wrapping
   monospace block.
4. State: `Copy this key now. It cannot be shown again.`
5. Offer a Copy key button and announce copy success without exposing the secret in a toast.
6. Show the usage form separately:
   `Authorization: Bearer <username>:v1:<hex>`.

Because the frontend has no durable authenticated-username source and
the mint response is not changing, copy only the returned `key_text`.
Keep `<username>` visibly marked as a placeholder in the usage example
rather than fabricating a copy-ready credential. Never put the secret
in a URL, log, toast, browser storage, or long-lived application
context. Discard it when the section unmounts, another key is minted,
or the user dismisses the panel.

### Revocation

Revoke is destructive, so require inline confirmation that identifies
the key index and expiry. Disable both revoke and mint controls while
a key mutation is pending. On `202`, discard a displayed secret if it
belongs to the affected list state, reload keys, and show a success
toast that contains only the index. On error, preserve the list and
confirmation so the user can retry or cancel.

## Code organization

Plan these frontend changes:

- `src/account/mod.rs`: always-compiled account module; export the pure model and, on WASM, the
  page frame, section definition, and section navigation.
- `src/account/password.rs`: password form state, credentialed request action, and session-rotation
  success flow.
- `src/account/api_keys.rs`: list resource, mint/revoke actions, confirmations, and one-time secret
  state.
- `src/account/model.rs`: pure parsing and presentation helpers where host-side unit tests add
  value, including key-list invariants, expiry status, and oldest-key replacement labels.
- `src/lib.rs`: export the account module on host and WASM so pure model tests run normally; keep
  browser components behind `cfg(target_arch = "wasm32")` inside the module.
- `src/main.rs`: register protected `/m`, `/m/password`, and `/m/api-keys` routes.
- `src/components/shell.rs`: add the account path, title, and signed-in Account link.
- `src/http.rs`: reuse and, only where needed, generalize authenticated JSON helpers; preserve the
  existing one-refresh retry behavior and 2xx status handling.
- `style.css`: add account layout, section navigation, key rows, confirmations, and secret-panel
  styles using existing tokens and breakpoints.
- `Cargo.toml`: make `chrono` available to the pure account model on the host, and add only the
  browser APIs required for clipboard support; avoid a new component or date library.

Prefer request and response types from `indielinks-shared::api`. Do
not duplicate wire models in the frontend once the backend contracts
exist.

## Error and authentication behavior

- A protected account route redirects a guest to `/s`, consistent with Home and Add Link.
- An authenticated request retries once after refreshing an expired access token.
- The password request always includes browser credentials so its replacement cookies are stored.
- If refresh is unavailable, clear the stale in-memory token, redirect to `/s`, and explain that
  the session expired.
- Treat every 2xx status accepted by the HTTP helper as transport success, while still decoding the
  required mint/list response documents.
- Do not include password or key material in error variants, tracing fields, toast text, technical
  details, or panic messages.
- Keep mutation controls disabled only for the mutation in flight; loading the initial key list
  blocks key actions but does not affect Password navigation.

## Accessibility and responsive behavior

- Every input has a visible label and associated help/error text.
- Pending operations expose `aria-busy`; feedback that changes without navigation uses an
  appropriate live region.
- Confirmation controls are reachable and dismissible by keyboard, return focus to their invoking
  button on cancel, and move focus to the result on success.
- Secret and header examples wrap rather than forcing horizontal page scrolling.
- Touch targets follow the shell's coarse-pointer sizing, and the section navigation remains usable
  at narrow mobile widths and browser zoom.
- Color never carries expiry, error, or destructive meaning by itself.

## Validation

Automated checks:

```bash
cargo check -p indielinks-fe --target wasm32-unknown-unknown
admin/run-linters
```

Add focused host-side tests only for pure logic that can regress independently of rendering:

- Decode all three externally tagged key-list variants; reject invalid indices for each variant.
- Identify expired, expiring, and non-expiring keys.
- Identify key `#0` as the replacement target only when two keys exist.
- Construct route paths correctly when `INDIELINKS_BASE` is empty or non-empty.

Manual browser validation:

- Direct navigation and refresh on all three `/m` routes, including a non-empty frontend base path.
- Guest redirect and expired-token refresh behavior.
- Password mismatch, server policy rejection, network failure, successful `202`, replacement-cookie
  storage, continued access, and the next token refresh.
- Key lists with zero, one, and two keys; expired and non-expiring dates; list retry.
- Mint without replacement, mint with explicit key-`#0` replacement, one-time secret dismissal, and
  clipboard success/failure.
- Revoke either index, cancel revocation, stale-index failure, and re-indexing after success.
- No password or key secret in console logs, URLs, storage, toasts, or error details.
- Keyboard-only use, visible focus, screen-reader labels/live announcements, 200% zoom, light/dark
  themes, reduced motion, phone/tablet/desktop widths, and supported browsers.

Do not run `admin/cargo-test-front-end` or the workspace integration suite for this frontend-only
change; the repository guidance states that they do not exercise the frontend.

## Implementation checklist

- [ ] Land shared API types and the change-password, list-keys, and revoke-key backend endpoints.
- [ ] Confirm password success returns matching replacement refresh/CSRF cookies before `202`.
- [ ] Add account routes and shell entry point.
- [ ] Build the reusable account page and responsive section navigation.
- [ ] Implement and validate the Password section.
- [ ] Implement key loading and zero/one/two-key presentation.
- [ ] Implement minting, explicit oldest-key replacement, and one-time secret handling.
- [ ] Implement indexed revocation and list refresh.
- [ ] Add focused pure-logic tests.
- [ ] Complete automated checks and the manual accessibility/responsive matrix.

## Acceptance criteria

- A guest cannot view any `/m` route; a signed-in user can reach it from every shell layout.
- Password and API keys are separate, directly addressable sections within one extensible account
  area.
- Changing a password submits only `new_password` with browser credentials, accepts `202`, stores a
  replacement refresh/CSRF cookie pair, and leaves the current user signed in.
- The key page accurately renders zero, one, or two keys with indices and expiries and never exposes
  existing key material.
- Minting with two keys cannot occur without a clear warning that key `#0` will be replaced.
- Newly minted `v1:<hex>` material is shown once, copied without logging or persistence, and paired
  with accurate instructions for the full Bearer credential.
- Either key can be revoked by its current index, and the displayed list is refreshed afterward.
- The account structure can add Profile or social-management sections without redesigning the
  shell, route prefix, or responsive section navigation.
