# Following and Followers Plan

Status: **planned**. This document defines the frontend feature and the small
shared/server contract change needed to accept account handles. It does not
authorize implementation.

## Purpose

Add two protected account sections:

- `/m/following` lists every actor followed by the signed-in user and provides
  a form for creating a new follow.
- `/m/followers` lists every actor following the signed-in user.

The UI follows the existing account layout and the visual-system direction in
[`ui-visual-system-overview.md`](ui-visual-system-overview.md): compact,
responsive account navigation; clear loading, empty, error, and success
states; semantic controls; keyboard operation; and visible focus states.

Although the pages are implemented in `indielinks-fe`, accepting a typed
`user@host` handle requires a bounded API change. The public follow request
will accept either an ActivityPub URL or an RFC 7565 account, and the server's
background task will resolve an account through WebFinger before sending the
ActivityPub `Follow` activity.

## Decisions and contracts

### Routes and access

- Add protected routes `/m/following` and `/m/followers`.
- Add `Following` and `Followers` immediately after `Profile` in the existing
  account section navigation. They use the same responsive column/tab layout,
  `aria-current="page"`, and account page frame as the existing sections.
- `/m` continues to redirect to `/m/profile`; neither new page becomes the
  account landing page.
- The pages obtain the signed-in username from the existing authenticated
  `GET /api/v1/users/profile` response. Do not derive it by parsing a bearer
  token or make users type it.

### Collection reads

The existing ActivityPub endpoints remain the read contract:

```http
GET /users/{username}/following
GET /users/{username}/followers
```

Each initial response is an `OrderedCollection` containing `id`,
`totalItems`, and an absolute `first` URL. Each page is a
`CollectionPage` with `orderedItems: [<actor URL>, ...]` and an optional
absolute `next` URL. The frontend must:

1. Request the collection endpoint using the configured API base and URL-encode
   the username as a path segment.
2. Request `first`, then follow `next` until it is absent, concatenating every
   `orderedItems` array in response order.
3. Treat an absent `first` or `orderedItems` on a non-empty collection as a
   malformed response and show the normal error state; an empty collection is
   represented by `totalItems: 0` and displays the appropriate empty state.
4. Use the existing authenticated request/retry helper for every request. It
   supplies the bearer token and preserves the one refresh-and-retry behavior.

The local, deserialize-only collection model should use
`#[serde(rename_all = "camelCase")]`, model `first`, `next`, and
`ordered_items` as `Option`s, and tolerate JSON-LD fields such as `@context`
and `type`. Keep the model inside `indielinks-fe`; it is a representation of
the ActivityPub collection document rather than a new application API.

### Follow request

Rename `indielinks_shared::api::FollowReq` to `FollowRequest` and retain its
JSON object shape with an `id` member. Change that member to a sum type:

```rust
pub struct FollowRequest {
    pub id: FollowTarget,
}

pub enum FollowTarget {
    ActivityPubId(Url),
    Account(Account),
}
```

Use Serde's default externally tagged representation. The frontend therefore
sends one of:

```json
{"id":{"ActivityPubId":"https://indieweb.social/users/sp1ff"}}
```

```json
{"id":{"Account":"sp1ff@indieweb.social"}}
```

`POST /api/v1/users/follow` remains authenticated and returns `202 Accepted`
once it queues the background task. It does not mean the remote actor has
accepted the follow. The UI must say that the request was sent and that the
actor appears in Following after the remote server accepts it; it must not
optimistically insert the target in the list.

### Account type placement and WebFinger resolution

`FollowRequest` is intentionally in `indielinks-shared`, but the current
`Account` refined type is in `indielinks`. Referencing it directly would make
the shared crate depend upward on the server crate. Move `acct.rs` to
`indielinks-shared` (preserving its parsing, serialization, and RFC 7565
semantics), update all server imports to use `indielinks_shared::acct`, and
have `indielinks::acct` re-export the shared types if preserving that public
path is desirable. This leaves `FollowTarget::Account` correctly typed without
creating a dependency cycle.

Rename `SendFollow.actorid` to `target`, give it the same `FollowTarget` type,
and resolve it at the start of `SendFollow::exec`:

- `ActivityPubId(url)` retains the current actor-document fetch and canonical
  actor-ID handling.
- `Account(account)` calls the existing `ApResolver::handle_to_actor` path and
  uses the returned actor's ID for the remaining inbox lookup, storage write,
  and outbound Follow.

Correct `ApResolver::handle_to_actor` while making this path live: construct
the WebFinger URL with `Url::query_pairs_mut().append_pair("resource",
&format!("acct:{account}"))`, rather than interpolating the account into a
URL string. This produces an HTTPS GET to
`https://{host}/.well-known/webfinger` with the required query value encoded,
including the `acct:` prefix and any percent-encoded account local part. Keep
the existing handle cache, local-instance resolution, signed ActivityPub
client, and error context; do not add a browser-side WebFinger request.

## Frontend behavior

### Following section

Render a `Following` heading and short explanation, followed by a labelled
form before the list:

- A single text input labelled `ActivityPub address`, with an example such as
  `sp1ff@indieweb.social` or `https://indieweb.social/users/sp1ff`.
- A primary `Follow` submit button.
- On submit, trim the input. Parse an HTTP or HTTPS URL as
  `FollowTarget::ActivityPubId`; otherwise parse it as `Account` and produce
  `FollowTarget::Account`. Empty and unparseable values produce an associated
  inline error and retain focus in the input. Do not implement ad-hoc handle
  splitting in the component; use the shared refined type.
- Disable repeated submission and show `Following…` while the request is
  pending. On `202`, clear the input and display an accessible success toast
  explaining that the follow request was sent and may take time to appear.
  On failure, preserve the input and show the existing error-toast pattern.
- Render loaded actor identifiers as a simple semantic list. Each row shows
  the complete ActivityPub URL as an external link, with the URL also serving
  as its accessible name. Use `target="_blank"` with `rel="noreferrer"`.
- While the initial collection and its pages load, use `LoadingState`. When it
  is empty, say that the user is not following anyone yet; keep the form
  available. On a read failure, render the standard error state with Retry.
  A successful POST reloads the collection once, while preserving the above
  asynchronous-acceptance message if the target is not present yet.

### Followers section

Render the same collection presentation without the form or mutation controls:

- Heading `Followers` and a brief explanation.
- Loading, error with Retry, empty state (`No one follows you yet.`), and a
  semantic list of external actor-URL links when populated.
- Read only `/users/{username}/followers`; it never calls the follow endpoint.

The shared list component may accept a title, empty copy, and collection kind,
but keep the Following form in the Following module so Followers stays
read-only and simple.

## File-level implementation plan

- [x] Move `indielinks/src/acct.rs` to `indielinks-shared/src/acct.rs`, register
   it in the shared crate, and update dependencies/imports. Preserve the
   copyright header, module documentation, refined-type tests, and wire
   serialization. If compatibility requires `indielinks::acct`, make it a
   documented re-export module rather than duplicate the types.
- [x] Update `indielinks-shared/src/api.rs`: add `FollowTarget`, rename
   `FollowReq` to `FollowRequest`, and use the new target in `id`. Update all
   callers and tests in lockstep.
- [x] Update `indielinks/src/users.rs` so `/users/follow` deserializes
   `FollowRequest` and queues `SendFollow` with its target. Update error
   context fields and log messages to call the value `target`, not `actorid`,
   before it has been resolved.
- [x] Update `indielinks/src/app-logic.rs`: change `SendFollow`'s stored field
   and constructor, resolve account targets through `ApResolver`, and retain
   the existing canonical actor fetch, inbox resolution, persistence, Follow
   delivery, timeline invalidation, timeout, and failure reporting.
- [x] Update `indielinks/src/ap-resolution.rs` to build the WebFinger query with
   the URL query-pair API. Keep resolution behavior centralized there so every
   handle lookup has correct encoding and cache behavior.
- [x] Add `account_following` and `account_followers` paths and accessors to
   `indielinks-fe/src/components/shell.rs`; include both destinations in page
   title mapping where account subpage titles are mapped.
- [x] Extend `indielinks-fe/src/account/page.rs` with `Following` and `Followers`
   section definitions and route wrapper components. Extend
   `indielinks-fe/src/account/mod.rs` with browser-only child modules.
- [x] Add `indielinks-fe/src/account/following.rs` for target parsing, the POST
   action, and the Following section. Add `followers.rs` for the follower page
   and a small browser-only shared collection loader/presentation module if
   doing so avoids duplicating pagination and feedback code.
- [x] Register both protected routes in `indielinks-fe/src/main.rs`. Reuse the
   existing protected-route condition and redirect behavior.
- [x] Add focused styles to `indielinks-fe/style.css` for the compact follow
   form and dense actor rows, built from existing theme tokens and account
   breakpoints. Do not introduce page-specific palette literals.

## Validation and acceptance criteria

### Rust and API tests

- [x] Add shared API serialization/deserialization tests for both `FollowTarget`
  variants and the renamed `FollowRequest` JSON documents.
- [x] Retain and adapt `Account` parsing/serialization tests after its move.
- [x] Add resolver tests using a mock WebFinger response that assert the requested
  HTTPS path is `/.well-known/webfinger`, the `resource` query decodes to
  `acct:user@host`, and the returned ActivityPub self link becomes the actor
  ID. Include an account local part that requires percent encoding.
- [x] Extend the follow integration test to submit each target form: an ActivityPub
  URL remains accepted, and a handle causes WebFinger resolution before the
  outbound Follow is sent and later appears in the Following collection after
  acceptance.
- [x] Run targeted relevant tests first, then `cargo test` for changed server and
  shared crates. Run `cargo check -p indielinks-fe --target
  wasm32-unknown-unknown` and `admin/run-linters` before frontend signoff.

### Frontend checks

- [x] Verify signed-out navigation to either route redirects through the existing
  protected-route behavior.
- [x] Verify URL and handle submissions serialize the two exact request variants;
  verify invalid/blank input, pending state, `202`, request failure, and one
  refresh-and-retry path.
- [x] Verify zero, one-page, and multi-page Following/Followers collections;
  verify Retry reloads a failed collection.
- [x] Inspect desktop and narrow layouts, tab/focus order, Enter submission,
  screen-reader labels/statuses, external-link behavior, color contrast, and
  light/dark themes.
- [x] Regenerate Tailwind and make a Trunk production build if CSS changes; inspect
  the generated HTML and hashed stylesheet for the new selectors and shared
  theme tokens.

## Sources

- Existing account frame and navigation:
  [`indielinks-fe/src/account/page.rs`](../../indielinks-fe/src/account/page.rs).
- Authenticated request and refresh behavior:
  [`indielinks-fe/src/http.rs`](../../indielinks-fe/src/http.rs).
- Current follow request and endpoint:
  [`indielinks-shared/src/api.rs`](../../indielinks-shared/src/api.rs) and
  [`indielinks/src/users.rs`](../../indielinks/src/users.rs).
- Collection pagination contract:
  [`indielinks/src/actor.rs`](../../indielinks/src/actor.rs).
- Existing account-aware WebFinger resolver:
  [`indielinks/src/ap-resolution.rs`](../../indielinks/src/ap-resolution.rs).
- Account grammar and nested query-encoding example:
  [`indielinks/src/acct.rs`](../../indielinks/src/acct.rs).
