# UI Visual System: Step 5 Plan

## Summary

Bring the remaining routes and transient states into the visual system established
by Steps 1 through 4. This step will redesign the Popular page, sign-in page,
account-request page, add-link form, saved-link editor, empty states, loading
states, error fallbacks, and toast notifications.

The result should feel like one application: compact, plain surfaces; deliberate
blue links; clear type and spacing; restrained borders and radii; and small
del.icio.us cues without reproducing its old table-based layout. The work will
preserve existing API contracts, route behavior, saved-link operations, and
authentication flow.

The frontend currently advertises a `/u` sign-up route, but neither the router nor
the backend implements self-service registration. This step will make `/u` a
polished account-request page using the contact path already shown on Popular.
Implementing registration remains a later backend and product task.

## Current State

The Popular page is assembled from four independently loading regions. Its recent
links and popular tags still use early utility-class layouts, refresh icons are
click handlers rather than buttons, and errors appear as repeated “Ooops!”
labels. Instance statistics expose Raft details in one long sentence, while the
introductory copy performs a second request for the same statistics.

Sign in and Add Link use bordered grid forms with underline-only inputs. They do
not communicate pending submission, their layouts become cramped on narrow
screens, and their action names alternate among “Login,” “Add Post,” and “save.”
The inline saved-link editor retains the same form treatment because Step 3
deliberately left general forms for this step.

The shell links guests to `/u`, but no route or component handles it. The backend
defines `SignupReq` and `SignupRsp` data types but does not register a
`/users/signup` endpoint. The Popular introduction instead asks visitors to email
the administrator for an account.

Initial token refresh, the saved-link feed, the network feed, and the Popular
regions each render different loading, empty, and error markup. Async action
failures use Thaw toasts, but their titles and helper functions are duplicated.
Some empty-state actions use root-relative URLs instead of the configured
frontend base.

## Shared State Presentation

Add a small `components::feedback` module for state and notification presentation.
Keep resource ownership, retry triggers, and request code in their current route
modules; the shared components receive plain labels, details, actions, and
callbacks.

### Loading State

Create a `LoadingState` component with a Thaw spinner and a short visible label.
Render it with `role="status"` and a polite live region so progress has a textual
equivalent. Support compact panel loading and a full-page startup variant through
an explicit enum rather than concatenated class names.

Use contextual labels:

- “Loading recent links…” and “Loading popular tags…” on Popular;
- “Loading instance details…” for statistics;
- “Loading saved links…” in the personal link panel;
- “Loading your network…” in the federated panel; and
- “Loading indielinks…” while the application attempts token refresh.

The startup variant will include the compact indielinks mark and occupy the
available viewport without flashing an unstyled paragraph. It will not describe
token mechanics to the user.

### Error State

Create an inline `ErrorState` component for failed resource loads. It will contain:

- `role="alert"` on the message region;
- a concise context-specific heading such as “Recent links could not be loaded”;
- a short recovery instruction;
- a native Retry button when the owning resource has an `ArcTrigger`; and
- a collapsed `<details>` disclosure containing the underlying error strings.

The primary message must remain useful without opening the technical details.
The disclosure preserves the diagnostic information currently available through
`InfoLabel`. Retry must notify only that region's trigger and communicate loading
through the replacement `LoadingState`.

Statistics and introductory copy will share one statistics resource, so a single
instance-details error replaces both duplicate failures. Recent links and popular
tags retain independent resources and failures, allowing the other Popular panel
to remain usable.

### Empty State

Create an `EmptyState` component with an optional decorative icon, heading, body,
and either a route link or native callback button. It will use a quiet inset area
rather than a new card inside an existing panel. Every state will state what is
empty and offer an action that can resolve it:

- no saved links: “Save your first link” to the base-aware Add Link route;
- no saved links matching filters: “No saved links match these filters,” with a
  “Clear filters” route action;
- no network posts: explain that following Fediverse accounts populates the
  timeline and retain a “Check again” button;
- no public links or popular tags: offer Add Link to signed-in users and Sign In
  to guests; and
- no conversation posts, if the server returns an empty context: keep the
  conversation header and provide Back rather than leaving a blank region.

Do not use an empty state while a request is unresolved or failed. Derive all
internal destinations from `Base` or `Paths`; no new root-relative route URLs will
be added.

## Shared Form Treatment

Add reusable form classes under `@layer components` rather than a general-purpose
Rust form builder. Sign in, Add Link, and the saved-link editor have different
state and payloads, and their native HTML should remain visible in their owning
components.

The shared treatment will provide:

- a centered route heading, short introductory copy, and constrained form panel;
- vertically stacked labels and controls at all widths;
- persistent labels above inputs rather than placeholders as labels;
- optional help text connected with `aria-describedby`;
- full-width bordered inputs and textareas using Step 1 tokens;
- a visible focus ring, error border, disabled surface, and inherited typography;
- a `fieldset` and `legend` for related checkbox options;
- checkbox labels whose whole text is clickable;
- a responsive action row with primary and secondary native buttons; and
- pending button labels and `aria-busy` without changing the form dimensions.

Desktop forms will remain compact instead of using a two-column label grid.
Labels, help, and validation stay adjacent to the control they describe. At
mobile widths, action buttons may fill the available width; text fields and
checkboxes must not create horizontal overflow.

Continue using native `required` validation for empty required fields. Use
`type="url"` for saved-link URLs, `autocomplete="username"` and
`autocomplete="current-password"` for sign-in credentials, and suitable
`inputmode` and autocomplete attributes where the field semantics are known.
Server and parsing failures remain textual and return focus to a relevant field
when the client can identify one.

## Sign-In Page

Present Sign In as a focused authentication page inside the existing shell:

- a visible “Sign in” `<h1>`;
- one sentence explaining that an account is required for a personal collection;
- labelled username and password controls;
- a primary “Sign in” submit button; and
- a secondary sentence linking to the account-request route.

Retain autofocus on username. Disable the submit button while the login action is
pending, set `aria-busy`, and change its label to “Signing in…”. Prevent duplicate
dispatch while pending. On success, preserve token storage and navigation to the
base-aware Home route.

On authentication or network failure, keep the user on the form, preserve both
entered values, return focus to username, and show the standardized error toast
titled “Sign in.” Password errors will not echo the password or distinguish
whether the username exists.

## Account-Request Page

Add a public `SignUp` component and register it at `/u`, matching the destination
already emitted by `Paths`. The page will explain that this instance currently
creates accounts by request and provide a clear `mailto:sp1ff@pobox.com` “Request
an account” action plus a secondary Sign In link.

Use the same authentication-page frame as Sign In, with a visible “Request an
account” heading and the small del.icio.us-inspired brand accent. Do not render a
registration form, collect account details, call the unused shared request types,
or imply that an account is created automatically. The copy and contact address
will match the current Popular introduction so the two routes do not contradict
one another.

This resolves the broken shell destination while accurately representing current
server capability. A later self-service registration feature will need a backend
route, validation and availability rules, abuse controls, email behavior, and a
separate implementation plan.

## Add-Link Page

Present Add Link as the primary creation task:

- a visible “Add link” heading and concise instruction;
- URL first, with `type="url"`, autofocus, and an example-free URL hint;
- Title, Notes, and Tags using the shared controls;
- Tags help text that explains the current comma-delimited format;
- a “Link options” fieldset for Private and Unread;
- an “After saving” fieldset or clearly separated checkbox for Add another; and
- a primary “Save link” native submit button.

Use sentence case consistently. Explain Private as limiting the link to the
owner and Unread as adding it to the reading queue. Preserve query-string
prefill, form conversion, current API request, the `replace` behavior, and
location-state return navigation.

Disable submission while pending and show “Saving…”. If Add another is checked,
retain its value, clear the other fields, return focus to URL, and show a brief
success toast titled “Link saved” so completion is apparent while remaining on
the page. Otherwise preserve the current navigation to the originating view or
Home. Parsing and submission failures use the standardized “Add link” error toast
and existing field-specific focus behavior.

## Saved-Link Editor

Apply the same controls, labels, help text, option fieldsets, and action buttons to
the Step 3 inline editor without making it resemble a separate page. Keep the
editor inside its saved-link row with the existing “edit saved link” heading and
brand leading rule.

Use unique control IDs derived from stable row data so labels remain correct even
though multiple rows may mount over the page's lifetime. Change the primary action
to “Save changes” and the secondary action to “Cancel.” Disable both as
appropriate while saving, expose pending state, and prevent duplicate dispatch.

Preserve the current payload, field-specific failure focus, cancel behavior, and
single-row editing constraint. Rename its error toast to “Edit link.” After a
successful save, refresh the saved-link resource before closing the editor so the
visible row reflects the saved values; keep focus near the edited row or its Edit
button when practical.

## Popular Page

### Page Introduction

Make “Popular” a visible page heading followed by compact lead copy identifying
the instance as “del.icio.us on the Fediverse.” Display the origin from cluster
statistics and keep the account contact link available without making the entire
introduction dependent on a second request.

Fetch cluster statistics once in `Instance` and use the result for both instance
identity and statistics. Keep recent links and tag loading independent. The page
will retain the Step 2 responsive two-panel grid below the introduction.

### Recent Public Links

Give the panel a real “Recent public links” heading and place a native labelled
Refresh button in the panel header. Render results as a semantic list of compact,
read-only bookmark rows informed by Step 3:

- title as the primary external link;
- host derived from the saved URL as quiet provenance;
- publication time in a `<time>` element;
- notes when present; and
- tags using the same visual tag language as saved links, without implying a
  filter destination that the public route does not support.

Adjacent rows use single dividers instead of independent boxes. Do not expose
private/unread controls or owner actions. Preserve API order, page size, and
same-tab external navigation.

### Popular Tags

Give the panel a “Popular tags” heading with the same native Refresh treatment.
Render a semantic ranked list. Each row will display the tag name first and its
score as muted supporting data with an accessible label such as “score 1.25.”
Use alignment and tabular numerals for scanning, but do not turn a tag into a link
until the application has a meaningful public tag destination.

The list should evoke del.icio.us's dense tag vocabulary through blue tag text and
compact spacing, while the ranking and score remain legible. It will not become a
decorative tag cloud whose font sizes imply unbounded values.

### Instance Statistics

Replace the long operational sentence with a quiet “About this instance” section.
Show user and saved-link counts as two compact labelled metrics. Put Raft
initialization, leader, and term values in a collapsed “Service details”
disclosure so the existing information remains available without dominating the
public page.

Use plain labels and values rather than icons alone. Handle every combination of
optional initialization and leader data without impossible-state assumptions.
Statistics refresh together with the shared instance-details resource.

## Toast Notifications

Move duplicated Thaw toast construction into `components::feedback` with a total
helper accepting intent, short title, and body. Keep request-specific decisions in
the caller. Standardize titles around the user action:

- “Sign in,” “Add link,” “Edit link,” and “Delete link” for saved-link work;
- “Reading status” for read/unread failures;
- “New posts” and “Older posts” for timeline update failures; and
- “Favorite,” “Reply,” and “Conversation” for federated-post failures.

Use error intent for failed work and success intent only when completion would
otherwise have no visible result, initially Add another. Titles use sentence case
and bodies retain the useful server or client error text. Do not place raw error
details in the title.

Style the Thaw toast viewport and toast surface with the visual-system tokens after
checking the generated DOM in the browser. Toasts will use restrained elevation,
the shared radius, a narrow intent accent, readable wrapping, and viewport margins
that clear the desktop rail and mobile navigation. Do not replace Thaw's live
region, dismissal, or lifetime behavior with a custom notification system.

## Component and Style Changes

Add `src/components/feedback.rs` and export it from `components/mod.rs`. Keep
`LoadingState`, `ErrorState`, `EmptyState`, and the toast helper narrowly typed;
prefer explicit enums for variants and optional action data over a component that
accepts arbitrary class strings.

Add `src/signup.rs` and export it from `lib.rs`. Register `/u` in `main.rs` and use
base-aware links in both authentication routes. Keep route ownership and token
refresh in `main.rs`.

Refactor `instance.rs` into private page sections for the shared introduction and
statistics resource, recent-link panel, public-link row, tag panel, and ranked tag
row. Remove `InfoLabel` and icon click handlers there. Resource loaders and API
types remain in the module.

Keep add-link form state and requests in `add-link.rs`, sign-in state and requests
in `signin.rs`, saved-link editing in `home.rs`, and network loading in `feeds.rs`.
Replace only their repeated feedback markup and obsolete utility-heavy form
markup. Update `post.rs` to use the shared toast helper without changing the Step
4 post design.

Add form, authentication-page, state, Popular-list, statistics, and toast rules to
`style.css` under `@layer components`. Use only Step 1 variables for colors,
strokes, focus, surfaces, shadows, spacing, and radii. Regenerate `tailwind.css`
after markup and CSS changes. No new dependency or raw color is required.

## Behavior to Preserve

- Token refresh still completes before the router is mounted, and failure still
  leaves the visitor signed out.
- Sign in uses the current cookie credentials, token context, and Home navigation.
- Add Link preserves query prefill, URL and title validation, serialized request,
  Private, Unread, Add another, return location, and focus recovery.
- Saved-link editing preserves one active editor, the current add/replace request,
  Cancel, and field-specific error focus.
- Popular uses the current recent-posts, top-k-tags, and cluster-stats endpoints
  with their existing limits and ordering.
- Saved-link filters, pagination, read state, and actions continue to work.
- Network initial load and manual reload retain current timeline behavior.
- Federated favorite, reply, and conversation requests retain Step 4 behavior.
- Thaw remains the toast provider and all existing failures remain visible.
- The application continues to support an empty `INDIELINKS_BASE` and a non-empty
  mounted base path.

## Implementation Sequence

1. Add shared loading, error, empty, and toast presentation in
   `components::feedback`, then replace the root token-refresh fallback.
2. Add the common form and authentication-page CSS, redesign Sign In, and add the
   honest `/u` account-request route.
3. Redesign Add Link and the inline saved-link editor, including pending controls,
   labels, help, fieldsets, focus behavior, and standardized feedback.
4. Consolidate Popular's statistics request, rebuild its introduction and metrics,
   and convert recent links and tags to semantic panel lists with native refresh
   controls.
5. Replace saved-link, network, Popular, and conversation empty, loading, and
   resource-error markup with contextual shared states and base-aware actions.
6. Route all action notifications through the shared toast helper, normalize
   titles, and style the Thaw surfaces in desktop and mobile viewports.
7. Regenerate Tailwind CSS, build the WASM target, run frontend linters, and
   exercise every route and state in the browser.

## Scope Boundaries

This step does not:

- implement self-service registration or add a backend signup endpoint;
- change authentication, refresh-cookie, or authorization behavior;
- add account recovery, password visibility controls, password managers, or
  social login;
- change saved-link, timeline, favorite, reply, conversation, statistics, or tag
  API contracts;
- add a public tag-results route, tag cloud, search, discovery, or user directory;
- add new public-link actions, previews, ownership data, or pagination;
- hide operational statistics that are currently visible, although it moves them
  into a disclosure;
- replace Thaw's toast provider or introduce another component library;
- revise the Step 1 token definitions, Step 2 shell structure, Step 3 saved-link
  display, or Step 4 federated-post anatomy beyond integrating shared forms and
  feedback; or
- perform the exhaustive cross-browser and accessibility audit assigned to Step
  6.

## Validation and Acceptance Criteria

- Popular has a visible heading, concise instance introduction, semantic recent-
  link and popular-tag lists, accessible refresh controls, and readable instance
  metrics.
- Cluster statistics are fetched once per load/refresh and all current values
  remain available, with Raft details visually secondary.
- Sign In has associated labels, correct autocomplete tokens, pending behavior,
  duplicate-submit protection, failure feedback, and unchanged successful
  navigation.
- `/u` resolves under empty and non-empty base paths, accurately describes
  account requests, and offers working email and Sign In actions.
- Add Link preserves every query-prefilled value and option, dispatches once,
  reports pending and failure states, and follows existing post-save navigation.
- Add another clears the saved fields, keeps the option selected, focuses URL,
  and announces successful completion.
- The inline editor uses unique label/control associations, reflects a successful
  save in the row, and preserves Cancel and error focus.
- Every resource region has mutually exclusive loading, loaded, empty, and error
  presentation. Errors include a useful message, optional details, and a working
  contextual Retry action.
- Empty saved-link filters can be cleared, empty network feeds can be retried, and
  no empty action loses the configured frontend base.
- Toasts use consistent action titles, readable bodies, correct intent, and fit
  above mobile navigation without obscuring primary controls.
- Keyboard users can reach every form control, disclosure, retry action, refresh
  action, and toast dismissal in a logical order with visible focus.
- At `320px`, `390px`, `768px`, `1024px`, and `1440px`, forms, Popular lists,
  state panels, details disclosures, and toasts have no horizontal overflow,
  clipping, or overlap with shell navigation.
- Loading and status text is programmatically exposed without announcing ordinary
  content changes assertively; error text is available without relying on color.
- Signed-out Popular, Sign In, account request, signed-in Popular, Add Link, saved-
  link editing, empty feeds, failed requests, and token refresh produce no new
  browser console errors.
- Tailwind CSS regenerates successfully with
  `npx @tailwindcss/cli -i style.css -o tailwind.css` from `indielinks-fe`.
- `cargo check -p indielinks-fe --target wasm32-unknown-unknown`,
  `cargo fmt --all -- --check`, and `admin/run-linters` pass from the workspace
  root. The integration suite is not run for this frontend-only change.

## Sources

- [UI visual-system overview](ui-visual-system-overview.md)
- [Step 1 visual-system plan](ui-visual-system-step-1.md)
- [Step 2 application-shell plan](ui-visual-system-step-2.md)
- [Step 3 saved-link plan](ui-visual-system-step-3.md)
- [Step 4 federated-post plan](ui-visual-system-step-4.md)
- Current route and token-refresh implementation in `src/main.rs`
- Current shell destinations in `src/components/shell.rs`
- Current Popular implementation in `src/instance.rs`
- Current forms in `src/signin.rs`, `src/add-link.rs`, and `src/home.rs`
- Current feed states in `src/home.rs` and `src/feeds.rs`
- Shared signup types in `../indielinks-shared/src/api.rs` and the current user
  router in `../indielinks/src/users.rs`
- [WAI Forms Tutorial](https://www.w3.org/WAI/tutorials/forms/)
- [WAI form notifications](https://www.w3.org/WAI/tutorials/forms/notifications/)
- [WAI form validation](https://www.w3.org/WAI/tutorials/forms/validation/)
- [WCAG 2.2 error identification](https://www.w3.org/WAI/WCAG22/Understanding/error-identification.html)
- [WCAG status messages](https://www.w3.org/WAI/WCAG22/Understanding/status-messages.html)
- [WHATWG autofill field names](https://html.spec.whatwg.org/multipage/form-control-infrastructure.html#autofill-field)
- [WHATWG input types](https://html.spec.whatwg.org/multipage/input.html)
