# UI Visual System: Step 3 Plan

## Summary

Redesign the saved-link feed as a dense, structured reading list. Each saved
link will present its title first, then origin and saved-time metadata, optional
notes, status, tags, and actions in a stable hierarchy. The treatment will keep
the plain typography, strong blue links, compact tags, and information density
associated with del.icio.us while using the spacing, responsive behavior, and
accessible controls expected of a modern application.

This step is confined to the saved-link column on Home. It preserves the
existing API requests, URL-backed filtering and pagination, inline editing,
authentication, and shell. Federated posts remain unchanged for Step 4, while
general form, empty, loading, error, and toast styling remains assigned to Step
5.

## Current State

`LinkFeed` currently renders every saved link as a separately bordered box.
Within each box, the title, timestamp, and tags have little visual separation,
saved notes are not displayed, and unread or private state is only indirectly
visible through action text. Four low-contrast text actions sit on one line;
the “conversation” control has no event handler. Pagination uses clickable icon
components rather than native links or buttons, and the same complete control
row is repeated above and below the list.

The redesign will retain the useful compactness while making each row easier to
scan and every interactive element understandable by mouse, touch, keyboard,
and assistive technology.

## Saved-Link Anatomy

Render the page as a semantic ordered list because the API returns links in a
defined feed order. Each list item contains one `<article>` with this visual
order:

1. A linked title, using the Step 1 `16px/22px` link-title style and semibold
   weight.
2. A compact metadata line containing the destination host, saved timestamp,
   and applicable private status.
3. The saved notes when present.
4. A footer containing tags followed by the row actions.

Use the destination host as a quick origin cue, derived from the already
validated `StorUrl`. A total helper will return the host when the URL has one
and fall back to its scheme or full display value for URLs without a host. The
title remains the only outbound link and keeps the existing same-tab behavior.
Long titles, hosts, notes, and URLs must wrap without widening the Home grid.

Render the saved timestamp with a semantic `<time datetime="...">` element.
Keep a concise, unambiguous UTC display (`YYYY-MM-DD HH:MM UTC`) so this step
does not add locale or relative-time machinery. The machine-readable value will
use RFC 3339.

Display notes as secondary body text below the metadata, preserving user-entered
line breaks with `white-space: pre-wrap`. Do not truncate or hide notes behind
an expansion control; the feed remains dense through spacing and typography,
not by withholding saved content.

## Row Surfaces and Density

The existing Step 2 “saved links” panel remains the outer surface. Remove the
margin and complete border from each individual link. Use a flat list with a
one-pixel divider between adjacent rows, `12px` vertical padding, and `16px`
horizontal padding. This creates a del.icio.us-like stream of compact entries
without a stack of cards.

Do not make the whole row clickable or add a row hover background, since that
would imply an action the row does not perform. Hover and focus treatment will
remain attached to the actual title, tag links, and buttons.

At widths below `480px`, the footer will stack tags above actions. At wider
widths it will use a wrapping flex row: tags occupy the available left side and
actions align to the right. No metadata or action is hidden at any breakpoint.

## Unread and Privacy State

Unread links receive a three-pixel brand-blue leading edge and the Step 1
subtle brand surface. They also display a small “unread” badge so the state is
not conveyed by color alone. Read links use the ordinary white panel surface.
The distinction is intentionally stronger than a font-weight change because a
page filtered to all links must remain quickly scannable.

Private links display a compact lock icon and the visible word “private” in the
metadata line. Public links receive no badge, reducing repeated information in
the common case. Use Feather `FiLock`; mark the icon decorative because the
adjacent text provides the accessible label.

Changing read state re-renders the row from the server response as it does
today. The visual modifier and badge must therefore follow `Post::unread()`
directly rather than keeping a second client-side copy of the state.

## Tags

Render tags as a semantic list of compact links on the subtle neutral surface,
with a one-pixel border, `4px` radius, metadata typography, and brand-colored
text. Keep the tag text plain rather than adding a `#`; this is a visual cue to
del.icio.us and avoids changing the stored tag's apparent value.

`Post` stores tags in a `HashSet`, so sort them alphabetically before rendering
to give rows, keyboard order, and screenshots a deterministic result. Generate
tag destinations through `QueryParams` and `serde_urlencoded` rather than
concatenating query text. Selecting a tag will retain the current unread filter,
set the selected tag, and return to the first page, matching the existing user
behavior. Continue deriving the Home path from `Base` for prefixed deployments.

When a link has no tags, omit the tag list entirely; do not render an empty
placeholder.

## Actions

Replace the current loose text controls with one compact action group in the
row footer. Keep the three working actions visible:

- “mark read” or “mark unread,” using `FiBookOpen`;
- “edit,” using `FiEdit2`;
- “delete,” using `FiTrash2` and the theme's danger foreground token.

Use native `<button type="button">` elements. Each button retains visible text
beside its decorative icon and receives the Step 1 focus ring. The action group
uses `role="group"` with an accessible label that includes the saved-link title.
On coarse pointers each button must provide at least a `44px` target; compact
desktop buttons may use their natural height only when spacing satisfies WCAG
2.2 target-size requirements.

The read action keeps its changing visible label rather than adding
`aria-pressed`; WAI's button guidance recommends a stable label when
`aria-pressed` represents a toggle. Edit continues to swap only this row into
the existing inline edit form. Delete continues to dispatch the existing
request and toast behavior; confirmation behavior is outside this visual step.

Remove the rendered “conversation” control in this step. It currently has no
handler, route, or content, so presenting it as an enabled button is misleading.
This does not remove conversation data or behavior because none exists for
saved links today. A working conversation affordance can be introduced with
its actual destination and state in a later feature.

Do not use the existing custom dropdown to hide row actions. Its module
documentation identifies it as inaccessible, and three visible actions fit the
approved responsive layout without requiring a second interaction.

## Feed Controls

Replace `BackButton`, `ToggleButton`, `ForwardButton`, and `Nav` with semantic
feed controls while retaining the same `QueryParams` calculations.

The top control row will contain:

- one filter link labelled “show unread” or “show all,” according to the next
  action it performs;
- a pager labelled “Saved links pages” with Previous, a human-facing
  one-based page number, and Next.

The bottom of a non-empty list repeats only the pager so a user who reads the
page does not need to return to the top. A filtered empty list keeps the top
controls and omits the redundant bottom pager.

Previous and Next will be ordinary links when destinations exist. At the first
or last page, render a noninteractive disabled counterpart with matching
geometry and `aria-disabled="true"`. Use Feather chevrons as decorative icons
and visible Previous/Next text where space permits; the text may be visually
hidden below `480px` while remaining in the accessible name. All links will be
constructed from the existing serialized query state so tags and the unread
filter survive paging.

This replaces click handlers attached directly to icon SVGs with controls that
participate naturally in tab order and support link browser behavior.

## Inline Editing

Rename saved-link UI symbols from “post” to “link” where they are private to
`home.rs`, including `ViewPost` and `EditPost`. The backend and shared entity
remain `Post`; this is a presentation-layer terminology cleanup consistent with
the module's existing distinction between saved links and ActivityPub items.

An editing row retains the existing form fields, validation, submit request,
cancel behavior, and toast behavior. Place it inside the same list-item surface
with a visible “edit saved link” heading and enough padding to align with view
rows. Only responsive containment and row integration change here. Step 5 will
apply the final shared form treatment to this form and the Add Link screen.

Continue allowing at most one editing row. Saving or cancelling returns that
row to its reading presentation; no modal or route is introduced.

## Component and Style Changes

Keep query parsing, resources, requests, actions, and saved-link components in
`src/home.rs`; their state and private types are tightly coupled, and extracting
a presentation module would require a public callback API without current
reuse. Factor the markup into small private components so the main list remains
readable:

- `FeedControls` for the filter and top pager;
- `Pager` for Previous, page status, and Next;
- `SavedLink` for the reading presentation;
- `LinkMetadata` for origin, time, unread, and private state;
- `LinkTags` for deterministic tag links;
- `LinkActions` for read, edit, and delete commands;
- `EditLink` for the existing inline editing state.

Change `Links` to render the semantic ordered list and list items, while
retaining ownership of the single editing signal. Make `DeleteParams` private
because it has no users outside `home.rs`.

Add saved-link classes under `@layer components` in `style.css`. Use Step 1
semantic Thaw variables for all surfaces, borders, foregrounds, brand state,
focus, radius, and danger color. Keep repeated row geometry and responsive
rules in CSS rather than assembling long conditional utility-class strings in
Rust. No new dependency or raw color value is required.

## Behavior to Preserve

- Loading and failures continue through the current `Transition` and
  `ErrorBoundary`.
- An account with no saved links retains the existing Add Link invitation.
- Tag, unread, and page state remain represented in the URL.
- Previous and Next preserve active tag and unread parameters.
- A tag selection preserves the unread filter and resets pagination.
- Mark Read/Unread, Edit, Save, Cancel, and Delete use the existing endpoints
  and trigger the existing feed refresh or toast.
- Link titles still open their stored URL in the current browsing context.
- `INDIELINKS_BASE` continues to apply to Home, tag, and Add Link destinations.

## Implementation Sequence

1. Replace the icon click handlers with `FeedControls` and `Pager`, reusing the
   typed query transformations and generating base-aware link destinations.
2. Rename the private view/edit components to saved-link terminology and split
   the reading row into metadata, tags, and action components.
3. Render the feed as an ordered list with flat divided rows and integrate the
   existing inline editor into the same row structure.
4. Add host and timestamp helpers, notes, sorted tag links, unread treatment,
   and the private badge.
5. Replace the four current controls with the three working native-button
   actions and remove the inert conversation control.
6. Add responsive saved-link and feed-control styles using the Step 1 tokens.
7. Regenerate Tailwind CSS, build the WASM target, run the frontend linters,
   and exercise saved-link states and actions in the browser.

## Scope Boundaries

This step does not:

- change saved-link API contracts, persistence, sorting, or page size;
- implement a saved-link conversation view;
- add bulk selection, search, new filters, drag-and-drop, or tag editing;
- add relative or localized time;
- add deletion confirmation or optimistic mutation;
- redesign the inline form fields, validation messages, toasts, loading
  spinner, error fallback, or no-links invitation;
- redesign the federated network feed or its post actions;
- change the application shell, routes, or authentication.

## Validation and Acceptance Criteria

- Every saved link has one clear title, a host cue, a semantic UTC timestamp,
  optional notes, deterministic tags, visible applicable status, and one
  action group.
- Unread rows are distinguishable without color; private rows visibly identify
  their privacy state.
- Notes preserve line breaks, and long titles, metadata, notes, and tags wrap
  without horizontal overflow.
- Tags render in alphabetical order and navigate to an encoded, base-aware Home
  URL that preserves the unread filter and resets the page.
- Previous and Next preserve all active query parameters, use native link
  semantics, and become noninteractive at their respective boundaries.
- The visible page number is one-based while the API offset and query behavior
  remain unchanged.
- Mark Read/Unread refreshes the row, Edit enters the existing inline form,
  Save and Cancel return to reading mode, and Delete retains its existing
  success and error behavior.
- The saved-link feed exposes no enabled control without behavior.
- Keyboard users can reach the filter, pager, title, every tag, and all row
  actions in visual order with a visible focus indicator.
- At `320px`, `390px`, `768px`, `1024px`, and `1440px`, rows and feed controls
  have no horizontal overflow, clipped content, or overlap with the shell.
- Coarse-pointer action targets are at least `44px`; smaller desktop targets
  meet WCAG 2.2 spacing requirements.
- Home still uses one document scrollbar, and the federated network column is
  visually and behaviorally unchanged.
- Signed-in load, tag filtering, unread filtering, pagination, inline editing,
  read-state changes, deletion, and action failures produce no new browser
  console errors.
- Tailwind CSS regenerates successfully with
  `npx @tailwindcss/cli -i style.css -o tailwind.css` from `indielinks-fe`.
- `cargo check -p indielinks-fe --target wasm32-unknown-unknown`,
  `cargo fmt --all -- --check`, and `admin/run-linters` pass from the workspace
  root. The integration suite is not run for this frontend-only change.

## Sources

- [UI visual-system overview](ui-visual-system-overview.md)
- [Step 1 visual-system plan](ui-visual-system-step-1.md)
- [Step 2 application-shell plan](ui-visual-system-step-2.md)
- Current saved-link implementation in `src/home.rs`
- Current Home page structure in `src/personal.rs`
- Shared `Post` and `StorUrl` types in `../indielinks-shared/src/entities.rs`
- [2007 del.icio.us preview](https://techcrunch.com/2007/09/06/exclusive-screen-shots-and-feature-overview-of-delicious-20-preview/)
- [WAI button pattern](https://www.w3.org/WAI/ARIA/apg/patterns/button/)
- [WCAG 2.2 target-size guidance](https://www.w3.org/WAI/WCAG22/Understanding/target-size-minimum.html)
- [WHATWG `time` element](https://html.spec.whatwg.org/multipage/text-level-semantics.html#the-time-element)
