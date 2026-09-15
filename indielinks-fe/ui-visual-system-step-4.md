# UI Visual System: Step 4 Plan

## Summary

Redesign the Home page's network column as a compact federated timeline. Each
item will use a clear actor, time, content, and action hierarchy inspired by
Mastodon and Pleroma while retaining the plain surfaces, blue links, restrained
borders, and information density established in the first three steps.

This step covers the initial timeline, incremental loading, individual
federated posts, inline replies, and the existing conversation drill-down. It
preserves the current ActivityPub API requests, sanitized post content,
timeline tokens, error reporting, and two-column shell. General loading, error,
empty, and toast presentation remains assigned to Step 5.

## Current State

The network column renders every post in a separately bordered box. A truncated
actor URL and local timestamp share one line, followed by the federated HTML.
The entire content area is clickable to open a conversation, including areas
that contain links. The row actions are bare SVG click targets without native
button semantics or accessible names.

Favorite and reply issue real requests. Share, quote, and copy-link only write
debug messages, although the UI presents them as working commands. The reply
composer uses icon click handlers instead of a form submission control. The
conversation view has a clickable arrow, a plain loading string, another outer
border, and no visual distinction among its parent, focal post, and replies.

The timeline's “new posts” and “older posts” controls work, but their styling
and pending state do not communicate how the feed updates. The redesign will
keep those mechanics and expose them with semantic, responsive controls.

## Available Data and Honest Identity Presentation

`FeedPost` currently provides only the post URL, actor URL, optional parent
URL, publication time, and HTML content. It does not provide a display name,
avatar URL, reaction counts, attachment metadata, boost attribution, or the
current user's favorite state. This step will not invent those values or add
per-row actor fetches.

Represent the actor using information that can be derived from the validated
actor URL:

- a neutral 36px avatar placeholder containing a decorative person icon;
- a semibold short identifier derived from the last non-empty path segment;
- the actor host as muted secondary text; and
- the complete actor URL as the destination and accessible title of the
  identity link.

Prefix an ordinary identifier with `@` for the familiar Fediverse cue, but do
not add a second `@host` to the short line because the host is displayed beside
it. A total helper will fall back from path segment to host, then scheme, then
the complete URL string. Long, unusual, or percent-encoded actor identifiers
must remain displayable and wrap without widening the column.

The placeholder is deliberately generic. Real display names and avatars
should replace it only after the timeline response supplies resolved actor
metadata in a later backend and shared-type change.

## Timeline Surface

Add a `content-panel--network` modifier to the existing Step 2 panel and remove
the panel body's padding. Render timeline items as a semantic ordered list in
API order. Each list item contains an `<article>` and adjacent items use a
single one-pixel divider instead of independent card borders.

Use `12px` vertical and `16px` horizontal padding at ordinary widths. The actor
placeholder occupies a narrow first column and the post body occupies a
`minmax(0, 1fr)` second column. At widths below `480px`, reduce the horizontal
padding and avatar size slightly while retaining the same two-column anatomy.
Content and actions may wrap, but nothing is hidden.

Do not make the article or content area clickable. Interactive descendants
retain their own link and button behavior, and conversation navigation receives
an explicit control. This avoids accidental conversation changes when a user
selects text or follows a link inside federated HTML.

Use the shared white panel surface for ordinary posts. Reserve the subtle brand
surface and leading brand rule for the focal item inside a conversation, where
the distinction communicates real navigation state.

## Post Anatomy

Each timeline article will use this visual and semantic order:

1. Actor placeholder and linked actor identity.
2. A linked publication timestamp aligned with the identity line.
3. Federated post content with scoped typography.
4. The inline reply composer when active.
5. A compact action group otherwise.

The actor identity is the post heading so assistive-technology users can scan
articles by author. Keep the short identifier visually primary and the host
secondary. The timestamp remains visible on the same header row when space
permits and wraps below the identity on narrow or unusually long content.

Render the timestamp with `<time datetime="...">`. Use the same concise,
unambiguous UTC display as saved links (`YYYY-MM-DD HH:MM UTC`) and RFC 3339 for
the machine-readable value. Link the time to `FeedPost::id`, following the
common federated-timeline convention of making the timestamp the post
permalink. Preserve same-tab browser behavior.

If `in_reply_to` is present, expose a small “reply” context label between the
identity and body. The label describes the item's place in a conversation but
does not link directly to the raw parent URL; the explicit conversation action
loads the server-provided context.

## Federated Content

Continue rendering the server-provided `FeedPost::content` as HTML. Wrap it in
a dedicated `federated-post__content` container and scope all descendant rules
to that class so remote markup cannot alter the surrounding shell.

Normalize the elements commonly emitted by ActivityPub implementations:

- remove the first paragraph's top margin and the last paragraph's bottom
  margin;
- use the Step 1 body size and comfortable line height;
- apply brand link colors, underlines, visited state, and visible focus;
- indent lists and block quotes without excessive whitespace;
- constrain images, video, and other replaced content to the available width;
- wrap long URLs and unbroken text; and
- allow `pre` content to scroll within the article rather than widening the
  Home grid.

Do not truncate content, add “show more,” rewrite links, synthesize previews,
or add media galleries. Those changes require content metadata and product
decisions beyond this visual step.

## Post Actions

Replace the current SVG click handlers and dropdown placeholders with one
native-button action group. Retain the working commands and add an explicit
conversation command:

- “favorite,” using `AiStarOutlined` and the existing like request;
- “reply,” using `BsReply` and the existing inline composer; and
- “conversation,” using `FiMessageCircle` and the existing context request.

Each `<button type="button">` has a visible icon and text at ordinary widths.
Below `480px`, the text may be visually hidden while the button retains an
`aria-label` that includes the action. The buttons use neutral foregrounds,
the shared focus ring, a restrained hover surface, and at least 44px targets on
coarse pointers. Do not use `role="toolbar"`; a simple labelled group preserves
ordinary Tab navigation without introducing arrow-key behavior.

Disable Favorite while its request is pending and expose the pending state
with `aria-busy`. The current API does not return favorite state or a count, so
do not render an active star, count, or toggle semantics. Continue showing the
existing error toast on failure.

Remove Share, Quote, and Copy Link from the rendered post UI in this step.
Their handlers currently perform no user-visible work. The post timestamp
provides a normal permalink that users can open or copy through the browser.
Keep the generic dropdown module available for future working menus; the post
component no longer needs `MenuId` or shared menu state.

## Inline Reply Composer

Retain one reply composer per post and the existing reply endpoint, success
refresh, and error toast. Replace the bare textarea and send/cancel SVGs with a
semantic `<form>` containing:

- a visually hidden label associated with the textarea;
- the current “Your reply…” placeholder;
- a primary “send reply” submit button with a send icon; and
- a secondary “cancel” button with `type="button"`.

The textarea uses the panel input surface, border, radius, inherited font, and
full available width. The button row aligns to the end on desktop and stretches
cleanly on narrow screens. Disable submission while the request is pending and
change the visible label to “sending…”; do not change server-side validation or
the reply payload in this step.

Move focus into the textarea when Reply opens. After Cancel or a successful
submission, close the composer and return focus to that post's Reply button
when practical. Activating the form's submit control must dispatch exactly one
request; Enter retains its normal textarea behavior of inserting a line break.

## Conversation Presentation

Keep the existing in-column conversation model and reactive stack. Opening a
conversation replaces only the selected timeline row, and Back pops one stack
entry or returns to the original post at the root. Do not add a route, modal,
browser-history entry, or separate overlay.

Present the conversation as a labelled section inside the network list item:

- a compact header with a native Back button and “conversation” heading;
- a status area using the existing spinner while context loads;
- the available parent, focal post, and direct children in a semantic list;
- a subtle vertical thread rail connecting related items; and
- a brand leading rule, subtle brand surface, and visible “current post” label
  on the focal item.

The API returns at most the represented parent plus direct children for the
current context. Do not draw nesting or connector branches that imply ancestry
the response does not contain. Labels such as “parent” and “replies” may group
the actual regions without stating reply counts that are unavailable.

Replace click-anywhere navigation on parent and child articles with each
article's explicit Conversation button. In the timeline this button opens the
root context; on a parent or child it pushes that post's context onto the
existing stack. Omit or disable the Conversation button on the focal article
and identify it as current instead.

When the conversation opens, move focus to its Back button or heading. When the
root conversation closes, return focus to the originating Conversation button.
Retain error toasts for failed context loads and keep the currently displayed
context usable after a later navigation failure.

## Timeline Update Controls

Restyle “new posts” and “older posts” as native feed buttons integrated with
the flat list. The top control uses a refresh icon and remains above the first
item; the bottom control uses a downward chevron and remains below the last
item. Both retain their existing timeline-token requests and insertion order.

While a request is pending, disable its button, set `aria-busy`, and display
“checking…” or “loading…”. If the server reports no posts, retain the control so
the user can check again; do not infer a permanent end of the federated feed.
Continue reporting failures through the existing toast path.

New items remain prepended and older items appended to the current `VecDeque`.
This step does not introduce automatic polling, infinite scroll, scroll
anchoring, deduplication beyond the existing keyed rendering, or a “new posts”
count.

## Component and Style Changes

Keep network requests and timeline-token state in `src/feeds.rs`. Factor the
feed markup into private semantic components such as `TimelineControls` and
`TimelineList`, while preserving `ItemFeedOuter` as the initial resource,
transition, and error boundary.

Keep post requests, conversation state, and post presentation in
`src/components/post.rs`. Split the markup into small private components:

- `ActorIdentity` for the placeholder, short identifier, and host;
- `PostHeader` for actor identity, reply cue, and semantic permalink time;
- `PostContent` for the scoped federated HTML container;
- `PostActions` for Favorite, Reply, and Conversation;
- `ReplyComposer` for the existing reply action;
- `Conversation` for the header, loading state, stack navigation, and thread
  list; and
- `FederatedPost` for the complete article in feed or conversation context.

Use an enum or explicit properties to distinguish ordinary, parent, current,
and child presentation. Avoid building state through concatenated class
strings. Keep the shared `FeedPost` type unchanged and use total helper
functions for actor labels and timestamps.

Remove post-specific imports and types for the inaccessible placeholder menus
from `post.rs` and `feeds.rs`. Do not redesign or delete
`src/components/dropdown.rs`; it is outside the working post UI once those
placeholder actions are removed.

Add federated-timeline classes under `@layer components` in `style.css`. Use
the Step 1 Thaw variables for backgrounds, foregrounds, strokes, brand state,
focus, radii, and control states. Keep layout and remote-content normalization
in CSS rather than long utility-class strings in Rust. No new dependency or raw
color is required.

## Behavior to Preserve

- Initial loading and failures continue through the current `Transition` and
  `ErrorBoundary`.
- “New posts” requests use the current `since` token and prepend returned
  posts in server order.
- “Older posts” requests use the current `before` token and append returned
  posts in server order.
- Favorite uses the current post and actor URLs and retains existing error
  reporting.
- Reply uses the current post and actor URLs, closes after success, and invokes
  the optional timeline refresh trigger.
- Conversation requests use the current post URL and retain push/pop stack
  navigation among returned contexts.
- Federated HTML remains rendered without changing the received content.
- Actor and post destinations retain ordinary same-tab link behavior.
- The no-posts invitation, spinner, error fallback, and toast visual treatment
  remain available for Step 5.
- The saved-link column and application shell remain visually and behaviorally
  unchanged.

## Implementation Sequence

1. Replace post-specific dropdown state and whole-content click navigation with
   explicit Favorite, Reply, and Conversation button callbacks.
2. Add total actor-display and timestamp helpers, then build the actor, header,
   content, and action subcomponents.
3. Render the network feed as an ordered list of semantic post articles and
   integrate native pending-aware update controls above and below it.
4. Convert the inline reply UI to a labelled form with submit, cancel, pending,
   and focus behavior while retaining its request action.
5. Restructure the conversation view with a semantic header, loading status,
   thread list, focal state, explicit branch navigation, and stack-aware focus.
6. Add responsive timeline, post-content, action, reply, and conversation
   styles using the Step 1 tokens.
7. Regenerate Tailwind CSS, build the WASM target, run frontend linters, and
   exercise feed loading, post actions, replies, and conversation navigation in
   the browser.

## Scope Boundaries

This step does not:

- change timeline, favorite, reply, or context API contracts;
- add actor resolution, display names, profile images, profile cards, or
  follow controls;
- add favorite state, reaction counts, boosts, quotes, sharing, copy-to-
  clipboard behavior, or action confirmation;
- add media galleries, link previews, polls, content warnings, language
  controls, translation, or content truncation;
- add automatic refresh, infinite scrolling, virtualization, or timeline
  persistence;
- add multi-level thread reconstruction beyond the context returned by the
  server;
- change remote-content sanitization or ActivityPub processing;
- redesign the saved-link feed, shell, general forms, empty states, loading
  states, error fallbacks, or toasts; or
- change routes, authentication, or backend behavior.

## Validation and Acceptance Criteria

- Every federated item is a semantic article with one clear actor identity,
  linked RFC 3339 timestamp, readable content, and labelled action group.
- The actor presentation never fabricates a display name or avatar and remains
  useful for unusual actor URLs.
- Remote paragraphs, links, lists, block quotes, long strings, images, and code
  stay inside the network column without altering shell styles.
- Following a content or actor link does not open the conversation; only the
  explicit Conversation button changes context.
- Favorite dispatches once, prevents duplicate dispatch while pending, and
  retains failure reporting.
- Reply opens a focused, labelled composer; Send, pending, success, failure,
  and Cancel retain their expected behavior.
- Share, Quote, and Copy Link are no longer exposed as working controls.
- Conversation Back, parent navigation, child navigation, focal state, loading,
  and errors work through native controls and the existing stack.
- The conversation's current post is identifiable without relying on color.
- New-post and older-post controls preserve token updates, post ordering, and
  retry behavior while communicating pending state.
- Keyboard users can reach actor links, timestamps, content links, all actions,
  reply controls, and conversation navigation in visual order with a visible
  focus indicator.
- At `320px`, `390px`, `768px`, `1024px`, and `1440px`, feed items,
  federated HTML, action rows, the reply composer, and conversation threads
  have no horizontal overflow, clipping, or overlap with the shell.
- Coarse-pointer controls provide at least 44px targets; compact desktop
  controls satisfy the Step 1 focus and target-spacing rules.
- Home retains one document scrollbar, the network feed stays aligned with the
  saved-link panel, and loading more items does not introduce nested scrolling.
- Signed-in load, new/older updates, Favorite, Reply, conversation drill-down,
  Back, and action failures produce no new browser console errors.
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
- Current timeline implementation in `src/feeds.rs`
- Current post and conversation implementation in `src/components/post.rs`
- Shared `FeedPost` type in `../indielinks-shared/src/api.rs`
- [Mastodon federation behavior and account identity](https://github.com/mastodon/mastodon/blob/main/FEDERATION.md)
- [Mastodon public timeline and conversation documentation](https://docs.joinmastodon.org/client/public/)
- [WAI button pattern](https://www.w3.org/WAI/ARIA/apg/patterns/button/)
- [WAI menu-button pattern](https://www.w3.org/WAI/ARIA/apg/patterns/menu-button/)
- [WCAG 2.2 target-size guidance](https://www.w3.org/WAI/WCAG22/Understanding/target-size-minimum.html)
- [WHATWG `time` element](https://html.spec.whatwg.org/multipage/text-level-semantics.html#the-time-element)
