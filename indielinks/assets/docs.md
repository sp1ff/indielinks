# Documentation

## Bookmarklet

%{host} offers a bookmarklet for quickly adding links from your browser. To install it, create a new bookmark in your browser, give it a name such as “Save to %{host},” and use the following JavaScript as the bookmark's URL:

```javascript
javascript:q=location.href;if(document.getSelection){d=document.getSelection();}else{d='';};p=document.title;void(open('%{scheme}://%{netloc}/bookmarklets/save?showtags=yes&url='+encodeURIComponent(q)+'&description='+encodeURIComponent(d)+'&title='+encodeURIComponent(p),'%{host}','toolbar=no,scrollbars=yes,width=750,height=700'));
```

Some browsers remove the `javascript:` prefix when text is pasted into a bookmark's URL field; if that happens, type the prefix back in.

When invoked, the bookmarklet collects the current page's URL and title, along with any text selected on the page. It uses this information to pre-fill a form for adding a link to %{host} (the selected text, if any, becomes the post's notes, as well as the Note you'll be creating if you make this link public). You can review the title and notes, add tags, choose whether the link is private or marked for later reading, and then save it. If you made the link public, a Note based on it will be published to your followers.

## RSS Feeds

%{host} provides RSS feeds in Atom 1.0 format in two ways:

1. public feeds
2. private, per-user feeds

In the first case, anyone can get an Atom feed of a user's public posts by sending a `GET` to `/users/{username}/feed`. The caller can specify up to three comma-separated tags in the `tags` query parameter, request only unread posts by specifying the `unread` parameter, and optionally specify the number of desired posts with the `num` parameter.

In the second, indielinks users can get an Atom feed of their *own* posts (public & private) at `/api/v1/posts/all` by setting the Accept header in their request to `application/atom+xml`. All the query parameters from the old del.icio.us API apply. As with the rest of the del.icio.us API, the user can authenticate in a number of ways:

- including an API key in the query string as the `auth_token` parameter
- including one of HTTP basic authentication, an API key, or a login token in an Autorization header

## API

%{host} provides an extensive public API (in fact, this front end is built solely on the same API available to everyone). For managing links, I've attempted to recreate the old del.icio.us API. That API is documented here (full documentation to come).

### Generalities

%{host} implements the subset of the del.icio.us/Pinboard API described below. Paths are relative to `/api/v1`.

All endpoints require authentication. The original del.icio.us API used HTTP basic authentication. The (pinboard)[https://pinboard.in] API does as well, but also allows the use (preferrable, in my mind) of API keys. On pinboard, the API key goes in the query string.

Generally, I prefer the use of signed, limited duration bearer tokens or request signing, but I'm going to remain backward compatible with both but also accept the API token in the `Authorization` header (using the "Bearer" scheme), meaning that a caller may supply credentials in one of three ways:

- HTTP Basic authentication with a(n) %{host} username and password
- an `Authorization: Bearer <username>:v1:<key>` header containing an API key;
  or
- an `Authorization: Bearer <token>` header containing a login token (which you'll need to get from an as-yet undocumented endpoint)

For compatibility with Pinboard clients, an API key may instead be supplied in the query string as `auth_token=<username>:v1:<key>`. Because query strings can be retained in browser history and server or proxy logs, the Authorization header is preferable. A missing or invalid credential produces `401 Unauthorized`.
Supplying more than one Authorization header produces `400 Bad Request`.

Parameters are URL query parameters for both GET and POST requests; POST requests do not use a JSON request body for backwards compatibility. Parameter values must be URL encoded. Timestamps use RFC 3339 in UTC, while calendar dates use `YYYY-MM-DD`. Tag lists are comma separated. Where an endpoint accepts more than one tag, a post must have every specified tag to match, and at most three tags may be supplied.

One point of divergence from both the del.ico.us and pinboard APIs is that %{host}'s API does *not* support responses in XML; responses are JSON unless otherwise noted. 

Mutation responses and handler errors generally have the form `{"result_code":"..."}`. Authentication errors instead have the form `{"error":"..."}`. Clients should inspect both the HTTP status and the response body (again, I didn't design this: this is for backward compatibility). The order of entries in a post's `tags` array, and the order of keys in a tag-count object, are unspecified.

### `GET /posts/update`

Returns the time at which the authenticated user's posts were last changed:

```json
{"update_time":"2026-11-09T20:15:00Z"}
```

If the user has never posted, the endpoint returns `200 OK` with a `result_code` explaining that there are no posts yet.

### `GET|POST /posts/add`

Creates a bookmark, or updates the bookmark for the same URL when replacement is enabled.

| Parameter     | Required | Meaning                                                                 |
|---------------|----------|-------------------------------------------------------------------------|
| `url`         | yes      | The URL being bookmarked.                                               |
| `description` | yes      | The bookmark title. The name is retained for del.icio.us compatibility. |
| `extended`    | no       | Notes attached to the bookmark.                                         |
| `tags`        | no       | Comma-separated tags.                                                   |
| `dt`          | no       | The posting time; defaults to the current time.                         |
| `replace`     | no       | Whether to replace an existing bookmark for `url`; defaults to `true`.  |
| `shared`      | no       | Whether the bookmark is public; defaults to `false`.                    |
| `toread`      | no       | Whether the bookmark is marked unread; defaults to `false`.             |

Creating a bookmark or replacing an existing bookmark returns `201 Created`. Finding an existing bookmark while `replace=false` returns `200 OK` without changing it. A successful response in either case is `{"result_code":"done"}`.

### `GET|POST /posts/delete`

Deletes the authenticated user's bookmark for the required `url` parameter. The endpoint returns `200 OK` with `{"result_code":"done"}` when a bookmark was deleted, or `{"result_code":"item not found"}` when no bookmark had that URL.

### `GET /posts/get`

Returns posts for one calendar day.

| Parameter | Required | Meaning                                           |
|-----------|----------|---------------------------------------------------|
| `dt`      | no       | The day to retrieve, as `YYYY-MM-DD`.             |
| `tag`     | no       | Up to three comma-separated tags.                 |
| `url`     | no       | Restrict the results to this URL.                 |
| `meta`    | no       | Accepted for compatibility but currently ignored. |

The `tag` and `url` filters are applied only when `dt` is supplied. Without `dt`, the response contains no posts and reports the user's most recent update time in `date`. A successful response has this shape:

```json
{
  "date": "2026-11-09T20:15:00Z",
  "user": "alice",
  "posts": []
}
```

If the user has never posted, the endpoint returns `200 OK` with a `result_code` explaining that there are no posts yet.

### `GET /posts/recent`

Returns the authenticated user's most recent posts in reverse chronological order.

| Parameter | Required | Meaning                                              |
|-----------|----------|------------------------------------------------------|
| `tag`     | no       | Up to three comma-separated tags.                    |
| `count`   | no       | Maximum number of posts to return; defaults to `10`. |

The response contains `date`, the user's most recent update time; `user`, the username; and `posts`, an array of post objects.

### `GET /posts/dates`

Returns the number of matching posts on each day. The optional `tag` parameter accepts up to three comma-separated tags. The response has this shape:

```json
{
  "user": "alice",
  "tag": "rust,reading",
  "dates": [
    {"count": 2, "date": "2026-11-09"}
  ]
}
```

The `tag` field is an empty string when no tag filter was supplied.

### `GET|POST /posts/all`

Returns all matching posts in reverse chronological order.

| Parameter | Required | Meaning                                                                                          |
|-----------|----------|--------------------------------------------------------------------------------------------------|
| `tag`     | no       | Up to three comma-separated tags.                                                                |
| `start`   | no       | Zero-based offset into the filtered results.                                                     |
| `results` | no       | Maximum number of posts to return.                                                               |
| `fromdt`  | no       | Include posts made at or after this timestamp.                                                   |
| `todt`    | no       | Include posts made before this timestamp.                                                        |
| `unread`  | no       | If `true`, include only unread posts; if omitted or `false`, include both read and unread posts. |
| `meta`    | no       | Accepted for compatibility but currently ignored.                                                |

Pagination is applied after tag, date, and unread filtering. The default JSON response contains `user`, the original `tag` value or an empty string, and a `posts` array.

Send `Accept: application/atom+xml` to receive the same posts as an Atom 1.0 feed. `Accept: application/json`, or omitting the header, selects JSON.

### `GET /tags/get`

Returns all of the authenticated user's tags as a JSON object whose keys are tag names and whose values are the number of bookmarks carrying each tag:

```json
{"rust":12,"reading":37}
```

### `GET|POST /tags/rename`

Renames the required `old` tag to the required `new` tag on all of the authenticated user's bookmarks. A successful request returns `200 OK` with `{"result_code":"done"}`.

### `GET|POST /tags/delete`

Removes the required `tag` from all of the authenticated user's bookmarks. It does not delete the bookmarks themselves. A successful request returns `200 OK` with `{"result_code":"done"}`.
