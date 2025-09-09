// Copyright (C) 2025 Michael Herstine <sp1ff@pobox.com>
//
// This file is part of indielinks.
//
// indielinks is free software: you can redistribute it and/or modify it under the terms of the GNU
// General Public License as published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// indielinks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with indielinks.  If not,
// see <http://www.gnu.org/licenses/>.

//! # indielinks-fe Home component

use gloo_net::http::Request;
use leptos::{
    either::Either,
    html::{self},
    prelude::*,
};
use leptos_router::hooks::use_query_map;
use tap::Pipe;
use tracing::{debug, error, info};

use indielinks_shared::{Post, PostAddReq, PostsAllRsp, StorUrl};

use crate::{
    http::{send_with_retry, string_for_status},
    types::{Api, Base, PageSize, Token, USER_AGENT},
};

#[component]
/// Render a `Post` in "view" mode (as opposed to "edit" mode)
///
/// We expect the HTTP client, indielinks API location, and the login token to be available in the context.
fn Post(
    post: Post,
    set_editing: WriteSignal<Option<StorUrl>>,
    set_page: WriteSignal<usize>,
    rerender: ArcTrigger,
) -> impl IntoView {
    debug!("Post invoked.");

    let api = use_context::<Api>().expect("No context for the API location!?");
    let token = use_context::<Token>().expect("No token Cell!?");
    let token = token.get_untracked().expect("No token!?");

    let title = post.title().to_owned();
    let posted = post.posted().format("%Y-%m-%d %H:%M:%S").to_string();
    let url = post.url().clone();

    #[derive(Clone, Debug)]
    pub struct EditParams {
        pub url: StorUrl,
        pub set_editing: WriteSignal<Option<StorUrl>>,
        pub rerender: ArcTrigger,
    }

    let edit_url = url.clone();
    let on_edit = move |_| {
        info!("Now editing {edit_url}");
        set_editing.set(Some(edit_url.clone()));
    };

    #[derive(Clone, Debug)]
    pub struct DeleteParams {
        pub api: Api,
        pub token: String,
        pub rerender: ArcTrigger,
        pub url: StorUrl,
    }

    let on_delete = Action::new_unsync(move |params: &DeleteParams| {
        let params = params.clone();
        let mut full_url =
            url::Url::parse(&format!("{}/api/v1/posts/delete", params.api.0)).expect("Bad URL!?");
        full_url.query_pairs_mut().append_pair("url", &params.url);
        info!("Sending: {full_url}");
        async move {
            match send_with_retry(|| {
                Request::post(full_url.as_str())
                    .header("User-Agent", USER_AGENT)
                    .header("Authorization", &format!("Bearer {}", params.token))
                    .send()
            })
            .await
            {
                Ok(_) => params.rerender.notify(),
                Err(err) => error!("Delete post: {err}"),
            }
        }
    });

    let base = use_context::<Base>().expect("No base!?").0;
    // These clones are getting out of hand... I understand I need to move them into the `View`, but
    // why do they need to be cloned *again* in closures inside the `View`?
    let a1 = api.clone();
    let t1 = token.clone();
    let r1 = rerender.clone();
    let u0 = url.clone().to_string();
    let t = format!("{base}/h?tag=");
    view! {
        <div class="post">
            <div class="post-title"><a href={ u0.clone() }> { title } </a></div>
            <div class="post-info">
                <div class="post-info-left"> { posted }</div>
                <div class="post-info-right">
                    {
                        post.tags().map(|tag| {
                            // Man, this involves a *lot* of cloning-- needed?
                            let tag: String = tag.clone().into();
                            let tag0 = tag.clone();
                            view! {
                                <a href={t.clone() + &tag0.clone()} on:click=move |_| {
                                    set_page.set(0);

                                } > { tag } </a> " "
                            }
                        }).collect::<Vec<_>>()
                    }
                </div>
            </div>
            <div class="post-controls">
            <a href="#" on:click=on_edit>edit</a>
            " "
            <a href="#" on:click=move |_| {
                on_delete.dispatch(DeleteParams {
                    api: a1.clone(), token: t1.clone(), rerender: r1.clone(), url: url.clone(),
                });
            }>delete</a>
            </div>
        </div>
    }
}

#[component]
fn EditPost(
    post: Post,
    set_editing: WriteSignal<Option<StorUrl>>,
    rerender: ArcTrigger,
) -> impl IntoView {
    debug!("EditPost invoked.");

    let api = use_context::<Api>().expect("No context for the API location!?");

    let token = use_context::<Token>().expect("No token Cell!?");
    let token = token.get().expect("No token!?");

    // TBH, I have *no* idea what this does:
    let url_element: NodeRef<html::Input> = NodeRef::new();
    let title_element: NodeRef<html::Input> = NodeRef::new();
    let notes_element: NodeRef<html::Input> = NodeRef::new();
    let tags_element: NodeRef<html::Input> = NodeRef::new();
    let private_element: NodeRef<html::Input> = NodeRef::new();
    let unread_element: NodeRef<html::Input> = NodeRef::new();

    let url = post.url().to_string();
    let title = post.title().to_owned();
    let notes = post.notes().map(|s| s.to_owned());
    let tags = post
        .tags()
        .cloned()
        .map(|tag_name| tag_name.into())
        .collect::<Vec<String>>()
        .join(" ");
    let private = !post.public();
    let unread = post.unread();

    let on_submit = Action::new_local(move |_: &()| {
        let api = api.0.clone();
        let token = token.clone();
        let rerender = rerender.clone();
        async move {
            // This seems *really* painful... is there no way to make this more elegant?
            let url: StorUrl = match url_element
                .get()
                .expect("url should be mounted")
                .value()
                .try_into()
            {
                Ok(url) => url,
                Err(err) => {
                    error!("{err:?}");
                    window().alert_with_message("Invalid URL").unwrap();
                    return;
                }
            };
            let title = title_element
                .get()
                .expect("title should be mounted")
                .value();
            let notes = notes_element
                .get()
                .expect("notes should be mounted")
                .value();
            let tags = tags_element
                .get()
                .expect("tags should be mounted")
                .value()
                .split(' ')
                .map(|s| s.to_owned())
                .collect::<Vec<String>>()
                .join(",");
            let shared = !private_element
                .get()
                .expect("shared should be mounted")
                .checked();
            let to_read = unread_element
                .get()
                .expect("unread should be mounted")
                .checked();

            let url = format!(
                "{}/api/v1/posts/add?{}",
                api,
                serde_urlencoded::to_string(&PostAddReq {
                    url,
                    title,
                    notes: if notes.is_empty() { None } else { Some(notes) },
                    tags: if tags.is_empty() { None } else { Some(tags) },
                    dt: None,
                    replace: Some(true),
                    shared: Some(shared),
                    to_read: Some(to_read),
                })
                .expect("Bad query!?")
            );
            info!("Sending {url}");
            match Request::post(&url)
                .header("User-Agent", USER_AGENT)
                .header("Authorization", &format!("Bearer {}", token))
                .send()
                .await
                .map_err(|err| err.to_string())
                .and_then(string_for_status)
            {
                Ok(_) => {
                    info!("Updated post successfully!");
                    set_editing.set(None);
                    rerender.notify();
                }
                Err(err) => {
                    error!("{err:?}");
                }
            }
        }
    });

    use leptos::prelude::window;

    view! {
        <div class="post">
            <form style="padding; 1em;" on:submit = move |ev| { // leptos::tachys::html::event::Event
                ev.prevent_default();
                on_submit.dispatch(());
            }>
            <div style="margin-bottom: 8px;">
                <input type="text" id="url" name="url" value={ url } node_ref=url_element required/>
            </div>
            <div style="margin-bottom: 8px;">
                <input type="text" id="title" name="title" value={ title } node_ref=title_element required/>
            </div>
            <div style="margin-bottom: 3px;">
                <label for="notes" style="font-size: smaller; color: #276173">notes</label>
            </div>
            <div style="margin-bottom: 8px;">
                <input type="textarea" id="notes" name="notes" value={ notes } node_ref=notes_element/>
            </div>
            <div style="margin-bottom: 3px;">
                <label for="tags" style="font-size: smaller; color: #276173">tags</label>
            </div>
            <div style="margin-bottom: 8px;">
                <input type="text" id="tags" name="tags" value={ tags } node_ref=tags_element/>
            </div>
            <div style="margin-bottom: 8px; font-size: smaller;">
                <label>
                    <input type="checkbox" id="private" name="private" checked={ private } node_ref=private_element/> private
                </label>
                " "
                <label>
                    <input type="checkbox" id="unread" name="unread" checked={ unread } node_ref=unread_element/> unread
                </label>
            </div>
            <div>
                <input type="submit" value="save"/>
                <button type="button" on:click=move |_| {
                    set_editing.set(None);
                }>"cancel"</button>
            </div>
            </form>
        </div>
    }
}

/// The indielinks' user "home" page
///
/// We expect the HTTP client & the indielinks API location to be available as context.
#[component]
pub fn Home() -> impl IntoView {
    debug!("Home invoked.");
    // RE posts: we can be in one of three situations:
    // 1. zero posts-- the user just signed-up & hasn't posted anything; don't display the
    // pagination controls & show an explanatory message in the list div
    // 2. up to one page's worth of posts: don't display the pagination controls, display the
    // posts in the list div
    // 3. more than one page's worth of posts: display the pagnination controls, display
    // the appropriate page in the list div

    // This is an interesting problem: we have a collection of posts on the server, and we need to
    // display one page's worth. That collection can change-out from beneath us. We have three
    // operations at our disposal:

    // - /posts/date : date of the most recent change
    // - /posts/recent : request the n most recent posts
    // - /posts/all : request a range of posts (offset + # to return)

    // I think my initial approach is going to be to simply maintain the "current page" as a signal
    // and manipulate it via an Effect/Action (still not sure when to use which). The idea then will
    // be to render the posts div according to that state on demand. This means we'll only re-render
    // when the page is changed, not when we discover that the server-side state has changed-out
    // beneath us. I'm not sure how to force a render; for now I'm planning on just adding a
    // "refresh" button.

    let api = use_context::<Api>()
        .expect("Failed to rerieve the API net location")
        .0;

    let page_size = use_context::<PageSize>()
        .expect("Failed to retrieve the page size configuration item")
        .0;

    // I'm still confused on how & when these `View` constructing functions are invoked, but it
    // seems that this function won't be invoked until after sign-in, so we can extract the token &
    // provide it to all our subordinate components via context rather than prop drilling.
    let token = use_context::<Token>().expect("No token Cell!?");
    let token = token.get_untracked().expect("No token!?");

    // Later: recognize some basic query parameters <https://book.leptos.dev/router/18_params_and_queries.html>

    // Create a bit of reactive state for the current page...
    let (page, set_page) = signal(0usize);
    // whether we're filtering read posts
    let (unread_only, set_unread_only) = signal(false);
    // and whether & which `Post` is currently being edited.
    let (editing, set_editing): (ReadSignal<Option<StorUrl>>, WriteSignal<Option<StorUrl>>) =
        signal(None);
    // Finally, setup a mechanism by which we can force this view to be re-rendered. A signal won't
    // really do it because there are places (say, after a delete) where we want to *force* a
    // re-render programmatically. "A trigger is a data-less signal with the sole purpose of
    // notifying other reactive code of a change."
    let rerender = ArcTrigger::new();

    // I'd really prefer to return a `Result<Vec<Post>>`, but I can't use this as a `LocalResource`
    // unless the return type is `Clone`, which errors generally are not. I *could* change my error
    // type to box all source errors and so be `Clone`, but before I go to that length, I want to be
    // sure I'm actually going to *use* the returned error. The hackernews example, for instance,
    // returns an `Option`, simply logging any errors it encounters before returning `None`.
    async fn load_data(
        api: String,
        token: String,
        page: usize,
        page_size: usize,
        tag: Option<String>,
        unread_only: bool,
    ) -> Option<Vec<Post>> {
        let mut url = format!(
            "{api}/api/v1/posts/all?start={}&results={page_size}&unread={unread_only}",
            page * page_size,
        );
        if let Some(tag) = tag {
            url += &format!("&tag={tag}");
        }
        send_with_retry(|| {
            Request::post(&url)
                .header("Authorization", &format!("Bearer {token}"))
                .send()
        })
        .await
        .map_err(|err| err.to_string())
        .and_then(string_for_status)
        .map_err(|err| error!("Requesting a page's worth of posts: {err:?}"))
        .ok()?
        .json::<PostsAllRsp>()
        .await
        .map_err(|err| error!("Deserializing a page's worth of posts; {err:?}"))
        .ok()?
        .pipe(|rsp| {
            debug!("Loaded {} posts.", rsp.posts.len());
            Some(rsp.posts)
        })
    }

    let params = use_query_map();
    let tracker = rerender.clone();
    let page_data = LocalResource::new(move || {
        tracker.track();
        load_data(
            api.clone(),
            token.clone(),
            page.get(),
            page_size,
            params.with(|m| m.get("tag")),
            unread_only.get(),
        )
    });

    let on_click = {
        let trig = rerender.clone();
        move |_| trig.notify()
    };

    let toggle_unread = {
        let trig = rerender.clone();
        move |_| {
            set_unread_only.update(|unread_only| *unread_only = !*unread_only);
            trig.notify()
        }
    };

    view! {
        <div class="user-view" style="display: flex;">
            // <Transition fallback=move || view!{ <p>"Loading..."</p> }>
            <Await future=page_data.into_future() let:_data>
                // User's posts
                <div class="posts-view">
                    <div class="posts-nav" style="display: flex; font-size: smaller; color: #888">
                        <span style="padding: 2px 6px;">
                            {
                                move || {
                                info!("page: {}", page.get());
                                if page.get() > 0 {
                                    Either::Left(view!{
                                        <a href="#" on:click=move |_| set_page.update(|n| *n -= 1)>"< prev"</a>
                                    })
                                } else {
                                    Either::Right(view!{"< prev"})
                                }
                                }
                            }
                        </span>
                        <span style="padding: 2px 6px;">"page " {page} " " <a href="#" on:click=on_click >"    refresh"</a></span>
                        <span style="padding: 2px 6px;">
                            <a href="#" on:click=toggle_unread>{ move || {
                                if unread_only.get() { "all posts" } else { "unread posts" }
                            }}</a>
                        </span>
                        <span style="padding: 2px 6px;">
                            {
                                move || {
                                    match page_data.get() {
                                        Some(Some(posts)) if posts.len() == page_size => Either::Left(view!{
                                            <a href="#" on:click=move |_| set_page.update(|n| *n += 1)>"> next"</a>
                                        }),
                                        _ => Either::Right(view!{"next >"})
                                    }}
                            }
                        </span>
                    </div>
                    <div class="post-list">
                        <For each=move || page_data.get().unwrap_or_default().unwrap_or_default()
                             key=|post| post.id()
                             let:post>
                            {
                                let r0 = rerender.clone();
                                let r1 = rerender.clone();
                                move || {
                                    if Some(post.url()) == editing.get().as_ref() {
                                        Either::Left(view!{<EditPost post=post.clone() set_editing rerender=r0.clone()/>})
                                    } else {
                                        Either::Right(view!{<Post post=post.clone() set_editing set_page rerender=r1.clone()/>})
                                    }
                            }}
                        </For>
                    </div>
                </div>
                // User's tags
                <div style="flex: 1; margin: 6px; border: 1px solid #ccc; display: flex; align-items: center; justify-content: space-around;">
                    <span>"Tags will go here!"</span>
                </div>
            </Await>
        </div>
    }
}
