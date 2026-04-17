// Copyright (C) 2026 Michael Herstine <sp1ff@pobox.com>
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

//! # A component for viewing a single post

use std::{cmp::PartialEq, result::Result as StdResult};

use gloo_net::http::Request;
use leptos::{either::Either, html, prelude::*};
use snafu::{Backtrace, ResultExt, Snafu};
use tracing::{debug, error};
use url::Url;

use indielinks_shared::api::{FeedPost, LikeRequest, ReplyRequest};

use crate::{
    components::dropdown::{Dropdown, DropdownMenuItem, DropdownMenuItems, DropdownTrigger},
    http::send_with_retry,
    types::{Api, Token},
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO(sp1ff): prune this
#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    #[snafu(display("While deserializing the initial timeline, {source}"))]
    Load {
        source: gloo_net::Error,
        backtrace: Backtrace,
    },
    Refresh,
    #[snafu(display("While sending an HTTP request, {source}"))]
    Request {
        source: gloo_net::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Got response status {status}"))]
    Status {
        status: u16,
        backtrace: Backtrace,
    },
    #[snafu(display("While deserializing an update to the timeline, {source}"))]
    UpdateSince {
        source: gloo_net::Error,
        backtrace: Backtrace,
    },
}

pub type Result<T> = StdResult<T, Error>;

// TODO(sp1ff): duplicated from `feeds.rs`!
fn error_for_status(rsp: gloo_net::http::Response) -> Result<gloo_net::http::Response> {
    let status = rsp.status();
    if status >= 200 && status < 300 {
        Ok(rsp)
    } else {
        Err(StatusSnafu { status }.build())
    }
}

impl From<gloo_net::Error> for Error {
    fn from(_value: gloo_net::Error) -> Error {
        Error::Refresh
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq)]
pub enum DropdownSort {
    Share,
    Miscellaneous,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MenuId {
    url: Url,
    sort: DropdownSort,
}

// TODO(sp1ff): It's a pity to represent both the post & actor using `Url`-- could I use newtypes to
// distinguish between the two?
fn use_replying(post_id: Url, actor_id: Url) -> Action<(), ()> {
    // This seems awfully complex, but the closure we pass to the `Action` constructor must
    // implement `Fn`; i.e. all of its captures must either be `Copy` or moved into the closure.

    // That wouldn't be so bad, except this closure needs *another* closure (that it will pass to
    // `send_with_retry()`), and *that* one must also be `Fn`!

    Action::new_local(move |_: &()| {
        // Now, both `post_id` and `actor_id`, being referenced inside this block, have bee *moved*
        // here. I would have thought that we could just move them again into the next closure, but
        // the if I do that, without keeping copies here, the borrow checker complains. So:
        let post_id = post_id.clone();
        let actor_id = actor_id.clone();

        // Now, they're going to be moved into this closure, but we need the post ID later,
        // so create one more copy:
        let logged_post_id = post_id.clone();
        let send_like_request = move || {
            // As mentioned above, this closure needs to be `Fn`, so we give ourselves clones
            // of `post_id` and `actor_id`...
            let post_id = post_id.clone(); // let post_id = value.clone()
            let actor_id = actor_id.clone();
            // and then move those clones into this `async move` block...
            async move {
                let api = expect_context::<Api>().0;

                // Regrettably, at this time, this lambda has to return a `gloo_net::Error`.
                let token = expect_context::<Token>()
                    .get()
                    .ok_or(gloo_net::Error::GlooError(
                        "Missing token; this is a bug.".to_owned(),
                    ))?;
                Request::post(&format!("{api}/api/v1/users/like"))
                    .header("Authorization", &format!("Bearer {token}"))
                    .json(&LikeRequest {
                        // taking care to clone them before using (so that this block can be invoked
                        // again, if needed).
                        id: post_id.clone(),
                        actor: actor_id.clone(),
                    })
                    .map_err(|err| gloo_net::Error::GlooError(format!("{err}")))?
                    .send()
                    .await
            }
        };
        async move {
            match send_with_retry(send_like_request)
                .await
                .context(RequestSnafu)
                .and_then(error_for_status)
            {
                Ok(_) => debug!("Liked post {logged_post_id}"),
                Err(err) => debug!("While liking post {logged_post_id}, {err}."),
            }
        }
    })
}

#[component]
fn ReplyingPost(
    // TODO(sp1ff): I tried making this borrows, but the compiler insisted that "this function's
    // return type contains a borrowed value, but there is no value for it to be borrowed from"...
    // which makes no sense to me. It has nothing to do with the `Action` returned from
    // `use_replying()`-- the error was induced *just by making `post_id` (or `actor_id`) be passed
    // by reference*, even if I never called `use_replying()`-- there must be something in the view!
    // macro doing this.
    post_id: Url,
    actor_id: Url,
    reply_elt: NodeRef<html::Input>,
    set_replying: WriteSignal<bool>,
) -> impl IntoView {
    let send_reply = use_replying(post_id, actor_id);

    view! {
        <div class="feed-item-reply">
            <div style="flex: 1 1 0; min-height 0; display: flex; flex-direction: column;">
                <input type="textarea" node_ref=reply_elt style="flex: 1 1 0; resize: none; width: 100%;"/>
            </div>
            <div class="feed-item-reply-actions">
                <button on:click=move |_| {
                    set_replying.set(false);
                    send_reply.dispatch(());
                }>"send"</button>
                <button on:click=move |_| set_replying.set(false)>"cancel"</button>
            </div>
        </div>
    }
}

fn use_post_controls(post_id: Url, actor_id: Url) -> Action<(), ()> {
    Action::new_local(move |_: &()| {
        // Both `post_id` and `actor_id`, being referenced inside this block, have bee *moved*
        // here. I would have thought that we could just move them again into the next closure, but
        // the if I do that, without keeping copies here, the borrow checker complains. So:
        let post_id = post_id.clone();
        let actor_id = actor_id.clone();
        let logged_post_id = post_id.clone();
        let send_like = move || {
            let post_id = post_id.clone(); // let post_id = value.clone()
            let actor_id = actor_id.clone();
            async move {
                let api = expect_context::<Api>().0;
                // Regrettably, at this time, this lambda has to return a `gloo_net::Error`.
                let token = expect_context::<Token>()
                    .get()
                    .ok_or(gloo_net::Error::GlooError(
                        "Missing token; this is a bug.".to_owned(),
                    ))?;
                Request::post(&format!("{api}/api/v1/users/like"))
                    .header("Authorization", &format!("Bearer {token}"))
                    .json(&LikeRequest {
                        id: post_id.clone(),
                        actor: actor_id.clone(),
                    })
                    .map_err(|err| gloo_net::Error::GlooError(format!("{err}")))?
                    .send()
                    .await
            }
        };
        async move {
            match send_with_retry(send_like)
                .await
                .context(RequestSnafu)
                .and_then(error_for_status)
            {
                Ok(_) => debug!("Liked post {logged_post_id}"),
                Err(err) => debug!("While liking post {logged_post_id}, {err}."),
            }
        }
    })
}

#[component]
fn PostControls(
    post_id: Url,
    actor_id: Url,
    open_menu: RwSignal<Option<MenuId>>,
    set_replying: WriteSignal<bool>,
) -> impl IntoView {
    let share_menu_id = MenuId {
        url: post_id.clone(),
        sort: DropdownSort::Share,
    };
    let misc_menu_id = MenuId {
        url: post_id.clone(),
        sort: DropdownSort::Miscellaneous,
    };

    let send_like = use_post_controls(post_id, actor_id);

    view! {
        <div class="feed-item-actions">
            <button on:click=move |_| {send_like.dispatch(());}>"like"</button>
            " "
            <Dropdown open_menu>
                <DropdownTrigger text="share".to_string() menu_id=share_menu_id.clone() />
                <DropdownMenuItems menu_id=share_menu_id.clone()>
                    <DropdownMenuItem
                        text="share".to_string()
                        handler=Callback::new(|()| debug!("Share selected"))/>
                    <DropdownMenuItem
                        text="quote".to_string()
                        handler=Callback::new(|()| debug!("Quote selected"))/>
                </DropdownMenuItems>
            </Dropdown>
            " "
            <button on:click=move |_| set_replying.set(true)>"reply"</button>
            " "
            <Dropdown open_menu>
                <DropdownTrigger text="more".to_string() menu_id=misc_menu_id.clone() />
                <DropdownMenuItems menu_id=misc_menu_id.clone()>
                    <DropdownMenuItem
                        text="copy lnk".to_string()
                        handler=Callback::new(|()| debug!("Copy link selected"))/>
                </DropdownMenuItems>
            </Dropdown>
        </div>
    }
}

/// `ViewPost` is a component that displays a single post at a time, with the possibility of
/// replying.
#[component]
pub fn ViewPost(post: FeedPost, open_menu: RwSignal<Option<MenuId>>) -> impl IntoView {
    let api = expect_context::<Api>().0;
    let token = expect_context::<Token>();

    let (replying, set_replying) = signal::<bool>(false);

    // I need to factor this out.
    fn string_for_node_ref(node: &NodeRef<html::Input>) -> String {
        node.get().expect("NodeRef not mounted?").value()
    }

    let api2 = api.clone();

    let reply_element: NodeRef<html::Input> = NodeRef::new();
    let post_id = post.id.clone();
    let post_actor = post.actor.clone();
    view! {
        <div class="feed-item" style="display:flex; flex-direction: column; gap: 0.5rem;">
            <div class="feed-item-content" style="text-align: center;" inner_html=post.content></div>
            {
                move || {
                    let post_id = post_id.clone();
                    let post_actor = post_actor.clone();
                    if replying.get() {
                        Either::Left(
                            view!{<ReplyingPost post_id
                                  actor_id=post_actor
                                  reply_elt=reply_element
                                  set_replying />}
                        )
                    } else {
                        Either::Right(
                            view! {
                                <PostControls
                                    post_id
                                    actor_id=post_actor
                                    open_menu
                                    set_replying />

                            }
                        )
                    }
                }
            }
        </div>
    }
}
