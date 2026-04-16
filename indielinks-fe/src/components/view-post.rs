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

#[derive(Debug, Snafu)]
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
    let send_like = Action::new_local(move |(id, actor): &(Url, Url)| {
        let api = api2.clone();
        let token = token.clone();
        let id2 = id.clone();
        let id = id.clone();
        let actor = actor.clone();
        let send_like = move || {
            let api = api.clone();
            let token = token.clone();
            let id = id.clone();
            let actor = actor.clone();
            async move {
                // Regrettably, at this time, this lambda has to return a `gloo_net::Error`.
                let token = token.get().ok_or(gloo_net::Error::GlooError(
                    "Missing token; this is a bug.".to_owned(),
                ))?;
                Request::post(&format!("{api}/api/v1/users/like"))
                    .header("Authorization", &format!("Bearer {token}"))
                    .json(&LikeRequest {
                        id: id.clone(),
                        actor: actor.clone(),
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
                Ok(_) => debug!("Liked post {id2}"),
                Err(err) => debug!("While liking post {id2}, {err}."),
            }
        }
    });

    let reply_element: NodeRef<html::Input> = NodeRef::new();
    let send_reply = Action::new_local(move |(id, actor): &(Url, Url)| {
        let api = api.clone();
        let token = token.clone();
        let id2 = id.clone();
        let id = id.clone();
        let actor = actor.clone();
        let send_reply = move || {
            let api = api.clone();
            let token = token.clone();
            let id = id.clone();
            let actor = actor.clone();
            let text = string_for_node_ref(&reply_element);
            async move {
                // Regrettably, at this time, this lambda has to return a `gloo_net::Error`.
                let token = token.get().ok_or(gloo_net::Error::GlooError(
                    "Missing token; this is a bug.".to_owned(),
                ))?;
                Request::post(&format!("{api}/api/v1/users/reply"))
                    .header("Authorization", &format!("Bearer {token}"))
                    .json(&ReplyRequest {
                        id: id.clone(),
                        actor: actor.clone(),
                        text,
                    })
                    .map_err(|err| gloo_net::Error::GlooError(format!("{err}")))?
                    .send()
                    .await
            }
        };
        async move {
            match send_with_retry(send_reply)
                .await
                .context(RequestSnafu)
                .and_then(error_for_status)
            {
                Ok(_) => debug!("Replied to post {id2}"),
                Err(err) => error!("While replying to post {id2}, {err}."),
            }
        }
    });

    let share_menu_id = MenuId {
        url: post.id.clone(),
        sort: DropdownSort::Share,
    };
    let misc_menu_id = MenuId {
        url: post.id.clone(),
        sort: DropdownSort::Miscellaneous,
    };

    let post_id = post.id.clone();
    let post_actor = post.actor.clone();
    view! {
        <div class="feed-item" style="display:flex; flex-direction: column; gap: 0.5rem;">
            <div class="feed-item-content" style="text-align: center;" inner_html=post.content></div>
            {
                move || {
                    let post_id = post_id.clone();
                    let post_actor = post_actor.clone();
                    let share_menu_id = share_menu_id.clone();
                    let misc_menu_id = misc_menu_id.clone();
                    if replying.get() {
                        Either::Left(view!{
                            <div class="feed-item-reply">
                                <div style="flex: 1 1 0; min-height 0; display: flex; flex-direction: column;">
                                    <input type="textarea" node_ref=reply_element style="flex: 1 1 0; resize: none; width: 100%;"/>
                                </div>
                                <div class="feed-item-reply-actions">
                                    <button on:click=move |_| {
                                        set_replying.set(false);
                                        send_reply.dispatch((post_id.clone(), post_actor.clone()));
                                    }>"send"</button>
                                    <button on:click=move |_| set_replying.set(false)>"cancel"</button>
                                </div>
                            </div>
                        })
                    } else {
                        Either::Right(
                            view! {
                                <div class="feed-item-actions">
                                    <button on:click=move |_| {send_like.dispatch((post_id.clone(), post_actor.clone()));}>"like"</button>
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
                        )
                    }
                }
            }
        </div>
    }
}
