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
//!
//! Displaying a given [FeedPost] turns out to be fairly complicated when taking into account likes,
//! shares, replies &c, as well as the fact that you can "drill down" into any given post to see the
//! conversation. This module hosts a top-level component, [Post] that handles all of this.
//!
//! While these have been factored-out into their own module, I've made little-to-no effort to make
//! these truly generic components: they're styled directly here, they show toast on errors, &c.

use std::{cmp::PartialEq, result::Result as StdResult, sync::Arc};

use gloo_net::http::Request;
use leptos::{either::Either, html, prelude::*};
use snafu::{ResultExt, Snafu};
use thaw::{Icon, Toast, ToastBody, ToastIntent, ToastOptions, ToastTitle, ToasterInjection};
use tracing::{debug, error};
use url::Url;

use indielinks_shared::api::{
    FeedPost, LikeRequest, ReplyRequest, ThreadContextRequest, ThreadContextResponse,
};

use crate::{
    components::dropdown::{Dropdown, DropdownIconTrigger, DropdownMenuItem, DropdownMenuItems},
    http::send_with_retry,
    types::Api,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

// I've made this type `Clone` by wrapping source errors in `Arc`s. I'm not sure I like this, but
// being `Clone` allows me to propagate errors out of `Action`s & `Resource`s. Two other
// possibilities:
//
// 1. stringify the error at the "reactive boundry"; that is, bubble-up the strongly-typed error
//    until you're ready to return from an `Actoin` or `Resource`, then convert from `Result<T>` to
//    `StdResult<T, String>`.
//
// 2. define a parallel (and `Clone`) error type to be used in the UI; implement `From<Error>` for
//    it; I think I like this one, but I want to get more experience in *handling* errors in the UI,
//    first.
#[derive(Clone, Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    #[snafu(display("While deserializing the initial timeline, {source}"))]
    Load {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    Refresh,
    #[snafu(display("While sending an HTTP request, {source}"))]
    Request {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    #[snafu(display("While sending an HTTP request, {source}"))]
    Request1 {
        source: crate::http::Error,
    },
    #[snafu(display("Got response status {status}"))]
    Status {
        status: u16,
    },
}

pub type Result<T> = StdResult<T, Error>;

// This is duplicated from `feeds.rs`; I'm going to refactor as a part of a general re-think of my
// HTTP handling.
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

fn pop_toast(toaster: ToasterInjection, intent: ToastIntent, title: String, message: String) {
    toaster.dispatch_toast(
        move || {
            view! {
                <Toast>
                    <ToastTitle>{title}</ToastTitle>
                    <ToastBody>
                        {message}
                    </ToastBody>
                </Toast>
            }
        },
        ToastOptions::default().with_intent(intent),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            ViewPost                                            //
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

// It's a pity to represent both the post & actor using `Url`-- could I use newtypes to distinguish
// between the two? I have a task to look at this more generally on the backend.
fn use_replying(post_id: Url, actor_id: Url) -> Action<String, Result<()>> {
    // This seems awfully complex, but the closure we pass to the `Action` constructor must
    // implement `Fn`; i.e. all of its captures must either be `Copy` or moved into the closure.

    // That wouldn't be so bad, except this closure needs *another* closure (that it will pass to
    // `send_with_retry()`), and *that* one must also be `Fn`!
    Action::new_local(move |text: &String| {
        // Now, both `post_id` and `actor_id`, being referenced inside this block, have bee *moved*
        // here. I would have thought that we could just move them again into the next closure, but
        // the if I do that, without keeping copies here, the borrow checker complains. So:
        let post_id = post_id.clone();
        let actor_id = actor_id.clone();
        let text = text.clone();
        async move {
            send_with_retry(
                move || {
                    let api = expect_context::<Api>().0;
                    Request::post(&format!("{api}/api/v1/users/reply"))
                },
                ReplyRequest {
                    // taking care to clone them before using (so that this block can
                    // be invoked again, if needed).
                    id: post_id.clone(),
                    actor: actor_id.clone(),
                    text: text.clone(),
                },
            )
            .await
            .context(Request1Snafu)
            .and_then(error_for_status)
            .map(|_| ())
        }
    })
}

#[component]
fn ReplyingPost(
    // I tried making this borrows, but the compiler insisted that "this function's return type
    // contains a borrowed value, but there is no value for it to be borrowed from"... which makes
    // no sense to me. It has nothing to do with the `Action` returned from `use_replying()`-- the
    // error was induced *just by making `post_id` (or `actor_id`) be passed by reference*, even if
    // I never called `use_replying()`-- there must be something in the view! macro doing this.
    post_id: Url,
    actor_id: Url,
    reply_elt: NodeRef<html::Textarea>,
    set_replying: WriteSignal<bool>,
) -> impl IntoView {
    let send_reply = use_replying(post_id, actor_id);

    let toaster = ToasterInjection::expect_context();

    Effect::new(move |_| {
        if let Some(Err(err)) = send_reply.value().get() {
            pop_toast(
                toaster,
                ToastIntent::Error,
                "Replying".into_owned(),
                format!("{err}"),
            )
        }
    });

    // I need to factor this out.
    fn string_for_node_ref(node: &NodeRef<html::Textarea>) -> String {
        node.get().expect("NodeRef not mounted?").value()
    }

    view! {
        <div class="flex flex-col">
            <textarea
                   rows="4"
                   node_ref=reply_elt
                   placeholder="Your reply..."
                   class="bg-transparent border-0 border-b border-r outline-none"/>
            <div class="space-x-2">
                <Icon icon=icondata::BsSend
                      class="text-gray-600"
                      on_click=move |_| {
                          let text = string_for_node_ref(&reply_elt);
                          set_replying.set(false);
                          send_reply.dispatch(text);
                      }/>
                <Icon icon=icondata::TbSendOffOutline
                      class="text-gray-600"
                      on_click=move |_| set_replying.set(false) />
            </div>
        </div>
    }
}

fn use_post_controls(post_id: Url, actor_id: Url) -> Action<(), Result<()>> {
    Action::new_local(move |_: &()| {
        // Both `post_id` and `actor_id`, being referenced inside this block, have bee *moved*
        // here. I would have thought that we could just move them again into the next closure, but
        // the if I do that, without keeping copies here, the borrow checker complains. So:
        let post_id = post_id.clone();
        let actor_id = actor_id.clone();
        async move {
            send_with_retry(
                move || {
                    let api = expect_context::<Api>().0;
                    Request::post(&format!("{api}/api/v1/users/like"))
                },
                LikeRequest {
                    id: post_id.clone(),
                    actor: actor_id.clone(),
                },
            )
            .await
            .context(Request1Snafu)
            .and_then(error_for_status)
            .map(|_| ())
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

    let toaster = ToasterInjection::expect_context();

    Effect::new(move |_| {
        if let Some(Err(err)) = send_like.value().get() {
            pop_toast(
                toaster,
                ToastIntent::Error,
                "Liking".into_owned(),
                format!("{err}"),
            )
        }
    });

    view! {
        <div class="text-sm">
            <Icon icon=icondata::AiStarOutlined
                class="text-gray-600 cursor-pointer"
                on_click=move |_| { send_like.dispatch(()); }
                />
            " "
            <Dropdown open_menu>
                <DropdownIconTrigger
                    icon=icondata::ChQuote
                    class="text-gray-600 cursor-pointer"
                    menu_id=share_menu_id.clone() />
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
            <Icon icon=icondata::BsReply
                class="text-gray-600 cursor-pointer"
                on_click=move |_| set_replying.set(true)
                />
            " "
            <Dropdown open_menu>
                <DropdownIconTrigger
                    icon=icondata::BsThreeDots
                    class="text-gray-600 cursor-pointer"
                    menu_id=misc_menu_id.clone() />
                <DropdownMenuItems menu_id=misc_menu_id.clone()>
                    <DropdownMenuItem
                        text="copy link".to_string()
                        handler=Callback::new(|()| debug!("Copy link selected"))/>
                </DropdownMenuItems>
            </Dropdown>
        </div>
    }
}

/// `ViewPost` is a component that displays a single post at a time, with the possibility of
/// replying.
#[component]
fn ViewPost(
    post: FeedPost,
    open_menu: RwSignal<Option<MenuId>>,
    #[prop(default = false)] current: bool,
    #[prop(optional)] on_click: Option<Callback<()>>,
) -> impl IntoView {
    let (replying, set_replying) = signal::<bool>(false);

    let reply_element: NodeRef<html::Textarea> = NodeRef::new();
    let post_id = post.id.clone();
    let post_actor = post.actor.clone();

    // Turn off the the cursor pointer if this is the currently focused post:
    let mut cls = "text-left m-2 text-gray-800 cursor-pointer".to_owned();
    if current {
        cls += " cursor-pointer";
    }

    view! {
        <div class="mx-auto flex flex-col m-2 p-2 border border-solid border-sky-100">
            <div
                class={cls}
                inner_html=post.content
                on:click=move |_| {
                    if let Some(cb) = on_click {
                        cb.run(());
                    }
                }
            >
            </div>
            // I should really be using `<Show>` here, but it's not clear to me
            // how to handle cloning `post_id` and `actor_id` in such a way as to still
            // memoize each view.
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

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        ViewConversation                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Creates a reactive stack of `ThreadContextResponse` values.
///
/// This is a Leptos hook in the SolidJS sense: a plain function that allocates
/// reactive state and returns it to the caller.  The component that calls it
/// owns the stack for its lifetime; nothing outside the component can observe
/// or mutate it.
///
/// The stack is a `Vec` used in the traditional sense — push to navigate
/// forward, pop to go back.  The *current* conversation node is always the
/// last element.  We start with an empty vec; the component pushes the initial
/// response once the Action resolves.
pub fn use_conversation_stack() -> RwSignal<Vec<ThreadContextResponse>> {
    RwSignal::new(vec![])
}

async fn get_context(url: Url) -> Result<ThreadContextResponse> {
    send_with_retry(
        move || {
            let api = expect_context::<Api>().0;
            Request::post(&format!("{api}/api/v1/users/context"))
        },
        ThreadContextRequest { ap_id: url },
    )
    .await
    .context(Request1Snafu)
    .and_then(error_for_status)?
    .json::<ThreadContextResponse>()
    .await
    .context(LoadSnafu)
}

/// Renders a conversation thread centred on the post at the top of the stack.
///
/// # Props
///
/// - `show` — the parent's `WriteSignal<bool>`.  Set to `false` when the user
///   presses the back arrow while on the root node, returning to the button.
/// - `initial_url` — URL of the first post to display.  Dispatched to the
///   stub Action when the component first mounts.
#[component]
pub fn ViewConversation(
    open_menu: RwSignal<Option<MenuId>>,
    show: WriteSignal<bool>,
    initial_url: Url,
) -> impl IntoView {
    let action: Action<Url, Result<ThreadContextResponse>> = Action::new_local(move |url: &Url| {
        let url = url.clone();
        async move { get_context(url.clone()).await }
    });
    let stack = use_conversation_stack();

    // Kick off the initial data load.  The Leptos component function runs only
    // once per instance, so this dispatch happens exactly once.
    action.dispatch(initial_url);

    let toaster = ToasterInjection::expect_context();

    // Whenever the Action resolves — from the initial load or any subsequent
    // navigation dispatch — push the new response onto the stack.
    Effect::new(move |_| {
        // So this seems a bit dodgy to me... what if we have multiple invocations of `action` in
        // flight simultaneously?
        if let Some(response) = action.value().get() {
            match response {
                Ok(ctx) => {
                    debug!("Got a new context of {ctx:?}");
                    stack.update(|s| s.push(ctx));
                }
                Err(err) => {
                    error!("Failed to retrieve thread context: {err:#?}");
                    pop_toast(
                        toaster,
                        ToastIntent::Error,
                        "Conversation".into_owned(),
                        format!("{err}"),
                    )
                }
            }
        }
    });

    // Back-arrow handler.
    // When the stack has exactly one entry (the root), popping it would leave
    // an empty stack with nothing to render, so we dismiss the component
    // instead by flipping the parent's boolean signal back to false.
    let on_back = move |_| {
        // `get_untracked` avoids creating a reactive dependency inside an
        // event handler where we don't want re-subscription side effects.
        if stack.get_untracked().len() <= 1 {
            stack.update(|s| {
                s.pop();
            });
            show.set(false);
        } else {
            stack.update(|s| {
                s.pop();
            });
        }
    };

    view! {
        <div class="border border-solid border-sky-600"
            // style="display: flex; flex-direction: column; height: 100%; \
            //         overflow-y: auto; padding: 4px; box-sizing: border-box;"
            >

            <div /*style="margin-bottom: 4px;"*/>
                <Icon icon=icondata::BiArrowBackRegular
                      class="text-gray-600 cursor-pointer"
                      on_click=on_back />
            </div>

            {move || {
                match stack.get().last().cloned() {
                    None => view! { <div>"Loading…"</div> }.into_any(),

                    Some(ctx) => view! {
                        <div>
                            // Parent — clicking navigates to it (push onto stack).
                            {ctx.parent.map(|p| {
                                let url = p.id.clone();
                                let cb = Callback::new(move |_: ()| { action.dispatch(url.clone()); });
                                view! { <ViewPost post=p open_menu on_click=cb /> }
                            })}

                            // Focal post — inert, no click handler.
                            <ViewPost post=ctx.post open_menu current=true />

                            // Children — each click navigates into that child.
                            {ctx.children
                                .into_iter()
                                .map(|c| {
                                    let url = c.id.clone();
                                    let cb = Callback::new(move |_: ()| { action.dispatch(url.clone()); });
                                    view! { <ViewPost post=c open_menu on_click=cb /> }
                                })
                                .collect::<Vec<_>>()}
                        </div>
                    }.into_any(),
                }
            }}
        </div>
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                              Post                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
pub fn Post(post: FeedPost, open_menu: RwSignal<Option<MenuId>>) -> impl IntoView {
    let (show_convo, set_show_convo) = signal::<bool>(false);
    let cb = Callback::new(move |_: ()| set_show_convo.set(true));
    view! {
        <Show
            when=move || show_convo.get()
            fallback={
                let post = post.clone();
                move || view!{
                    <ViewPost post=post.clone() open_menu on_click=cb/>
                }
            }>
            <ViewConversation
                open_menu
                show=set_show_convo
                initial_url=post.clone().id />
        </Show>
    }
}
