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

use gloo_net::http::Request;
use leptos::{IntoView, component, context::use_context, html, prelude::*, view};
use leptos_router::hooks::{use_location, use_navigate, use_query_map};
use tracing::{debug, error, info};

use indielinks_shared::{api::PostAddReq, entities::StorUrl};

use crate::{
    http::string_for_status,
    types::{Api, Base, Token, USER_AGENT},
};

/// Add a link
#[component]
pub fn AddLink() -> impl IntoView {
    debug!("AddLink invoked.");

    let api = use_context::<Api>()
        .expect("Failed to rerieve the API net location")
        .0;

    let base = use_context::<Base>().expect("No base!?").0;

    let token = use_context::<Token>()
        .expect("No token Cell!?")
        .get_untracked()
        .expect("No token!?");

    let loc = use_location();
    let from = move || {
        let st = loc.state.get().to_js_value(); // JsValue
        web_sys::js_sys::Reflect::get(&st, &"from".into())
            .ok()
            .and_then(|v| v.as_string())
    };

    let navigate = use_navigate();

    let query = use_query_map();

    let url_element: NodeRef<html::Input> = NodeRef::new();
    let title_element: NodeRef<html::Input> = NodeRef::new();
    let notes_element: NodeRef<html::Textarea> = NodeRef::new();
    let tags_element: NodeRef<html::Input> = NodeRef::new();
    let private_element: NodeRef<html::Input> = NodeRef::new();
    let unread_element: NodeRef<html::Input> = NodeRef::new();
    let another_element: NodeRef<html::Input> = NodeRef::new();

    let on_submit = Action::new_local(move |_: &()| {
        let api = api.clone();
        let token = token.clone();
        let navigate = navigate.clone();
        let from = from.clone();
        let base = base.clone();
        async move {
            // This seems *really* painful... is there no way to make this more elegant? Also, it
            // largely replicates the logic in `EditPost`
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
                    info!("Saved post successfully!");
                }
                Err(err) => {
                    error!("{err:?}");
                }
            }

            let add_another = another_element
                .get()
                .expect("add another should be mounted")
                .checked();

            if add_another {
                url_element.get().map(|elt| elt.set_value(""));
                title_element.get().map(|elt| elt.set_value(""));
                notes_element.get().map(|elt| elt.set_value(""));
                tags_element.get().map(|elt| elt.set_value(""));
                private_element.get().map(|elt| elt.set_checked(false));
                unread_element.get().map(|elt| elt.set_checked(false));
            } else {
                let home = format!("{}/h", base);
                navigate(
                    from().as_deref().unwrap_or(home.as_str()),
                    Default::default(),
                );
            }
        }
    });

    view! {
        <form class="add-post" on:submit = move |ev| {
                ev.prevent_default();
                on_submit.dispatch(());
            }>
            <p>"Add Post"</p>
            <div style="font-size: smaller; color: #276173;">
                <div style="margin-bottom: 3px;">
                    <label for="url" style="font-size: smaller; color: #276173;">URL</label>
                </div>
                <div style="margin-bottom: 8px;">
                    <input type="text" id="url" name="url" value=move || query.read().get("url") node_ref=url_element required style="width: 70%;"/>
                </div>
                <div style="margin-bottom: 3px;">
                    <label for="title" style="font-size: smaller; color: #276173">title</label>
                </div>
                <div style="margin-bottom: 8px;">
                    <input type="text" id="title" name="title" value=move || query.read().get("title") node_ref=title_element required style="width: 70%;"/>
                </div>
                <div style="margin-bottom: 3px;">
                    <label for="notes" style="font-size: smaller; color: #276173">notes</label>
                </div>
                <div style="margin-bottom: 8px;">
                    <textarea id="notes" name="notes" node_ref=notes_element style="width: 70%;" rows="4">
                        {move || query.read().get("notes")}
                    </textarea>
                </div>
                <div style="margin-bottom: 3px;">
                    <label for="tags" style="font-size: smaller; color: #276173">tags</label>
                </div>
                <div style="margin-bottom: 8px;">
                    <input type="text" id="tags" name="tags" value=move || query.read().get("tags") node_ref=tags_element style="width: 70%;"/>
                </div>
                <div style="margin-bottom: 16px; font-size: smaller;">
                    <label>
                        <input type="checkbox" id="private" name="private" checked=move || query.read().get("private") node_ref=private_element/> private
                    </label>
                    " "
                    <label>
                        <input type="checkbox" id="unread" name="unread" checked=move || query.read().get("unread") node_ref=unread_element/> unread
                    </label>
                </div>
                <div style="display: flex; align-items: center; gap: 2.0rem;">
                    <input type="submit" value="save"/>
                    <label style="display: inline-flex; align-items: center;">
                        <input type="checkbox" id="another" name="another" checked=move || query.read().get("another") node_ref=another_element/>
                        "add another"
                    </label>
                </div>
            </div>
        </form>
    }
}
