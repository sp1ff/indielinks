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
// You should have received a copy of the GNU General Public License along with mpdpopm.  If not,
// see <http://www.gnu.org/licenses/>.

use indielinks::{
    delicious::{GenericRsp, TagsGetRsp, UpdateRsp},
    entities::{Tagname, Username},
};

use chrono::Utc;
use reqwest::{
    header::{HeaderMap, HeaderValue},
    StatusCode, Url,
};

/// Exercise the delicious API in a few simple ways (i.e. a "smoke test"); panic on failure.
pub fn delicious_smoke_test(url: &Url, username: &Username, api_key: &str) {
    // Hit `/posts/update` with no posts
    let rsp = reqwest::blocking::get(
        url.join(&format!(
            "/api/v1/posts/update?auth_token={}:{}",
            username, api_key
        ))
        .expect("Invalid URL"),
    );
    let rsp = rsp.expect("Request failed");
    println!("response: {:?}", rsp);
    assert!(StatusCode::OK == rsp.status());
    let body = rsp.json::<GenericRsp>().expect("Unexpected response");
    assert!(body.result_code == format!("{} has no posts, yet", username));

    // Hit `/posts/get` without an auth token; should be 401'd
    assert!(
        StatusCode::UNAUTHORIZED
            == reqwest::blocking::get(url.join("/api/v1/posts/get").expect("Invalid URL"))
                .expect("Failed get request")
                .status()
    );

    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}:{}", username, api_key)).unwrap(/* Known good */),
    );

    let client = reqwest::blocking::Client::builder().default_headers(headers).build().unwrap(/* Known good */);

    // Hit it again-- should get a 200 OK, but an error message indicating that the test user has no
    // posts.
    let rsp = client
        .get(url.join("/api/v1/posts/get").expect("Invalid URL"))
        .send();
    println!("response: {:?}", rsp);
    let rsp = rsp.expect("Request failed");
    assert!(StatusCode::OK == rsp.status());
    let body = rsp.json::<GenericRsp>().expect("Unexpected response");
    assert!(body.result_code == format!("{} has no posts, yet", username));

    // Add our first post
    let rsp = client.get(url.join("/api/v1/posts/add?url=https://instapundit.com&description=Instapundit&tags=blog,daily,glenn-reynolds").expect("Invalid URL"))
        .send();
    println!("response: {:?}", rsp);
    let rsp = rsp.expect("Request failed");
    assert!(StatusCode::CREATED == rsp.status());
    let body = rsp.json::<GenericRsp>().expect("Unexpected response");
    assert!(body.result_code == "done");

    // Add another
    let rsp = client.get(url.join("/api/v1/posts/add?url=https://thefp.com&description=The%20Free%20Press&tags=news,daily,bari-weiss").expect("Invalid URL"))
        .send();
    println!("response: {:?}", rsp);
    let rsp = rsp.expect("Request failed");
    assert!(StatusCode::CREATED == rsp.status());
    let body = rsp.json::<GenericRsp>().expect("Unexpected response");
    assert!(body.result_code == "done");

    // Add a third
    let rsp = client.get(url.join("/api/v1/posts/add?url=https://wsj.com&description=The%20Wall%20Street%20Journal&tags=news,daily,economy").expect("Invalid URL"))
        .send();
    println!("response: {:?}", rsp);
    let rsp = rsp.expect("Request failed");
    assert!(StatusCode::CREATED == rsp.status());
    let body = rsp.json::<GenericRsp>().expect("Unexpected response");
    assert!(body.result_code == "done");

    // RN, we have the following tags:
    // - blog
    // - daily : 3
    // - glenn-reynolds
    // - news : 2
    // - bari-weiss
    // - economy
    // Delete the wsj-- tag should now be
    // - blog
    // - daily : 2
    // - glenn-reynolds
    // - news : 1
    // - bari-weiss
    // - economy: 0
    let rsp = client
        .get(
            url.join("/api/v1/posts/delete?url=https://wsj.com")
                .expect("Invalid URL"),
        )
        .send();
    println!("response: {:?}", rsp);
    let rsp = rsp.expect("Request failed");
    assert!(StatusCode::OK == rsp.status());
    let body = rsp.json::<GenericRsp>().expect("Unexpected response");
    assert!(body.result_code == "done");

    let rsp = client
        .get(url.join("/api/v1/tags/get").expect("Invalid URL"))
        .send();
    println!("response: {:?}", rsp);
    let rsp = rsp.expect("Request failed");
    assert!(StatusCode::OK == rsp.status());
    let body = rsp.json::<TagsGetRsp>().expect("Unexpected response");
    let mut tags = body.map.into_iter().collect::<Vec<(Tagname, usize)>>();
    tags.sort_by(|lhs, rhs| lhs.0.cmp(&rhs.0));
    println!("tags: {:#?}", tags);
    assert!(
        tags == vec![
            (Tagname::new("bari-weiss").unwrap(), 1usize),
            (Tagname::new("blog").unwrap(), 1usize),
            (Tagname::new("daily").unwrap(), 2usize),
            (Tagname::new("economy").unwrap(), 0usize),
            (Tagname::new("glenn-reynolds").unwrap(), 1usize),
            (Tagname::new("news").unwrap(), 1usize),
        ]
    );

    // Hit `/posts/update` once more
    let rsp = client
        .get(url.join("/api/v1/posts/update").expect("Invalid URL"))
        .send();
    let rsp = rsp.expect("Request failed");
    println!("response: {:?}", rsp);
    assert!(StatusCode::OK == rsp.status());
    let body = rsp.json::<UpdateRsp>().expect("Unexpected response");
    let diff = Utc::now() - body.update_time;
    assert!(diff.num_seconds() < 1);
}
