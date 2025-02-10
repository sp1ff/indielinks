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

//! Integration tests for the del.icio.us API.
//!
//! Backend-agnostic test logic for the del.icio.us API goes here.
use std::sync::Arc;

use indielinks::{
    delicious::{
        GenericRsp, PostsAllRsp, PostsDatesRsp, PostsGetRsp, PostsRecentRsp, TagsGetRsp, UpdateRsp,
    },
    entities::{Post, Tagname, Username},
};

use chrono::Utc;
use libtest_mimic::Failed;
use reqwest::{
    header::{HeaderMap, HeaderValue},
    StatusCode, Url,
};

use crate::Helper;

/// Exercise the delicious API in a few simple ways (i.e. a "smoke test"); panic on failure.
pub async fn delicious_smoke_test(
    url: Url,
    username: Username,
    api_key: String,
    utils: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    utils.clear_posts(&username).await?;

    // Hit `/posts/update` with no posts
    let rsp = reqwest::get(url.join(&format!(
        "/api/v1/posts/update?auth_token={}:{}",
        username, api_key
    ))?)
    .await?;
    assert!(StatusCode::OK == rsp.status());
    let body = rsp.json::<GenericRsp>().await?;
    assert!(body.result_code == format!("{} has no posts, yet", username));

    // Now, hit `/posts/get` without an auth token; should be 401'd
    assert!(
        StatusCode::UNAUTHORIZED == reqwest::get(url.join("/api/v1/posts/get")?).await?.status()
    );

    // From here in, we'll use the Authorization header, so let's set-up a proper client:
    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}:{}", username, api_key))?,
    );

    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;

    // Hit it again-- should get a 200 OK, but an error message indicating that the test user has no
    // posts.
    let rsp = client.get(url.join("/api/v1/posts/get")?).send().await?;
    assert!(StatusCode::OK == rsp.status());
    let body = rsp.json::<GenericRsp>().await?;
    assert!(body.result_code == format!("{} has no posts, yet", username));

    // Add our first post...
    let rsp = client.get(url.join("/api/v1/posts/add?url=https://instapundit.com&description=Instapundit&tags=blog,daily,glenn-reynolds")
                         ?)
        .send().await?;
    assert!(StatusCode::CREATED == rsp.status());
    let body = rsp.json::<GenericRsp>().await?;
    assert!(body.result_code == "done");

    // add another...
    let rsp = client.get(url.join("/api/v1/posts/add?url=https://thefp.com&description=The%20Free%20Press&tags=news,daily,bari-weiss")
                         ?)
        .send().await?;
    assert!(StatusCode::CREATED == rsp.status());
    let body = rsp.json::<GenericRsp>().await?;
    assert!(body.result_code == "done");

    // and add a third:
    let rsp = client.get(url.join("/api/v1/posts/add?url=https://wsj.com&description=The%20Wall%20Street%20Journal&tags=news,daily,economy")
                         ?)
        .send().await?;
    assert!(StatusCode::CREATED == rsp.status());
    let body = rsp.json::<GenericRsp>().await?;
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

    let rsp = client
        .get(url.join("/api/v1/posts/delete?url=https://wsj.com")?)
        .send()
        .await?;
    assert!(StatusCode::OK == rsp.status());
    let body = rsp.json::<GenericRsp>().await?;
    assert!(body.result_code == "done");

    let rsp = client.get(url.join("/api/v1/tags/get")?).send().await?;
    assert!(StatusCode::OK == rsp.status());
    let body = rsp.json::<TagsGetRsp>().await?;
    let mut tags = body.map.into_iter().collect::<Vec<(Tagname, usize)>>();
    tags.sort_by(|lhs, rhs| lhs.0.cmp(&rhs.0));
    assert!(
        tags == vec![
            (Tagname::new("bari-weiss")?, 1usize),
            (Tagname::new("blog")?, 1usize),
            (Tagname::new("daily")?, 2usize),
            (Tagname::new("glenn-reynolds")?, 1usize),
            (Tagname::new("news")?, 1usize),
        ]
    );

    // Hit `/posts/update` once more
    let rsp = client.get(url.join("/api/v1/posts/update")?).send().await?;
    assert!(StatusCode::OK == rsp.status());
    let body = rsp.json::<UpdateRsp>().await?;
    let diff = Utc::now() - body.update_time;
    assert!(diff.num_seconds() < 1);

    // Finally, let's exercise `/posts/get` in a few ways:
    let rsp = client.get(url.join("/api/v1/posts/get")?).send().await?;
    assert!(StatusCode::OK == rsp.status());
    let body = rsp.json::<PostsGetRsp>().await?;
    assert!(body.posts.is_empty());

    let day = body.date.format("%Y-%m-%d").to_string();
    let rsp = client
        .get(url.join(&format!("/api/v1/posts/get?dt={}", day))?)
        .send()
        .await?;
    assert!(StatusCode::OK == rsp.status());

    let body = rsp.json::<PostsGetRsp>().await?;
    assert!(body.posts.len() == 2);

    // Let's try filtering on the basis of a few tags
    let rsp = client
        .get(url.join(&format!("/api/v1/posts/get?dt={}&tag=news", day))?)
        .send()
        .await?;
    assert!(StatusCode::OK == rsp.status());

    let body = rsp.json::<PostsGetRsp>().await?;
    assert!(body.posts.len() == 1);

    let rsp = client
        .get(url.join(&format!("/api/v1/posts/get?dt={}&tag=news,daily", day))?)
        .send()
        .await?;
    assert!(StatusCode::OK == rsp.status());

    let body = rsp.json::<PostsGetRsp>().await?;
    assert!(body.posts.len() == 1);

    let rsp = client
        .get(url.join(&format!(
            "/api/v1/posts/get?dt={}&tag=news,daily,splat",
            day
        ))?)
        .send()
        .await?;
    assert!(StatusCode::OK == rsp.status());

    let body = rsp.json::<PostsGetRsp>().await?;
    assert!(body.posts.is_empty());

    // Alright-- finally, get `/posts/dates`
    let rsp = client.get(url.join("/api/v1/posts/dates")?).send().await?;
    assert!(StatusCode::OK == rsp.status(), "status={}", rsp.status());

    let body = rsp.json::<PostsDatesRsp>().await?;
    assert!(body.dates.len() == 1);

    Ok(())
}

/// Test `/posts/recent`
pub async fn posts_recent(
    url: Url,
    username: Username,
    api_key: String,
    utils: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    utils.clear_posts(&username).await?;

    // OK, the game is to make a bunch of posts and then read 'em back
    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}:{}", username, api_key)).unwrap(/* Known good */),
    );

    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;
    // Add our first post...
    let rsp = client.get(url.join("/api/v1/posts/add?url=https://instapundit.com&description=Instapundit&tags=blog,daily,glenn-reynolds") ?)
        .send()
        .await?;
    assert!(StatusCode::CREATED == rsp.status());

    let rsp = client.get(url.join("/api/v1/posts/add?url=https://thefp.com&description=The%20Free%20Press&tags=news,daily,bari-weiss")
                         ?)
        .send().await?;
    assert!(StatusCode::CREATED == rsp.status());

    // and add a third:
    let rsp = client.get(url.join("/api/v1/posts/add?url=https://wsj.com&description=The%20Wall%20Street%20Journal&tags=news,daily,economy")
                         ?)
        .send().await?;
    assert!(StatusCode::CREATED == rsp.status());

    // OK, no matter what, those should be our most recent three posts
    let mut rsp = client
        .get(url.join("/api/v1/posts/recent?count=3")?)
        .send()
        .await?;

    // WORKAROUND: there appears to be a bug in ScyllaDB's Alternator implementation. The first time
    // I query by the `posts_by_posted` secondary local index, specifying a reverse scan, the query
    // fails with an internal error. The second time, however, it will succeed.
    //
    // You can replicate the bug by running `scylla-up`, then executing:
    //
    //     aws --endpoint-url=http://localhost:8042 dynamodb query \
    //     --table-name posts \
    //     --index-name posts_by_posted \
    //     --key-condition-expression "user_id=:id" \
    //     --expression-attribute-values '{":id":{"S":"9a1df092-cd69-4c64-91f7-b8fb4022ea49"}}' \
    //     --no-scan-index-forward
    //
    // The first time it will fail; the second it will succeed.

    if StatusCode::INTERNAL_SERVER_ERROR == rsp.status() {
        rsp = client
            .get(url.join("/api/v1/posts/recent?count=3")?)
            .send()
            .await?;
    }
    assert!(StatusCode::OK == rsp.status());

    let body = rsp.json::<PostsRecentRsp>().await?;
    assert!(body.posts.len() == 3);

    let post0: Post = body.posts[0].clone();
    assert!(&post0.url().to_string() == "https://wsj.com/");

    Ok(())
}

// Argh. See WOKRAROUND comment, above.
async fn workaround(client: &reqwest::Client, url: Url) -> Result<PostsAllRsp, Failed> {
    let mut rsp = client.get(url.clone()).send().await?;
    if rsp.status() == StatusCode::INTERNAL_SERVER_ERROR {
        rsp = client.get(url).send().await?;
    }
    Ok(rsp.json::<PostsAllRsp>().await?)
}

/// Test `/posts/all`
pub async fn posts_all(
    url: Url,
    username: Username,
    api_key: String,
    utils: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    utils.clear_posts(&username).await?;

    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}:{}", username, api_key))?,
    );

    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;
    let rsp = client
        .get(url.join("/api/v1/posts/add?url=https://foo1.com&description=Test%20site&tags=a,b,c")?)
        .send()
        .await?;
    assert!(StatusCode::CREATED == rsp.status());

    let rsp = client
        .get(url.join("/api/v1/posts/add?url=https://foo2.com&description=Test%20site&tags=a,b,c")?)
        .send()
        .await?;
    assert!(StatusCode::CREATED == rsp.status());

    let rsp = client
        .get(url.join("/api/v1/posts/add?url=https://foo3.com&description=Test%20site&tags=a,b,c")?)
        .send()
        .await?;
    assert!(StatusCode::CREATED == rsp.status());
    let rsp = client
        .get(url.join("/api/v1/posts/add?url=https://foo4.com&description=Test%20site&tags=a,b,c")?)
        .send()
        .await?;
    assert!(StatusCode::CREATED == rsp.status());

    let body = workaround(&client, url.join("/api/v1/posts/all")?).await?;
    assert!(body.posts.len() == 4);
    assert!(body.posts[0].url().to_string() == "https://foo4.com/");

    // pagination in-range: [1, 3)
    let body = workaround(&client, url.join("/api/v1/posts/all?start=1&results=2")?).await?;
    assert!(body.posts.len() == 2);
    assert!(body.posts[0].url().to_string() == "https://foo3.com/");
    assert!(body.posts[1].url().to_string() == "https://foo2.com/");

    // pagination out-of-range: [1, 11)
    let body = workaround(&client, url.join("/api/v1/posts/all?start=1&results=10")?).await?;
    assert!(body.posts.len() == 3);
    assert!(body.posts[0].url().to_string() == "https://foo4.com/");
    assert!(body.posts[1].url().to_string() == "https://foo3.com/");
    assert!(body.posts[2].url().to_string() == "https://foo2.com/");

    // pagination out-of-range: [5, 15)
    let body = workaround(&client, url.join("/api/v1/posts/all?start=5&results=10")?).await?;
    assert!(body.posts.is_empty());

    // partial pagination: [3..)
    let body = workaround(&client, url.join("/api/v1/posts/all?start=3")?).await?;
    assert!(body.posts.len() == 1);
    assert!(body.posts[0].url().to_string() == "https://foo4.com/");

    Ok(())
}

/// Test `/posts/rename` & `/posts/delete`
pub async fn tags_rename_and_delete(
    url: Url,
    username: Username,
    api_key: String,
    utils: Arc<dyn Helper + Send + Sync>,
) -> Result<(), Failed> {
    utils.clear_posts(&username).await?;

    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}:{}", username, api_key))?,
    );

    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;
    let rsp = client
        .get(url.join("/api/v1/posts/add?url=https://foo1.com&description=Test%20site&tags=a,b,c")?)
        .send()
        .await?;
    assert!(StatusCode::CREATED == rsp.status());

    let rsp = client
        .get(url.join("/api/v1/posts/add?url=https://foo2.com&description=Test%20site&tags=a,d,e")?)
        .send()
        .await?;
    assert!(StatusCode::CREATED == rsp.status());

    let rsp = client
        .get(url.join("/api/v1/posts/add?url=https://foo3.com&description=Test%20site&tags=a,e")?)
        .send()
        .await?;
    assert!(StatusCode::CREATED == rsp.status());
    let rsp = client
        .get(url.join("/api/v1/posts/add?url=https://foo4.com&description=Test%20site&tags=a")?)
        .send()
        .await?;
    assert!(StatusCode::CREATED == rsp.status());

    // OK-- sanity check: get the tag counts
    let rsp = client.get(url.join("/api/v1/tags/get")?).send().await?;
    assert!(StatusCode::OK == rsp.status());
    let body = rsp.json::<TagsGetRsp>().await?;
    let mut tags = body.map.into_iter().collect::<Vec<(Tagname, usize)>>();
    tags.sort_by(|lhs, rhs| lhs.0.cmp(&rhs.0));
    assert!(
        tags == vec![
            (Tagname::new("a")?, 4usize),
            (Tagname::new("b")?, 1usize),
            (Tagname::new("c")?, 1usize),
            (Tagname::new("d")?, 1usize),
            (Tagname::new("e")?, 2usize),
        ]
    );

    // Now let's rename "e" to "splat" and delete "b"
    let rsp = client
        .get(url.join("/api/v1/tags/rename?old=e&new=splat")?)
        .send()
        .await?;
    assert!(StatusCode::OK == rsp.status());
    let body = rsp.json::<GenericRsp>().await?;
    assert_eq!("done", body.result_code);

    let rsp = client
        .get(url.join("/api/v1/tags/delete?tag=b")?)
        .send()
        .await?;
    assert!(StatusCode::OK == rsp.status());
    let body = rsp.json::<GenericRsp>().await?;
    assert_eq!("done", body.result_code);

    let rsp = client.get(url.join("/api/v1/tags/get")?).send().await?;
    assert!(StatusCode::OK == rsp.status());
    let body = rsp.json::<TagsGetRsp>().await?;
    let mut tags = body.map.into_iter().collect::<Vec<(Tagname, usize)>>();
    tags.sort_by(|lhs, rhs| lhs.0.cmp(&rhs.0));
    assert!(
        tags == vec![
            (Tagname::new("a")?, 4usize),
            (Tagname::new("c")?, 1usize),
            (Tagname::new("d")?, 1usize),
            (Tagname::new("splat")?, 2usize),
        ]
    );

    Ok(())
}
