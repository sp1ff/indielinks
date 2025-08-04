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

use http::header::CONTENT_TYPE;
use indielinks_cache::raft::Metrics;
use libtest_mimic::Failed;
use reqwest::blocking::{Client, ClientBuilder};
use tracing::debug;

use indielinks_cache_test::{CacheInsertRequest, CacheLookupRequest, CacheLookupResponse};

fn get_metrics(client: &Client, port: u16) -> Result<Metrics, Failed> {
    Ok(client
        .get(format!("http://127.0.0.1:{port}/admin/metrics"))
        .send()?
        .error_for_status()?
        .json::<Metrics>()?)
}

// Not sure why rustc complains these test functions are never used.
#[allow(dead_code)]
pub fn test(base_port: u16) -> Result<(), Failed> {
    // We expect a five-node cluster to be up & ready to take traffic on localhost, port `base_port`
    // through `base_port` + 4.
    let client = ClientBuilder::new()
        .user_agent("indielinks-cache-test/0.0.1")
        .build()?;
    let metrics = get_metrics(&client, base_port)?;
    assert_eq!(metrics.raft.id, 0); // sanity check
    // Raft cluster not initialized => no leader.
    assert_eq!(metrics.raft.current_leader, None);

    debug!("Initializing a three-node cluster");
    client
        .post(format!("http://127.0.0.1:{base_port}/admin/init"))
        .header(CONTENT_TYPE, "application/json")
        .json::<Vec<(u64, String)>>(&vec![
            (0, format!("127.0.0.1:{}", base_port)),
            (1, format!("127.0.0.1:{}", base_port + 1)),
            (2, format!("127.0.0.1:{}", base_port + 2)),
        ])
        .send()?
        .error_for_status()?;

    // Pretty-sure Raft initialization is taking place async-- may need to wait here until the
    // metrics report a non-None leader.
    debug!("Lookup the value corresponding to key \"foo\"");
    let rsp = client
        .get(format!("http://127.0.0.1:{}/cache/lookup", base_port + 1))
        .header(CONTENT_TYPE, "application/json")
        .json(&CacheLookupRequest {
            cache: 1,
            key: serde_json::to_value("foo")?,
        })
        .send()?
        .error_for_status()?
        .json::<CacheLookupResponse>()?;
    assert_eq!(rsp.value, None);

    debug!("Insert \"foo\" :=> 11");
    client
        .post(format!("http://127.0.0.1:{}/cache/insert", base_port + 2))
        .header(CONTENT_TYPE, "application/json")
        .json(&CacheInsertRequest {
            cache: 1,
            key: serde_json::to_value("foo")?,
            value: serde_json::to_value(11)?,
        })
        .send()?
        .error_for_status()?
        .json::<()>()?;
    debug!("Lookup the value corresponding to key \"foo\"");
    let rsp = client
        .get(format!("http://127.0.0.1:{}/cache/lookup", base_port))
        .header(CONTENT_TYPE, "application/json")
        .json(&CacheLookupRequest {
            cache: 1,
            key: serde_json::to_value("foo")?,
        })
        .send()?
        .error_for_status()?
        .json::<CacheLookupResponse>()?;
    assert_eq!(
        rsp.value
            .map(|val| serde_json::from_value::<usize>(val))
            .transpose()?,
        Some(11)
    );

    debug!("Add nodes 3 & 4 to the Raft as learners");
    // You have to take care to make this call to the *leader*...
    client
        .post(format!("http://127.0.0.1:{}/admin/add-learner", base_port))
        .header(CONTENT_TYPE, "application/json")
        .json::<(u64, String)>(&(3, format!("127.0.0.1:{}", base_port + 3)))
        .send()?
        .error_for_status()?;
    client
        .post(format!("http://127.0.0.1:{}/admin/add-learner", base_port))
        .header(CONTENT_TYPE, "application/json")
        .json::<(u64, String)>(&(4, format!("127.0.0.1:{}", base_port + 4)))
        .send()?
        .error_for_status()?;

    std::thread::sleep(std::time::Duration::from_secs(2));

    debug!("Update Raft membership");
    // along with this one.
    client
        .post(format!("http://127.0.0.1:{base_port}/admin/membership",))
        .header(CONTENT_TYPE, "application/json")
        .json(&vec![0, 1, 2, 3, 4])
        .send()?
        .error_for_status()?;

    debug!("Lookup the value corresponding to key \"foo\"");
    let rsp = client
        .get(format!("http://127.0.0.1:{}/cache/lookup", base_port + 3))
        .header(CONTENT_TYPE, "application/json")
        .json(&CacheLookupRequest {
            cache: 1,
            key: serde_json::to_value("foo")?,
        })
        .send()?
        .error_for_status()?
        .json::<CacheLookupResponse>()?;
    // This will invalidate the cache
    assert_eq!(
        rsp.value
            .map(|val| serde_json::from_value::<usize>(val))
            .transpose()?,
        None
    );

    Ok(())
}

#[allow(dead_code)]
pub fn single_node(port: u16) -> Result<(), Failed> {
    // We expect a single-node cluster to be up & ready to take traffic on localhost, listening on port `port`.
    let client = ClientBuilder::new()
        .user_agent("indielinks-cache-test/0.0.1")
        .build()?;
    let metrics = get_metrics(&client, port)?;
    assert_eq!(metrics.raft.id, 0); // sanity check
    // Raft cluster not initialized => no leader.
    assert_eq!(metrics.raft.current_leader, None);

    debug!("Initializing a single-node cluster");
    client
        .post(format!("http://127.0.0.1:{port}/admin/init"))
        .header(CONTENT_TYPE, "application/json")
        .json::<Vec<(u64, String)>>(&vec![(0, format!("127.0.0.1:{}", port))])
        .send()?
        .error_for_status()?;

    // Pretty-sure Raft initialization is taking place async-- may need to wait here until the
    // metrics report a non-None leader.
    debug!("Lookup the value corresponding to key \"foo\"");
    let rsp = client
        .get(format!("http://127.0.0.1:{port}/cache/lookup"))
        .header(CONTENT_TYPE, "application/json")
        .json(&CacheLookupRequest {
            cache: 1,
            key: serde_json::to_value("foo")?,
        })
        .send()?
        .error_for_status()?
        .json::<CacheLookupResponse>()?;
    assert_eq!(rsp.value, None);

    debug!("Insert \"foo\" :=> 11");
    client
        .post(format!("http://127.0.0.1:{port}/cache/insert"))
        .header(CONTENT_TYPE, "application/json")
        .json(&CacheInsertRequest {
            cache: 1,
            key: serde_json::to_value("foo")?,
            value: serde_json::to_value(11)?,
        })
        .send()?
        .error_for_status()?
        .json::<()>()?;

    debug!("Lookup the value corresponding to key \"foo\"");
    let rsp = client
        .get(format!("http://127.0.0.1:{port}/cache/lookup"))
        .header(CONTENT_TYPE, "application/json")
        .json(&CacheLookupRequest {
            cache: 1,
            key: serde_json::to_value("foo")?,
        })
        .send()?
        .error_for_status()?
        .json::<CacheLookupResponse>()?;
    assert_eq!(
        rsp.value
            .map(|val| serde_json::from_value::<usize>(val))
            .transpose()?,
        Some(11)
    );

    Ok(())
}
