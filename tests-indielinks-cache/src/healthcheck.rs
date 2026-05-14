// Copyright (C) 2025-2026 Michael Herstine <sp1ff@pobox.com>
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

use libtest_mimic::Failed;
use reqwest::blocking::ClientBuilder;

pub fn test(base_port: u16, num_nodes: u16) -> Result<(), Failed> {
    let client = ClientBuilder::new()
        .user_agent("indielinks-cache-test/0.0.1")
        .build()?;
    for i in 0..num_nodes {
        let port = base_port + i;
        let body = client
            .get(format!("http://127.0.0.1:{port}/healthcheck"))
            .send()?
            .error_for_status()?
            .text()?;
        assert_eq!(
            body, "GOOD",
            "unexpected healthcheck response from node {i} on port {port}"
        );
    }
    Ok(())
}

pub fn single_node(port: u16) -> Result<(), Failed> {
    test(port, 1)
}
