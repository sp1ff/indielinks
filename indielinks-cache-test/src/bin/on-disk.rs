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

use std::{io, net::SocketAddrV4, str::FromStr};

use bpaf::{Parser, construct};
use indielinks_cache::types::NodeId;
use tracing::{Level, info, subscriber::set_global_default};
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};

#[derive(Debug)]
struct Options {
    no_color: bool,
    verbose: bool,
    _id: NodeId,
    _addr: SocketAddrV4,
}

fn options() -> impl Parser<Options> {
    construct!(Options {
        no_color(bpaf::short('c').long("no-color").help("Disable logging in color").switch()),
        verbose(bpaf::short('v').long("verbose").help("Increase the verbosity").switch()),
        _id(bpaf::positional::<NodeId>("ID").help("Node ID, expressed as an unsigned integer")),
        _addr(bpaf::positional::<String>("SOCKADDR").parse(|s| SocketAddrV4::from_str(&s)))
    })
}

#[tokio::main]
async fn main() {
    let opts = options().to_options().run();
    set_global_default(
        Registry::default()
            .with(
                fmt::Layer::default()
                    .compact()
                    .with_ansi(!opts.no_color)
                    .with_writer(io::stdout),
            )
            .with(
                EnvFilter::builder()
                    .with_default_directive(if opts.verbose {
                        Level::DEBUG.into()
                    } else {
                        Level::INFO.into()
                    })
                    .from_env()
                    .expect("Failed to retrieve RUST_LOG"),
            ),
    )
    .expect("Failed to set the global default tracing subscriber");

    info!("Logging initialized");

    panic!("Write me!")
}
