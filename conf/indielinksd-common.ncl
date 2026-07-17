# Copyright (C) 2026 Michael Herstine <sp1ff@pobox.com>
# 
# This file is part of indielinks.
# 
# indielinks is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
# 
# indielinks is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along with
# indielinks. If not, see <http://www.gnu.org/licenses/>.
#
#
# This file contains source code in the Nickel <https://nickel-lang.org/>
# language. Nickel is a gradually-typed configuration generation language. For
# more information on how Nickel is used in this project, see
# `indielinks/src/_docs.rs`. The assorted TOML configuration files are generated
# in the salient `build.rs` files.
#
# This particular Nickel file contains configuration for the `indielinksd`
# daemon that is fixed & unchanging across all generated variants. It is
# currently untyped, because, at the time of this writing, I am just beginning
# the conversion from hand-authored TOML to Nickel. I plan to gradually
# introduce typing.
let peppers = import "peppers.ncl" in
{
    version = "1",
    user-agent = "indielinks/0.0.1; +sp1ff@pobox.com",
    collection-page-size = 64,
    otlp-export = { endpoint = "http://localhost:9090/api/v1/otlp/v1/metrics" },
    pepper = peppers,
    signing-keys = {
        signing-keys = {
            keys = {
                "keyid:20250214" = [
                    207, 30, 37, 178, 222, 42, 68, 222, 209, 206, 199, 250, 219, 94, 3, 163,
                    30, 21, 161, 140, 1, 170, 140, 187, 185, 228, 47, 177, 215, 11, 196, 152, 
                    221, 128, 208, 77, 104, 247, 28, 178, 207, 241, 6, 239, 71, 11, 22, 221,
                    105, 152, 60, 109, 121, 214, 201, 12, 252, 96, 19, 160, 95, 124, 100, 234
               ]
            } 
        },
        token-lifetime = [300, 0], # seconds, nanoseconds => 5 min
        refresh-token-lifetime = [129600, 0] # seconds, nanoseconds => 36 hours
    },
    users-config = {
        same-site = "Lax",
        secure-cookies = true,
        allowed-origins = [
            {
                scheme = "http",
                port = 18080,
                host = { RegName = "localhost" }
            },
            {
                scheme = "http",
                port = 18080,
                host = { Ipv4Address = "127.0.0.1" }
            },
            {
                scheme = "http",
                port = 20676,
                host = { RegName = "localhost" }
            },
            {
                scheme = "http",
                port = 20676,
                host = { Ipv4Address = "127.0.0.1" }
            },
            {
                scheme = "http",
                port = 18443,
                host = { RegName = "localhost" }
            },
            {
               scheme = "http",
               port = 18443,
               host = { Ipv4Address = "127.0.0.1" }
            } 
        ] 
    },
    background-tasks = {
        default-timeout = { secs = 5, nanos = 0 },
        max-concurrent-tasks = 16,
        sleep-duration = { secs = 1, nanos = 0 },
        shutdown-timeout = { secs = 0, nanos = 500000000 },
        pickup-timeout = { secs = 1, nanos = 0 }
    },
    raft-config = {
        cluster-name = "indielinks",
        heartbeat-interval = { secs = 0, nanos = 500000000 },
        election-timeout-min = { secs = 0, nanos = 1500000000 },
        election-timeout-max = { secs = 0, nanos = 3000000000 }
    },
    client-exponential-backoff = {
        jitter = 10,
        num-attempts = 3,
        durations = { min = { secs = 1, nanos = 0 }, max = { secs = 3, nanos = 0 } }
    },
    client-rate-limits = { per-hour = 5760, custom = [] },
    local-rate-limits = { per-hour = 8320, custom = [] },
    general-purpose-rate-limits = { per-hour = 2880, custom = [] }
}
