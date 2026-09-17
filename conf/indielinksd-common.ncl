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
#
# This file deliberately contains no secrets: peppers & signing keys are
# merged into development configurations by `indielinks/build.rs` (from
# `dev-peppers.ncl` & `dev-signing-keys.ncl`), while production configurations
# reference their secrets by SSM parameter name.
{
    version = "1",
    user-agent = "indielinks/0.0.1; +sp1ff@pobox.com",
    collection-page-size = 64,
    otlp-export = { endpoint = "http://localhost:9090/api/v1/otlp/v1/metrics" },
    signing-keys = {
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
        shutdown-timeout = { secs = 0, nanos = 500000000 }, # 500ms
        pickup-timeout = { secs = 1, nanos = 0 }
    },
    raft-config = {
        cluster-name = "indielinks",
        # These are 4x the defaults-- setting the heartbeat interval to 50ms
        # makes for *very* chatty logs when openraft logging is turned on.
        heartbeat-interval = { secs = 0, nanos = 200000000 },   # 200ms
        election-timeout-min = { secs = 0, nanos = 600000000 }, # 600ms
        election-timeout-max = { secs = 1, nanos = 200000000 }, # 1.2s
        snapshot-policy | default = { LogsSinceLast = 5000 }
    },
    client-exponential-backoff = {
        jitter = 10,
        num-attempts = 3,
        durations = { min = { secs = 1, nanos = 0 }, max = { secs = 3, nanos = 0 } }
    },
    client-configuration = {
        rate-limits = { per-hour = 5760, custom = [] },
        timeout = [5, 0] # five seconds, zero nanoseconds
    },
    local-client-configuration = {
        rate-limits = { per-hour = 8320, custom = [] },
        timeout = [2, 0] # two seconds, zero nanoseconds
    },
    general-purpose-client-configuration = {
        rate-limits = { per-hour = 2880, custom = [] },
        timeout = [3, 0] # three seconds, zero nanoseconds
    }
}
