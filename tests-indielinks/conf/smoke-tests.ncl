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
# This particular Nickel file contains the code for producing configuration
# files for the `smoke-tests` integration test.
let runner-record = import "runner.ncl" in
let peppers = import "peppers.ncl" in
let mk-scylla-backend = import "test-scylla-backend.ncl" in
let mk-alternator-backend = import "test-alternator-backend.ncl" in
fun stack => {
    runner = runner-record,
    domain = {
        scylla-env-file = "%{stack.stack-name}.env",
        username = "sp1ff",
        api-key = "v1:5eb56ceebb7425aafe36eabca8b923054b4907d9375acd0b9950c51b57b201fb73e437428050f451b57632f99a3bbd5bed1c0f51cc0df752147090ed26e975f4",
        pepper = peppers,
        single-node = {
            indielinks = "http://indiemark.local:%{stack.public-port}",
            ops = "http://localhost:%{stack.private-port}",
            grpc = "127.0.0.1:%{stack.grpc-port}",
            # This needs to agree with the `log-file` item in
            # `indielinksd-pre-alpha-{alternator,scylla}.toml` (which it should now
            # that we're both using Nickel)
            local-state-dir = stack.local-state-dir,
            indielinks-alternator-config-file = "../target/conf/%{stack.stack-name}/indielinksd-%{stack.stack-name}-alternator.toml",
            indielinks-scylla-config-file = "../target/conf/%{stack.stack-name}/indielinksd-%{stack.stack-name}-scylla.toml"
        },
        clustered = {
            indielinks-scylla-cluster-config-base = "../target/conf/%{stack.stack-name}/indielinksd-scylla-",
            indielinks-alternator-cluster-config-base = "../target/conf/%{stack.stack-name}/indielinksd-alternator-",
            nodes = [
                ["http://indiemark.local:%{stack.public-port}",
                 "http://127.0.0.1:%{stack.private-port}",
                 "127.0.0.1:%{stack.grpc-port}"],
                ["http://indiemark.local:%{stack.public-port + 3}",
                 "http://127.0.0.1:%{stack.private-port + 3}",
                 "127.0.0.1:%{stack.grpc-port + 3}"],
                ["http://indiemark.local:%{stack.public-port + 6}",
                 "http://127.0.0.1:%{stack.private-port + 6}",
                 "127.0.0.1:%{stack.grpc-port + 6}"]],
            cluster-size = 3,
            haproxy-id = stack.haproxy-id,
            haproxy-port = stack.haproxy-port,
            # This needs to agree with the `log-file` item in
            # `indielinksd-{alternator,scylla}-{0,1,2,3,4}.toml` (which it should
            # now that we're using Nickel)
            cluster-local-state-base = stack.cache-state-dir-prefix

        },
        scylla = mk-scylla-backend stack,
        dynamo = mk-alternator-backend stack
    }
}

