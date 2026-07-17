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
# This particular Nickel file sits at the very bottom of our Nickel
# dependencies. It defines a (typed) record representing an integration testing
# "stack" and evaluates to a function that returns an instance of that record
# given the parameters needed to define a particular stack. It is intended to be
# imported to other Nickel files like so:
#
#     # `stack-1.ncl`
#     let stack = import "stack.ncl" in
#     stack {
#         scylla-subnet = ...
#
# The resulting stack will presumably be used as input parameters to other
# records that actually configure various programs in this project.
#
# TODO(sp1ff): Add a contract asserting that
#     1) the port ranges don't overlap
#     2) the given addresses actually lie in the given subnet
let mk-stack : String ->
               String ->
               String ->
               String ->
               String ->
               Number ->
               Number ->
               Number ->
               Number ->
               Number ->
               Number ->
               Number ->
               { stack-name: String,
                 scylla-subnet: String,
                 scylla-node-1-address: String,
                 scylla-node-2-address: String,
                 scylla-node-3-address: String,
                 scylla-node-1: Number,
                 scylla-node-2: Number,
                 scylla-node-3: Number,
                 scylla-native-port-1: Number,
                 scylla-native-port-2: Number,
                 scylla-native-port-3: Number,
                 scylla-alternator-port-1: Number,
                 scylla-alternator-port-2: Number,
                 scylla-alternator-port-3: Number,
                 scylla-env-file: String,
                 public-port: Number,
                 private-port: Number,
                 grpc-port: Number,
                 local-state-dir: String,
                 log-file: String,
                 haproxy-port: Number,
                 haproxy-id: Number,
                 cache-state-dir-prefix: String,
                 cache-tests-base-port: Number
               }
    = fun name subnet node-1-address node-2-address node-3-address
          node-1 native-port-1 alternator-port-1 indielinks-base-port
          shared-port haproxyid base-port
           => {
    stack-name = name,
    scylla-subnet = subnet,
    scylla-node-1-address = node-1-address,
    scylla-node-2-address = node-2-address,
    scylla-node-3-address = node-3-address,
    scylla-node-1 = node-1,
    scylla-node-2 = node-1 + 1,
    scylla-node-3 = node-1 + 2,
    scylla-native-port-1 = native-port-1,
    scylla-native-port-2 = native-port-1 + 1,
    scylla-native-port-3 = native-port-1 + 2,
    scylla-alternator-port-1 = alternator-port-1,
    scylla-alternator-port-2 = alternator-port-1 + 1,
    scylla-alternator-port-3 = alternator-port-1 + 2,
    scylla-env-file = "%{name}.env",
    public-port = indielinks-base-port,
    private-port = indielinks-base-port + 1,
    grpc-port = indielinks-base-port + 2,
    local-state-dir = "/tmp/indielinks-%{stack-name}/",
    log-file = "%{local-state-dir}indielinksd.log",
    haproxy-port = shared-port,
    haproxy-id = haproxyid,
    cache-state-dir-prefix = "/tmp/indielinksd-%{name}-",
    cache-tests-base-port = base-port
} in
mk-stack
