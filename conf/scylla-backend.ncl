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
# This Nickel file provides a function for producing a ScyllaDB backend
# configuration, which is used by `indielinksd` as well as multiple integration
# tests. This would be a good place to begin introducing type information.
let backend = fun stack => {
    Scylla = {
        hosts = ["127.0.0.1:%{stack.scylla-native-port-1}"],
        translations = [
            ["%{stack.scylla-node-1-address}:%{stack.scylla-native-port-1}", "127.0.0.1:%{stack.scylla-native-port-1}"],
            ["%{stack.scylla-node-2-address}:%{stack.scylla-native-port-1}", "127.0.0.1:%{stack.scylla-native-port-2}"],
            ["%{stack.scylla-node-3-address}:%{stack.scylla-native-port-1}", "127.0.0.1:%{stack.scylla-native-port-3}"]
        ]
    }
} in
backend

