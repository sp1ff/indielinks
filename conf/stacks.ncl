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
# This particular Nickel file defines our stacks all in one place.
let mk-stack = import "mk-stack.ncl"
in {
    "master"    = mk-stack "master"
                           "172.11.0.0/24" # ScyllaDB docker compose subnet
                           "172.11.0.2"    # IP address of the first ScyllaDB instance
                           "172.11.0.3"    # ...second
                           "172.11.0.4"    # ...third
                           1               # index of the first ScyllaDB docker container name
                           9043            # first Cassandra port
                           8043            # first Alternator port
                           20679           # indielinksd base port
                           20673           # haproxy port
                           0               # haproxy id
                           20800,          # cache-tests base port
    "bugfix" = mk-stack "bugfix"
                        "172.13.0.0/24"
                        "172.13.0.2"
                        "172.13.0.3"
                        "172.13.0.4"
                        7
                        9049
                        8049
                        20709
                        20675
                        1
                        20810,
    "front-end" = mk-stack "front-end"
                           "172.12.0.0/24"
                           "172.12.0.2"
                           "172.12.0.3"
                           "172.12.0.4"
                           4
                           9046
                           8046
                           20694
                           20674
                           2
                           20805,
    "pre-alpha" = mk-stack "pre-alpha"
                           "172.14.0.0/24"
                           "172.14.0.2"
                           "172.14.0.3"
                           "172.14.0.4"
                           10
                           9052
                           8052
                           20724
                           20739
                           3
                           20815,
}
