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
# This particular Nickel file contains the code for generating the indielinksd
# configuration file for the AWS production cluster at https://indiemark.sh. It
# is *not* stack-parameterized: it evaluates directly to a single configuration
# with a placeholder `raft-config.this-node` that the Debian postinst replaces
# at installation time. The generated file contains no secrets: peppers &
# signing keys are referenced by their SSM Parameter Store parameter names, and
# DynamoDB credentials are omitted (the daemon uses the EC2 instance profile).
# Raft peer addresses are deliberately absent, as well: they are supplied to
# the cluster via `POST /ops/cache/init-cluster` at bootstrap time.
let common = import "indielinksd-common.ncl" in
common & {
    log-file = "/var/log/indielinks/indielinksd.log",
    public-origin = {
        scheme = "https",
        host = {
            RegName = "indiemark.sh"
        }
    },
    # The public & Raft listeners bind the IPv6 wildcard; the admin listener &
    # OTLP receiver stay on loopback
    public-address = "[::]:20676",
    private-address = "127.0.0.1:20677",
    raft-grpc-address = "[::]:20678",
    # The CloudWatch Agent's OTLP/HTTP receiver (full signal path)
    otlp-export | force = {
        endpoint = "http://127.0.0.1:4318/v1/metrics"
    },
    storage-config = {
        Dynamo = {
            location = "us-west-2"
        }
    },
    assets = "/usr/share/indielinks/assets",
    pepper = "/indielinks/prod/peppers",
    signing-keys = {
        signing-keys = "/indielinks/prod/signing-keys"
    },
    users-config | force = {
        same-site = "Lax",
        secure-cookies = true,
        allowed-origins = [
            {
                scheme = "https",
                host = {
                    RegName = "indiemark.sh"
                }
            }
        ]
    },
    raft-config = common.raft-config & {
        this-node = "PLACEHOLDER"
    }
}
