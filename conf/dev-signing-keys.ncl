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
# This particular Nickel file contains the *development-only* signing keys (for
# signing JWTs), merged into development configurations by
# `indielinks/build.rs`. Token lifetimes are policy, not secrets, and remain in
# `indielinksd-common.ncl`; production deployments reference their secrets by
# SSM parameter name instead.
{
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
        }
    }
}
