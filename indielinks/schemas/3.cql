/* Copyright (C) 2026 Michael Herstine <sp1ff@pobox.com>

   This file is part of indielinks.

   indielinks is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published
   by the Free Software Foundation, either version 3 of the License,
   or (at your option) any later version.

   indielinks is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
   General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with indielinks. If not, see <http://www.gnu.org/licenses/>.

 */

use indielinks;

/* Re-create incoming_likes_replies_shares */
drop table if exists incoming_likes_replies_shares;

create table if not exists incoming_likes_replies_shares (
  user_id uuid,
  received timestamp,
  /* The (remote) ActivityPub ID of this like, reply or share */
  ap_id ascii,
  attributed_to ascii,
  content text,
  /* The UUID of the thing being liked, replied to, or shared, if it's on this instance */
  in_reply_to uuid,
  /* The sort of thing being liked, replied to, or shared, if it's on this instance */
  in_reply_to_sort tinyint,
  /* Is this a like, a reply, or a share? */
  sort tinyint,
  visibility tinyint,
  /* AP ID of this post's replies (remote) */
  replies ascii,
  /* AP ID of this post's replies (remote) */
  shares ascii,  
  PRIMARY KEY (user_id, received, ap_id)
) WITH CLUSTERING ORDER BY (received desc);

create index if not exists incoming_likes_replies_shares_in_reply_to on incoming_likes_replies_shares (in_reply_to);

