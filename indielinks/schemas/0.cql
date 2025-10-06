/* Copyright (C) 2024-2025 Michael Herstine <sp1ff@pobox.com>

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


   This CQL (Cassandra Query Language) file will initialize a ScyllaDB
   database with the indielinks schema, version zero.

   */

create keyspace if not exists indielinks
  with replication = {
    'class': 'NetworkTopologyStrategy',
    'replication_factor' : 3
  } and tablets = {
    'enabled': false
  };

use indielinks;

create table if not exists schema_migrations (
  version bigint PRIMARY KEY,
  instance_state blob,
  applied timestamp,
);

/* Paritition key is the user *ID* (not the username). Should give
   high cardinality, smooth distribution and uniform growth of each
   partition. No clustering key (at the moment I have no range queries
   over users).

  The last two attributes are required for two different reasons. The
  second is needed to service the /posts/recent endpoint; the post day
  is part of the "posts" table primary key, so without this, I'd have
  to start with today, then query for yesterday, and so on, looking
  for the most recent post.

  first_update exists to help me put a lower bound on a search of a
  user's complete history. */
create table if not exists users (
  id uuid PRIMARY KEY,
  username ascii,
  discoverable boolean,
  display_name text,
  summary text,
  pub_key_pem ascii,
  priv_key_pem ascii,
  api_keys blob,
  first_update timestamp,
  last_update timestamp,
  password_hash ascii,
  pepper_version ascii,
);

create index if not exists users_by_username on users (username);

create table if not exists unique_usernames (
  username ascii PRIMARY KEY,
  id uuid
);

/* I think that (user_id, actor_id) is the natural primary key, here.
   We wouldn't even *have* an `id` field if it weren't for the fact
   that ActivityPub activities need it. If I do decide to actually
   serve `Follow`s, I can always add an index in `id`. */
create table if not exists following (
  user_id uuid,
  actor_id ascii,
  id uuid,
  created timestamp,
  accepted boolean,
  PRIMARY KEY (user_id, actor_id)
);

create index if not exists following_by_actor on following (actor_id);

/* Similarly, here (cf. table `following`, above). I considered
   keeping both followers & followings in one table, but decided to
   reserve the option to represent them differently. */
create table if not exists followers (
  user_id uuid,
  actor_id ascii,
  id uuid,
  created timestamp,
  accepted boolean,
  PRIMARY KEY (user_id, actor_id)
);

create index if not exists followers_by_actor on followers (actor_id);

/* I've gone through a good deal of churn on representing posts. Given
   that I'm always accessing posts by user, `user_id` needs to be
   incorporated into the partition key. I'm worried about hot
   partitions, and experimented with adding `day` to the partition
   key, but that would force the `day` field to be specified for all
   my queries, which is really inconvenient. For now, I'm just going
   to use `user_id`. The sort key then kind of *has* to be `url`,
   since `(user_id, url)` is the natural primary key.

   For a number of my queries, I need to search on `day` or `posted`.
   I believe I can do this with a pair of materialized views: I'll use
   the same partition key (`user_id`), but different sort keys.

   The `id` is really just a convenience (for ActivityPub-related
   tasks, it would be inconvenient to name a post by user_id & URL).
   This schema in no way guarantees that `id` is unique-- that's up
   to the application code.

   Prior thoughts on `user_id` as the partition key:

   All my queries are by user ID, so I need to incorporate that into
   the partition key to prevent full-table/cluster scans. That gives
   me high cardinality, but not even distribution: some users will
   inevitably upload more posts than others, sometimes dramatically
   more. In an attempt to address that, and to prevent partitions from
   growing without bound, I used a technique called "bucketing"; I
   added a column corresponding to just the day of the posted
   timestamp, and adding that column to the partition key.

   While this will bound the size of any one partition, it doesn't
   completely eliminate the hot partition issue. There are some other
   options, such as making the buckets even smaller, or adding some
   other element to the partition key (perhaps a hash on the URL?),
   but I decided I was overthinking this.
*/

create table if not exists posts (
  user_id uuid,
  url ascii,
  id uuid,
  posted timestamp,
  day ascii,
  title text,
  notes text,
  tags set<text>,
  public boolean,
  unread boolean,
  PRIMARY KEY (user_id, url)
);

create index if not exists day_idx on posts (day);

create index if not exists tags_idx on posts (tags);

create materialized view if not exists posts_by_posted as
  select * from posts where url is not null and posted is not null
  primary key (user_id, posted, url) -- user_id is the partition key
  with clustering order by (posted desc);

create materialized view if not exists posts_by_day as
  select * from posts where url is not null and day is not null
  primary key (user_id, day, url);

create index if not exists post_id_idx on posts (id);

create table if not exists likes (
  user_id uuid,
  url ascii,
  id uuid,
  created timestamp,
  like_id ascii,
  PRIMARY KEY ((user_id, url), like_id)
);

create table if not exists replies (
  user_id uuid,
  url ascii,
  id uuid,
  created timestamp,
  reply_id ascii,
  visibility tinyint,
  PRIMARY KEY ((user_id, url), reply_id)
);

create table if not exists shares (
  user_id uuid,
  url ascii,
  id uuid,
  created timestamp,
  share_id ascii,
  visibility tinyint,
  PRIMARY KEY ((user_id, url), share_id)
);

create table if not exists activity_pub_posts (
  user_id uuid,
  post_id ascii,
  posted timestamp,
  flavor tinyint,
  visibility tinyint,
  PRIMARY KEY (user_id, post_id)
);

create materialized view if not exists activity_pub_posts_by_posted as
  select * from activity_pub_posts where post_id is not null and posted is not null
  primary key (user_id, posted, post_id) -- user_id is still the partition key
  with clustering order by (posted desc);

/* Background tasks */
create table if not exists tasks (
  id uuid PRIMARY KEY,
  created timestamp,
  task blob,
  tag uuid,
  lease_expires timestamp,
  done boolean
);

/* Raft log */
create table if not exists raft_log (
  node_id varint,
  log_id varint,
  entry blob,
  PRIMARY KEY (node_id, log_id)
);

/* Raft metadata */
create table if not exists raft_metadata (
  node_id varint,
  flavor tinyint,
  data blob,
  PRIMARY KEY (node_id, flavor)
);
