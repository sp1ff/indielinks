/* CQL (Cassandra Query Language) file for charging a local database.

   This is intended, along with `compose.local.yaml` for local
   indielinks development. I.e. the assumption is that the dev will
   stand-up a local ScyllaDB cluster using `docker compose`, then use
   this script to create the keyspace & tables required by indielinks.

   I'll write a second script to load some initial data. */

create keyspace indielinks
  with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3}
  and tablets = {'enabled': false}
  ;

use indielinks;

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
create table users (
  id uuid PRIMARY KEY,
  username ascii,
  discoverable boolean,
  display_name text,
  summary text,
  pub_key_pem ascii,
  priv_key_pem ascii,
  api_key blob,
  first_update timestamp,
  last_update timestamp,
  password_hash ascii,
  pepper_version ascii,
  followers set<ascii>
);

create index users_by_username on users (username);

create table unique_usernames (
  username ascii PRIMARY KEY,
  id uuid
);

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

create type reply (
  id ascii,
  visibility tinyint,
);

create type share (
  id ascii,
  visibility tinyint,
);

create table posts (
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
  likes set<ascii>,
  replies set<frozen<reply>>,
  shares set<frozen<share>>,
  PRIMARY KEY (user_id, url)
);

create index day_idx on posts (day);

create index tags_idx on posts (tags);

create materialized view posts_by_posted as
  select * from posts where url is not null and posted is not null
  primary key (user_id, posted, url) -- user_id is the partition key
  with clustering order by (posted desc);

create materialized view posts_by_day as
  select * from posts where url is not null and day is not null
  primary key (user_id, day, url);

create index post_id_idx on posts (id);

/* Background tasks */
create table tasks (
  id uuid PRIMARY KEY,
  created timestamp,
  task blob,
  tag uuid,
  lease_expires timestamp,
  done boolean,  
);

