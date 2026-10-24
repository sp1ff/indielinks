# AGENTS.md for tests-indielinks

## Integration-test architecture

`tests-indielinks` tests the `indielinksd` binary using a custom test harness
(`harness = false` in `Cargo.toml`) and the `tests-support` crate. The
`tests-support` framework is built around three ideas (see
`tests-support/src/lib.rs`):

- **Domain** — a subsystem under test (here, the indielinks web service).
- **Fixture** — a concrete instantiation of the domain's environment (e.g.
  Scylla single-node, DynamoDB cluster). Each fixture implements the `Fixture`
  trait, providing `setup`, `teardown`, and `new_backend`.
- **Backend** — a domain-specific helper passed to each test. For the web-service
  tests the backend is the `Helper` trait object.

Tests are registered with the `inventory` crate and may declare themselves
`germane` to only a subset of fixtures. `tests-support` runs each fixture's
setup, executes the germane tests, and then runs teardown.

## Keep tests fixture-independent

**Test logic must not branch on the fixture in which it is running.** If a test
needs behavior that differs between fixtures, that behavior belongs in the
`Helper` trait (or another backend abstraction), whose implementations are
already fixture-aware.

For example, a test that needs to stop and restart `indielinksd` should not
inspect the fixture ID to decide whether to call `infra/indielinks-down` or
`infra/indielinks-cluster-down`. Instead, add methods such as
`indielinks_up`, `indielinks_down`, and `purge_raft_logs` to the `Helper`
trait and implement them in `DynamoDBHelper` and `ScyllaHelper`. Those
implementations can use `self.nodes.len()` to distinguish single-node from
clustered fixtures.

The `purge-raft-logs` integration test demonstrates this pattern:

- `tests-indielinks/src/helper.rs` defines `Helper::indielinks_up`,
  `Helper::indielinks_down`, and `Helper::purge_raft_logs`, plus parameter
  structs `IndielinksUpParams`, `IndielinksDownParams`, and
  `PurgeRaftLogsParams`. `DynamoDBHelper` and `ScyllaHelper` implement the
  methods; each picks the single-node or cluster launcher based on the number
  of nodes and builds the correct `indio` arguments for its datastore.
- `tests-indielinks/src/raft-logs.rs` contains the test body `smoke`, which
  calls `helper.indielinks_down(...)`, `helper.purge_raft_logs(...)`,
  `helper.indielinks_up(...)`, and HTTP endpoints against `helper.ops()`.
  It never sees a `FixtureId`.
- `tests-indielinks/tests/smoke-tests.rs` registers a single test,
  `200purge_raft_logs`, and marks it germane to the four datastore/topology
  fixtures.

In short: write tests against the `Helper` abstraction; put fixture-specific
logic in the `Helper` implementations, not in the test body.
