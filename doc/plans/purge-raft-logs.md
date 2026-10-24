# Implement `indio purge-raft-logs`

Status: **planned; implementation has not begun**.

## Goal and success criterion

Implement `indielinks_clients::raft_logs::clean_dynamodb_raft_logs()` and
`clean_scylla_raft_logs()` so that `indio purge-raft-logs`, while every
`indielinksd` process is stopped, removes every item from `raft_log` and
`raft_metadata` without disturbing any other table.

The acceptance test will run against exactly these smoke-test fixtures:

- `scylla-single-node`
- `scylla-cluster`
- `dynamodb-single-node`
- `dynamodb-cluster`

For each fixture it will prove this state transition:

```text
datastore up + indielinks running + Raft initialized
    -> stop indielinks, leaving the datastore up
    -> run indio purge-raft-logs
    -> restart indielinks without initializing Raft
    -> observe raft_initialized == None
    -> initialize Raft
    -> observe raft_initialized == Some(timestamp)
```

Passing that test in all four fixtures is the completion criterion. The two
pre-charged fixtures are excluded because their additional application data is
irrelevant to this operator command.

## Current-code findings that affect the implementation

- The command shell and public function signatures already exist in
  [`indielinks-clients/src/bin/indio.rs`](../../indielinks-clients/src/bin/indio.rs)
  and [`indielinks-clients/src/raft-logs.rs`](../../indielinks-clients/src/raft-logs.rs).
- `indio` currently parses its HTTP API configuration before dispatching the
  subcommand. A storage-only purge would therefore require an unrelated API
  token and API origins. Move that parsing into the `add-user` arm so
  `purge-raft-logs` depends only on its own datastore arguments.
- The smoke-test fixture currently couples datastore and service start/stop.
  The test needs service-only start/stop helpers so the datastore stays
  available while `indio` deletes its Raft rows.
- [`infra/indielinks-cluster-up`](../../infra/indielinks-cluster-up) already has
  `--no-raft-init`; [`infra/indielinks-up`](../../infra/indielinks-up) always
  initializes the one-node Raft. Add the corresponding option to the latter so
  the acceptance test can observe the purged state before reinitializing it.
- The unauthenticated `GET /ops/cluster-stats` endpoint returns the shared
  [`ClusterStatsResponse`](../../indielinks-shared/src/api.rs), whose
  `raft_initialized` field is the assertion target.
- DynamoDB's Raft keys are `(node_id, log_id)` for `raft_log` and
  `(node_id, flavor)` for `raft_metadata`, as defined in
  [`indielinks/src/dynamodb-schemas.rs`](../../indielinks/src/dynamodb-schemas.rs).
- The server already contains client-construction logic for both backends in
  [`indielinks/src/dynamodb.rs`](../../indielinks/src/dynamodb.rs) and
  [`indielinks/src/scylla.rs`](../../indielinks/src/scylla.rs). The client crate
  cannot depend on `indielinks`, so the reusable construction code belongs in
  `indielinks-shared` behind its existing `backend` feature.

## Phase 1: add the failing black-box integration test

1. Extend the smoke-test lifecycle code with operations that start and stop
   only `indielinksd` (one instance or the configured cluster) while preserving
   Scylla/Alternator. Keep full fixture setup and teardown responsible for the
   datastore, and keep teardown idempotent so it still cleans up after an
   assertion or command failure.
2. Add `-R`/`--no-raft-init` to `infra/indielinks-up`, mirroring the cluster
   launcher's behavior. When present, wait until the daemon reports its live,
   uninitialized health state and skip the `POST /ops/cache/init-cluster` call.
   Use the existing cluster flag for clustered fixtures.
3. Add shared test helpers to:
   - fetch `/ops/cluster-stats`, require a successful status, deserialize
     `ClusterStatsResponse`, and assert whether `raft_initialized` is `Some` or
     `None`;
   - initialize Raft from the fixture's complete `(node_id, gRPC address)`
     membership, for either one node or the configured cluster size;
   - construct the fixture-specific `indio purge-raft-logs` invocation:
     `--location` plus optional credentials for DynamoDB/Alternator, or repeated
     `--node` and `--translation` plus optional credentials for native Scylla;
   - run `indio` as a subprocess so the test covers clap parsing, dispatch,
     client construction, error propagation, and process exit status rather
     than calling the library functions directly.
4. Register one common purge test for each of the four fixture IDs. Passing the
   fixture ID explicitly to the common body keeps backend and topology choices
   unambiguous without changing every existing smoke-test callback.
5. In the test body:
   - rely on fixture setup to start the datastore and service and initialize
     Raft;
   - assert `raft_initialized.is_some()`;
   - stop only the service process or processes;
   - invoke `indio purge-raft-logs` and require success;
   - restart the service or cluster with Raft initialization disabled;
   - assert `raft_initialized.is_none()`;
   - call the existing initialization endpoint with the full membership;
   - assert `raft_initialized.is_some()` again.
6. Move `indio`'s general HTTP configuration parsing into the `add-user`
   dispatch arm. This lets the new test invoke the purge command in a clean
   environment using only datastore options.
7. Compile the new test, then run it once to record the expected failure at the
   `todo!()` boundary before implementing either backend.

## Phase 2: share datastore client construction

1. Move the backend-neutral DynamoDB client factory from `indielinks` into
   `indielinks_shared::dynamodb` under the `backend` feature, including its
   three credential modes:
   - explicit credentials when supplied;
   - the AWS default credential chain for a regional DynamoDB location;
   - throwaway signing credentials for a local Alternator URL.
2. Move the native Scylla session factory and address-translator implementation
   into `indielinks_shared::scylla` under the same feature. Give shared client
   construction its own SNAFU error type, including missing address
   translations and session-creation failures.
3. Add `aws-config` to the shared crate's `backend` feature and update the
   server, schema utility, and test helpers to import the shared factories.
   Remove the old copies only after every caller has moved, preserving the
   current behavior for server startup and schema management.
4. Keep the purge module's errors operation-specific and attach table names at
   the call sites so an operator can distinguish connection, scan, request
   construction, batch deletion, retry exhaustion, keyspace selection, and CQL
   execution failures.

## Phase 3: implement DynamoDB/Alternator purging

1. Create a DynamoDB client with the shared factory and process `raft_log` and
   `raft_metadata` through one table-parameterized private helper.
2. Scan only each table's key attributes. Follow `last_evaluated_key` with
   `exclusive_start_key` until all pages have been consumed; a single `Scan`
   response is not sufficient for an arbitrarily sized table.
3. Convert each returned key map directly into a `DeleteRequest`. Do not
   deserialize complete Raft records, because the command needs only the keys
   and should not couple deletion to stored payload formats.
4. Submit `BatchWriteItem` requests in groups of at most 25 writes, with a
   single table represented in each request.
5. After every batch, extract that table's `UnprocessedItems` and resubmit them
   until none remain. Sleep between attempts using exponential backoff with a
   cap. Bound the number of attempts and return a contextual error if items
   remain persistently unprocessed, rather than reporting a partial purge as
   success or hanging forever.
6. Run the acceptance test first for `dynamodb-single-node`, then
   `dynamodb-cluster`. Both must complete the initialized -> purged ->
   reinitialized transition before starting the Scylla implementation.

## Phase 4: implement native Scylla purging

1. Create the Scylla session with the shared factory, including optional
   credentials and address translations.
2. Select the `indielinks` keyspace explicitly.
3. Execute `TRUNCATE TABLE raft_log`, followed by
   `TRUNCATE TABLE raft_metadata`, and return success only after both operations
   complete. The service remains stopped throughout, as required by the
   command's operational contract.
4. Run the acceptance test for `scylla-single-node` and `scylla-cluster`, then
   run it across all four fixtures together.

## Validation and completion

After each implementation phase, run formatting and focused compilation before
the relevant fixture tests. Final validation will include:

1. `cargo fmt --check`
2. `cargo check -p indielinks-shared -p indielinks-clients -p indielinks -p tests-indielinks`
3. the purge test on each of the four named cloud-stack fixtures
4. `admin/run-linters`
5. `admin/check-dependencies`
6. `admin/cargo-test-cloud`

Inspect the final diff for accidental changes to non-Raft tables, generated
configuration, and pre-existing smoke tests. Update the command help text to
name `raft_metadata` accurately while touching that section. Do not commit the
implementation until its staged files, cached diff, and proposed commit message
have been presented for explicit approval.

## Operational boundaries

- The purge is deliberately non-atomic on DynamoDB. The command is safe under
  its documented precondition that every `indielinksd` node is down; a failure
  reports an error and can be retried.
- The command does not drop or recreate tables and does not alter schema
  migrations, application data, snapshots outside the two Raft tables, or
  local daemon state directories.
- Successful execution means both tables were emptied. Empty tables are also a
  successful, idempotent case.
