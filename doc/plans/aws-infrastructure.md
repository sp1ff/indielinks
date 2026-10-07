# Deploy indielinks to AWS — third draft

## Context

This plan deploys `indielinks` at `https://indiemark.sh` as a small,
fixed EC2 cluster in `us-west-2`. It supersedes the first and second AWS
drafts without modifying them.

The front end is mounted at the origin root, so a browser request for
`https://indiemark.sh/` returns the Leptos application. API, ActivityPub,
WebFinger, health-check, and bookmarklet routes keep their existing paths.

The deployment continues to use Debian packages and systemd. It does not
introduce containers, ECR, a NAT gateway, an EC2 Instance Connect Endpoint,
SSM Run Command, or an S3 package repository. Operators deploy packages one
node at a time with `scp` and `ssh` over each node's public IPv6 address.

The public hostname is a permanent part of the system's identity. ActivityPub
actor IDs containing `https://indiemark.sh` will be stored and federated once
the instance becomes public, so changing that origin later is a data migration,
not a routine infrastructure edit.

### Final decisions

| Area            | Decision                                                                                |
|-----------------|-----------------------------------------------------------------------------------------|
| Region and name | `us-west-2`, `indiemark.sh`                                                             |
| Compute         | Three Debian 12 x86-64 EC2 instances; `var.instance_type` defaults to `t3a.nano`        |
| Node identity   | Explicit map keyed by durable Raft node ID; no use of `count`                           |
| Network         | Dual-stack VPC and public subnets; ordinary Internet Gateway; no NAT or egress-only IGW |
| Node ingress    | Stable public IPv6 for ALB traffic, Raft, ICMPv6, and operator SSH                      |
| Node egress     | IPv6 only; IPv4-only destinations are unreachable by design                             |
| Public ingress  | Dual-stack ALB with ACM; ALB forwards to an IPv6 target group                           |
| Administration  | Direct IPv6 SSH, restricted to configured operator IPv6 CIDRs                           |
| Deployment      | Build locally, `scp` over IPv6, install over SSH, one node at a time                    |
| Telemetry       | CloudWatch Agent for application logs, host metrics, and OTLP metrics                   |
| Secrets         | Existing SSM Parameter Store support; parameters provisioned manually                   |
| DynamoDB        | `PAY_PER_REQUEST` (on-demand) for every table and global secondary index               |
| Packaging       | General-purpose and AWS-specific Debian packages using `cargo-deb` variants             |
| State           | Encrypted, versioned S3 OpenTofu backend with native lockfiles                          |

`t3a.nano` is a starting value, not a capacity conclusion. The instance type is
an input and can be increased without changing node IDs or addresses. The input
accepts x86-64 Nitro instances; ARM packaging is deferred.

---

## Current code and remaining prerequisites

The second draft's readiness, membership, snapshot, and secret-loading findings
have been resolved in the current tree:

- `GET /healthcheck` returns `201 GOOD` while the daemon is alive but its Raft
  node is uninitialized, then `202 READY` once it belongs to a Raft cluster.
- `RemoveNodes` now retains nodes not present in the removal set.
- Raft snapshots are persisted through the storage backend and restored at
  startup.
- Peppers and signing keys can be resolved from SSM Parameter Store.

Do not reimplement those items. The remaining application prerequisites are
AWS credential resolution for DynamoDB and moving the front-end mount from
`/fe` to `/`.

### Use the EC2 instance profile for DynamoDB

In `indielinks/src/dynamodb.rs`, change the region branch of `create_client` so
that omitted explicit credentials leave the AWS SDK's default credential
provider chain intact. This lets the SDK obtain credentials from the instance
profile through IMDS.

Keep these existing behaviors:

- Explicit credentials override the provider chain when supplied.
- The endpoint-URL branch used by local Alternator installs test credentials
  when explicit credentials are absent, because Alternator requests still need
  to be signed.
- A configured region remains first in the region provider chain.

Use current AWS SDK configuration APIs while making this edit; the region arm
currently uses the deprecated `aws_config::from_env()` entry point and can move
to `aws_config::defaults(BehaviorVersion::latest())` as part of the same focused
change.

For AWS runs, enable dual-stack AWS service endpoints. Enable the IPv6 IMDS
endpoint on each instance as well as requiring IMDSv2. The public IPv4 address
then remains a fallback for external IPv4-only ActivityPub peers, rather than
the normal path to DynamoDB, SSM Parameter Store, or CloudWatch.

### Mount the front end at the origin root

Change `make_world_router` so `GET /` serves the packaged `index.html` and the
front-end assets are served from root URLs. Serve each client-side route used by
the current Leptos router (`/s`, `/h`, `/a`, and `/u`) with `index.html` so direct
navigation and browser refresh work.

Keep the server routes more specific and authoritative: `/api/v1`, `/users`,
`/inbox`, `/actor`, `/.well-known/webfinger`, `/bookmarklets`, and
`/healthcheck` must continue to reach their existing handlers. Do not install a
catch-all fallback that turns unknown API or federation requests into HTML 200
responses. Remove the `/fe` routes; this deployment has no established `/fe`
URLs requiring a compatibility redirect.

Update the bookmarklet stylesheet URL from `/fe/style.css` to `/style.css` and
update the front-end handler's diagnostics and tests to describe the root
mount. The on-disk asset directory remains `/usr/share/indielinks/assets`; only
public URL paths change.

---

## Architecture

```text
 IPv4 and IPv6 clients
          │
 Route 53 alias: indiemark.sh
          │
 dual-stack ALB :443 (ACM)
          │ IPv6 target group, GET /healthcheck must return 202
          │
 ┌────────┴───────────────────────────────────────────────────────┐
 │ dual-stack VPC, two public subnets in two availability zones   │
 │ ::/0 and 0.0.0.0/0 routes to an Internet Gateway               │
 │                                                                │
 │ node 0               node 1               node 2               │
 │ stable public IPv6   stable public IPv6   stable public IPv6   │
 │ this-node = 0        this-node = 1        this-node = 2        │
 │      └──────── Raft gRPC over stable IPv6 :20678 ───────┘      │
 │                                                                │
 │ each node: indielinksd + CloudWatch Agent + systemd            │
 └───────────────────────────┬────────────────────────────────────┘
                             │
                 DynamoDB and SSM Parameter Store
```

### Addressing and routing

Allocate an Amazon-provided IPv6 `/56` to the VPC and a `/64` to each public
subnet. Spread the three default nodes across two availability zones. Both
subnets use one route table with:

- `::/0` to the Internet Gateway;
- `0.0.0.0/0` to the same Internet Gateway.

There is no egress-only Internet Gateway. The ordinary Internet Gateway is
required because the ALB, SSH clients, and Raft peers initiate IPv6 connections
to the nodes as well as the nodes initiating outbound connections.

Each node's primary ENI is a first-class OpenTofu resource with an explicitly
chosen IPv6 address marked as the ENI's primary IPv6 address. (The ENI is
managed directly rather than through `aws_instance` because the AWS provider
ignores `enable_primary_ipv6` at instance creation —
hashicorp/terraform-provider-aws#41571.) The node map pins that address to the
durable Raft node ID. It persists across stop/start, and the ENI survives
instance termination, so a replacement instance reattaches the same ENI and
address.

Nodes have no public IPv4 address. All node egress is IPv6: AWS APIs via
dual-stack endpoints, Debian package mirrors, and IPv6-capable ActivityPub
peers. IPv4-only destinations — including IPv4-only ActivityPub servers — are
unreachable by design. Inbound IPv4 remains available at the dual-stack ALB.

Prefer IPv6 everywhere under operator control:

- Register the instances in an IPv6 target group.
- Put bracketed IPv6 socket addresses in Raft membership, for example
  `[2001:db8::10]:20678`.
- Use IPv6 literals or per-node AAAA records for SSH and `scp`.
- Configure AWS SDKs and the CloudWatch Agent to use dual-stack endpoints.

### Security groups

The ALB security group permits TCP 443 from both `0.0.0.0/0` and `::/0`. TCP 80
may be admitted on both families only for an HTTP-to-HTTPS redirect.

Port numbers throughout this plan are those of the AWS configuration
(`conf/indielinksd-aws.ncl`): 20676 public, 20677 private (loopback only), and
20678 Raft gRPC. They deliberately differ from the master-stack defaults.

The node security group permits:

- TCP 20676 from the ALB security group over IPv6;
- TCP 20678 from the node security group over IPv6;
- TCP 22 from each entry in `var.operator_ipv6_cidrs`;
- ICMPv6, including Packet Too Big messages needed for path MTU discovery.

It contains no IPv4 rules at all. Allow outbound IPv6 so federation, Debian
package installation, and AWS APIs work without NAT.

Keep the local listener on loopback. Its unauthenticated `/ops` endpoints and
Prometheus endpoint are reached through SSH port forwarding, not exposed by a
security-group rule.

### Load balancer and DNS

Use one internet-facing, dual-stack Application Load Balancer across both
subnets. Terminate TLS with an ACM certificate validated through the existing
Route 53 hosted zone, which OpenTofu adopts through a data source rather than
creating or owning.

Use a target group with `ip_address_type = "ipv6"`, target type `instance`, and
port 20676. Its `/healthcheck` success matcher is exactly status `202`. This
keeps a freshly started node out of service until its persisted or newly
initialized Raft membership is available. Status `201` is alive but not ready
and must remain unhealthy at the ALB.

Create Route 53 alias A and AAAA records for `indiemark.sh` pointing to the
ALB. Optional per-node AAAA records such as `node-0.ops.indiemark.sh` may point
to the pinned node addresses for operator convenience; do not create per-node A
records.

---

## Debian packages

Define two `cargo-deb` package variants in `indielinks/Cargo.toml` and build
both for `x86_64-unknown-linux-gnu`:

1. The base `indielinks` package remains the general-purpose package for bare
   metal and locally managed hosts. Preserve its current service, configuration,
   and maintainer scripts. Its packaged front end moves to the origin root with
   the server.
2. The `aws` variant produces a separately named `indielinks-aws` package. It
   contains the AWS configuration template, AWS systemd unit, CloudWatch/logging
   support files, and front-end assets built for the production endpoint.

Use the variant inheritance and asset-merging features rather than duplicating
the entire package manifest. The packages must conflict with each other so that
both cannot install ownership of the same binaries and asset paths on one host.

### Front-end build isolation

`INDIELINKS_FE_API` is compiled into the WASM bundle. The AWS build must run
`trunk build` with:

```text
INDIELINKS_FE_API=https://indiemark.sh
INDIELINKS_BASE=
```

An empty `INDIELINKS_BASE` is intentional: the front end constructs its routes
by appending `/`, `/h`, `/s`, `/a`, and `/u`, so setting it to `/` would produce
double slashes.

Build the general and AWS front ends into separate staging directories before
running `cargo deb`; do not let the second front-end build overwrite assets for
the first package. The AWS package must select only the production bundle. Add
a release check that rejects an AWS WASM file containing a localhost endpoint
or lacking `https://indiemark.sh`.

Create one build wrapper, `admin/build-debian-packages`, which:

- builds release binaries once for x86-64;
- creates the general front-end bundle with the existing general-package flow;
- creates the AWS bundle with the fixed production variables above;
- invokes `cargo deb` once for the base package and once with `--variant=aws`;
- records package version and git revision in the artifact names or adjacent
  checksum manifest.

### AWS configuration and unit

Generate an AWS Nickel configuration template from the same configuration types
as the existing stacks. It must set:

- public origin `https://indiemark.sh`;
- front-end assets and SPA navigation mounted at `/`;
- public and Raft listeners on IPv6 wildcard addresses;
- the private listener and OTLP receiver on loopback;
- DynamoDB location `us-west-2`, with credentials omitted;
- Raft node ID and peer addresses are omitted; the node ID will be supplied via debconf
  and the peer addresses will be supplied by the operator when the cluster is brought up
- peppers and signing keys absent from the file so the existing SSM resolver is
  used.

The AWS package installs a separate `indielinks-aws.service`. Run the daemon in
the foreground with JSON logging, append stdout and stderr to a file owned by
`indielinks`, and configure restart-on-failure. Mask the general
`indielinks.service` before enabling the AWS unit so a package upgrade cannot
start two daemons on the same ports.

---

## Secrets and IAM

Before bootstrap, the operator manually creates these standard-tier
`SecureString` parameters in `us-west-2`:

- `indielinks/prod/peppers`;
- `indielinks/prod/signing-keys`.

Use the serialized formats already accepted by the application's SSM secret
resolver. Do not create values through OpenTofu, commit them, put them in user
data, or pass them on a command line recorded in shell history. The deployment
preflight checks parameter existence and access without printing decrypted
values.

Attach one least-privilege instance role to all nodes. It grants:

- DynamoDB operations required by the application's fixed tables and indexes;
- `ssm:GetParameter` for the two exact parameter ARNs;
- KMS decrypt permission when a customer-managed key is used;
- CloudWatch Logs and metrics permissions needed by the CloudWatch Agent.

Do not install SSM Agent or grant Systems Manager managed-instance permissions.
Parameter Store access comes from the application through the AWS SDK and does
not require SSM Agent.

Require IMDSv2, set the hop limit to one, and enable the IPv6 IMDS endpoint. The
role has no S3 artifact permission because packages arrive over `scp`.

---

## OpenTofu layout

Create a new `infra/aws/` root. Follow existing repository conventions for
provider constraints, common tags, validations, and separate security-group
rule resources.

### Inputs

The important interfaces are:

```hcl
variable "instance_type" {
  type    = string
  default = "t3a.nano"
}

variable "operator_ipv6_cidrs" {
  type = list(string)
}

variable "nodes" {
  type = map(object({
    subnet_key  = string
    ipv6_suffix = string
  }))

  default = {
    "0" = { subnet_key = "a", ipv6_suffix = "10" }
    "1" = { subnet_key = "b", ipv6_suffix = "11" }
    "2" = { subnet_key = "a", ipv6_suffix = "12" }
  }
}
```

Derive full node addresses from each subnet `/64` and the suffix. Validate that
node-map keys parse as unsigned Raft IDs, suffixes are unique, subnet keys
exist, and every derived address lies within its selected subnet. Use
`for_each = var.nodes` for instances, target attachments, DNS records, and
other per-node resources. Never derive a Raft ID from list position.

Validate that `instance_type` belongs to the chosen x86-64/Nitro allow-list or
query compatible instance metadata so an operator gets a plan-time error before
selecting ARM or an instance type that cannot use the required networking.

### Resources

The root owns:

- the dual-stack VPC, two public subnets, Internet Gateway, and routes;
- ALB and node security groups;
- three default EC2 instances and their primary IPv6 addresses;
- the IAM role and instance profile;
- dual-stack ALB, IPv6 target group, listeners, and target attachments;
- ACM certificate, validation records, and public alias records;
- CloudWatch log group and alarms.

It reads rather than owns:

- the existing Route 53 hosted zone;
- the two manually created SSM parameters;
- DynamoDB tables created by `indielinks-schemas`;
- the manually bootstrapped S3 state bucket.

Use the latest Debian 12 amd64 official AMI selected by owner and explicit
filters. Record the resolved AMI ID in each plan. Keep replacement controlled:
do not automatically roll all three instances merely because a newer AMI is
published.

### OpenTofu state

Use S3 only for OpenTofu state. Provision the bucket once, outside this root,
with:

- versioning enabled;
- default server-side encryption;
- all public access blocked;
- native OpenTofu locking via `use_lockfile = true`;
- a lifecycle policy retaining enough noncurrent versions for recovery.

Do not create a DynamoDB lock table; native S3 lockfiles replace it. Do not add
an S3 package bucket: package transport is IPv6 `scp`.

Keep durable application state outside the service root. Destroying
`infra/aws/` removes EC2, ALB, ACM validation records, and the VPC, while
leaving the hosted zone, SecureString parameters, DynamoDB tables, and state
bucket intact.

---

## Bootstrap and operations

All admin scripts use Bash with `set -euo pipefail`, accept the OpenTofu root or
workspace explicitly, and print the node ID before every remote action.

### First bootstrap

1. Create the S3 state bucket and configure the backend.
2. Manually create both `SecureString` parameters, then run a preflight that
   checks caller access and parameter existence without retrieving values into
   output.
3. Run `tofu init`, review `tofu plan`, and apply the infrastructure.
4. Wait for cloud-init and SSH on every pinned IPv6 address. Cloud-init installs
   only base runtime dependencies and the CloudWatch Agent; it does not receive
   application secrets or the application package.
5. Build `indielinks-aws`, verify its checksum and embedded front-end endpoint,
   then copy it to all nodes with `scp -6`.
6. Install the package over `ssh -6`, render each node's configuration from its
   durable ID and the full IPv6 peer map, mask `indielinks.service`, and enable
   `indielinks-aws.service` without starting it.
7. On node 0, run `indielinks-schemas -p ddb us-west-2`. This creates or
   migrates the DynamoDB schema and establishes the persisted instance state.
8. Start the AWS service on all nodes. Confirm that each returns `201 GOOD`.
9. Through node 0's loopback admin listener, initialize Raft once with all three
   node IDs, bracketed IPv6 gRPC addresses, and the two application slots.
10. Wait for every node to return status `202` with body `READY`, then verify all
    ALB targets are healthy and `https://indiemark.sh/healthcheck` returns the
    same response.

The bootstrap script is safe to resume. It checks the observed status of each
completed step and never attempts to initialize an already initialized Raft
cluster as a new cluster.

### Rolling deployment

`admin/aws-deploy` accepts the package path and deploys nodes in stable Raft-ID
order. For each node:

1. Verify the cluster currently has three voting members and the other two
   nodes return `202 READY`.
2. Copy the package and checksum to a temporary path with `scp -6`.
3. Verify the checksum remotely.
4. Install with `apt-get`, re-render configuration if the package template
   changed, and restart `indielinks-aws.service`.
5. Wait for that node's direct IPv6 `/healthcheck` to return exactly `202 READY`
   and for its ALB target to become healthy before continuing.

Abort the rollout on the first failure. Never restart two nodes concurrently;
a three-voter Raft cluster tolerates only one unavailable voter.

### Node replacement and membership changes

Replacing failed compute under an existing node ID reuses its pinned IPv6
address and DynamoDB-backed Raft state. Recreate and validate that node before
changing membership.

To add a distinct Raft node, add a new keyed entry, apply, deploy it, add it as a
learner through the leader, wait for it to catch up, and then commit the new
membership. To remove a node, change Raft membership first, verify quorum and
request handling, then remove its OpenTofu map entry. A plan for either operation
must show only the intended keyed node and its attachments changing.

### Observability

Configure the CloudWatch Agent to:

- tail the AWS unit's JSON application log into a configurable-retention log
  group with one stream per instance;
- collect CPU, memory, disk, swap, and network metrics;
- receive OTLP/HTTP on `127.0.0.1:4318` and publish application metrics to
  CloudWatch with SigV4;
- use dual-stack AWS endpoints.

Add alarms for ALB unhealthy hosts, sustained 5xx responses, EC2 status-check
failure, high memory usage, and disk exhaustion. Keep alert destinations as an
input so infrastructure can be applied before an SNS subscription is chosen.

---

## Verification and acceptance

### Local checks

1. Run `admin/signoff`, `admin/cargo-test-cloud`, and the dependency check after
   the DynamoDB credential change.
2. Add a focused test proving the DynamoDB region path preserves the default
   credential provider when credentials are absent, while the Alternator path
   still receives test credentials.
3. Build and inspect both Debian variants. Confirm their names, conflicts,
   architecture, assets, configuration, and systemd units.
4. Install each variant independently in the existing Debian package harness.
5. Inspect the AWS WASM bundle for `https://indiemark.sh` and the absence of
   localhost API endpoints. Request `/`, every client-side route, and every
   packaged asset directly; confirm they load without a `/fe` prefix. Confirm
   `/fe` and unknown API paths return 404 rather than the SPA document.
6. Run `tofu fmt -check`, `tofu validate`, and inspect plans for default
   creation, one-node addition, one-node removal, and instance-size changes.

### AWS checks

1. From an allowed IPv6 source, verify direct `ssh -6`, `scp -6`, and SSH port
   forwarding to the loopback admin listener. Verify SSH is closed from other
   IPv6 sources and there is no node IPv4 ingress.
2. Confirm normal AWS API traffic uses dual-stack endpoints and confirm the
   nodes have no public IPv4 addresses.
3. Before Raft initialization, verify direct health is `201 GOOD` and the ALB
   target is unhealthy. After initialization, verify direct and ALB health are
   `202 READY`.
4. Request `https://indiemark.sh/` and confirm it loads the front end and its
   root-relative JS, WASM, and CSS assets. Refresh `/s`, `/h`, `/a`, and `/u`
   directly and confirm the SPA loads while the existing public server routes
   still return their expected API or federation content types.
5. Confirm one leader, two followers, the expected membership, and durable
   snapshots. Stop and replace one follower under the same node ID and address;
   verify it rejoins without a membership edit.
6. Create a user, add a bookmark, read the home timeline repeatedly through the
   ALB, and confirm cross-node forwarding works.
7. Perform a rolling deployment and confirm continuous availability and Raft
   quorum throughout.
8. Exercise one node addition and removal; surviving keyed instances must not be
   replaced.
9. Verify JSON logs, host metrics, application OTLP metrics, and alarms in
   CloudWatch.
10. Federate with an IPv6-capable remote server and an IPv4-only remote server.
11. Destroy the service root and confirm that DynamoDB tables, SSM parameters,
    hosted zone, and S3 state remain, while EC2, ALB, VPC, and their public
    addresses are gone.

The deployment is ready for public use only after the full bootstrap, timeline,
rolling-update, node-loss, observability, and federation checks pass.

---

## Assumptions

- There is one production environment in this account and region. The current
  DynamoDB table names are not environment-prefixed.
- `t3a.nano` is the default for cost discovery. Memory pressure is monitored and
  `instance_type` is increased before production if the daemon and CloudWatch
  Agent do not fit reliably.
- The three default nodes are voting Raft members. Deployments and maintenance
  take down at most one at a time.
- Stable IPv6 addresses are the node identities used by ALB, Raft, and operators.
  Nodes have no public IPv4; outbound IPv4 federation is sacrificed (inbound
  IPv4 still reaches the dual-stack ALB).
- The operator already controls the Route 53 hosted zone for `indiemark.sh` and
  supplies an EC2 key pair plus one or more narrow IPv6 source CIDRs.
- ARM support, NAT, EICE, SSM Agent, Run Command, containers, ECR, and an S3
  package repository are outside this draft.

## Sources

- [Amazon VPC Internet Gateways](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_Internet_Gateway.html)
- [Primary IPv6 addresses for EC2](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-primary-ip-addresses.html)
- [Application Load Balancer IP address types](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/application-load-balancers.html#ip-address-type)
- [CloudWatch Agent endpoints and quotas](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html)
- [EC2 instance metadata options](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-options.html)
- [OpenTofu S3 backend](https://opentofu.org/docs/language/settings/backends/s3/)
- [`cargo-deb` variants](https://github.com/kornelski/cargo-deb/blob/main/README.md#variants)

---

## Implementation task list

The plan above is the source of truth when a checklist item needs more detail. The
production origin is `https://indiemark.sh`, and all DynamoDB tables and global
secondary indexes remain in `PAY_PER_REQUEST` billing mode.

### How to use this list

- Complete tasks in dependency order; tasks with satisfied dependencies may run
  in parallel.
- `Either` means a human or coding agent can do the repository work. `Human`
  means the task changes the AWS account, DNS delegation, or secret values.
- Give each repository commit one purpose and include the task ID in its commit
  message body.
- Check a task only after its stated verification passes. Record AWS resource
  IDs, commands, and live-test evidence in the deployment log without recording
  secrets.
- Run repository commands from the `cloud` stack and follow `AGENTS.md`.

### 0. Operator-owned prerequisites

- [x] **AWS-001 — Establish the production AWS context** (`Human`; no dependencies)
  - Select the production account and confirm `us-west-2`; record the account ID
    and the AWS CLI profile name in a private operator note.
  - Done when `aws sts get-caller-identity` succeeds with the intended account
    and the profile can read EC2, IAM, Route 53, ACM, DynamoDB, SSM, S3, and
    CloudWatch metadata.

- [x] **AWS-002 — Establish DNS authority for `indiemark.sh`** (`Human`; depends on AWS-001)
  - Create or identify the public Route 53 hosted zone and delegate the domain
    from its registrar to that zone.
  - Done when the zone's NS records resolve publicly and its zone ID is available
    as an OpenTofu input. Do not create application A or AAAA records manually.

- [x] **AWS-003 — Bootstrap the OpenTofu state bucket** (`Human`; depends on AWS-001)
  - Create the private S3 bucket in `us-west-2`, enable versioning and default
    encryption, block all public access, and configure noncurrent-version
    retention.
  - Done when the OpenTofu S3 backend can initialize with `use_lockfile = true`.
    Do not create a DynamoDB locking table.

- [x] **AWS-004 — Create production secrets** (`Human`; depends on AWS-001)
  - Create standard-tier `SecureString` parameters
    `/indielinks/prod/peppers` and `/indielinks/prod/signing-keys` in `us-west-2`
    using values accepted by the current resolver.
  - Done when parameter metadata can be read by the operator and no value has
    entered Git, OpenTofu state, user data, command output, or a deployment log.

- [x] **AWS-005 — Supply operator SSH inputs** (`Human`; depends on AWS-001)
  - Create or select an EC2 key pair and identify the narrow IPv6 CIDRs allowed
    to administer the nodes.
  - Done when the key name and `operator_ipv6_cidrs` value are available to
    OpenTofu and the private key has correct local permissions.

### 1. Application prerequisites

- [x] **APP-001 — Use the default AWS credential chain for DynamoDB** (`Either`; no dependencies)
  - In the regional `create_client` path, preserve the SDK default credential
    provider when explicit credentials are absent. Preserve explicit credentials
    and the Alternator endpoint path's test credentials.
  - Use the current AWS configuration API and add focused tests for regional,
    explicit, and Alternator credential selection.
  - Done when the tests pass and the regional client can use an EC2 instance
    profile without static credentials.

- [x] **APP-002 — Mount the front end at `/`** (`Either`; no dependencies)
  - Serve `index.html`, root-level static assets, and the `/s`, `/h`, `/a`, and
    `/u` client routes without a `/fe` prefix.
  - Preserve the existing API, ActivityPub, WebFinger, health-check, and
    bookmarklet handlers; unknown API and federation paths must remain 404s.
  - Update the bookmarklet stylesheet to `/style.css` and remove the old `/fe`
    routes without adding a redirect.
  - Done when router tests cover the root page, assets, direct SPA navigation,
    existing server routes, and negative cases.

- [x] **APP-003 — Generate the AWS Nickel configuration** (`Either`; depends on APP-001)
  - Add a production configuration/template that sets origin
    `https://indiemark.sh`, DynamoDB region `us-west-2`, IPv6 public and Raft
    listeners, loopback-only admin and OTLP listeners, and deployment-time Raft
    identity/address placeholders.
  - Omit DynamoDB credentials, peppers, and signing keys. The latter two must
    resolve from their existing SSM names.
  - Done when Nickel evaluates the template, existing stack configurations are
    unchanged, and the generated TOML parses as the application's configuration
    type.

- [x] **APP-004 — Keep DynamoDB on-demand billing explicit** (`Either`; depends on APP-001)
  - Retain `BillingMode::PayPerRequest` for all 11 tables and their six global
    secondary indexes. Do not add provisioned RCU/WCU or auto-scaling settings.
  - Done when schema creation against Alternator still succeeds and source
    inspection confirms every DynamoDB create-table request uses
    `PAY_PER_REQUEST`.

### 2. Debian packaging

- [x] **PKG-001 — Define the two `cargo-deb` variants** (`Either`; depends on APP-003)
  - Preserve the base `indielinks` package for general use and add an `aws`
    variant that produces `indielinks-aws` for x86-64.
  - Use variant inheritance/asset merging and declare mutual package conflicts.
  - Use a *different* `maintainer-scripts` setting; in the AWS-specific `maintainer-scripts`,
    create a `templates` file:
	```
    Template: indielinks/node-id
    Type: string
    Description: Raft node ID for this instance
	```
  - add a `postinst` that does `database_get indielinks/node-id` and updates the configuration
    file accordingly
  - Done when both package manifests build and `dpkg-deb --info` reports the
    expected names, architecture, dependencies, conflicts, and maintainer data.

- [x] **PKG-002 — Add AWS-only runtime assets** (`Either`; depends on PKG-001)
  - Package the AWS configuration template, `indielinks-aws.service`, JSON log
    destination setup, and log rotation support only in the AWS variant.
  - Keep the general service unchanged. Ensure AWS installation masks the
    general service before enabling the AWS unit.
  - Done when each package owns only its intended files and only one service can
    bind the application ports.

- [x] **PKG-003 — Isolate and build both front-end bundles** (`Either`; depends on APP-002, PKG-001)
  - Build general and AWS assets in separate staging directories. Build the AWS
    bundle with `INDIELINKS_FE_API=https://indiemark.sh` and an empty
    `INDIELINKS_BASE`.
  - Reject AWS artifacts containing a localhost API endpoint, `/fe` base, or an
    origin other than `https://indiemark.sh`.
  - Done when both package variants contain their correct, independently built
    JS, WASM, CSS, and HTML assets.

- [x] **PKG-004 — Add the package build command** (`Either`; depends on PKG-002, PKG-003)
  - Create `admin/build-debian-packages` to build release binaries once, stage
    both front ends, invoke base and AWS `cargo deb` builds, and emit checksums.
  - Use Bash with `set -euo pipefail`; accept no hidden dependency on a dirty
    working tree or previously staged front-end assets.
  - Done when two clean consecutive builds produce the expected packages and
    checksum manifests.

- [x] **PKG-005 — Extend the Debian package harness** (`Either`; depends on PKG-004)
  - Install and exercise each variant independently. Verify root front-end
    routing, configuration parsing, service behavior, file ownership, upgrade,
    and package conflict handling.
  - Done when the existing general-package behavior passes and the AWS package
    passes without contacting a real AWS account.

### 3. OpenTofu infrastructure

- [x] **INFRA-001 — Scaffold `infra/aws/` and its interfaces** (`Either`; depends on AWS-003)
  - Add provider/backend constraints, shared tags, `us-west-2` default, hosted
    zone ID, key name, `operator_ipv6_cidrs`, configurable `instance_type`
    defaulting to `t3a.nano`, and the Raft-ID-keyed node map.
  - Validate x86-64 Nitro compatibility, node IDs, subnet keys, unique IPv6
    suffixes, and operator IPv6 CIDRs.
  - Done when `tofu init` and `tofu validate` succeed with no resources yet
    applied.

- [x] **INFRA-002 — Build the dual-stack network** (`Either`; depends on INFRA-001)
  - Create the VPC with an Amazon `/56`, two public `/64` subnets in two
    availability zones, an ordinary Internet Gateway, and `::/0` plus
    `0.0.0.0/0` routes to it.
  - Add no NAT gateway, egress-only IGW, private subnet, or EICE.
  - Done when a plan shows only the specified network and no per-hour network
    appliance.

- [x] **INFRA-003 — Define least-privilege security groups** (`Either`; depends on INFRA-002)
  - Permit ALB 443 from IPv4 and IPv6, optional 80 only for redirect, node 20676
    from the ALB SG, node 20678 from the node SG, SSH from operator IPv6 CIDRs,
    and ICMPv6. Permit node egress on IPv6.
  - Add no node IPv4 ingress and no public admin-listener rule.
  - Done when automated plan assertions or policy checks enforce these
    invariants.

- [x] **INFRA-004 — Create the instance role and profile** (`Either`; depends on INFRA-001, AWS-004)
  - Grant required DynamoDB actions on the fixed table/index ARNs, exact SSM
    parameter reads, conditional customer-key decrypt, and CloudWatch Agent
    logs/metrics actions.
  - Grant no S3 package access or Systems Manager managed-instance policy.
  - Done when IAM policy validation passes and a policy review finds no wildcard
    resource where an exact ARN is available.

- [x] **INFRA-005 — Create keyed EC2 nodes** (`Either`; depends on INFRA-002, INFRA-003, INFRA-004)
  - Select Debian 12 amd64, use `for_each` keyed by Raft ID, manage each node's
    primary ENI as a first-class resource with its derived IPv6 marked primary,
    assign no public IPv4, require IMDSv2, enable IPv6 IMDS, and attach the
    instance profile and key pair.
  - Keep AMI replacement explicit rather than automatically replacing all nodes
    when the data source finds a newer image.
  - Done when deleting map key `1` in a speculative plan does not renumber or
    replace keys `0` and `2`.

- [x] **INFRA-006 — Add the dual-stack ALB, TLS, and DNS** (`Either`; depends on AWS-002, INFRA-005)
  - Create ACM DNS validation, a dual-stack ALB, HTTPS listener, HTTP redirect,
    IPv6 instance target group on port 20676, and per-node attachments.
  - Match only status 202 from `/healthcheck`. Create `indiemark.sh` alias A and
    AAAA records and optional per-node operator AAAA records.
  - Done when the plan creates no per-node A record and the hosted zone remains
    a data source outside destroy ownership.

- [x] **INFRA-007 — Add CloudWatch resources** (`Either`; depends on INFRA-004, INFRA-005)
  - Create the application log group, configurable retention, CloudWatch Agent
    configuration, and alarms for unhealthy targets, 5xx responses, EC2 status,
    memory, and disk.
  - Configure file-based JSON logs, host metrics, loopback OTLP/HTTP, SigV4, and
    dual-stack AWS endpoints. Keep notification destinations optional inputs.
  - Done when agent configuration validation succeeds and a plan contains no
    public telemetry listener.

- [x] **INFRA-008 — Finish outputs and plan tests** (`Either`; depends on INFRA-006, INFRA-007)
  - Output ALB/DNS identifiers, node IDs, pinned IPv6 addresses, instance IDs,
    and ready-to-use IPv6 SSH commands without outputting secrets.
  - Test default creation, one-node addition/removal, instance resize, bad ARM
    type, duplicate address, malformed CIDR, and service destroy.
  - Done when formatting, validation, linting, and every plan scenario passes.

### 4. Provisioning and deployment automation

- [x] **OPS-001 — Implement minimal node bootstrap** (`Either`; depends on PKG-002, INFRA-005, INFRA-007)
  - Install base runtime dependencies and CloudWatch Agent, configure telemetry,
    and prepare the `indielinks` user/directories. Do not fetch the application
    package or secrets in user data.
  - Seed the Debian configuration database with each node's node ID; this could
    be part of cloud-init:
	```
    echo "indielinks indielinks/node-id string $NODE_ID" | debconf-set-selections
    export DEBIAN_FRONTEND=noninteractive
    dpkg -i indielinks_0.0.1_amd64.deb   # or apt-get install -y ./indielinks_0.0.1_amd64.deb
	```
  - Done when cloud-init is idempotent and a fresh node becomes reachable by
    IPv6 SSH with CloudWatch Agent healthy.

- [x] **OPS-002 — Add deployment preflight** (`Either`; depends on AWS-002 through AWS-005, INFRA-008)
  - Create `admin/aws-preflight` to validate tools, account, region, clean inputs,
    DNS authority, state backend, SSM parameter metadata/access, SSH key, and
    IPv6 connectivity without printing secrets.
  - Done when every expected failure produces a specific error before any node
    or application mutation occurs.

- [ ] **OPS-003 — Automate first cluster bootstrap** (`Either`; depends on APP-004, PKG-005, OPS-001, OPS-002)
  - Create `admin/aws-bootstrap` to wait for cloud-init, transfer and verify the
    AWS package with IPv6 `scp`, install/configure it over IPv6 SSH, migrate the
    schema on node 0, start all nodes, initialize Raft once, and wait for
    `202 READY` plus healthy ALB targets.
  - Make every completed step resumable without exposing parameter values.
  - Done when a fresh environment reaches readiness from one command after the
    human prerequisites and `tofu apply` are complete.

- [ ] **OPS-004 — Automate rolling package deployment** (`Either`; depends on OPS-003)
  - Create `admin/aws-deploy` to verify quorum, deploy nodes in Raft-ID order,
    transfer and check each package, restart one node, and wait for direct and
    ALB readiness before proceeding.
  - Abort on the first failure and never restart two voters concurrently.
  - Done when a test release completes without loss of public availability.

- [ ] **OPS-005 — Document node replacement and membership changes** (`Either`; depends on OPS-003)
  - Add exact leader-discovery, learner-addition, catch-up, membership-change,
    removal, replacement, and rollback commands using node IDs and bracketed
    IPv6 socket addresses.
  - Done when adding and removing a temporary node changes no surviving EC2
    resource and replacement under an existing ID needs no membership edit.

- [ ] **OPS-006 — Implement safe service teardown** (`Either`; depends on OPS-003)
  - Create a guarded command that destroys the service OpenTofu root and then
    checks for orphaned EC2, ALB, VPC, and address resources.
  - Explicitly verify that DynamoDB tables, SSM parameters, hosted zone, and S3
    state remain.
  - Done when a rehearsal removes all service charges without deleting durable
    identity or application data.

### 5. Documentation and repository acceptance

- [ ] **DOC-001 — Write the AWS operator runbook** (`Either`; depends on OPS-003 through OPS-006)
  - Document prerequisites, initial apply, package build, bootstrap, SSH and
    port forwarding, rolling deploy, secret rotation, scaling, node lifecycle,
    alarms, recovery, and teardown.
  - Use `indiemark.sh` consistently and state that ActivityPub actor origins are
    permanent after federation begins.
  - Done when another operator can follow the runbook without consulting chat
    history or the earlier AWS drafts.

- [ ] **DOC-002 — Update project-facing documentation** (`Either`; depends on DOC-001)
  - Add the deployment commands and AWS configuration conventions to the
    appropriate hacking documentation and record user-visible root front-end
    routing in `NEWS` under `UNRELEASED`.
  - Done when documentation searches contain no production reference to
    the former domain or a `/fe` production mount.

- [ ] **VAL-001 — Pass local repository signoff** (`Either`; depends on APP-001 through DOC-002)
  - Run `admin/cargo-test-cloud`, package harnesses, dependency checks, linters,
    formatting checks, and `admin/signoff` as applicable.
  - Done when all required checks pass from a clean tracked working tree.

### 6. Live acceptance and launch

- [ ] **LIVE-001 — Apply production infrastructure** (`Human`; depends on AWS-001 through AWS-005, VAL-001)
  - Review the final plan, apply it, and save the resource inventory and apply
    output without secrets.
  - Done when three nodes, ALB, DNS, TLS, IAM, and CloudWatch resources exist as
    specified.

- [ ] **LIVE-002 — Bootstrap and verify network access** (`Human`; depends on LIVE-001, OPS-003)
  - Run preflight and bootstrap. Verify allowed IPv6 SSH/SCP and port forwarding,
    blocked unauthorized IPv6 SSH, no node IPv4 ingress, AWS dual-stack API use,
    and outbound IPv4 fallback.
  - Done when all three nodes return `202 READY` and all ALB targets are healthy.

- [ ] **LIVE-003 — Verify application behavior** (`Human`; depends on LIVE-002)
  - Load `https://indiemark.sh/` and direct SPA routes; verify root-relative
    assets, sign-in, user creation, bookmark creation, home timeline, cross-node
    forwarding, and correct API/ActivityPub content types.
  - Done when root hosting works without `/fe` and unknown API paths remain 404.

- [ ] **LIVE-004 — Verify resilience and operations** (`Human`; depends on LIVE-003, OPS-004, OPS-005)
  - Exercise one-node loss/replacement, rolling deployment, temporary node
    addition/removal, log and metric queries, and every configured alarm.
  - Done when quorum and public availability survive each rehearsal and durable
    state restores correctly.

- [ ] **LIVE-005 — Verify federation and approve launch** (`Human`; depends on LIVE-004)
  - Federate with IPv6-capable remote servers and confirm posts reach remote
    timelines under `https://indiemark.sh` actor IDs. Confirm that IPv4-only
    remote servers fail cleanly (unreachable by design), not silently.
  - Done when federation succeeds, costs and alarms are reviewed, backups are
    confirmed, and the operator records approval to make the instance public.
