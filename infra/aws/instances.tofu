# Three Raft-ID-keyed Debian 12 nodes. Each node's primary ENI is a
# first-class resource for two reasons:
#
#  1. The AWS provider ignores enable_primary_ipv6 on aws_instance at
#     instance creation (hashicorp/terraform-provider-aws#41571); the ENI
#     create path honors it, and IPv6 target-group registration requires a
#     designated primary IPv6 address.
#  2. The ENI survives instance termination because AWS forbids
#     delete_on_termination on an attachment that references an existing
#     network interface ID. A replacement instance reattaches the same ENI
#     and keeps the same pinned IPv6 address.
#
# Nodes have no public IPv4 addresses by design: all egress is IPv6 (AWS
# APIs via dual-stack endpoints, Debian mirrors, IPv6-capable ActivityPub
# peers). user_data renders cloud-init.yaml.tftpl per node: base packages,
# the indielinks user and log directory, a debconf pre-seed of the Raft node
# ID, and the CloudWatch Agent fetching its configuration from SSM. SSH works
# out of the box as user "admin" with the configured key pair.

# Latest Debian 12 (Bookworm) AMI in the selected region.
# Owner 136693071363 is the official Debian AWS account.
data "aws_ami" "debian_12" {
  most_recent = true
  owners      = ["136693071363"] # Debian

  filter {
    name   = "name"
    values = ["debian-12-amd64-*"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "root-device-type"
    values = ["ebs"]
  }
}

# Check that the key name actually exists.
data "aws_key_pair" "operator" {
  key_name = var.ec2_key_name
}

locals {
  # The pinned address is derived from the subnet /64 and the node's hex
  # suffix, so it always lies within the subnet; an out-of-range suffix fails
  # cidrhost at plan time. Single source of truth for the ENI and outputs.
  node_ipv6_addresses = {
    for id, node in var.nodes :
    id => cidrhost(
      aws_subnet.public[node.subnet_key].ipv6_cidr_block,
      parseint(node.ipv6_suffix, 16)
    )
  }
}

resource "aws_network_interface" "nodes" {
  for_each = var.nodes

  subnet_id       = aws_subnet.public[each.value.subnet_key].id
  security_groups = [aws_security_group.nodes.id]

  # As the ENI's only GUA, the pinned address becomes its primary IPv6
  # address and persists across stop/start.
  ipv6_addresses      = [local.node_ipv6_addresses[each.key]]
  enable_primary_ipv6 = true

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-node-${each.key}"
  })
}

resource "aws_instance" "nodes" {
  for_each = var.nodes

  ami           = data.aws_ami.debian_12.id
  instance_type = var.instance_type

  primary_network_interface {
    network_interface_id = aws_network_interface.nodes[each.key].id
  }

  key_name             = data.aws_key_pair.operator.key_name
  iam_instance_profile = aws_iam_instance_profile.nodes.name

  # user_data_replace_on_change defaults to false, which applies user_data
  # edits in place via stop/start -- but cloud-init only processes user-data
  # on an instance's first boot, so in-place updates silently never run.
  # Require real replacement; the ENI-as-resource design keeps the pinned
  # IPv6 address across it. Roll one node at a time when editing
  # cloud-init.yaml.tftpl.
  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/cloud-init.yaml.tftpl", {
    node_id = each.key
    region  = local.aws_region
  })

  # cloud-init's fetch-config reads the agent configuration at first boot;
  # guarantee the parameter exists before any instance is created.
  depends_on = [aws_ssm_parameter.cloudwatch_agent_config]

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required" # IMDSv2
    http_put_response_hop_limit = 1
    http_protocol_ipv6          = "enabled"
  }

  root_block_device {
    volume_type = "gp3"
  }

  # New Debian AMIs must never roll the cluster implicitly. Fresh applies and
  # newly added nodes get the latest AMI; existing nodes are upgraded
  # explicitly, one at a time, e.g.
  #   tofu apply -replace='aws_instance.nodes["1"]'
  lifecycle {
    ignore_changes = [ami]

    precondition {
      condition     = contains(data.aws_ec2_instance_type.selected.supported_architectures, "x86_64")
      error_message = "instance_type must support x86_64; ARM packaging is deferred."
    }

    precondition {
      condition     = data.aws_ec2_instance_type.selected.hypervisor == "nitro"
      error_message = "instance_type must run on the Nitro hypervisor."
    }
  }

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-node-${each.key}"
  })
}

# One Elastic IP per instance for outbound IPv4 connectivity (ActivityPub
# federation with IPv4-only peers). Inbound IPv4 remains closed: the node
# security group has no IPv4 rules. Addresses are not pinned; they change
# across instance replacement.
resource "aws_eip" "nodes" {
  for_each = var.nodes

  instance = aws_instance.nodes[each.key].id
  domain   = "vpc"

  depends_on = [aws_internet_gateway.main]

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-node-${each.key}"
  })
}
