# Three Raft-ID-keyed Debian 12 nodes with pinned primary IPv6 addresses.
# No user_data: cloud-init arrives with OPS-001. SSH works out of the box as
# user "admin" with the configured key pair.

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

resource "aws_instance" "nodes" {
  for_each = var.nodes

  ami           = data.aws_ami.debian_12.id
  instance_type = var.instance_type
  subnet_id     = aws_subnet.public[each.value.subnet_key].id

  # The pinned address is derived from the subnet /64 and the node's hex
  # suffix, so it always lies within the subnet; an out-of-range suffix fails
  # cidrhost at plan time. As the ENI's only GUA it becomes the primary IPv6
  # address and persists across stop/start. When replacing an instance,
  # release the old ENI and reuse the same address for the replacement.
  ipv6_addresses = [cidrhost(
    aws_subnet.public[each.value.subnet_key].ipv6_cidr_block,
    parseint(each.value.ipv6_suffix, 16)
  )]
  enable_primary_ipv6 = true

  # Outbound fallback for IPv4-only ActivityPub peers and package mirrors.
  # There is no IPv4 ingress and the address is not published in DNS; it may
  # change on stop/start without affecting SSH, Raft, TLS, or the ALB.
  associate_public_ip_address = true

  key_name               = data.aws_key_pair.operator.key_name
  iam_instance_profile   = aws_iam_instance_profile.nodes.name
  vpc_security_group_ids = [aws_security_group.nodes.id]

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
  }

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-node-${each.key}"
  })
}
