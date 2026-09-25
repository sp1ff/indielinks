# Dual-stack network: one VPC with an Amazon-provided IPv6 /56, two public
# /64 subnets in two availability zones, and an ordinary Internet Gateway.
# Deliberately absent: NAT gateway, egress-only IGW, private subnets, EICE.

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  # Variable validation cannot reference locals, so the subnet_key validation
  # in variables.tofu repeats these keys literally; keep them in sync.
  subnets = {
    "a" = { az_index = 0, ipv4_cidr = "10.0.1.0/24", ipv6_netnum = 0 }
    "b" = { az_index = 1, ipv4_cidr = "10.0.2.0/24", ipv6_netnum = 1 }
  }
}

resource "aws_vpc" "main" {
  # Subnets require an IPv4 CIDR even though nodes have no public IPv4
  # addresses; ENIs still hold (free, unused-for-egress) private IPv4.
  cidr_block                       = "10.0.0.0/16"
  assign_generated_ipv6_cidr_block = true

  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-vpc"
  })
}

resource "aws_subnet" "public" {
  for_each = local.subnets

  vpc_id            = aws_vpc.main.id
  availability_zone = data.aws_availability_zones.available.names[each.value.az_index]
  cidr_block        = each.value.ipv4_cidr
  # /64 carved out of the VPC's Amazon-provided /56.
  ipv6_cidr_block = cidrsubnet(aws_vpc.main.ipv6_cidr_block, 8, each.value.ipv6_netnum)

  # No public IPv4: nodes are IPv6-only by design (outbound IPv4 federation
  # is sacrificed; inbound IPv4 terminates at the dual-stack ALB).
  map_public_ip_on_launch = false

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-public-${each.key}"
  })
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-igw"
  })
}

# One route table for both public subnets. The ordinary Internet Gateway is
# required (not an egress-only IGW) because the ALB, SSH clients, and Raft
# peers initiate IPv6 connections to the nodes.
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  route {
    ipv6_cidr_block = "::/0"
    gateway_id      = aws_internet_gateway.main.id
  }

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-public-rt"
  })
}

resource "aws_route_table_association" "public" {
  for_each = aws_subnet.public

  subnet_id      = each.value.id
  route_table_id = aws_route_table.public.id
}
