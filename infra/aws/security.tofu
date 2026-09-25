# Least-privilege security groups. The ALB group is created now although the
# ALB itself arrives with INFRA-006. Ports follow conf/indielinksd-aws.ncl:
# 20676 public, 20677 private (loopback only, no rule here), 20678 Raft gRPC.

resource "aws_security_group" "alb" {
  name        = "${local.name_prefix}-alb-sg"
  description = "indiemark.sh ALB: HTTPS (and HTTP redirect) from anywhere"
  vpc_id      = aws_vpc.main.id

  lifecycle {
    create_before_destroy = true
  }

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-alb-sg"
  })
}

resource "aws_vpc_security_group_ingress_rule" "alb_https_ipv4" {
  security_group_id = aws_security_group.alb.id

  ip_protocol = "tcp"
  from_port   = 443
  to_port     = 443
  cidr_ipv4   = "0.0.0.0/0"

  description = "HTTPS from anywhere (IPv4)"
}

resource "aws_vpc_security_group_ingress_rule" "alb_https_ipv6" {
  security_group_id = aws_security_group.alb.id

  ip_protocol = "tcp"
  from_port   = 443
  to_port     = 443
  cidr_ipv6   = "::/0"

  description = "HTTPS from anywhere (IPv6)"
}

resource "aws_vpc_security_group_ingress_rule" "alb_http_ipv4" {
  security_group_id = aws_security_group.alb.id

  ip_protocol = "tcp"
  from_port   = 80
  to_port     = 80
  cidr_ipv4   = "0.0.0.0/0"

  description = "HTTP from anywhere (IPv4); HTTP-to-HTTPS redirect only"
}

resource "aws_vpc_security_group_ingress_rule" "alb_http_ipv6" {
  security_group_id = aws_security_group.alb.id

  ip_protocol = "tcp"
  from_port   = 80
  to_port     = 80
  cidr_ipv6   = "::/0"

  description = "HTTP from anywhere (IPv6); HTTP-to-HTTPS redirect only"
}

resource "aws_vpc_security_group_egress_rule" "alb_to_nodes" {
  security_group_id = aws_security_group.alb.id

  ip_protocol                  = "tcp"
  from_port                    = 20676
  to_port                      = 20676
  referenced_security_group_id = aws_security_group.nodes.id

  description = "Forward to the indielinksd public listener (IPv6 target group)"
}

resource "aws_security_group" "nodes" {
  name        = "${local.name_prefix}-nodes-sg"
  description = "indielinks nodes: ALB traffic, Raft gRPC, operator SSH, ICMPv6; no IPv4 ingress"
  vpc_id      = aws_vpc.main.id

  lifecycle {
    create_before_destroy = true
  }

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-nodes-sg"
  })
}

resource "aws_vpc_security_group_ingress_rule" "nodes_public_from_alb" {
  security_group_id = aws_security_group.nodes.id

  ip_protocol                  = "tcp"
  from_port                    = 20676
  to_port                      = 20676
  referenced_security_group_id = aws_security_group.alb.id

  description = "indielinksd public listener from the ALB (IPv6 target group)"
}

resource "aws_vpc_security_group_ingress_rule" "nodes_raft_grpc" {
  security_group_id = aws_security_group.nodes.id

  ip_protocol                  = "tcp"
  from_port                    = 20678
  to_port                      = 20678
  referenced_security_group_id = aws_security_group.nodes.id

  description = "Raft gRPC between nodes over stable IPv6"
}

resource "aws_vpc_security_group_ingress_rule" "nodes_ssh" {
  for_each = toset(var.operator_ipv6_cidrs)

  security_group_id = aws_security_group.nodes.id

  ip_protocol = "tcp"
  from_port   = 22
  to_port     = 22
  cidr_ipv6   = each.value

  description = "SSH from operator IPv6 CIDR ${each.value}"
}

resource "aws_vpc_security_group_ingress_rule" "nodes_icmpv6" {
  security_group_id = aws_security_group.nodes.id

  ip_protocol = "icmpv6"
  from_port   = -1
  to_port     = -1
  cidr_ipv6   = "::/0"

  description = "All ICMPv6, including Packet Too Big (type 2) for path MTU discovery"
}

resource "aws_vpc_security_group_egress_rule" "nodes_all_ipv6" {
  security_group_id = aws_security_group.nodes.id

  ip_protocol = "-1"
  cidr_ipv6   = "::/0"

  description = "All outbound IPv6: federation, package mirrors, dual-stack AWS APIs"
}

resource "aws_vpc_security_group_egress_rule" "nodes_all_ipv4" {
  security_group_id = aws_security_group.nodes.id

  ip_protocol = "-1"
  cidr_ipv4   = "0.0.0.0/0"

  description = "All outbound IPv4: IPv4-only ActivityPub peers and package mirrors"
}
