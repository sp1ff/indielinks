terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region     = var.aws_region
}

variable "aws_region" {
  type        = string
  description = "AWS region to deploy into (e.g. us-west-2)."
  default     = "us-west-2"
}

variable "operator_cidr" {
  type        = string
  description = "Operator public IP in CIDR form, e.g. 203.0.113.4/32."
  validation {
    condition     = can(cidrnetmask(var.operator_cidr))
    error_message = "operator_cidr must be a valid IPv4/IPv6 CIDR (e.g. 203.0.113.4/32)."
  }
}

variable "ec2_key_name" {
  type        = string
  description = "Name of an existing EC2 key pair in this region/account."
  validation {
    condition     = length(trimspace(var.ec2_key_name)) > 0
    error_message = "ec2_key_name must be non-empty."
  }
}

# check that the key name actually exists
data "aws_key_pair" "existing" {
  key_name = var.ec2_key_name
}


locals {
  vpc_cidr	= "10.0.0.0/16"
  subnet_cidr	= "10.0.1.0/24"
  instance_type = "t3.micro"
  name_prefix	= "indielinks-deb-packaging-systemd"
  tags = {
    Project = local.name_prefix
    OpenTofu = true
  }
}

resource "aws_vpc" "main" {
  cidr_block = local.vpc_cidr

  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-vpc"
  })
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = local.subnet_cidr
  map_public_ip_on_launch = true

  # Keep it simple: let AWS pick an AZ in the chosen region.
  tags = merge(local.tags, {
    Name = "${local.name_prefix}-public-subnet"
  })
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-igw"
  })
}

# Route table for the public subnet: 0.0.0.0/0 -> IGW
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-public-rt"
  })
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "deb_sg" {
  name        = "${local.name_prefix}-deb-sg"
  description = "Allow SSH from operator; allow all egress."
  vpc_id      = aws_vpc.main.id
  # https://search.opentofu.org/provider/opentofu/aws/latest/docs/resources/security_group
  lifecycle {
    create_before_destroy = true
  }

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-deb-sg"
  })
}

resource "aws_vpc_security_group_ingress_rule" "ssh_in" {
  security_group_id = aws_security_group.deb_sg.id

  ip_protocol = "tcp"
  from_port   = 22
  to_port     = 22
  cidr_ipv4   = var.operator_cidr

  description = "SSH from operator_cidr"
}

resource "aws_vpc_security_group_egress_rule" "all_out" {
  security_group_id = aws_security_group.deb_sg.id

  ip_protocol = "-1"
  cidr_ipv4   = "0.0.0.0/0"

  description = "Allow all outbound IPv4"
}

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

resource "aws_instance" "debian" {
  ami				= data.aws_ami.debian_12.id
  instance_type			= local.instance_type
  subnet_id			= aws_subnet.public.id
  vpc_security_group_ids	= [aws_security_group.deb_sg.id]
  key_name                      = data.aws_key_pair.existing.key_name

  user_data = <<-EOF
              #!/bin/bash
              set -ex
              export DEBIAN_FRONTEND=noninteractive
              sudo apt-get update
              sudo apt-get install -y acl libssl-dev binutils
              EOF

  # redundant with map_public_ip_on_launch=true, but explicit
  associate_public_ip_address = true

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-debian"
  })
}

output "instance_public_ip" {
  description = "Public IPv4 address of the Debian EC2 instance."
  value       = aws_instance.debian.public_ip
}
