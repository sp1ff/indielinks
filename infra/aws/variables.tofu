variable "instance_type" {
  type        = string
  description = "x86-64 Nitro instance type for all nodes. t3a.nano is a starting value for cost discovery, not a capacity conclusion."
  default     = "t3a.nano"
}

variable "operator_ipv6_cidrs" {
  type        = list(string)
  description = "IPv6 CIDRs permitted to SSH to the nodes, e.g. [\"2601:1c2:4080:14c0::/64\"]."

  validation {
    condition = alltrue([
      for cidr in var.operator_ipv6_cidrs :
      can(cidrhost(cidr, 0)) && can(regex(":", cidr))
    ])
    error_message = "Each entry must be a valid IPv6 CIDR (e.g. 2601:1c2:4080:14c0::/64)."
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

variable "hosted_zone_id" {
  type        = string
  description = "Route 53 hosted zone ID for indiemark.sh. Declared here; consumed by the ALB/DNS commit (INFRA-006)."

  validation {
    condition     = length(trimspace(var.hosted_zone_id)) > 0
    error_message = "hosted_zone_id must be non-empty."
  }
}

variable "ssm_kms_key_arn" {
  type        = string
  description = "ARN of the customer-managed KMS key encrypting the SSM SecureString parameters, if any. Null when the parameters use the AWS-managed key."
  default     = null
}

variable "nodes" {
  type = map(object({
    subnet_key  = string
    ipv6_suffix = string
  }))
  description = "Nodes keyed by durable Raft node ID. Never derive a Raft ID from list position. ipv6_suffix is hex within the subnet /64."
  default = {
    "0" = { subnet_key = "a", ipv6_suffix = "10" }
    "1" = { subnet_key = "b", ipv6_suffix = "11" }
    "2" = { subnet_key = "a", ipv6_suffix = "12" }
  }

  validation {
    condition = alltrue([
      for id, _node in var.nodes :
      can(regex("^[0-9]+$", id))
    ])
    error_message = "Node map keys must be non-negative integers (Raft node IDs)."
  }

  validation {
    condition = alltrue([
      for _id, node in var.nodes :
      can(parseint(node.ipv6_suffix, 16))
    ])
    error_message = "ipv6_suffix must be hexadecimal."
  }

  validation {
    condition = length(distinct([
      for _id, node in var.nodes : node.ipv6_suffix
    ])) == length(var.nodes)
    error_message = "ipv6_suffix values must be unique."
  }

  validation {
    condition = alltrue([
      for _id, node in var.nodes :
      contains(["a", "b"], node.subnet_key)
    ])
    error_message = "subnet_key must be \"a\" or \"b\" (keep in sync with the subnets local in network.tofu)."
  }
}

# Plan-time instance-type validation: fails the data source on an unknown type
# name, and the check block on ARM or non-Nitro types.
data "aws_ec2_instance_type" "selected" {
  instance_type = var.instance_type
}

check "instance_type_x86_64_nitro" {
  assert {
    condition     = contains(data.aws_ec2_instance_type.selected.supported_architectures, "x86_64")
    error_message = "instance_type must support x86_64; ARM packaging is deferred."
  }
  assert {
    condition     = data.aws_ec2_instance_type.selected.hypervisor == "nitro"
    error_message = "instance_type must run on the Nitro hypervisor."
  }
}
