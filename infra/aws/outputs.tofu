# Minimal outputs for this commit's goal (SSH access). INFRA-008 expands them
# with ALB/DNS identifiers and plan-test support.

output "nodes" {
  description = "Node ID -> instance ID, pinned IPv6 address, and ready-to-use SSH command."
  value = {
    for id, instance in aws_instance.nodes : id => {
      instance_id = instance.id
      ipv6        = instance.ipv6_addresses[0]
      ssh         = "ssh -6 admin@${instance.ipv6_addresses[0]}"
    }
  }
}

output "debian_ami_id" {
  description = "Resolved Debian 12 AMI ID, recorded in each plan."
  value       = data.aws_ami.debian_12.id
}

output "alb_security_group_id" {
  description = "ALB security group; referenced by the ALB commit (INFRA-006)."
  value       = aws_security_group.alb.id
}

output "nodes_security_group_id" {
  description = "Node security group; referenced by later commits."
  value       = aws_security_group.nodes.id
}
