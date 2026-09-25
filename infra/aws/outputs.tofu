# Minimal outputs for this commit's goal (SSH access). INFRA-008 expands them
# with ALB/DNS identifiers and plan-test support.

output "nodes" {
  description = "Node ID -> instance ID, pinned IPv6 address, and ready-to-use SSH command."
  value = {
    for id, instance in aws_instance.nodes : id => {
      instance_id = instance.id
      ipv6        = local.node_ipv6_addresses[id]
      ssh         = "ssh -6 admin@${local.node_ipv6_addresses[id]}"
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

output "alb_dns_name" {
  description = "Public DNS name of the dual-stack ALB."
  value       = aws_lb.app.dns_name
}

output "alb_zone_id" {
  description = "Route 53 hosted zone ID of the ALB."
  value       = aws_lb.app.zone_id
}

output "acm_certificate_arn" {
  description = "ARN of the validated ACM certificate (indiemark.sh + www SAN)."
  value       = aws_acm_certificate_validation.main.certificate_arn
}

output "target_group_arn" {
  description = "ARN of the IPv6 instance target group."
  value       = aws_lb_target_group.app.arn
}

output "log_group_name" {
  description = "CloudWatch log group for indielinksd JSON logs."
  value       = aws_cloudwatch_log_group.app.name
}

output "alarms_topic_arn" {
  description = "SNS topic receiving all alarm notifications."
  value       = aws_sns_topic.alarms.arn
}

output "hosted_zone_id" {
  description = "Route 53 hosted zone ID adopted as a data source (used by admin/aws-preflight)."
  value       = var.hosted_zone_id
}

output "public_url" {
  description = "Public origin of the service."
  value       = "https://${local.domain}"
}

output "healthcheck_hint" {
  description = "Expect 201 before Raft initialization (OPS-003), 202 after."
  value       = "curl -s -o /dev/null -w '%%{http_code}' https://${local.domain}/healthcheck"
}
