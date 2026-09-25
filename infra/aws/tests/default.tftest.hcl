# Default-configuration plan invariants (INFRA-008). Runs with a mocked AWS
# provider: no credentials needed, test state is ephemeral.

mock_provider "aws" {
  override_data {
    target = data.aws_availability_zones.available
    values = {
      names = ["us-west-2a", "us-west-2b"]
    }
  }
  override_data {
    target = data.aws_ec2_instance_type.selected
    values = {
      supported_architectures = ["x86_64"]
      hypervisor              = "nitro"
    }
  }
  override_data {
    target = data.aws_caller_identity.current
    values = {
      account_id = "713005939292"
    }
  }
  override_data {
    target = data.aws_ssm_parameter.peppers
    values = {
      arn = "arn:aws:ssm:us-west-2:713005939292:parameter/indielinks/prod/peppers"
    }
  }
  override_data {
    target = data.aws_ssm_parameter.signing_keys
    values = {
      arn = "arn:aws:ssm:us-west-2:713005939292:parameter/indielinks/prod/signing-keys"
    }
  }
  override_data {
    target = data.aws_route53_zone.main
    values = {
      name = "indiemark.sh."
    }
  }
  override_resource {
    target = aws_vpc.main
    values = {
      ipv6_cidr_block = "2001:db8:1234:5600::/56"
    }
  }
  # Mock-generated values must satisfy provider schema validators: ARNs and
  # IAM policy JSON are pinned to valid shapes.
  override_resource {
    target = aws_sns_topic.alarms
    values = {
      arn = "arn:aws:sns:us-west-2:713005939292:indielinks-prod-alarms"
    }
  }
  override_resource {
    target = aws_lb.app
    values = {
      arn = "arn:aws:elasticloadbalancing:us-west-2:713005939292:loadbalancer/app/indielinks-prod-alb/0000000000000000"
    }
  }
  override_resource {
    target = aws_lb_target_group.app
    values = {
      arn = "arn:aws:elasticloadbalancing:us-west-2:713005939292:targetgroup/indielinks-prod-tg/0000000000000000"
    }
  }
  override_resource {
    target = aws_acm_certificate.main
    values = {
      arn = "arn:aws:acm:us-west-2:713005939292:certificate/00000000-0000-0000-0000-000000000000"
    }
  }
  override_data {
    target = data.aws_iam_policy_document.assume_ec2
    values = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
  override_data {
    target = data.aws_iam_policy_document.nodes
    values = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}

variables {
  ec2_key_name        = "indielinks-sp1ff-keypair"
  hosted_zone_id      = "Z07598301IF2GN8JH9W19"
  operator_ipv6_cidrs = ["2601:1c2:4080:14c0::/64"]
}

run "default_plan" {
  command = plan

  assert {
    condition     = length(aws_instance.nodes) == 3
    error_message = "expected three EC2 nodes by default"
  }

  assert {
    condition     = length(aws_lb_target_group_attachment.nodes) == 3
    error_message = "expected one target-group attachment per node"
  }

  assert {
    condition     = length(aws_network_interface.nodes) == 3
    error_message = "expected one primary ENI per node"
  }

  assert {
    condition = alltrue([
      for eni in aws_network_interface.nodes : eni.enable_primary_ipv6
    ])
    error_message = "every node ENI must designate a primary IPv6 address"
  }

  assert {
    condition = alltrue([
      for subnet in aws_subnet.public : subnet.map_public_ip_on_launch != true
    ])
    error_message = "subnets must not auto-assign public IPv4 addresses"
  }

  assert {
    condition = alltrue([
      for id, instance in aws_instance.nodes :
      strcontains(instance.user_data, "indielinks/node-id string ${id}")
    ])
    error_message = "each node's cloud-init user_data must pre-seed its Raft node ID via debconf"
  }

  assert {
    condition = alltrue([
      for _id, instance in aws_instance.nodes :
      strcontains(instance.user_data, "s3.dualstack.us-west-2.amazonaws.com")
    ])
    error_message = "cloud-init must fetch the CloudWatch Agent over a dual-stack S3 endpoint"
  }

  assert {
    condition     = aws_lb_target_group.app.ip_address_type == "ipv6"
    error_message = "target group must be IPv6"
  }

  assert {
    condition     = aws_lb_target_group.app.health_check[0].matcher == "202"
    error_message = "health check matcher must be exactly 202"
  }

  assert {
    condition     = aws_lb_target_group.app.health_check[0].path == "/healthcheck"
    error_message = "health check path must be /healthcheck"
  }

  assert {
    condition     = aws_lb_listener.http_redirect.default_action[0].type == "redirect"
    error_message = "port 80 must redirect to HTTPS"
  }

  assert {
    condition     = aws_lb.app.ip_address_type == "dualstack"
    error_message = "ALB must be dual-stack"
  }

  assert {
    condition = alltrue([
      for rule in aws_vpc_security_group_ingress_rule.nodes_ssh :
      rule.cidr_ipv4 == null
    ])
    error_message = "SSH ingress must be IPv6-only"
  }

  assert {
    condition = (
      aws_vpc_security_group_ingress_rule.nodes_public_from_alb.cidr_ipv4 == null &&
      aws_vpc_security_group_ingress_rule.nodes_raft_grpc.cidr_ipv4 == null &&
      aws_vpc_security_group_ingress_rule.nodes_icmpv6.cidr_ipv4 == null
    )
    error_message = "node security group must have no IPv4 ingress"
  }

  assert {
    condition     = aws_route53_record.apex_ipv4.name == "indiemark.sh"
    error_message = "apex A record must be indiemark.sh"
  }

  assert {
    condition     = aws_route53_record.apex_ipv6.type == "AAAA"
    error_message = "apex must have an AAAA alias record"
  }

  assert {
    condition     = length(aws_sns_topic_subscription.email) == 0
    error_message = "no email subscription should exist when alert_email is null"
  }

  assert {
    condition     = aws_cloudwatch_log_group.app.retention_in_days == 30
    error_message = "default log retention must be 30 days"
  }

  assert {
    condition = (
      length(aws_cloudwatch_metric_alarm.node_status_check) == 3 &&
      length(aws_cloudwatch_metric_alarm.node_memory) == 3 &&
      length(aws_cloudwatch_metric_alarm.node_disk) == 3
    )
    error_message = "expected one status-check, memory, and disk alarm per node"
  }
}
