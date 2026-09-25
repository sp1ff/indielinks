# Node-map scaling invariants (INFRA-008): adding or removing a keyed entry
# changes exactly that node's resources, because every per-node resource is
# for_each-keyed by the durable Raft ID.

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

run "four_nodes" {
  command = plan

  variables {
    nodes = {
      "0" = { subnet_key = "a", ipv6_suffix = "10" }
      "1" = { subnet_key = "b", ipv6_suffix = "11" }
      "2" = { subnet_key = "a", ipv6_suffix = "12" }
      "3" = { subnet_key = "b", ipv6_suffix = "13" }
    }
  }

  assert {
    condition     = length(aws_instance.nodes) == 4
    error_message = "expected four EC2 nodes"
  }

  assert {
    condition     = length(aws_lb_target_group_attachment.nodes) == 4
    error_message = "expected four target-group attachments"
  }

  assert {
    condition     = length(aws_network_interface.nodes) == 4
    error_message = "expected four primary ENIs"
  }

  assert {
    condition     = length(aws_cloudwatch_metric_alarm.node_status_check) == 4
    error_message = "expected four status-check alarms"
  }
}

run "two_nodes" {
  command = plan

  variables {
    nodes = {
      "0" = { subnet_key = "a", ipv6_suffix = "10" }
      "2" = { subnet_key = "a", ipv6_suffix = "12" }
    }
  }

  assert {
    condition     = length(aws_instance.nodes) == 2
    error_message = "expected two EC2 nodes"
  }

  assert {
    condition     = length(aws_lb_target_group_attachment.nodes) == 2
    error_message = "expected two target-group attachments"
  }

  assert {
    condition     = length(aws_network_interface.nodes) == 2
    error_message = "expected two primary ENIs"
  }

  assert {
    condition = (
      length(aws_cloudwatch_metric_alarm.node_status_check) == 2 &&
      length(aws_cloudwatch_metric_alarm.node_memory) == 2 &&
      length(aws_cloudwatch_metric_alarm.node_disk) == 2
    )
    error_message = "expected per-node alarms to track the node map"
  }
}
