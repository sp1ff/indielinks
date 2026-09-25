# Bad-input plan failures (INFRA-008). Each run must fail at variable
# validation with the intended error, before any resource is planned.

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

run "ipv4_operator_cidr_rejected" {
  command = plan

  variables {
    operator_ipv6_cidrs = ["203.0.113.0/24"]
  }

  expect_failures = [var.operator_ipv6_cidrs]
}

run "malformed_operator_cidr_rejected" {
  command = plan

  variables {
    operator_ipv6_cidrs = ["not-a-cidr"]
  }

  expect_failures = [var.operator_ipv6_cidrs]
}

run "duplicate_ipv6_suffix_rejected" {
  command = plan

  variables {
    nodes = {
      "0" = { subnet_key = "a", ipv6_suffix = "10" }
      "1" = { subnet_key = "b", ipv6_suffix = "10" }
    }
  }

  expect_failures = [var.nodes]
}

run "unknown_subnet_key_rejected" {
  command = plan

  variables {
    nodes = {
      "0" = { subnet_key = "c", ipv6_suffix = "10" }
    }
  }

  expect_failures = [var.nodes]
}

run "non_numeric_node_id_rejected" {
  command = plan

  variables {
    nodes = {
      "zero" = { subnet_key = "a", ipv6_suffix = "10" }
    }
  }

  expect_failures = [var.nodes]
}

run "bad_log_retention_rejected" {
  command = plan

  variables {
    log_retention_days = 31
  }

  expect_failures = [var.log_retention_days]
}
