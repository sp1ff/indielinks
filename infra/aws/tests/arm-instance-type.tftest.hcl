# ARM instance types must fail the plan (INFRA-008). The mock pins the
# instance-type data source to an ARM shape; the aws_instance precondition
# must reject it.

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
      supported_architectures = ["arm64"]
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
  instance_type       = "m7g.medium"
}

run "arm_instance_type_rejected" {
  command = plan

  expect_failures = [aws_instance.nodes]
}
