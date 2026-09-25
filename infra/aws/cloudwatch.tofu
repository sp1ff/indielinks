# Application log group, CloudWatch Agent configuration, SNS alarm topic,
# and alarms (INFRA-007). The agent itself is installed by OPS-001's
# cloud-init, which fetches the configuration parameter below via the
# instance role (no SSM Agent). Until then, agent-sourced alarms sit in
# INSUFFICIENT_DATA, which treat_missing_data = "missing" keeps from paging.

resource "aws_cloudwatch_log_group" "app" {
  # The instance-role policy (iam.tofu) already grants this exact ARN.
  name              = local.log_group_name
  retention_in_days = var.log_retention_days

  tags = local.tags
}

# Non-secret by construction: log paths, metric names, and loopback
# endpoints only. jsonencode keeps the value valid by construction.
resource "aws_ssm_parameter" "cloudwatch_agent_config" {
  name = "/indielinks/prod/cloudwatch-agent-config"
  type = "String"
  tier = "Standard"
  value = jsonencode({
    agent = {
      metrics_collection_interval = 60
      run_as_user                 = "cwagent"
      # First-class dual-stack support; endpoint_override has a known
      # ignored-IPv6-endpoint bug (aws/amazon-cloudwatch-agent#1639).
      use_dualstack_endpoint = true
    }
    logs = {
      logs_collected = {
        files = {
          collect_list = [
            {
              file_path       = "/var/log/indielinks/indielinksd.log"
              log_group_name  = local.log_group_name
              log_stream_name = "{instance_id}"
            }
          ]
        }
      }
    }
    metrics = {
      namespace = "Indielinks/Host"
      append_dimensions = {
        InstanceId = "$${aws:InstanceId}"
      }
      metrics_collected = {
        cpu = {
          measurement = ["cpu_usage_active"]
          totalcpu    = true
        }
        mem = {
          measurement = ["mem_used_percent"]
        }
        disk = {
          measurement = ["disk_used_percent"]
          resources   = ["/"]
        }
        swap = {
          measurement = ["swap_used_percent"]
        }
        net = {
          measurement = ["net_bytes_recv", "net_bytes_sent"]
        }
        # Receives the daemon's OTLP/HTTP exporter (otlp-export in
        # conf/indielinksd-aws.ncl) and republishes to CloudWatch Metrics
        # with SigV4 under the instance profile.
        otlp = {
          http_endpoint = "127.0.0.1:4318"
        }
      }
    }
  })

  tags = local.tags
}

resource "aws_sns_topic" "alarms" {
  name = "indielinks-prod-alarms"

  tags = local.tags
}

resource "aws_sns_topic_subscription" "email" {
  count = var.alert_email == null ? 0 : 1

  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Will be ALARM by design until Raft initialization (OPS-003): the target
# group matcher is exactly 202 and uninitialized nodes return 201.
resource "aws_cloudwatch_metric_alarm" "alb_unhealthy_hosts" {
  alarm_name        = "${local.name_prefix}-alb-unhealthy-hosts"
  alarm_description = "Targets behind the indiemark.sh ALB are unhealthy (healthcheck != 202)."

  namespace   = "AWS/ApplicationELB"
  metric_name = "UnHealthyHostCount"
  statistic   = "Maximum"
  dimensions = {
    LoadBalancer = aws_lb.app.arn_suffix
    TargetGroup  = aws_lb_target_group.app.arn_suffix
  }

  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  period              = 60
  evaluation_periods  = 2
  treat_missing_data  = "missing"

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]

  tags = local.tags
}

resource "aws_cloudwatch_metric_alarm" "alb_target_5xx" {
  alarm_name        = "${local.name_prefix}-alb-target-5xx"
  alarm_description = "Sustained 5xx responses from indielinksd behind the ALB."

  namespace   = "AWS/ApplicationELB"
  metric_name = "HTTPCode_Target_5XX_Count"
  statistic   = "Sum"
  dimensions = {
    LoadBalancer = aws_lb.app.arn_suffix
    TargetGroup  = aws_lb_target_group.app.arn_suffix
  }

  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 5
  period              = 60
  evaluation_periods  = 3
  treat_missing_data  = "missing"

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]

  tags = local.tags
}

resource "aws_cloudwatch_metric_alarm" "node_status_check" {
  for_each = var.nodes

  alarm_name        = "${local.name_prefix}-node-${each.key}-status-check"
  alarm_description = "EC2 status check failed for node ${each.key}."

  namespace   = "AWS/EC2"
  metric_name = "StatusCheckFailed"
  statistic   = "Maximum"
  dimensions = {
    InstanceId = aws_instance.nodes[each.key].id
  }

  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  period              = 60
  evaluation_periods  = 2
  treat_missing_data  = "missing"

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]

  tags = local.tags
}

resource "aws_cloudwatch_metric_alarm" "node_memory" {
  for_each = var.nodes

  alarm_name        = "${local.name_prefix}-node-${each.key}-memory"
  alarm_description = "Memory usage above 85% on node ${each.key} (CloudWatch Agent)."

  namespace   = "Indielinks/Host"
  metric_name = "mem_used_percent"
  statistic   = "Average"
  dimensions = {
    InstanceId = aws_instance.nodes[each.key].id
  }

  comparison_operator = "GreaterThanThreshold"
  threshold           = 85
  period              = 300
  evaluation_periods  = 3
  treat_missing_data  = "missing"

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]

  tags = local.tags
}

resource "aws_cloudwatch_metric_alarm" "node_disk" {
  for_each = var.nodes

  alarm_name        = "${local.name_prefix}-node-${each.key}-disk"
  alarm_description = "Root filesystem usage above 85% on node ${each.key} (CloudWatch Agent)."

  namespace   = "Indielinks/Host"
  metric_name = "disk_used_percent"
  statistic   = "Average"
  # The agent publishes disk metrics under the full device/path/fstype
  # dimension set. Nitro root volume on the Debian 12 AMI: /dev/nvme0n1p1,
  # ext4. If the device differs, this alarm idles in INSUFFICIENT_DATA;
  # LIVE-004 exercises every alarm and will catch it.
  dimensions = {
    InstanceId = aws_instance.nodes[each.key].id
    path       = "/"
    device     = "nvme0n1p1"
    fstype     = "ext4"
  }

  comparison_operator = "GreaterThanThreshold"
  threshold           = 85
  period              = 300
  evaluation_periods  = 3
  treat_missing_data  = "missing"

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]

  tags = local.tags
}
