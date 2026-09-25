# One least-privilege instance role shared by all nodes. Grants exact-ARN
# DynamoDB and SSM access plus the CloudWatch Agent permissions OPS-001 will
# need. No S3 package access (packages arrive over scp) and no Systems Manager
# managed-instance policy (no SSM Agent).

data "aws_caller_identity" "current" {}

# The parameters already exist (AWS-004); reading them here doubles as a
# plan-time preflight. Only their ARNs enter the configuration and state —
# never their values.
data "aws_ssm_parameter" "peppers" {
  name = "/indielinks/prod/peppers"
}

data "aws_ssm_parameter" "signing_keys" {
  name = "/indielinks/prod/signing-keys"
}

locals {
  # The fixed application tables, created by indielinks-schemas at bootstrap
  # (after this root is applied). Data sources would fail the plan because the
  # tables do not exist yet, so ARNs are constructed from the known names.
  dynamodb_table_names = [
    "users",
    "unique_usernames",
    "posts",
    "following",
    "followers",
    "tasks",
    "raft_log",
    "raft_metadata",
    "schema_migrations",
    "likes_replies_shares",
    "incoming_likes_replies_shares",
  ]
  dynamodb_table_arns = [
    for table in local.dynamodb_table_names :
    "arn:aws:dynamodb:${local.aws_region}:${data.aws_caller_identity.current.account_id}:table/${table}"
  ]
  dynamodb_index_arns = [
    for arn in local.dynamodb_table_arns : "${arn}/index/*"
  ]

  # Created by INFRA-007; kept here so the policy and the future log group
  # cannot drift apart.
  log_group_name = "/indielinks/prod/indielinksd"
  log_group_arn  = "arn:aws:logs:${local.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:${local.log_group_name}"
}

data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "nodes" {
  name               = "${local.name_prefix}-nodes"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json

  tags = local.tags
}

data "aws_iam_policy_document" "nodes" {
  statement {
    sid = "DynamoDBTables"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:BatchGetItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:TransactGetItems",
      "dynamodb:TransactWriteItems",
      "dynamodb:DescribeTable",
      # indielinks-schemas runs from node 0 under the instance profile.
      "dynamodb:CreateTable",
      "dynamodb:DeleteTable",
      "dynamodb:UpdateTable",
    ]
    resources = concat(local.dynamodb_table_arns, local.dynamodb_index_arns)
  }

  statement {
    sid     = "SSMParameterReads"
    actions = ["ssm:GetParameter"]
    resources = [
      data.aws_ssm_parameter.peppers.arn,
      data.aws_ssm_parameter.signing_keys.arn,
      # The CloudWatch Agent configuration (cloudwatch.tofu); cloud-init
      # fetches it via the instance role when starting the agent. Non-secret.
      aws_ssm_parameter.cloudwatch_agent_config.arn,
    ]
  }

  # Emitted only when the parameters are encrypted with a customer-managed
  # key; the production parameters use the AWS-managed key.
  dynamic "statement" {
    for_each = var.ssm_kms_key_arn == null ? [] : [var.ssm_kms_key_arn]
    content {
      sid       = "SSMParameterDecryption"
      actions   = ["kms:Decrypt"]
      resources = [statement.value]
    }
  }

  statement {
    sid = "CloudWatchAgentLogs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
      "logs:PutRetentionPolicy",
    ]
    resources = [local.log_group_arn, "${local.log_group_arn}:*"]
  }

  statement {
    sid       = "CloudWatchMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"] # PutMetricData supports no resource scoping
  }
}

resource "aws_iam_role_policy" "nodes" {
  name   = "${local.name_prefix}-nodes"
  role   = aws_iam_role.nodes.id
  policy = data.aws_iam_policy_document.nodes.json
}

resource "aws_iam_instance_profile" "nodes" {
  name = "${local.name_prefix}-nodes"
  role = aws_iam_role.nodes.name
}
