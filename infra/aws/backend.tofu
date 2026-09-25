terraform {
  required_version = ">= 1.6.0"

  backend "s3" {
    bucket = "s3-713005939292-indielinks"
    key    = "indielinks/prod.tfstate"
    region = "us-west-2"
    # Native OpenTofu lockfile; no DynamoDB lock table.
    use_lockfile = true
    encrypt      = true
  }

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # >= 5.77 for enable_primary_ipv6 on aws_instance.
      version = ">= 5.77"
    }
  }
}

provider "aws" {
  region = local.aws_region
}

locals {
  # Single-region deployment. The backend block above repeats this literally
  # because backend blocks cannot reference variables or locals.
  aws_region  = "us-west-2"
  name_prefix = "indielinks-prod"
  tags = {
    Project     = "indielinks"
    Environment = "prod"
    OpenTofu    = true
  }
}
