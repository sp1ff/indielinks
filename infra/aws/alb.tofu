# Dual-stack Application Load Balancer, ACM certificate, and Route 53 DNS
# (INFRA-006). The hosted zone is adopted via data source, never owned:
# destroying this root removes records but cannot touch the zone itself.
#
# The origin is a permanent part of the system's identity: ActivityPub actor
# IDs containing https://indiemark.sh will be stored and federated once the
# instance becomes public.

data "aws_route53_zone" "main" {
  zone_id = var.hosted_zone_id
}

locals {
  # e.g. "indiemark.sh." -> "indiemark.sh"; no hostname is hardcoded.
  domain = trimsuffix(data.aws_route53_zone.main.name, ".")
}

resource "aws_acm_certificate" "main" {
  domain_name               = local.domain
  subject_alternative_names = ["www.${local.domain}"]
  validation_method         = "DNS"

  lifecycle {
    create_before_destroy = true
  }

  tags = local.tags
}

resource "aws_route53_record" "cert_validation" {
  for_each = {
    for dvo in aws_acm_certificate.main.domain_validation_options :
    dvo.domain_name => dvo
  }

  zone_id         = data.aws_route53_zone.main.zone_id
  name            = each.value.resource_record_name
  type            = each.value.resource_record_type
  records         = [each.value.resource_record_value]
  ttl             = 60
  allow_overwrite = true
}

resource "aws_acm_certificate_validation" "main" {
  certificate_arn         = aws_acm_certificate.main.arn
  validation_record_fqdns = [for record in aws_route53_record.cert_validation : record.fqdn]
}

resource "aws_lb" "app" {
  name               = "${local.name_prefix}-alb"
  load_balancer_type = "application"
  internal           = false
  ip_address_type    = "dualstack"

  subnets         = [for subnet in aws_subnet.public : subnet.id]
  security_groups = [aws_security_group.alb.id]

  # No access logs: they require an S3 bucket, which this deployment
  # deliberately avoids. Deletion protection stays off for now; the operator
  # runbook (DOC-001) notes it as a hardening toggle.
  tags = merge(local.tags, {
    Name = "${local.name_prefix}-alb"
  })
}

# The ALB sends health checks and traffic to each instance's primary IPv6
# address -- the consumer of enable_primary_ipv6 in instances.tofu.
resource "aws_lb_target_group" "app" {
  name            = "${local.name_prefix}-tg"
  vpc_id          = aws_vpc.main.id
  target_type     = "instance"
  ip_address_type = "ipv6"
  port            = 20676
  protocol        = "HTTP"

  # Matcher is exactly 202: a freshly started node returns 201 GOOD (alive,
  # Raft uninitialized) and must stay out of service until its persisted or
  # newly initialized Raft membership is available.
  health_check {
    path                = "/healthcheck"
    protocol            = "HTTP"
    matcher             = "202"
    interval            = 15
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-tg"
  })
}

resource "aws_lb_target_group_attachment" "nodes" {
  for_each = var.nodes

  target_group_arn = aws_lb_target_group.app.arn
  target_id        = aws_instance.nodes[each.key].id
  port             = 20676
}

resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.app.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = aws_acm_certificate_validation.main.certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.app.arn
  }
}

resource "aws_lb_listener" "http_redirect" {
  load_balancer_arn = aws_lb.app.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = "redirect"

    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
}

resource "aws_route53_record" "apex_ipv4" {
  zone_id = data.aws_route53_zone.main.zone_id
  name    = local.domain
  type    = "A"

  alias {
    name                   = aws_lb.app.dns_name
    zone_id                = aws_lb.app.zone_id
    evaluate_target_health = true
  }
}

resource "aws_route53_record" "apex_ipv6" {
  zone_id = data.aws_route53_zone.main.zone_id
  name    = local.domain
  type    = "AAAA"

  alias {
    name                   = aws_lb.app.dns_name
    zone_id                = aws_lb.app.zone_id
    evaluate_target_health = true
  }
}
