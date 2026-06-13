/*
  Amazon MSK (Managed Kafka) module

  3-broker multi-AZ cluster with:
  - TLS encryption in transit
  - KMS encryption at rest
  - SASL/SCRAM authentication
  - CloudWatch metrics (enhanced monitoring)
  - Auto-scaling storage
*/

resource "aws_msk_cluster" "fraud_detection" {
  cluster_name           = "fraud-detection-${var.environment}"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type  = var.broker_instance_type
    client_subnets = var.private_subnet_ids
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_storage_gb
        provisioned_throughput {
          enabled           = var.environment == "prod"
          volume_throughput = var.environment == "prod" ? 250 : null
        }
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
    encryption_at_rest_kms_key_arn = var.kms_key_arn
  }

  client_authentication {
    sasl {
      scram = true
    }
    unauthenticated = false
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
      s3 {
        enabled = true
        bucket  = var.log_bucket_name
        prefix  = "msk-logs/"
      }
    }
  }

  tags = {
    Name = "fraud-detection-kafka-${var.environment}"
  }
}

resource "aws_msk_configuration" "main" {
  name              = "fraud-detection-config-${var.environment}"
  kafka_versions    = ["3.6.0"]
  server_properties = <<-PROPERTIES
    # Retention: 7 days for fraud audit trail
    log.retention.hours=168
    log.retention.bytes=107374182400

    # Compact the transactions.scored topic for latest-value semantics
    log.cleanup.policy=delete

    # Replication factor for HA
    default.replication.factor=3
    min.insync.replicas=2

    # Optimise for high-throughput fraud scoring
    num.partitions=12
    message.max.bytes=10485760
    socket.send.buffer.bytes=102400
    socket.receive.buffer.bytes=102400
  PROPERTIES
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/fraud-detection-${var.environment}"
  retention_in_days = 30
  kms_key_id        = var.kms_key_arn
}

resource "aws_security_group" "msk" {
  name        = "msk-fraud-detection-${var.environment}"
  description = "MSK cluster — allows Kafka traffic from within VPC only"
  vpc_id      = var.vpc_id

  ingress {
    description = "Kafka TLS from VPC"
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  ingress {
    description = "ZooKeeper (internal only)"
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Auto-scaling storage
resource "aws_appautoscaling_target" "msk_storage" {
  max_capacity       = var.broker_storage_gb * 3    # 3× initial size
  min_capacity       = 1
  resource_id        = aws_msk_cluster.fraud_detection.arn
  scalable_dimension = "kafka:broker-storage:VolumeSize"
  service_namespace  = "kafka"
}

resource "aws_appautoscaling_policy" "msk_storage" {
  name               = "msk-storage-autoscaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.msk_storage.resource_id
  scalable_dimension = aws_appautoscaling_target.msk_storage.scalable_dimension
  service_namespace  = aws_appautoscaling_target.msk_storage.service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "KafkaBrokerStorageUtilization"
    }
    target_value = 70.0    # Scale up when storage hits 70%
  }
}
