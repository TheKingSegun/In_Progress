"""Structured JSON logger for Spark jobs — outputs to CloudWatch via stdout."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class StructuredFormatter(logging.Formatter):
    """Emits each log record as a JSON line for easy CloudWatch Insights querying."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "job": os.environ.get("JOB_NAME", "unknown"),
            "env": os.environ.get("ENVIRONMENT", "unknown"),
            "run_id": os.environ.get("AIRFLOW_RUN_ID", "manual"),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    log_level = getattr(logging, (level or os.environ.get("LOG_LEVEL", "INFO")).upper())
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredFormatter())
        logger.addHandler(handler)
    logger.setLevel(log_level)
    logger.propagate = False
    return logger
