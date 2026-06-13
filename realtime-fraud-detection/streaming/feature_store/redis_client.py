"""
Online Feature Store client backed by Redis.

Features stored per card_id:
  - tx_count_1min      Number of transactions in the last 60 seconds
  - tx_count_5min      Number of transactions in the last 5 minutes
  - avg_amount_30d     30-day rolling average transaction amount
  - last_latitude      Latitude of the most recent transaction
  - last_longitude     Longitude of the most recent transaction
  - last_tx_timestamp  ISO timestamp of the most recent transaction
  - seen_device_fingerprints  JSON list of device fingerprints seen in last 90d
  - merchant_categories_30d   JSON list of merchant categories in last 30d

All keys use a consistent naming convention:
  card:{card_id}:feature_name

TTLs are set to prevent stale data accumulation:
  - Velocity counters: 5-minute TTL (rolling window via Redis sorted sets)
  - Profile features: 30/90-day TTL

Using Redis sorted sets for velocity windows — members are transaction IDs,
scores are epoch timestamps. Window queries are ZRANGEBYSCORE calls.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

import redis


class FeatureStoreClient:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        ssl: bool = False,
        socket_timeout: float = 0.05,   # 50ms — fail fast to not block the scoring pipeline
    ):
        self._pool = redis.ConnectionPool(
            host=host,
            port=port,
            password=password,
            ssl=ssl,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_timeout,
            decode_responses=True,
            max_connections=20,
        )
        self._redis = redis.Redis(connection_pool=self._pool)

    def get_features(self, card_id: str, customer_id: str) -> Dict[str, Any]:
        """
        Fetch all features for a card in a single pipeline call.
        Returns an empty dict (safe defaults) on any Redis error — we prefer
        a false negative over blocking the payment.
        """
        now = time.time()
        one_min_ago = now - 60
        five_min_ago = now - 300

        try:
            pipe = self._redis.pipeline(transaction=False)
            pipe.zcount(f"card:{card_id}:velocity", one_min_ago, now)
            pipe.zcount(f"card:{card_id}:velocity", five_min_ago, now)
            pipe.hget(f"card:{card_id}:profile", "avg_amount_30d")
            pipe.hget(f"card:{card_id}:profile", "last_latitude")
            pipe.hget(f"card:{card_id}:profile", "last_longitude")
            pipe.hget(f"card:{card_id}:profile", "last_tx_timestamp")
            pipe.hget(f"card:{card_id}:profile", "seen_device_fingerprints")
            pipe.hget(f"card:{card_id}:profile", "merchant_categories_30d")
            results = pipe.execute()

            return {
                "tx_count_1min":               int(results[0] or 0),
                "tx_count_5min":               int(results[1] or 0),
                "avg_amount_30d":              float(results[2] or 0),
                "last_latitude":               float(results[3]) if results[3] else None,
                "last_longitude":              float(results[4]) if results[4] else None,
                "last_tx_timestamp":           results[5],
                "seen_device_fingerprints":    json.loads(results[6] or "[]"),
                "merchant_categories_30d":     json.loads(results[7] or "[]"),
            }

        except redis.RedisError:
            # Fail open — return empty features to avoid blocking payments
            # when Redis is unhealthy. This is a conscious trade-off: we accept
            # slightly higher fraud risk over transaction availability.
            return {}

    def update_velocity(
        self, card_id: str, amount: float, timestamp: str
    ) -> None:
        """
        Update the velocity sorted set for a card.
        Uses a pipeline for atomic-ish updates (not strictly atomic but good enough).
        TTL of 5 minutes prevents unbounded set growth.
        """
        try:
            now = time.time()
            five_min_ago = now - 300

            pipe = self._redis.pipeline(transaction=False)
            # Add this transaction to the sorted set (score = epoch timestamp)
            pipe.zadd(f"card:{card_id}:velocity", {timestamp: now})
            # Prune entries older than 5 minutes
            pipe.zremrangebyscore(f"card:{card_id}:velocity", 0, five_min_ago)
            # Ensure key expires if card goes inactive
            pipe.expire(f"card:{card_id}:velocity", 300)
            pipe.execute()
        except redis.RedisError:
            pass    # Non-critical — velocity will be slightly stale

    def set_profile_features(self, card_id: str, features: Dict[str, Any]) -> None:
        """
        Bulk-write profile features from the nightly batch refresh.
        TTL of 35 days ensures stale profiles expire even if the batch job fails.
        """
        try:
            serialised = {}
            for k, v in features.items():
                if isinstance(v, (list, dict)):
                    serialised[k] = json.dumps(v)
                else:
                    serialised[k] = str(v)

            self._redis.hset(f"card:{card_id}:profile", mapping=serialised)
            self._redis.expire(f"card:{card_id}:profile", 86400 * 35)
        except redis.RedisError:
            pass
