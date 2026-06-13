"""
Deterministic fraud rules engine.

Rules are evaluated in priority order — the first matching BLOCK rule short-circuits.
REVIEW rules are accumulated (multiple soft signals can escalate to a block).

Adding a new rule requires:
  1. Write a method prefixed with _rule_
  2. Add it to RULE_PRIORITY_ORDER
  3. Write a unit test in tests/test_rules_engine.py

No machine learning here — pure business logic. Fast, explainable, auditable.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from feature_store.redis_client import FeatureStoreClient


@dataclass
class FraudDecision:
    action: str            # BLOCK | REVIEW | PASS
    triggered_rules: List[str]
    reason: str
    ml_score: float = 0.0


class RulesEngine:
    # Configurable thresholds — in production loaded from a config service
    # so risk team can tune without a deployment.
    VELOCITY_1MIN_THRESHOLD = 5
    VELOCITY_5MIN_THRESHOLD = 10
    AMOUNT_SPIKE_MULTIPLIER = 3.0
    HIGH_AMOUNT_NEW_DEVICE = 500.0
    GEO_SPEED_KMH_MAX = 900.0           # Slightly above fastest commercial flight
    REVIEW_SCORE_ACCUMULATE = 2         # N REVIEW signals → escalate to BLOCK

    BLACKLISTED_MERCHANTS: set = set()  # Loaded from config/DB at runtime

    def __init__(self, feature_store: FeatureStoreClient):
        self.fs = feature_store

    def evaluate(self, tx: Dict[str, Any]) -> FraudDecision:
        features = self.fs.get_features(tx["card_id"], tx["customer_id"])
        triggered = []

        # --- Hard blocks (short-circuit on first match) ---
        if self._rule_blacklisted_merchant(tx):
            return FraudDecision(
                action="BLOCK",
                triggered_rules=["blacklisted_merchant"],
                reason=f"Merchant {tx['merchant_id']} is on the fraud watchlist",
            )

        if self._rule_velocity_1min(features):
            return FraudDecision(
                action="BLOCK",
                triggered_rules=["velocity_1min"],
                reason=f"Card exceeded {self.VELOCITY_1MIN_THRESHOLD} transactions in 60s",
            )

        # --- Soft signals (accumulate) ---
        if self._rule_velocity_5min(features):
            triggered.append("velocity_5min")

        if self._rule_amount_spike(tx, features):
            triggered.append("amount_spike")

        if self._rule_geo_impossibility(tx, features):
            triggered.append("geo_impossibility")
            # Geo-impossibility is a hard block on its own
            return FraudDecision(
                action="BLOCK",
                triggered_rules=triggered,
                reason="Transaction location is physically impossible given previous activity",
            )

        if self._rule_new_device_high_amount(tx, features):
            triggered.append("new_device_high_amount")

        if self._rule_unusual_merchant_category(tx, features):
            triggered.append("unusual_merchant_category")

        # --- Decision from accumulated soft signals ---
        if len(triggered) >= self.REVIEW_SCORE_ACCUMULATE:
            return FraudDecision(
                action="BLOCK",
                triggered_rules=triggered,
                reason=f"Multiple fraud signals detected: {', '.join(triggered)}",
            )
        elif triggered:
            return FraudDecision(
                action="REVIEW",
                triggered_rules=triggered,
                reason=f"Suspicious signals: {', '.join(triggered)}",
            )

        return FraudDecision(action="PASS", triggered_rules=[], reason="No fraud signals")

    # ------------------------------------------------------------------
    # Individual rule implementations
    # ------------------------------------------------------------------

    def _rule_blacklisted_merchant(self, tx: Dict) -> bool:
        return tx.get("merchant_id") in self.BLACKLISTED_MERCHANTS

    def _rule_velocity_1min(self, features: Dict) -> bool:
        return features.get("tx_count_1min", 0) >= self.VELOCITY_1MIN_THRESHOLD

    def _rule_velocity_5min(self, features: Dict) -> bool:
        return features.get("tx_count_5min", 0) >= self.VELOCITY_5MIN_THRESHOLD

    def _rule_amount_spike(self, tx: Dict, features: Dict) -> bool:
        avg_30d = features.get("avg_amount_30d", 0)
        if avg_30d == 0:
            return False
        return float(tx["amount_usd"]) > (avg_30d * self.AMOUNT_SPIKE_MULTIPLIER)

    def _rule_geo_impossibility(self, tx: Dict, features: Dict) -> bool:
        """
        Check if the current transaction location is reachable from the last known
        location given the elapsed time. Uses Haversine distance.
        """
        last_lat = features.get("last_latitude")
        last_lon = features.get("last_longitude")
        last_ts_str = features.get("last_tx_timestamp")

        if any(v is None for v in [last_lat, last_lon, last_ts_str]):
            return False

        tx_lat = tx.get("latitude")
        tx_lon = tx.get("longitude")
        if tx_lat is None or tx_lon is None:
            return False

        distance_km = _haversine(last_lat, last_lon, tx_lat, tx_lon)

        last_ts = datetime.fromisoformat(last_ts_str)
        tx_ts = datetime.fromisoformat(tx["event_time"].replace("Z", "+00:00"))
        elapsed_hours = (tx_ts - last_ts).total_seconds() / 3600.0

        if elapsed_hours <= 0:
            return False

        speed_kmh = distance_km / elapsed_hours
        return speed_kmh > self.GEO_SPEED_KMH_MAX

    def _rule_new_device_high_amount(self, tx: Dict, features: Dict) -> bool:
        seen_devices = features.get("seen_device_fingerprints", [])
        is_new_device = tx.get("device_fingerprint") not in seen_devices
        is_high_amount = float(tx["amount_usd"]) > self.HIGH_AMOUNT_NEW_DEVICE
        return is_new_device and is_high_amount

    def _rule_unusual_merchant_category(self, tx: Dict, features: Dict) -> bool:
        """Flag if the merchant category has never appeared in the customer's history."""
        seen_categories = features.get("merchant_categories_30d", [])
        current_category = tx.get("merchant_category")
        return bool(current_category) and current_category not in seen_categories


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great-circle distance in km between two (lat, lon) points."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))
