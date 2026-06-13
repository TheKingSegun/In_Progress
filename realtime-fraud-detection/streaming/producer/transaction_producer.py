"""
Transaction event simulator for local development and load testing.

Generates realistic synthetic transaction events and produces them to Kafka.
Injects configurable fraud patterns to test the detection pipeline.

Usage:
  python transaction_producer.py --rate 100 --fraud-rate 0.02 --duration 300
  (produces 100 tx/sec with 2% fraud for 5 minutes)
"""

from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer

# Realistic geo clusters (city lat/lon bounding boxes)
GEO_CLUSTERS = [
    {"city": "London",    "lat": (51.45, 51.55),  "lon": (-0.20, 0.10)},
    {"city": "New York",  "lat": (40.65, 40.85),  "lon": (-74.05, -73.90)},
    {"city": "Lagos",     "lat": (6.40, 6.65),    "lon": (3.25, 3.55)},
    {"city": "Singapore", "lat": (1.25, 1.45),    "lon": (103.75, 104.00)},
]

MERCHANT_CATEGORIES = [
    "grocery", "restaurant", "travel", "entertainment", "electronics",
    "clothing", "pharmacy", "fuel", "online_retail", "atm_withdrawal",
]

CHANNELS = ["web", "mobile_ios", "mobile_android", "in_store"]


class TransactionSimulator:
    def __init__(self, fraud_rate: float = 0.02):
        self.fraud_rate = fraud_rate
        self._cards = [str(uuid.uuid4()) for _ in range(500)]      # Pool of card IDs
        self._customers = [str(uuid.uuid4()) for _ in range(200)]  # Pool of customer IDs
        self._card_customer_map = {
            card: random.choice(self._customers) for card in self._cards
        }
        self._card_geo = {
            card: random.choice(GEO_CLUSTERS) for card in self._cards
        }
        self._card_avg_amount = {
            card: random.uniform(20, 200) for card in self._cards
        }

    def generate(self) -> dict:
        card_id = random.choice(self._cards)
        is_fraud = random.random() < self.fraud_rate

        if is_fraud:
            return self._generate_fraud_transaction(card_id)
        return self._generate_legitimate_transaction(card_id)

    def _generate_legitimate_transaction(self, card_id: str) -> dict:
        geo = self._card_geo[card_id]
        avg = self._card_avg_amount[card_id]
        return {
            "transaction_id":    str(uuid.uuid4()),
            "card_id":           card_id,
            "customer_id":       self._card_customer_map[card_id],
            "merchant_id":       f"MERCH-{random.randint(1000, 9999)}",
            "amount_usd":        round(random.gauss(avg, avg * 0.3), 2),
            "currency":          "USD",
            "merchant_category": random.choice(MERCHANT_CATEGORIES[:7]),   # Common categories
            "device_fingerprint": f"device-{card_id[:8]}",                 # Familiar device
            "ip_address":        f"192.168.{random.randint(1,254)}.{random.randint(1,254)}",
            "latitude":          round(random.uniform(*geo["lat"]), 6),
            "longitude":         round(random.uniform(*geo["lon"]), 6),
            "event_time":        datetime.now(timezone.utc).isoformat(),
        }

    def _generate_fraud_transaction(self, card_id: str) -> dict:
        """Inject one of several fraud patterns."""
        pattern = random.choice(["velocity", "geo", "amount_spike", "new_device"])

        base = self._generate_legitimate_transaction(card_id)

        if pattern == "velocity":
            # Rapid-fire — simulate card testing
            base["amount_usd"] = round(random.uniform(1, 5), 2)
            base["_fraud_pattern"] = "velocity"

        elif pattern == "geo":
            # Transaction on the other side of the world from home geo
            opposite_geo = random.choice([g for g in GEO_CLUSTERS if g != self._card_geo[card_id]])
            base["latitude"] = round(random.uniform(*opposite_geo["lat"]), 6)
            base["longitude"] = round(random.uniform(*opposite_geo["lon"]), 6)
            base["_fraud_pattern"] = "geo"

        elif pattern == "amount_spike":
            avg = self._card_avg_amount[card_id]
            base["amount_usd"] = round(avg * random.uniform(4, 10), 2)
            base["_fraud_pattern"] = "amount_spike"

        elif pattern == "new_device":
            base["device_fingerprint"] = f"device-UNKNOWN-{uuid.uuid4().hex[:8]}"
            base["amount_usd"] = round(random.uniform(600, 2000), 2)
            base["_fraud_pattern"] = "new_device"

        return base


def delivery_report(err, msg):
    if err:
        print(f"[PRODUCER ERROR] {err}")


def run(
    kafka_bootstrap: str,
    rate: int,
    fraud_rate: float,
    duration: Optional[int],
):
    producer = Producer({
        "bootstrap.servers": kafka_bootstrap,
        "client.id": "transaction-simulator",
        "acks": "1",
        "compression.type": "snappy",
    })

    simulator = TransactionSimulator(fraud_rate=fraud_rate)
    interval = 1.0 / rate
    start = time.monotonic()
    count = 0

    print(f"Producing at {rate} tx/sec, {fraud_rate*100:.1f}% fraud rate")

    try:
        while True:
            if duration and (time.monotonic() - start) >= duration:
                break

            tx = simulator.generate()
            producer.produce(
                topic="transactions.raw",
                key=tx["card_id"],
                value=json.dumps(tx),
                callback=delivery_report,
            )
            count += 1

            if count % (rate * 10) == 0:
                producer.poll(0)
                elapsed = time.monotonic() - start
                print(f"Produced {count} transactions in {elapsed:.1f}s ({count/elapsed:.0f} tx/s)")

            time.sleep(interval)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"Done. Produced {count} transactions.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-bootstrap-servers", default="localhost:9092")
    parser.add_argument("--rate", type=int, default=100, help="Transactions per second")
    parser.add_argument("--fraud-rate", type=float, default=0.02)
    parser.add_argument("--duration", type=int, default=None, help="Seconds to run (None=forever)")
    args = parser.parse_args()

    run(
        kafka_bootstrap=args.kafka_bootstrap_servers,
        rate=args.rate,
        fraud_rate=args.fraud_rate,
        duration=args.duration,
    )
