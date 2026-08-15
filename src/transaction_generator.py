"""Publishes simulated card transactions to a Pub/Sub topic.

Runs a steady baseline stream of normal-looking transactions, interrupted at
random by card-testing attack bursts: many low-value transactions from a
single source, cycling through distinct card numbers, with a high decline
rate.
"""

import argparse
import json
import os
import random
import string
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from google.cloud import pubsub_v1

MERCHANTS = [
    "coffee-shop-01",
    "grocery-mart-14",
    "streaming-service",
    "online-electronics",
    "gas-station-09",
    "food-delivery-app",
    "clothing-store-22",
]

BASELINE_AMOUNT_RANGE = (5.00, 300.00)
ATTACK_AMOUNT_RANGE = (1.00, 5.00)
BASELINE_DECLINE_RATE = 0.05
ATTACK_DECLINE_RATE = 0.80


@dataclass
class Transaction:
    transaction_id: str
    timestamp: str
    amount: float
    merchant_id: str
    card_id: str
    source_ip: str
    status: str

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")


def random_card_id() -> str:
    last4 = "".join(random.choices(string.digits, k=4))
    return f"4111-XXXX-XXXX-{last4}"


def random_ip() -> str:
    return ".".join(str(random.randint(1, 254)) for _ in range(4))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_baseline_transaction() -> Transaction:
    declined = random.random() < BASELINE_DECLINE_RATE
    return Transaction(
        transaction_id=str(uuid.uuid4()),
        timestamp=now_iso(),
        amount=round(random.uniform(*BASELINE_AMOUNT_RANGE), 2),
        merchant_id=random.choice(MERCHANTS),
        card_id=random_card_id(),
        source_ip=random_ip(),
        status="declined" if declined else "approved",
    )


def make_attack_burst() -> list[Transaction]:
    """A card-testing burst: one source, many cards, low value, mostly declined."""
    source_ip = random_ip()
    merchant = random.choice(MERCHANTS)
    burst_size = random.randint(15, 40)

    transactions = []
    for _ in range(burst_size):
        declined = random.random() < ATTACK_DECLINE_RATE
        transactions.append(
            Transaction(
                transaction_id=str(uuid.uuid4()),
                timestamp=now_iso(),
                amount=round(random.uniform(*ATTACK_AMOUNT_RANGE), 2),
                merchant_id=merchant,
                card_id=random_card_id(),
                source_ip=source_ip,
                status="declined" if declined else "approved",
            )
        )
    return transactions


def publish(publisher: pubsub_v1.PublisherClient, topic_path: str, txn: Transaction, is_attack: bool) -> None:
    future = publisher.publish(
        topic_path,
        data=txn.to_json(),
        is_attack=str(is_attack).lower(),
    )
    future.add_done_callback(lambda f: _log_publish_result(f, txn.transaction_id))


def _log_publish_result(future, transaction_id: str) -> None:
    try:
        future.result()
    except Exception as exc:  # noqa: BLE001 - just surfacing publish failures
        print(f"failed to publish transaction {transaction_id}: {exc}")


def run(publisher: pubsub_v1.PublisherClient, topic_path: str, rate: float, attack_probability: float) -> None:
    tick_interval = 1.0 / rate
    print(f"publishing to {topic_path} at ~{rate} txn/s (attack chance {attack_probability:.1%} per tick)")

    while True:
        if random.random() < attack_probability:
            burst = make_attack_burst()
            print(f"attack burst: {len(burst)} transactions from {burst[0].source_ip}")
            for txn in burst:
                publish(publisher, topic_path, txn, is_attack=True)
                time.sleep(0.05)
        else:
            txn = make_baseline_transaction()
            publish(publisher, topic_path, txn, is_attack=False)
            time.sleep(tick_interval)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project",
        default=os.environ.get("GOOGLE_CLOUD_PROJECT"),
        help="GCP project id (defaults to $GOOGLE_CLOUD_PROJECT)",
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("PUBSUB_TOPIC_ID", "fraud-data"),
        help="Pub/Sub topic id (defaults to $PUBSUB_TOPIC_ID or 'fraud-data')",
    )
    parser.add_argument("--rate", type=float, default=5.0, help="baseline transactions per second")
    parser.add_argument(
        "--attack-probability",
        type=float,
        default=0.02,
        help="probability of an attack burst firing on any given tick",
    )
    args = parser.parse_args()

    if not args.project:
        parser.error("--project is required (or set GOOGLE_CLOUD_PROJECT)")

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic)

    try:
        run(publisher, topic_path, args.rate, args.attack_probability)
    except KeyboardInterrupt:
        print("\nstopped")


if __name__ == "__main__":
    main()
