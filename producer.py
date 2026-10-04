"""Replays PaySim transactions into Kafka (as JSON), like a live payment feed.

    docker compose run --rm app python src/producer.py --rate 500
"""
import argparse
import json
import time

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config import (KAFKA_BOOTSTRAP, KAFKA_TOPIC, NUM_PARTITIONS, VAL_MAX_STEP,
                    find_csv)

COLUMNS = ["step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
           "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud"]


def ensure_topic():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    futures = admin.create_topics([NewTopic(KAFKA_TOPIC, num_partitions=NUM_PARTITIONS, replication_factor=1)])
    for _, f in futures.items():
        try:
            f.result()
            print(f"Created topic '{KAFKA_TOPIC}' with {NUM_PARTITIONS} partitions")
        except Exception as e:  # already exists is fine
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                print(f"Topic note: {e}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=500, help="messages per second (0 = as fast as possible)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N messages (0 = all)")
    ap.add_argument("--min-step", type=int, default=VAL_MAX_STEP + 1,
                    help="only replay transactions with step >= this (default: the held-out test period)")
    args = ap.parse_args()

    ensure_topic()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 20})

    def on_delivery(err, _msg):
        if err is not None:
            print(f"Delivery failed: {err}")

    sent, start = 0, time.time()
    txn_id = 0
    for chunk in pd.read_csv(find_csv(), usecols=COLUMNS, chunksize=100_000):
        ids = range(txn_id, txn_id + len(chunk))
        txn_id += len(chunk)          # txn_id = row number in the CSV (stable across runs)
        chunk = chunk.assign(txn_id=list(ids))
        chunk = chunk[chunk["step"] >= args.min_step]
        for rec in chunk.to_dict("records"):
            producer.produce(KAFKA_TOPIC, key=str(rec["nameOrig"]),
                             value=json.dumps(rec).encode("utf-8"), callback=on_delivery)
            sent += 1
            producer.poll(0)
            if args.rate > 0 and sent % 100 == 0:
                ahead = sent / args.rate - (time.time() - start)
                if ahead > 0:
                    time.sleep(ahead)
            if sent % 10_000 == 0:
                print(f"sent {sent:,} messages")
            if args.limit and sent >= args.limit:
                break
        if args.limit and sent >= args.limit:
            break

    producer.flush()
    print(f"Done. Sent {sent:,} messages in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
