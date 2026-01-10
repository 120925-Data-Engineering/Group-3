"""
Kafka Batch Consumer - Ingest to Landing Zone

Consumes messages from Kafka for a time window and writes to landing zone as JSON.

Pattern: Kafka Topic -> (This Script) -> ./data/landing/{topic}_{timestamp}.json
"""
from kafka import KafkaConsumer
import json
import time
import os
import argparse
from datetime import datetime 


def consume_batch(topic: str, batch_duration_sec: int, output_path: str) -> int:
    """
    Consume from Kafka for specified duration and write to landing zone.
    
    Args:
        topic: Kafka topic to consume from
        batch_duration_sec: How long to consume before writing
        output_path: Directory to write output JSON files
        
    Returns:
        Number of messages consumed
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9094',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    )

    messages = []
    start_time = time.time()

    while time.time() - start_time < batch_duration_sec:
        records = consumer.poll(timeout_ms=1000)
        for _, msgs in records.items():
            for msg in msgs:
                messages.append(msg.value)
    
    consumer.close()

    if not messages:
        return 0

    os.makedirs(output_path, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_path, f"{topic}_{timestamp}.json")

    with open(file_path, "w") as f:
        json.dump(messages, f)

    print(f"Wrote {len(messages)} messages to {file_path}")
    return len(messages)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    consume_batch(args.topic, args.duration, args.output_path)