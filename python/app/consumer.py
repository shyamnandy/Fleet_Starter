"""M2: Kafka consumer -> Postgres. Run with: python -m app.consumer"""
import json
import logging
import signal

from confluent_kafka import Consumer, KafkaError
from pydantic import ValidationError

from app import config
from app.db import get_pool
from app.kafka_client import EventPublisher
from app.models import TelemetryEvent
from app.alerts import overspeed_run_start
from app.repository import insert_overspeed_alert, insert_telemetry, recent_telemetry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("consumer")


def parse_event(raw_value: bytes | str) -> TelemetryEvent:
    """Decode and validate the exact v1 message shape expected by the DB."""
    if isinstance(raw_value, bytes):
        raw_value = raw_value.decode("utf-8")
    payload = json.loads(raw_value)
    if not isinstance(payload, dict) or payload.get("schema") != "v1":
        raise ValueError("message must be a JSON object with schema='v1'")
    return TelemetryEvent.model_validate(payload)


def publish_dlq(publisher: EventPublisher, msg, error: Exception) -> None:
    """Publish a malformed input and wait for Kafka acknowledgement."""
    raw_value = msg.value()
    if isinstance(raw_value, bytes):
        raw_value = raw_value.decode("utf-8", errors="replace")
    key = msg.key()
    if isinstance(key, bytes):
        key = key.decode("utf-8", errors="replace")
    publisher.publish(
        config.TOPIC_DLQ,
        key or "unknown",
        {
            "error": str(error),
            "raw_value": raw_value,
            "source_topic": msg.topic(),
            "source_partition": msg.partition(),
            "source_offset": msg.offset(),
        },
    )
    if publisher.flush() != 0:
        raise RuntimeError("failed to publish malformed message to DLQ")


def run() -> None:
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": config.CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([config.TOPIC_RAW])
    publisher = EventPublisher()
    pool = get_pool()
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    log.info("consuming %s as group %s", config.TOPIC_RAW, config.CONSUMER_GROUP)
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
                continue

            try:
                event = parse_event(msg.value())
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError, ValidationError) as error:
                log.warning("sending malformed message to DLQ partition=%s offset=%s: %s",
                            msg.partition(), msg.offset(), error)
                publish_dlq(publisher, msg, error)
                consumer.commit(message=msg, asynchronous=False)
                continue

            with pool.connection() as conn:
                inserted = insert_telemetry(conn, event)
                alert = None
                if inserted:
                    rows = recent_telemetry(conn, event.vin)
                    started_at = overspeed_run_start(rows, config.OVERSPEED_KMPH, config.OVERSPEED_MIN_PINGS)
                    if started_at is not None:
                        alert = insert_overspeed_alert(conn, event.vin, started_at, config.OVERSPEED_KMPH)

            consumer.commit(message=msg, asynchronous=False)
            if alert is not None:
                publisher.publish(config.TOPIC_ALERTS, event.vin, alert)
                publisher.flush()
                log.info("overspeed alert vin=%s started_at=%s", event.vin, alert["started_at"])
            log.info("stored telemetry vin=%s ts=%s inserted=%s partition=%s offset=%s",
                     event.vin, event.ts_utc, inserted, msg.partition(), msg.offset())
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
