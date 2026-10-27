"""Consumer lag of a Flink job's consumer group (Flink commits offsets on checkpoint completion)."""
from __future__ import annotations

from confluent_kafka import Consumer, TopicPartition


def consumer_lag(bootstrap: str, group: str, topic: str) -> dict:
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False})
    try:
        meta = c.list_topics(topic, timeout=10).topics[topic]
        tps = [TopicPartition(topic, p) for p in meta.partitions]
        committed = {tp.partition: tp.offset for tp in c.committed(tps, timeout=10)}
        total, per = 0, {}
        for p in meta.partitions:
            _, end = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            off = committed.get(p, -1)
            lag = end - (off if off >= 0 else 0)
            per[str(p)] = {"end": end, "committed": off, "lag": lag}
            total += lag
        return {"total": total, "partitions": per}
    finally:
        c.close()
