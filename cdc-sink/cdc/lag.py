"""Consumer-group lag via the Admin API — read-only, never joins the group.

Creating a KafkaConsumer with group_id makes the caller an ACTIVE group
member; for monitoring/verify tooling that triggers a rebalance storm that
kicks the real sink out of its partitions (Session-4 CRITIC catch). All lag
computation therefore goes through KafkaAdminClient
(list_consumer_group_offsets), which reads committed offsets without joining,
plus a groupless consumer for end offsets.
"""

from __future__ import annotations

from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from cdc import config


def group_lag() -> tuple[int, dict[str, int]]:
    """Returns (total_lag, per_topic_lag) for the sink group on captured topics."""
    existing: set[str] = set()
    try:
        admin = KafkaAdminClient(bootstrap_servers=config.kafka_bootstrap())
        existing = set(admin.list_topics())
        committed = admin.list_consumer_group_offsets(config.group_id())
        admin.close()
    except KafkaError:
        raise
    committed_offsets = {tp: om.offset for tp, om in (committed or {}).items()}

    consumer = KafkaConsumer(bootstrap_servers=config.kafka_bootstrap())  # no group_id
    tps = [
        TopicPartition(topic, partition)
        for topic in config.topics() if topic in existing
        for partition in sorted(consumer.partitions_for_topic(topic) or ())
    ]
    end_offsets = consumer.end_offsets(tps)
    consumer.close()

    per_topic: dict[str, int] = {}
    total = 0
    for tp in tps:
        lag = max(0, end_offsets.get(tp, 0) - committed_offsets.get(tp, 0))
        per_topic[tp.topic] = lag
        total += lag
    return total, per_topic
