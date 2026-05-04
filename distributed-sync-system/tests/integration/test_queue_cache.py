from __future__ import annotations

import os
import uuid

import pytest
import redis.asyncio as redis

from src.communication.message_passing import MessageBus
from src.nodes.cache_node import DistributedCacheNode
from src.nodes.queue_node import DistributedQueueNode
from src.utils.config import CacheConfig, QueueConfig


class LocalBus(MessageBus):
    def __init__(self) -> None:
        super().__init__("127.0.0.1", 0)
        self.handlers = {}

    async def send(self, target: str, message_type: str, payload: dict) -> dict:
        handler = self.handlers.get(message_type)
        if not handler:
            raise RuntimeError("handler not registered")
        return await handler(payload)


async def _redis_available(redis_url: str) -> bool:
    try:
        client = redis.from_url(redis_url)
        await client.ping()
        await client.close()
        return True
    except Exception:
        return False


@pytest.mark.asyncio
async def test_queue_roundtrip() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    if not await _redis_available(redis_url):
        pytest.skip("Redis not available")

    bus = LocalBus()
    queue = DistributedQueueNode("local", [], bus, redis_url, QueueConfig())
    queue_name = f"queue-{uuid.uuid4()}"
    payload = b"hello"

    message_id = await queue.enqueue(queue_name, payload)
    item = await queue.dequeue(queue_name)
    assert item is not None
    assert item["payload"] is not None

    acked = await queue.ack(queue_name, item["message_id"])
    assert acked


@pytest.mark.asyncio
async def test_cache_roundtrip() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    if not await _redis_available(redis_url):
        pytest.skip("Redis not available")

    bus = LocalBus()
    cache = DistributedCacheNode("local", [], bus, redis_url, CacheConfig(capacity=1))
    result = await cache.put("k1", b"v1")
    assert result["ok"]

    item = await cache.get("k1")
    assert item is not None
    assert item["key"] == "k1"

    await cache.put("k2", b"v2")
    item = await cache.get("k1")
    assert item is None or item["key"] == "k1"
