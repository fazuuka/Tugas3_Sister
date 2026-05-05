from __future__ import annotations

import asyncio
import base64
import os
import time
import uuid
from typing import Optional

import redis.asyncio as redis

from src.communication.message_passing import (
    MessageBus,
    RBACPolicy,
    build_client_ssl_context,
    build_server_ssl_context,
    parse_blocklist,
)
from src.nodes.base_node import BaseNode
from src.utils.config import NodeConfig, QueueConfig, RBACConfig, SecurityConfig
from src.utils.consistent_hash import ConsistentHashRing
from src.utils.metrics import REQUEST_COUNTER, REQUEST_LATENCY


class DistributedQueueNode:
    def __init__(
        self,
        node_endpoint: str,
        peer_endpoints: list[str],
        bus: MessageBus,
        redis_url: str,
        config: QueueConfig,
    ) -> None:
        self._endpoint = node_endpoint
        self._bus = bus
        self._redis = redis.from_url(redis_url, decode_responses=False)
        self._config = config
        self._static_nodes = [node_endpoint, *peer_endpoints]
        self._ring = ConsistentHashRing(self._static_nodes, config.virtual_nodes)
        self._replication_factor = max(1, config.replication_factor)
        self._membership_key_name = "membership:queue"

    async def enqueue(self, queue_name: str, payload: bytes, message_id: Optional[str] = None) -> str:
        replicas = self._select_replicas(queue_name)
        encoded = base64.b64encode(payload).decode("ascii")
        message_id = message_id or str(uuid.uuid4())
        for target in replicas:
            if target == self._endpoint:
                return await self.enqueue_replica(queue_name, payload, message_id)
            try:
                await self._bus.send(
                    target,
                    "queue.enqueue",
                    {
                        "queue": queue_name,
                        "payload": encoded,
                        "message_id": message_id,
                        "replica_only": True,
                    },
                )
                return message_id
            except Exception:
                continue
        return await self.enqueue_replica(queue_name, payload, message_id)

    async def dequeue(self, queue_name: str) -> Optional[dict]:
        replicas = self._select_replicas(queue_name)
        for target in replicas:
            if target == self._endpoint:
                item = await self._dequeue_local(queue_name)
                if item is not None:
                    return item
                continue
            try:
                response = await self._bus.send(
                    target,
                    "queue.dequeue",
                    {"queue": queue_name, "replica_only": True},
                )
                item = response.get("item")
                if item is not None:
                    return item
            except Exception:
                continue
        return None

    async def ack(self, queue_name: str, message_id: str) -> bool:
        replicas = self._select_replicas(queue_name)
        if not replicas:
            return await self._ack_local(queue_name, message_id)
        for target in replicas:
            if target == self._endpoint:
                return await self._ack_local(queue_name, message_id)
            try:
                response = await self._bus.send(
                    target,
                    "queue.ack",
                    {
                        "queue": queue_name,
                        "message_id": message_id,
                        "replica_only": True,
                    },
                )
                return response.get("success", False)
            except Exception:
                continue
        return await self._ack_local(queue_name, message_id)

    async def recover_expired(self) -> int:
        now = int(time.time())
        recovered = 0
        for queue_name in await self._list_queues():
            inflight_key = self._inflight_key(queue_name)
            ready_key = self._ready_key(queue_name)
            items = await self._redis.hgetall(inflight_key)
            for message_id, raw_ts in items.items():
                try:
                    ts = int(raw_ts)
                except ValueError:
                    continue
                if now - ts >= self._config.visibility_timeout_s:
                    await self._redis.hdel(inflight_key, message_id)
                    await self._redis.rpush(ready_key, message_id)
                    recovered += 1
        return recovered

    async def enqueue_replica(self, queue_name: str, payload: bytes, message_id: str) -> str:
        with REQUEST_LATENCY.labels("queue", "enqueue").time():
            REQUEST_COUNTER.labels("queue", "enqueue").inc()
            if not message_id:
                message_id = str(uuid.uuid4())
            await self._enqueue_replica(queue_name, payload, message_id)
            return message_id

    async def dequeue_replica(self, queue_name: str) -> Optional[dict]:
        return await self._dequeue_local(queue_name)

    async def ack_replica(self, queue_name: str, message_id: str) -> bool:
        return await self._ack_local(queue_name, message_id)

    async def register_node(self) -> None:
        await self._redis.sadd(self._membership_key(), self._endpoint)

    async def unregister_node(self) -> None:
        await self._redis.srem(self._membership_key(), self._endpoint)

    async def refresh_membership(self) -> None:
        try:
            raw_members = await self._redis.smembers(self._membership_key())
            members = []
            for item in raw_members:
                value = item.decode("ascii") if isinstance(item, bytes) else str(item)
                host = value.split(":", 1)[0]
                if host in {"0.0.0.0", "::"}:
                    continue
                members.append(value)
            if self._endpoint not in members:
                members.append(self._endpoint)
            if members:
                self._ring.set_nodes(sorted(members))
            else:
                self._ring.set_nodes(self._static_nodes)
        except Exception:
            self._ring.set_nodes(self._static_nodes)

    def _select_replicas(self, queue_name: str) -> list[str]:
        available = self._ring.nodes()
        max_replicas = min(self._replication_factor, len(available))
        if max_replicas <= 0:
            return []
        return self._ring.get_nodes(queue_name, max_replicas)

    async def _enqueue_replica(self, queue_name: str, payload: bytes, message_id: str) -> None:
        payload_key = self._payload_key(queue_name)
        ready_key = self._ready_key(queue_name)
        await self._redis.hset(payload_key, message_id, payload)
        await self._redis.rpush(ready_key, message_id)

    async def _dequeue_local(self, queue_name: str) -> Optional[dict]:
        with REQUEST_LATENCY.labels("queue", "dequeue").time():
            REQUEST_COUNTER.labels("queue", "dequeue").inc()
            ready_key = self._ready_key(queue_name)
            inflight_key = self._inflight_key(queue_name)
            payload_key = self._payload_key(queue_name)
            message_id = await self._redis.lpop(ready_key)
            if not message_id:
                return None
            timestamp = int(time.time())
            await self._redis.hset(inflight_key, message_id, str(timestamp).encode("ascii"))
            payload = await self._redis.hget(payload_key, message_id)
            if payload is None:
                await self._redis.hdel(inflight_key, message_id)
                return None
            return {
                "message_id": message_id.decode("ascii") if isinstance(message_id, bytes) else message_id,
                "payload": base64.b64encode(payload).decode("ascii"),
            }

    async def _ack_local(self, queue_name: str, message_id: str) -> bool:
        with REQUEST_LATENCY.labels("queue", "ack").time():
            REQUEST_COUNTER.labels("queue", "ack").inc()
            inflight_key = self._inflight_key(queue_name)
            payload_key = self._payload_key(queue_name)
            await self._redis.hdel(inflight_key, message_id)
            removed = await self._redis.hdel(payload_key, message_id)
            return removed > 0

    async def _list_queues(self) -> list[str]:
        keys = await self._redis.keys(b"queue:*:ready")
        queue_names = []
        for key in keys:
            parts = key.decode("ascii").split(":")
            if len(parts) >= 3:
                queue_names.append(parts[1])
        return queue_names

    @staticmethod
    def _ready_key(queue_name: str) -> str:
        return f"queue:{queue_name}:ready"

    @staticmethod
    def _payload_key(queue_name: str) -> str:
        return f"queue:{queue_name}:payloads"

    @staticmethod
    def _inflight_key(queue_name: str) -> str:
        return f"queue:{queue_name}:inflight"

    def _membership_key(self) -> str:
        return self._membership_key_name


class QueueNode(BaseNode):
    def __init__(
        self,
        node_config: NodeConfig,
        queue_config: QueueConfig,
        redis_url: str,
        security: SecurityConfig,
        rbac: RBACConfig,
        token: Optional[str],
    ) -> None:
        bus = _build_message_bus(node_config, security, rbac, token)
        super().__init__(node_config.node_id, node_config.host, node_config.port, node_config.peers, bus)
        endpoints = [f"{peer.host}:{peer.port}" for peer in node_config.peers]
        if ":" in node_config.node_id:
            endpoint = node_config.node_id
        elif node_config.host in {"0.0.0.0", "::"}:
            endpoint = f"{node_config.node_id}:{node_config.port}"
        else:
            endpoint = f"{node_config.host}:{node_config.port}"
        self._queue = DistributedQueueNode(endpoint, endpoints, bus, redis_url, queue_config)
        self._queue_config = queue_config

        self.message_bus.register("queue.enqueue", self._handle_enqueue)
        self.message_bus.register("queue.dequeue", self._handle_dequeue)
        self.message_bus.register("queue.ack", self._handle_ack)

        self._recovery_task: asyncio.Task | None = None

    async def start(self) -> None:
        await super().start()
        await self._queue.register_node()
        self._recovery_task = asyncio.create_task(self._recovery_loop())

    async def stop(self) -> None:
        if self._recovery_task:
            self._recovery_task.cancel()
        await self._queue.unregister_node()
        await super().stop()

    async def _handle_enqueue(self, payload: dict) -> dict:
        raw_payload = base64.b64decode(payload["payload"])
        message_id = payload.get("message_id")
        if payload.get("replica_only"):
            stored_id = await self._queue.enqueue_replica(payload["queue"], raw_payload, message_id or "")
            return {"message_id": stored_id}
        message_id = await self._queue.enqueue(payload["queue"], raw_payload, message_id)
        return {"message_id": message_id}

    async def _handle_dequeue(self, payload: dict) -> dict:
        if payload.get("replica_only"):
            item = await self._queue.dequeue_replica(payload["queue"])
        else:
            item = await self._queue.dequeue(payload["queue"])
        return {"item": item}

    async def _handle_ack(self, payload: dict) -> dict:
        if payload.get("replica_only"):
            success = await self._queue.ack_replica(payload["queue"], payload["message_id"])
        else:
            success = await self._queue.ack(payload["queue"], payload["message_id"])
        return {"success": success}

    async def _recovery_loop(self) -> None:
        while self.is_running:
            await self._queue.refresh_membership()
            await self._queue.recover_expired()
            interval = min(
                self._queue_config.recovery_interval_s,
                self._queue_config.membership_refresh_s,
            )
            await asyncio.sleep(interval)


def _build_message_bus(
    node_config: NodeConfig,
    security: SecurityConfig,
    rbac: RBACConfig,
    token: Optional[str],
) -> MessageBus:
    server_context = None
    client_context = None
    if security.tls_enabled:
        if not security.tls_cert_file or not security.tls_key_file:
            raise ValueError("TLS enabled but cert/key missing")
        server_context = build_server_ssl_context(security.tls_cert_file, security.tls_key_file)
        client_context = build_client_ssl_context(security.tls_ca_file)
    policy = RBACPolicy(rbac.enabled, rbac.token_map, rbac.role_permissions)
    blocked_targets = parse_blocklist(os.getenv("PARTITION_BLOCKLIST", ""))
    drop_rate = float(os.getenv("PARTITION_DROP_RATE", "0"))
    return MessageBus(
        node_config.host,
        node_config.port,
        token=token,
        rbac_policy=policy,
        ssl_context=server_context,
        client_ssl_context=client_context,
        blocked_targets=blocked_targets,
        drop_rate=drop_rate,
    )


async def run_node(node: QueueNode) -> None:
    await node.start()
    try:
        while node.is_running:
            await asyncio.sleep(1)
    finally:
        await node.stop()
