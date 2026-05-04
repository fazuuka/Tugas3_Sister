from __future__ import annotations

import asyncio
import base64
import os
import time
from dataclasses import dataclass
from typing import Dict, Optional

import redis.asyncio as redis

from src.communication.message_passing import (
    MessageBus,
    RBACPolicy,
    build_client_ssl_context,
    build_server_ssl_context,
    parse_blocklist,
)
from src.nodes.base_node import BaseNode
from src.utils.config import CacheConfig, NodeConfig, RBACConfig, SecurityConfig
from src.utils.consistent_hash import ConsistentHashRing
from src.utils.metrics import REQUEST_COUNTER, REQUEST_LATENCY


@dataclass
class CacheEntry:
    value: bytes
    state: str
    version: int
    last_access: float
    freq: int = 0


class DistributedCacheNode:
    def __init__(
        self,
        node_endpoint: str,
        peer_endpoints: list[str],
        bus: MessageBus,
        redis_url: str,
        config: CacheConfig,
    ) -> None:
        self._endpoint = node_endpoint
        self._bus = bus
        self._redis = redis.from_url(redis_url, decode_responses=False)
        self._config = config
        self._static_nodes = [node_endpoint, *peer_endpoints]
        self._ring = ConsistentHashRing(self._static_nodes)
        self._cache: Dict[str, CacheEntry] = {}
        self._membership_key_name = "membership:cache"

    async def get(self, key: str, requester: Optional[str] = None) -> Optional[dict]:
        with REQUEST_LATENCY.labels("cache", "get").time():
            REQUEST_COUNTER.labels("cache", "get").inc()
            requester_id = requester or self._endpoint
            entry = self._cache.get(key)
            if entry and entry.state in {"M", "E", "S"}:
                if requester_id != self._endpoint and entry.state in {"M", "E"}:
                    entry.state = "S"
                self._touch(entry)
                return {
                    "key": key,
                    "value": base64.b64encode(entry.value).decode("ascii"),
                    "state": entry.state,
                    "version": entry.version,
                }
            owner = self._ring.get_node(key)
            if owner != self._endpoint:
                response = await self._bus.send(
                    owner,
                    "cache.get",
                    {"key": key, "requester": requester_id},
                )
                return response["item"]
            value, version = await self._read_store(key)
            if value is None:
                return None
            state = "E" if requester_id == self._endpoint else "S"
            entry = CacheEntry(value=value, state=state, version=version, last_access=time.time())
            self._ensure_capacity()
            self._cache[key] = entry
            return {
                "key": key,
                "value": base64.b64encode(value).decode("ascii"),
                "state": entry.state,
                "version": entry.version,
            }

    async def put(self, key: str, value: bytes) -> dict:
        with REQUEST_LATENCY.labels("cache", "put").time():
            REQUEST_COUNTER.labels("cache", "put").inc()
            owner = self._ring.get_node(key)
            if owner != self._endpoint:
                response = await self._bus.send(
                    owner,
                    "cache.put",
                    {"key": key, "value": base64.b64encode(value).decode("ascii")},
                )
                return response
            version = await self._write_store(key, value)
            entry = CacheEntry(value=value, state="M", version=version, last_access=time.time())
            self._ensure_capacity()
            self._cache[key] = entry
            await self._broadcast_invalidate(key, version)
            return {"ok": True, "version": version}

    async def invalidate(self, key: str, version: int) -> None:
        entry = self._cache.get(key)
        if entry and entry.version <= version:
            entry.state = "I"
            entry.version = version

    async def register_node(self) -> None:
        await self._redis.sadd(self._membership_key(), self._endpoint)

    async def unregister_node(self) -> None:
        await self._redis.srem(self._membership_key(), self._endpoint)

    async def refresh_membership(self) -> None:
        try:
            raw_members = await self._redis.smembers(self._membership_key())
            members = [
                item.decode("ascii") if isinstance(item, bytes) else str(item)
                for item in raw_members
            ]
            if self._endpoint not in members:
                members.append(self._endpoint)
            if members:
                self._ring.set_nodes(sorted(members))
            else:
                self._ring.set_nodes(self._static_nodes)
        except Exception:
            self._ring.set_nodes(self._static_nodes)

    async def _broadcast_invalidate(self, key: str, version: int) -> None:
        tasks = [
            self._bus.send(peer, "cache.invalidate", {"key": key, "version": version})
            for peer in self._ring.nodes()
            if peer != self._endpoint
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _read_store(self, key: str) -> tuple[Optional[bytes], int]:
        raw = await self._redis.hget(self._store_key(), key)
        raw_ver = await self._redis.hget(self._version_key(), key)
        if raw is None:
            return None, 0
        version = int(raw_ver.decode("ascii")) if raw_ver else 0
        return raw, version

    async def _write_store(self, key: str, value: bytes) -> int:
        version = int(time.time() * 1000)
        await self._redis.hset(self._store_key(), key, value)
        await self._redis.hset(self._version_key(), key, str(version).encode("ascii"))
        return version

    def _touch(self, entry: CacheEntry) -> None:
        entry.last_access = time.time()
        entry.freq += 1

    def _ensure_capacity(self) -> None:
        capacity = self._config.capacity
        if capacity <= 0:
            return
        while len(self._cache) >= capacity:
            victim_key = self._select_victim()
            if victim_key is None:
                break
            self._cache.pop(victim_key, None)

    def _select_victim(self) -> Optional[str]:
        if not self._cache:
            return None
        invalid_keys = [key for key, entry in self._cache.items() if entry.state == "I"]
        if invalid_keys:
            return invalid_keys[0]
        policy = self._config.replacement_policy.upper()
        if policy == "LFU":
            return min(self._cache.items(), key=lambda item: (item[1].freq, item[1].last_access))[0]
        return min(self._cache.items(), key=lambda item: item[1].last_access)[0]

    @staticmethod
    def _store_key() -> str:
        return "cache:store"

    @staticmethod
    def _version_key() -> str:
        return "cache:version"

    def _membership_key(self) -> str:
        return self._membership_key_name


class CacheNode(BaseNode):
    def __init__(
        self,
        node_config: NodeConfig,
        cache_config: CacheConfig,
        redis_url: str,
        security: SecurityConfig,
        rbac: RBACConfig,
        token: Optional[str],
    ) -> None:
        bus = _build_message_bus(node_config, security, rbac, token)
        super().__init__(node_config.node_id, node_config.host, node_config.port, node_config.peers, bus)
        endpoints = [f"{peer.host}:{peer.port}" for peer in node_config.peers]
        endpoint = node_config.node_id if ":" in node_config.node_id else f"{node_config.host}:{node_config.port}"
        self._cache = DistributedCacheNode(endpoint, endpoints, bus, redis_url, cache_config)
        self._cache_config = cache_config

        self._membership_task: asyncio.Task | None = None

        self.message_bus.register("cache.get", self._handle_get)
        self.message_bus.register("cache.put", self._handle_put)
        self.message_bus.register("cache.invalidate", self._handle_invalidate)

    async def start(self) -> None:
        await super().start()
        await self._cache.register_node()
        self._membership_task = asyncio.create_task(self._membership_loop())

    async def stop(self) -> None:
        if self._membership_task:
            self._membership_task.cancel()
        await self._cache.unregister_node()
        await super().stop()

    async def _handle_get(self, payload: dict) -> dict:
        item = await self._cache.get(payload["key"], payload.get("requester"))
        return {"item": item}

    async def _handle_put(self, payload: dict) -> dict:
        value = base64.b64decode(payload["value"])
        result = await self._cache.put(payload["key"], value)
        return result

    async def _handle_invalidate(self, payload: dict) -> dict:
        await self._cache.invalidate(payload["key"], payload.get("version", 0))
        return {"ok": True}

    async def _membership_loop(self) -> None:
        while self.is_running:
            await self._cache.refresh_membership()
            await asyncio.sleep(self._cache_config.membership_refresh_s)


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


async def run_node(node: CacheNode) -> None:
    await node.start()
    try:
        while node.is_running:
            await asyncio.sleep(1)
    finally:
        await node.stop()
