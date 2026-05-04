from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from src.communication.message_passing import (
    MessageBus,
    RBACPolicy,
    build_client_ssl_context,
    build_server_ssl_context,
    parse_blocklist,
)
from src.consensus.raft import RaftNode
from src.nodes.base_node import BaseNode
from src.utils.config import NodeConfig, RBACConfig, RaftConfig, SecurityConfig
from src.utils.metrics import REQUEST_COUNTER, REQUEST_LATENCY


@dataclass
class LockEntry:
    mode: Optional[str] = None
    holders: Set[str] = field(default_factory=set)
    wait_queue: List[Tuple[str, str]] = field(default_factory=list)


class DistributedLockManager:
    def __init__(self, raft: RaftNode) -> None:
        self._locks: Dict[str, LockEntry] = {}
        self._raft = raft

    async def acquire_shared(self, key: str, owner: str) -> bool:
        return await self._request_lock(key, owner, "shared")

    async def acquire_exclusive(self, key: str, owner: str) -> bool:
        return await self._request_lock(key, owner, "exclusive")

    async def release(self, key: str, owner: str) -> bool:
        success, _leader = await self._raft.replicate(
            {"type": "lock.release", "key": key, "owner": owner}
        )
        return success

    async def apply_command(self, command: dict) -> None:
        if command.get("type") == "lock.acquire":
            self._apply_acquire(command)
        elif command.get("type") == "lock.release":
            self._apply_release(command)

    async def _request_lock(self, key: str, owner: str, mode: str) -> bool:
        if self._detect_deadlock(owner, key):
            return False
        success, _leader = await self._raft.replicate(
            {"type": "lock.acquire", "key": key, "owner": owner, "mode": mode}
        )
        return success

    def _apply_acquire(self, command: dict) -> None:
        key = command["key"]
        owner = command["owner"]
        mode = command["mode"]
        entry = self._locks.setdefault(key, LockEntry())
        if self._can_grant(entry, owner, mode):
            entry.mode = mode if entry.mode is None else entry.mode
            entry.holders.add(owner)
            entry.wait_queue = [item for item in entry.wait_queue if item[0] != owner]
            if mode == "exclusive":
                entry.mode = "exclusive"
            elif entry.mode != "exclusive":
                entry.mode = "shared"
            return
        if (owner, mode) not in entry.wait_queue:
            entry.wait_queue.append((owner, mode))

    def _apply_release(self, command: dict) -> None:
        key = command["key"]
        owner = command["owner"]
        entry = self._locks.get(key)
        if not entry:
            return
        entry.holders.discard(owner)
        if not entry.holders:
            entry.mode = None
            self._grant_waiters(entry)

    def _grant_waiters(self, entry: LockEntry) -> None:
        granted: List[Tuple[str, str]] = []
        for owner, mode in entry.wait_queue:
            if self._can_grant(entry, owner, mode):
                entry.holders.add(owner)
                entry.mode = mode if entry.mode is None else entry.mode
                granted.append((owner, mode))
                if mode == "exclusive":
                    break
            else:
                break
        entry.wait_queue = [item for item in entry.wait_queue if item not in granted]
        if entry.holders:
            if any(mode == "exclusive" for _, mode in granted):
                entry.mode = "exclusive"
            elif entry.mode != "exclusive":
                entry.mode = "shared"

    def _can_grant(self, entry: LockEntry, owner: str, mode: str) -> bool:
        if entry.mode is None:
            return True
        if mode == "shared" and entry.mode == "shared":
            return True
        if entry.mode == "shared" and mode == "exclusive" and entry.holders == {owner}:
            return True
        return False

    def _detect_deadlock(self, owner: str, key: str) -> bool:
        graph = self._build_wait_graph()
        entry = self._locks.get(key)
        if entry and entry.holders:
            graph.setdefault(owner, set()).update(entry.holders)
        return self._has_cycle(graph, owner)

    def _build_wait_graph(self) -> Dict[str, Set[str]]:
        graph: Dict[str, Set[str]] = {}
        for entry in self._locks.values():
            if not entry.holders:
                continue
            for owner, _mode in entry.wait_queue:
                graph.setdefault(owner, set()).update(entry.holders)
        return graph

    def _has_cycle(self, graph: Dict[str, Set[str]], start: str) -> bool:
        visited: Set[str] = set()
        stack: Set[str] = set()

        def visit(node: str) -> bool:
            if node in stack:
                return True
            if node in visited:
                return False
            visited.add(node)
            stack.add(node)
            for neighbor in graph.get(node, set()):
                if visit(neighbor):
                    return True
            stack.remove(node)
            return False

        return visit(start)


class LockManagerNode(BaseNode):
    def __init__(
        self,
        node_config: NodeConfig,
        raft_config: RaftConfig,
        redis_url: Optional[str],
        security: SecurityConfig,
        rbac: RBACConfig,
        token: Optional[str],
    ) -> None:
        bus = _build_message_bus(node_config, security, rbac, token)
        super().__init__(node_config.node_id, node_config.host, node_config.port, node_config.peers, bus)
        self._raft = RaftNode(
            node_config.node_id,
            node_config.peers,
            bus,
            raft_config,
            self._apply_lock_command,
            redis_url=redis_url,
        )
        self._lock_manager = DistributedLockManager(self._raft)

        self.message_bus.register("lock.acquire.shared", self._handle_acquire_shared)
        self.message_bus.register("lock.acquire.exclusive", self._handle_acquire_exclusive)
        self.message_bus.register("lock.release", self._handle_release)

    async def start(self) -> None:
        await super().start()
        await self._raft.start()

    async def stop(self) -> None:
        await self._raft.stop()
        await super().stop()

    async def _apply_lock_command(self, command: dict) -> None:
        await self._lock_manager.apply_command(command)

    async def _handle_acquire_shared(self, payload: dict) -> dict:
        with REQUEST_LATENCY.labels("lock", "acquire_shared").time():
            REQUEST_COUNTER.labels("lock", "acquire_shared").inc()
            key = payload["key"]
            owner = payload["owner"]
            success = await self._lock_manager.acquire_shared(key, owner)
            return {"success": success}

    async def _handle_acquire_exclusive(self, payload: dict) -> dict:
        with REQUEST_LATENCY.labels("lock", "acquire_exclusive").time():
            REQUEST_COUNTER.labels("lock", "acquire_exclusive").inc()
            key = payload["key"]
            owner = payload["owner"]
            success = await self._lock_manager.acquire_exclusive(key, owner)
            return {"success": success}

    async def _handle_release(self, payload: dict) -> dict:
        with REQUEST_LATENCY.labels("lock", "release").time():
            REQUEST_COUNTER.labels("lock", "release").inc()
            key = payload["key"]
            owner = payload["owner"]
            success = await self._lock_manager.release(key, owner)
            return {"success": success}


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


async def run_node(node: LockManagerNode) -> None:
    await node.start()
    try:
        while node.is_running:
            await asyncio.sleep(1)
    finally:
        await node.stop()
