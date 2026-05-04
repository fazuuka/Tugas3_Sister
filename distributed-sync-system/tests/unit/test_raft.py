from __future__ import annotations

import asyncio
from typing import Dict, List

import pytest

from src.consensus.raft import RaftNode
from src.nodes.base_node import PeerInfo
from src.utils.config import RaftConfig


class InMemoryNetwork:
    def __init__(self) -> None:
        self.buses: Dict[str, InMemoryBus] = {}

    def add(self, endpoint: str, bus: "InMemoryBus") -> None:
        self.buses[endpoint] = bus

    async def deliver(self, target: str, message_type: str, payload: dict) -> dict:
        bus = self.buses[target]
        handler = bus.handlers[message_type]
        return await handler(payload)


class InMemoryBus:
    def __init__(self, endpoint: str, network: InMemoryNetwork) -> None:
        self.endpoint = endpoint
        self.network = network
        self.handlers = {}

    def register(self, message_type: str, handler) -> None:
        self.handlers[message_type] = handler

    async def send(self, target: str, message_type: str, payload: dict) -> dict:
        return await self.network.deliver(target, message_type, payload)


async def _wait_for_leader(nodes: List[RaftNode], timeout_s: float = 2.0) -> RaftNode:
    end_time = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < end_time:
        for node in nodes:
            if node.state == "leader":
                return node
        await asyncio.sleep(0.05)
    raise AssertionError("Leader not elected")


@pytest.mark.asyncio
async def test_raft_election_and_replication() -> None:
    network = InMemoryNetwork()
    endpoints = ["node-1:8000", "node-2:8001", "node-3:8002"]
    buses = {}
    for endpoint in endpoints:
        bus = InMemoryBus(endpoint, network)
        network.add(endpoint, bus)
        buses[endpoint] = bus

    nodes: List[RaftNode] = []
    applied: Dict[str, List[dict]] = {endpoint: [] for endpoint in endpoints}
    for endpoint in endpoints:
        host, port = endpoint.split(":")
        peers = [
            PeerInfo(node_id=peer, host=peer.split(":")[0], port=int(peer.split(":")[1]))
            for peer in endpoints
            if peer != endpoint
        ]
        async def apply_callback(command: dict, endpoint=endpoint) -> None:
            applied[endpoint].append(command)

        raft = RaftNode(
            node_id=endpoint,
            peers=peers,
            bus=buses[endpoint],
            config=RaftConfig(election_timeout_ms=50, heartbeat_interval_ms=20),
            apply_callback=apply_callback,
        )
        nodes.append(raft)

    for node in nodes:
        await node.start()

    leader = await _wait_for_leader(nodes)
    success, _leader_id = await leader.replicate({"type": "lock.acquire", "key": "k", "owner": "o"})
    assert success

    end_time = asyncio.get_event_loop().time() + 2.0
    while asyncio.get_event_loop().time() < end_time:
        if all(applied[endpoint] for endpoint in endpoints):
            break
        await asyncio.sleep(0.05)

    for endpoint in endpoints:
        assert applied[endpoint], f"No applied entries on {endpoint}"

    for node in nodes:
        await node.stop()
