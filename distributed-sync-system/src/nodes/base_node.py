from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from src.communication.message_passing import MessageBus


@dataclass(frozen=True)
class PeerInfo:
    node_id: str
    host: str
    port: int


class BaseNode:
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        peers: List[PeerInfo],
        message_bus: Optional[MessageBus] = None,
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        self.is_running = False
        self.message_bus = message_bus

    async def start(self) -> None:
        self.is_running = True
        if self.message_bus:
            await self.message_bus.start()

    async def stop(self) -> None:
        self.is_running = False
        if self.message_bus:
            await self.message_bus.stop()

    def peer_map(self) -> Dict[str, PeerInfo]:
        return {peer.node_id: peer for peer in self.peers}
