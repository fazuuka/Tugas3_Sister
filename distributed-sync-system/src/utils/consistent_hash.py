from __future__ import annotations

import hashlib
from typing import Dict, Iterable, List, Tuple


class ConsistentHashRing:
    def __init__(self, nodes: Iterable[str], virtual_nodes: int = 50) -> None:
        self._virtual_nodes = max(1, virtual_nodes)
        self._ring: Dict[int, str] = {}
        self._sorted_keys: List[int] = []
        self._build(nodes)

    def _build(self, nodes: Iterable[str]) -> None:
        self._ring.clear()
        for node in nodes:
            for replica in range(self._virtual_nodes):
                key = self._hash(f"{node}:{replica}")
                self._ring[key] = node
        self._sorted_keys = sorted(self._ring.keys())

    def get_node(self, key: str) -> str:
        if not self._sorted_keys:
            raise ValueError("Hash ring is empty")
        h = self._hash(key)
        for ring_key in self._sorted_keys:
            if h <= ring_key:
                return self._ring[ring_key]
        return self._ring[self._sorted_keys[0]]

    def get_nodes(self, key: str, count: int) -> List[str]:
        if not self._sorted_keys:
            raise ValueError("Hash ring is empty")
        if count <= 0:
            return []
        h = self._hash(key)
        start_index = 0
        for idx, ring_key in enumerate(self._sorted_keys):
            if h <= ring_key:
                start_index = idx
                break
        nodes: List[str] = []
        ring_size = len(self._sorted_keys)
        for offset in range(ring_size):
            ring_key = self._sorted_keys[(start_index + offset) % ring_size]
            node = self._ring[ring_key]
            if node not in nodes:
                nodes.append(node)
                if len(nodes) >= count:
                    break
        return nodes

    def nodes(self) -> List[str]:
        return sorted(set(self._ring.values()))

    def set_nodes(self, nodes: Iterable[str]) -> None:
        self._build(nodes)

    def _hash(self, value: str) -> int:
        digest = hashlib.md5(value.encode("utf-8"), usedforsecurity=False).hexdigest()
        return int(digest, 16)
