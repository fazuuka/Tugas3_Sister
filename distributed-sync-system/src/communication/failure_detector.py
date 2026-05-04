from __future__ import annotations

class FailureDetector:
    def __init__(self) -> None:
        self._suspected = set()

    def mark_suspected(self, node_id: str) -> None:
        self._suspected.add(node_id)

    def clear(self, node_id: str) -> None:
        self._suspected.discard(node_id)

    def is_suspected(self, node_id: str) -> bool:
        return node_id in self._suspected
