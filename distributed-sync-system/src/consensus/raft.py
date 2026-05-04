from __future__ import annotations

import asyncio
import json
import random
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional

import redis.asyncio as redis

from src.communication.message_passing import MessageBus
from src.nodes.base_node import PeerInfo
from src.utils.config import RaftConfig


@dataclass
class LogEntry:
    term: int
    command: Dict[str, Any]


ApplyCallback = Callable[[Dict[str, Any]], Awaitable[None]]


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: List[PeerInfo],
        bus: MessageBus,
        config: RaftConfig,
        apply_callback: Optional[ApplyCallback] = None,
        redis_url: Optional[str] = None,
    ) -> None:
        self.node_id = node_id
        self.peers = peers
        self.bus = bus
        self.config = config
        self.apply_callback = apply_callback

        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        self.commit_index = -1
        self.last_applied = -1

        self.state = "follower"
        self.leader_id: Optional[str] = None
        self._election_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._election_reset = asyncio.Event()

        self._next_index: Dict[str, int] = {}
        self._match_index: Dict[str, int] = {}

        self._redis = redis.from_url(redis_url, decode_responses=True) if redis_url else None

    async def start(self) -> None:
        self.bus.register("raft.request_vote", self._handle_request_vote)
        self.bus.register("raft.append_entries", self._handle_append_entries)
        self._stop_event.clear()
        await self._load_state()
        self._election_task = asyncio.create_task(self._run_election_timer())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._election_task:
            self._election_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    async def replicate(self, command: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        if self.state != "leader":
            return False, self.leader_id
        entry = LogEntry(term=self.current_term, command=command)
        self.log.append(entry)
        await self._persist_state()

        await self._replicate_all()
        if self._advance_commit_index():
            await self._apply_entries()
            return True, self.node_id
        return False, self.node_id

    async def _run_election_timer(self) -> None:
        while not self._stop_event.is_set():
            timeout = random.uniform(
                self.config.election_timeout_ms,
                self.config.election_timeout_ms * 2,
            ) / 1000
            self._election_reset.clear()
            try:
                await asyncio.wait_for(self._election_reset.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                await self._start_election()

    async def _start_election(self) -> None:
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        await self._persist_state()

        votes = 1
        quorum = (len(self.peers) + 1) // 2 + 1
        last_log_index, last_log_term = self._last_log_info()
        tasks = [
            self.bus.send(
                f"{peer.host}:{peer.port}",
                "raft.request_vote",
                {
                    "term": self.current_term,
                    "candidate_id": self.node_id,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term,
                },
            )
            for peer in self.peers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                continue
            if result.get("term", 0) > self.current_term:
                await self._step_down(result["term"])
                return
            if result.get("vote_granted"):
                votes += 1
        if votes >= quorum:
            await self._become_leader()

    async def _become_leader(self) -> None:
        self.state = "leader"
        self.leader_id = self.node_id
        self._init_leader_state()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._run_heartbeat())

    async def _run_heartbeat(self) -> None:
        while self.state == "leader" and not self._stop_event.is_set():
            await self._replicate_all()
            await asyncio.sleep(self.config.heartbeat_interval_ms / 1000)

    async def _replicate_all(self) -> None:
        results = await asyncio.gather(
            *[self._replicate_to_peer(peer) for peer in self.peers],
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, Exception):
                continue
            if result.get("term", 0) > self.current_term:
                await self._step_down(result["term"])

    async def _replicate_to_peer(self, peer: PeerInfo) -> Dict[str, Any]:
        peer_id = self._peer_id(peer)
        next_index = self._next_index.get(peer_id, len(self.log))
        prev_index = next_index - 1
        prev_term = self.log[prev_index].term if prev_index >= 0 else 0
        entries = [entry.__dict__ for entry in self.log[next_index:]]
        payload = {
            "term": self.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_index,
            "prev_log_term": prev_term,
            "entries": entries,
            "leader_commit": self.commit_index,
        }
        response = await self.bus.send(f"{peer.host}:{peer.port}", "raft.append_entries", payload)
        if response.get("success"):
            self._match_index[peer_id] = prev_index + len(entries)
            self._next_index[peer_id] = self._match_index[peer_id] + 1
        else:
            self._next_index[peer_id] = max(0, next_index - 1)
        return response

    async def _handle_request_vote(self, payload: dict) -> dict:
        term = payload.get("term", 0)
        candidate_id = payload.get("candidate_id")
        last_log_index = payload.get("last_log_index", -1)
        last_log_term = payload.get("last_log_term", 0)
        if term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}
        if term > self.current_term:
            await self._step_down(term)
        up_to_date = self._is_up_to_date(last_log_index, last_log_term)
        if (self.voted_for is None or self.voted_for == candidate_id) and up_to_date:
            self.voted_for = candidate_id
            await self._persist_state()
            self._reset_election_timer()
            return {"term": self.current_term, "vote_granted": True}
        return {"term": self.current_term, "vote_granted": False}

    async def _handle_append_entries(self, payload: dict) -> dict:
        term = payload.get("term", 0)
        if term < self.current_term:
            return {"term": self.current_term, "success": False}
        if term > self.current_term:
            await self._step_down(term)
        self.state = "follower"
        self.leader_id = payload.get("leader_id")
        self._reset_election_timer()

        prev_index = payload.get("prev_log_index", -1)
        prev_term = payload.get("prev_log_term", 0)
        if prev_index >= 0:
            if prev_index >= len(self.log):
                return {"term": self.current_term, "success": False}
            if self.log[prev_index].term != prev_term:
                return {"term": self.current_term, "success": False}
        entries = [LogEntry(**entry) for entry in payload.get("entries", [])]
        if entries:
            del self.log[prev_index + 1 :]
            self.log.extend(entries)
            await self._persist_state()
        leader_commit = payload.get("leader_commit", -1)
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            await self._apply_entries()
        return {"term": self.current_term, "success": True}

    def _last_log_info(self) -> tuple[int, int]:
        if not self.log:
            return -1, 0
        return len(self.log) - 1, self.log[-1].term

    def _is_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        my_index, my_term = self._last_log_info()
        if last_log_term != my_term:
            return last_log_term > my_term
        return last_log_index >= my_index

    async def _apply_entries(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            if self.apply_callback:
                await self.apply_callback(self.log[self.last_applied].command)

    def _init_leader_state(self) -> None:
        last_index = len(self.log)
        for peer in self.peers:
            peer_id = self._peer_id(peer)
            self._next_index[peer_id] = last_index
            self._match_index[peer_id] = -1

    def _advance_commit_index(self) -> bool:
        match_indexes = list(self._match_index.values()) + [len(self.log) - 1]
        if not match_indexes:
            return False
        match_indexes.sort()
        quorum_index = match_indexes[(len(match_indexes) - 1) // 2]
        if quorum_index > self.commit_index:
            if self.log[quorum_index].term == self.current_term:
                self.commit_index = quorum_index
                return True
        return False

    async def _step_down(self, new_term: int) -> None:
        self.current_term = new_term
        self.state = "follower"
        self.voted_for = None
        await self._persist_state()

    def _reset_election_timer(self) -> None:
        self._election_reset.set()

    async def _load_state(self) -> None:
        if not self._redis:
            return
        data = await self._redis.hgetall(self._state_key())
        if not data:
            return
        self.current_term = int(data.get("term", 0))
        self.voted_for = data.get("voted_for") or None
        raw_log = data.get("log")
        if raw_log:
            self.log = [LogEntry(**entry) for entry in json.loads(raw_log)]

    async def _persist_state(self) -> None:
        if not self._redis:
            return
        payload = {
            "term": str(self.current_term),
            "voted_for": self.voted_for or "",
            "log": json.dumps([entry.__dict__ for entry in self.log]),
        }
        await self._redis.hset(self._state_key(), mapping=payload)

    def _state_key(self) -> str:
        return f"raft:{self.node_id}:state"

    @staticmethod
    def _peer_id(peer: PeerInfo) -> str:
        return f"{peer.host}:{peer.port}"
