from __future__ import annotations

import argparse
import asyncio
import base64
import json
import time
import uuid
from typing import Dict

from aiohttp import ClientSession


def _b64(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


def _print_header(title: str) -> None:
    line = "=" * 70
    print(line)
    print(title)
    print(line)


def _print_step(index: int, text: str) -> None:
    print(f"[{index}] {text}")


def _print_result(result: Dict) -> None:
    print(json.dumps(result, indent=2, ensure_ascii=True))


async def _post(session: ClientSession, url: str, message_type: str, payload: Dict, token: str | None) -> Dict:
    headers = {}
    if token:
        headers["X-Auth-Token"] = token
    async with session.post(url, json={"type": message_type, "payload": payload}, headers=headers) as response:
        response.raise_for_status()
        return await response.json()


async def _get(session: ClientSession, url: str) -> bool:
    async with session.get(url) as response:
        response.raise_for_status()
        await response.read()
        return True


async def demo_lock(session: ClientSession, base_url: str, token: str | None) -> None:
    payload = {"key": "resource-1", "owner": "client-1"}
    _print_header("DEMO 1: DISTRIBUTED LOCK MANAGER (Raft Consensus)")
    _print_step(1, "Acquire shared lock")
    _print_result(await _post(session, base_url, "lock.acquire.shared", payload, token))
    _print_step(2, "Release lock")
    _print_result(await _post(session, base_url, "lock.release", payload, token))
    _print_step(3, "Acquire exclusive lock")
    _print_result(await _post(session, base_url, "lock.acquire.exclusive", payload, token))
    _print_step(4, "Release lock")
    _print_result(await _post(session, base_url, "lock.release", payload, token))


async def demo_queue(session: ClientSession, base_url: str, token: str | None) -> None:
    queue_name = f"demo-{uuid.uuid4()}"
    payload = {"queue": queue_name, "payload": _b64("hello-queue")}
    _print_header("DEMO 2: DISTRIBUTED QUEUE (Consistent Hashing)")
    _print_step(1, "Enqueue message")
    enqueue = await _post(session, base_url, "queue.enqueue", payload, token)
    _print_result(enqueue)
    _print_step(2, "Dequeue message")
    message_id = None
    for _ in range(3):
        dequeue = await _post(session, base_url, "queue.dequeue", {"queue": queue_name}, token)
        _print_result(dequeue)
        item = (dequeue.get("result") or {}).get("item") or {}
        message_id = item.get("message_id")
        if message_id:
            break
        await asyncio.sleep(0.5)
    if message_id:
        _print_step(3, "Ack message")
        _print_result(
            await _post(
                session,
                base_url,
                "queue.ack",
                {"queue": queue_name, "message_id": message_id},
                token,
            )
        )


async def demo_cache(session: ClientSession, base_url: str, token: str | None) -> None:
    _print_header("DEMO 3: DISTRIBUTED CACHE (MESI Protocol)")
    _print_step(1, "Put key")
    _print_result(
        await _post(
            session,
            base_url,
            "cache.put",
            {"key": "k1", "value": _b64("value-1")},
            token,
        )
    )
    _print_step(2, "Get key")
    _print_result(await _post(session, base_url, "cache.get", {"key": "k1"}, token))


async def demo_health(session: ClientSession, lock_url: str, queue_url: str, cache_url: str) -> None:
    _print_header("DEMO 4: NODE HEALTH & FAILURE DETECTION")
    nodes = {
        "Lock Node": lock_url.replace("/messages", "/metrics"),
        "Queue Node": queue_url.replace("/messages", "/metrics"),
        "Cache Node": cache_url.replace("/messages", "/metrics"),
    }
    _print_step(1, "Checking health of all nodes")
    for name, url in nodes.items():
        try:
            await _get(session, url)
            print(f"- {name}: OK")
        except Exception as exc:
            print(f"- {name}: FAIL ({exc})")


async def demo_benchmark(session: ClientSession, lock_url: str, queue_url: str, cache_url: str, token: str | None) -> None:
    _print_header("PERFORMANCE BENCHMARK")

    _print_step(1, "Queue throughput test (50 messages)")
    queue_name = f"bench-{uuid.uuid4()}"
    start = time.perf_counter()
    for i in range(50):
        await _post(
            session,
            queue_url,
            "queue.enqueue",
            {"queue": queue_name, "payload": _b64(f"payload-{i}")},
            token,
        )
    elapsed = time.perf_counter() - start
    print(f"50 messages in {elapsed:.2f}s = {50 / max(elapsed, 0.001):.1f} msg/s")

    _print_step(2, "Cache read/write throughput (50 ops)")
    start = time.perf_counter()
    for i in range(25):
        await _post(
            session,
            cache_url,
            "cache.put",
            {"key": f"bench-{i}", "value": _b64(f"v-{i}")},
            token,
        )
        await _post(session, cache_url, "cache.get", {"key": f"bench-{i}"}, token)
    elapsed = time.perf_counter() - start
    print(f"50 cache ops in {elapsed:.2f}s = {50 / max(elapsed, 0.001):.1f} ops/s")

    _print_step(3, "Lock acquire/release cycle (10 cycles)")
    start = time.perf_counter()
    payload = {"key": "bench-lock", "owner": "bench-client"}
    for _ in range(10):
        await _post(session, lock_url, "lock.acquire.exclusive", payload, token)
        await _post(session, lock_url, "lock.release", payload, token)
    elapsed = time.perf_counter() - start
    print(f"10 lock cycles in {elapsed:.2f}s = {10 / max(elapsed, 0.001):.1f} cycles/s")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Demo for lock, queue, and cache")
    parser.add_argument("--lock", default="http://localhost:8000/messages", help="Lock node /messages URL")
    parser.add_argument("--queue", default="http://localhost:8100/messages", help="Queue node /messages URL")
    parser.add_argument("--cache", default="http://localhost:8200/messages", help="Cache node /messages URL")
    parser.add_argument("--token", default=None, help="RBAC token (e.g. dev-token-1)")
    args = parser.parse_args()

    async with ClientSession() as session:
        await demo_lock(session, args.lock, args.token)
        await demo_queue(session, args.queue, args.token)
        await demo_cache(session, args.cache, args.token)
        await demo_health(session, args.lock, args.queue, args.cache)
        await demo_benchmark(session, args.lock, args.queue, args.cache, args.token)


if __name__ == "__main__":
    asyncio.run(main())
