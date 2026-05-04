from __future__ import annotations

import argparse
import asyncio
import base64
import uuid
from typing import Dict

from aiohttp import ClientSession


def _b64(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


async def _post(session: ClientSession, url: str, message_type: str, payload: Dict, token: str | None) -> Dict:
    headers = {}
    if token:
        headers["X-Auth-Token"] = token
    async with session.post(url, json={"type": message_type, "payload": payload}, headers=headers) as response:
        response.raise_for_status()
        return await response.json()


async def demo_lock(session: ClientSession, base_url: str, token: str | None) -> None:
    payload = {"key": "resource-1", "owner": "client-1"}
    print("[lock] acquire shared")
    print(await _post(session, base_url, "lock.acquire.shared", payload, token))
    print("[lock] release")
    print(await _post(session, base_url, "lock.release", payload, token))
    print("[lock] acquire exclusive")
    print(await _post(session, base_url, "lock.acquire.exclusive", payload, token))
    print("[lock] release")
    print(await _post(session, base_url, "lock.release", payload, token))


async def demo_queue(session: ClientSession, base_url: str, token: str | None) -> None:
    queue_name = f"demo-{uuid.uuid4()}"
    payload = {"queue": queue_name, "payload": _b64("hello-queue")}
    print("[queue] enqueue")
    enqueue = await _post(session, base_url, "queue.enqueue", payload, token)
    print(enqueue)
    print("[queue] dequeue")
    message_id = None
    for _ in range(3):
        dequeue = await _post(session, base_url, "queue.dequeue", {"queue": queue_name}, token)
        print(dequeue)
        item = (dequeue.get("result") or {}).get("item") or {}
        message_id = item.get("message_id")
        if message_id:
            break
        await asyncio.sleep(0.5)
    if message_id:
        print("[queue] ack")
        print(
            await _post(
                session,
                base_url,
                "queue.ack",
                {"queue": queue_name, "message_id": message_id},
                token,
            )
        )


async def demo_cache(session: ClientSession, base_url: str, token: str | None) -> None:
    print("[cache] put")
    print(
        await _post(
            session,
            base_url,
            "cache.put",
            {"key": "k1", "value": _b64("value-1")},
            token,
        )
    )
    print("[cache] get")
    print(await _post(session, base_url, "cache.get", {"key": "k1"}, token))


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


if __name__ == "__main__":
    asyncio.run(main())
