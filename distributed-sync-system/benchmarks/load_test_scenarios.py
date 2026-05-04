from __future__ import annotations

import base64
import os
import random
import uuid

from locust import HttpUser, between, task


def _pick_node(raw: str) -> str:
	nodes = [item.strip() for item in raw.split(",") if item.strip()]
	if not nodes:
		raise RuntimeError("No nodes configured for locust")
	return random.choice(nodes)


def _rbac_headers() -> dict:
	token = os.getenv("RBAC_TOKEN")
	if not token:
		return {}
	return {"X-Auth-Token": token}


class DistributedSystemUser(HttpUser):
	wait_time = between(0.1, 0.5)

	lock_nodes = os.getenv("LOCK_NODES", "localhost:8000,localhost:8001,localhost:8002")
	queue_nodes = os.getenv("QUEUE_NODES", "localhost:8100,localhost:8101,localhost:8102")
	cache_nodes = os.getenv("CACHE_NODES", "localhost:8200,localhost:8201,localhost:8202")
	scenario = os.getenv("SCENARIO", "all").lower()

	@task(3)
	def lock_shared_roundtrip(self) -> None:
		if self.scenario not in {"all", "lock"}:
			return
		node = _pick_node(self.lock_nodes)
		key = f"lock-{random.randint(1, 100)}"
		owner = f"client-{uuid.uuid4()}"
		payload = {"key": key, "owner": owner}
		self.client.post(
			f"http://{node}/messages",
			json={"type": "lock.acquire.shared", "payload": payload},
			headers=_rbac_headers(),
			name="lock.acquire.shared",
		)
		self.client.post(
			f"http://{node}/messages",
			json={"type": "lock.release", "payload": payload},
			headers=_rbac_headers(),
			name="lock.release",
		)

	@task(2)
	def queue_roundtrip(self) -> None:
		if self.scenario not in {"all", "queue"}:
			return
		node = _pick_node(self.queue_nodes)
		queue_name = f"queue-{random.randint(1, 10)}"
		payload_raw = f"payload-{uuid.uuid4()}".encode("ascii")
		payload = base64.b64encode(payload_raw).decode("ascii")
		response = self.client.post(
			f"http://{node}/messages",
			json={"type": "queue.enqueue", "payload": {"queue": queue_name, "payload": payload}},
			headers=_rbac_headers(),
			name="queue.enqueue",
		)
		if response.status_code != 200:
			return
		dequeue = self.client.post(
			f"http://{node}/messages",
			json={"type": "queue.dequeue", "payload": {"queue": queue_name}},
			headers=_rbac_headers(),
			name="queue.dequeue",
		)
		if dequeue.status_code != 200:
			return
		body = dequeue.json().get("result") or {}
		item = body.get("item") or {}
		message_id = item.get("message_id")
		if message_id:
			self.client.post(
				f"http://{node}/messages",
				json={
					"type": "queue.ack",
					"payload": {"queue": queue_name, "message_id": message_id},
				},
				headers=_rbac_headers(),
				name="queue.ack",
			)

	@task(2)
	def cache_roundtrip(self) -> None:
		if self.scenario not in {"all", "cache"}:
			return
		node = _pick_node(self.cache_nodes)
		key = f"cache-{random.randint(1, 100)}"
		value_raw = f"value-{uuid.uuid4()}".encode("ascii")
		value = base64.b64encode(value_raw).decode("ascii")
		self.client.post(
			f"http://{node}/messages",
			json={"type": "cache.put", "payload": {"key": key, "value": value}},
			headers=_rbac_headers(),
			name="cache.put",
		)
		self.client.post(
			f"http://{node}/messages",
			json={"type": "cache.get", "payload": {"key": key}},
			headers=_rbac_headers(),
			name="cache.get",
		)
