from __future__ import annotations

import random
import ssl
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, Iterable, Optional, Set

from aiohttp import ClientSession, web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

Handler = Callable[[dict], Awaitable[dict]]


@dataclass(frozen=True)
class RBACPolicy:
    enabled: bool
    token_map: Dict[str, str]
    role_permissions: Dict[str, Iterable[str]] = field(default_factory=dict)

    def authorize(self, token: Optional[str], message_type: str) -> bool:
        if not self.enabled:
            return True
        if not token or token not in self.token_map:
            return False
        role = self.token_map[token]
        allowed = self.role_permissions.get(role, [])
        for rule in allowed:
            if rule == "*":
                return True
            if rule.endswith(".*") and message_type.startswith(rule[:-1]):
                return True
            if rule == message_type:
                return True
        return False


class MessageBus:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        token: Optional[str] = None,
        rbac_policy: Optional[RBACPolicy] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
        client_ssl_context: Optional[ssl.SSLContext] = None,
        blocked_targets: Optional[Iterable[str]] = None,
        drop_rate: float = 0.0,
    ) -> None:
        self.host = host
        self.port = port
        self.token = token
        self.rbac_policy = rbac_policy or RBACPolicy(False, {})
        self.ssl_context = ssl_context
        self.client_ssl_context = client_ssl_context
        self.blocked_targets: Set[str] = set(blocked_targets or [])
        self.drop_rate = max(0.0, min(1.0, drop_rate))
        self.handlers: Dict[str, Handler] = {}
        self._runner: Optional[web.AppRunner] = None

    def register(self, message_type: str, handler: Handler) -> None:
        self.handlers[message_type] = handler

    async def start(self) -> None:
        app = web.Application()
        app.router.add_post("/messages", self._handle_message)
        app.router.add_get("/metrics", self._handle_metrics)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, host=self.host, port=self.port, ssl_context=self.ssl_context)
        await site.start()

    async def stop(self) -> None:
        if self._runner is None:
            return
        await self._runner.cleanup()
        self._runner = None

    async def send(self, target: str, message_type: str, payload: dict) -> dict:
        if target in self.blocked_targets or random.random() < self.drop_rate:
            raise ConnectionError("Simulated network drop")
        scheme = "https" if self.client_ssl_context else "http"
        url = f"{scheme}://{target}/messages"
        headers = {}
        if self.token:
            headers["X-Auth-Token"] = self.token
        async with ClientSession() as session:
            async with session.post(
                url,
                json={"type": message_type, "payload": payload},
                headers=headers,
                ssl=self.client_ssl_context,
            ) as response:
                response.raise_for_status()
                data = await response.json()
                if isinstance(data, dict) and data.get("ok") and "result" in data:
                    return data["result"]
                return data

    async def _handle_message(self, request: web.Request) -> web.Response:
        body = await request.json()
        message_type = body.get("type")
        payload = body.get("payload", {})
        token = request.headers.get("X-Auth-Token")
        if not message_type:
            return web.json_response({"error": "missing message type"}, status=400)
        if not self.rbac_policy.authorize(token, message_type):
            return web.json_response({"error": "unauthorized"}, status=403)
        handler = self.handlers.get(message_type)
        if not handler:
            return web.json_response({"error": "unknown message type"}, status=404)
        result = await handler(payload)
        return web.json_response({"ok": True, "result": result})

    async def _handle_metrics(self, request: web.Request) -> web.Response:
        data = generate_latest()
        return web.Response(body=data, headers={"Content-Type": CONTENT_TYPE_LATEST})


def build_server_ssl_context(cert_file: str, key_file: str) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=cert_file, keyfile=key_file)
    return context


def build_client_ssl_context(ca_file: Optional[str]) -> ssl.SSLContext:
    if ca_file:
        return ssl.create_default_context(cafile=ca_file)
    return ssl.create_default_context()


def parse_blocklist(value: str) -> Set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}
