import asyncio
import os

from src.nodes.base_node import PeerInfo
from src.nodes.cache_node import CacheNode, run_node as run_cache_node
from src.nodes.lock_manager import LockManagerNode, run_node as run_lock_node
from src.nodes.queue_node import QueueNode, run_node as run_queue_node
from src.utils.config import (
    CacheConfig,
    NodeConfig,
    QueueConfig,
    RaftConfig,
    RBACConfig,
    SecurityConfig,
    parse_permissions,
    parse_token_map,
)


def _load_node_config() -> NodeConfig:
    node_id = os.getenv("NODE_ID", "node-1")
    host = os.getenv("NODE_HOST", "0.0.0.0")
    port = int(os.getenv("NODE_PORT", "8000"))
    peers_raw = os.getenv("PEERS", "")
    peers = []
    for item in [p.strip() for p in peers_raw.split(",") if p.strip()]:
        if ":" not in item:
            continue
        peer_host, peer_port = item.split(":", 1)
        peers.append(PeerInfo(node_id=f"{peer_host}:{peer_port}", host=peer_host, port=int(peer_port)))
    return NodeConfig(node_id=node_id, host=host, port=port, peers=peers)


def _load_security_config() -> SecurityConfig:
    return SecurityConfig(
        tls_enabled=os.getenv("TLS_ENABLED", "false").lower() == "true",
        tls_cert_file=os.getenv("TLS_CERT_FILE"),
        tls_key_file=os.getenv("TLS_KEY_FILE"),
        tls_ca_file=os.getenv("TLS_CA_FILE"),
    )


def _load_rbac_config() -> RBACConfig:
    enabled = os.getenv("RBAC_ENABLED", "false").lower() == "true"
    token_map = parse_token_map(os.getenv("RBAC_TOKENS", ""))
    role_permissions = parse_permissions(os.getenv("RBAC_PERMISSIONS", ""))
    return RBACConfig(enabled=enabled, token_map=token_map, role_permissions=role_permissions)


def _load_queue_config() -> QueueConfig:
    return QueueConfig(
        replication_factor=int(os.getenv("QUEUE_REPLICATION", "2")),
        visibility_timeout_s=int(os.getenv("QUEUE_VISIBILITY_TIMEOUT", "30")),
        recovery_interval_s=int(os.getenv("QUEUE_RECOVERY_INTERVAL", "10")),
        virtual_nodes=int(os.getenv("QUEUE_VNODES", "50")),
        membership_refresh_s=int(os.getenv("QUEUE_MEMBERSHIP_REFRESH", "5")),
    )


def _load_cache_config() -> CacheConfig:
    return CacheConfig(
        replacement_policy=os.getenv("CACHE_POLICY", "LRU"),
        capacity=int(os.getenv("CACHE_CAPACITY", "1000")),
        membership_refresh_s=int(os.getenv("CACHE_MEMBERSHIP_REFRESH", "5")),
    )


def main() -> None:
    node_config = _load_node_config()
    raft_config = RaftConfig()
    security = _load_security_config()
    rbac = _load_rbac_config()
    token = os.getenv("RBAC_TOKEN")
    role = os.getenv("NODE_ROLE", "lock").lower()
    if role == "queue":
        queue_config = _load_queue_config()
        redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
        node = QueueNode(node_config, queue_config, redis_url, security, rbac, token)
        asyncio.run(run_queue_node(node))
    elif role == "cache":
        cache_config = _load_cache_config()
        redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
        node = CacheNode(node_config, cache_config, redis_url, security, rbac, token)
        asyncio.run(run_cache_node(node))
    else:
        redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
        node = LockManagerNode(node_config, raft_config, redis_url, security, rbac, token)
        asyncio.run(run_lock_node(node))


if __name__ == "__main__":
    main()
