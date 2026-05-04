from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List

from src.nodes.base_node import PeerInfo


@dataclass(frozen=True)
class NodeConfig:
    node_id: str
    host: str
    port: int
    peers: List[PeerInfo]


@dataclass(frozen=True)
class RaftConfig:
    election_timeout_ms: int = 150
    heartbeat_interval_ms: int = 50


@dataclass(frozen=True)
class QueueConfig:
    replication_factor: int = 2
    visibility_timeout_s: int = 30
    recovery_interval_s: int = 10
    virtual_nodes: int = 50
    membership_refresh_s: int = 5


@dataclass(frozen=True)
class CacheConfig:
    replacement_policy: str = "LRU"
    capacity: int = 1000
    membership_refresh_s: int = 5


@dataclass(frozen=True)
class SecurityConfig:
    tls_enabled: bool = False
    tls_cert_file: str | None = None
    tls_key_file: str | None = None
    tls_ca_file: str | None = None


@dataclass(frozen=True)
class RBACConfig:
    enabled: bool = False
    token_map: Dict[str, str] = field(default_factory=dict)
    role_permissions: Dict[str, Iterable[str]] = field(default_factory=dict)


def parse_token_map(value: str) -> Dict[str, str]:
    if not value:
        return {}
    mapping: Dict[str, str] = {}
    for item in value.split(","):
        token_role = item.strip()
        if not token_role or ":" not in token_role:
            continue
        token, role = token_role.split(":", 1)
        mapping[token.strip()] = role.strip()
    return mapping


def parse_permissions(value: str) -> Dict[str, Iterable[str]]:
    if not value:
        return {}
    mapping: Dict[str, Iterable[str]] = {}
    for item in value.split(","):
        role_rules = item.strip()
        if not role_rules or ":" not in role_rules:
            continue
        role, rules = role_rules.split(":", 1)
        mapping[role.strip()] = [rule.strip() for rule in rules.split("|") if rule.strip()]
    return mapping
