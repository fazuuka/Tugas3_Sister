from __future__ import annotations

from prometheus_client import Counter, Histogram

REQUEST_COUNTER = Counter("requests_total", "Total requests", ["component", "operation"])
REQUEST_LATENCY = Histogram("request_latency_seconds", "Request latency", ["component", "operation"])
