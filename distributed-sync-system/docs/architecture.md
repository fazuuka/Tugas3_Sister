# Arsitektur

## overview

Sistem ini terdiri dari tiga subsistem terdistribusi yang berbagi message bus aman yang sama:

- Lock manager: konsensus berbasis Raft untuk shared/exclusive locks dan deadlock detection.
- Queue: consistent hashing untuk ownership, Redis persistence, at-least-once delivery.
- Cache: MESI coherence di atas penyimpanan berbasis Redis dengan invalidation propagation.

## Diagram Komponen

```
                      +------------------+
                      |      Redis       |
                      +------------------+
                        ^    ^    ^    ^
                        |    |    |    |
                        |    |    |    |
      +-----------------+    |    |    +-----------------+
      |                      |    |                      |
      |                      |    |                      |
 +-----------------+    +-----------------+        +-----------------+
 |  lock cluster   |    | queue cluster   |        | Cache cluster   |
 |  L1  L2  L3     |    |  Q1  Q2  Q3     |        |  C1  C2  C3     |
 +-----------------+    +-----------------+        +-----------------+

 Catatan koneksi internal (TLS/RBAC):
 - lock cluster  : L1 <-> L2 <-> L3 (full mesh)
 - queue cluster : Q1 <-> Q2 <-> Q3 (full mesh)
 - Cache cluster : C1 <-> C2 <-> C3 (full mesh)
```

## Message Bus

Semua komunikasi antar-node menggunakan satu HTTP endpoint (`/messages`) dengan dukungan TLS
dan lightweight RBAC token policy. Message types diberi prefiks berdasarkan komponen:

- `raft.*` untuk consensus traffic
- `lock.*` untuk lock acquisition/release
- `queue.*` untuk enqueue/dequeue/ack
- `cache.*` untuk get/put/invalidate
