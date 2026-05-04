# Panduan Deployment

## Lokal (Python)

1. Buat virtual environment dan instal dependensi:
	- `pip install -r requirements.txt`
2. Konfigurasikan `.env` berdasarkan [.env.example](../.env.example)
3. Jalankan sebuah node:
	- `NODE_ROLE=lock python -m src`
	- `NODE_ROLE=queue python -m src`
	- `NODE_ROLE=cache python -m src`

## Docker Compose

Dari root repositori:

1. `docker compose -f docker/docker-compose.yml up --build`
2. Services dikelompokkan berdasarkan role:
	- Lock nodes: `lock-1..3` pada port 8000-8002
	- Queue nodes: `queue-1..3` pada port 8100-8102
	- Cache nodes: `cache-1..3` pada port 8200-8202

## TLS Setup (Opsional)

1. Buat self-signed certs (contoh):
	- `openssl req -x509 -newkey rsa:4096 -nodes -keyout certs/node.key -out certs/node.pem -days 365`
	- `cp certs/node.pem certs/ca.pem`
2. Aktifkan TLS:
	- `TLS_ENABLED=true`
	- `TLS_CERT_FILE=./certs/node.pem`
	- `TLS_KEY_FILE=./certs/node.key`
	- `TLS_CA_FILE=./certs/ca.pem`

## Simulasi Partisi (Opsional)

- Drop traffic ke target (dipisahkan koma):
	- `PARTITION_BLOCKLIST=lock-2:8001,lock-3:8002`
- Drop persentase traffic (0.0 - 1.0):
	- `PARTITION_DROP_RATE=0.2`

## Troubleshooting

- Error RBAC: pastikan `RBAC_TOKEN` ada di `RBAC_TOKENS`.
- Queue not delivering: periksa konektivitas Redis (`REDIS_URL`).
- Cache staleness: pastikan semua cache nodes dapat dijangkau untuk invalidations.

