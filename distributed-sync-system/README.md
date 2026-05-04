# Distributed Sync System

Kerangka proyek awal untuk sistem sinkronisasi terdistribusi.

## Mulai Cepat

1. Buat dan aktifkan virtual environment.
2. Instal dependensi: `pip install -r requirements.txt`
3. Salin `.env.example` ke `.env` dan sesuaikan nilainya.
4. Jalankan sebuah node:
	- `NODE_ROLE=lock python -m src`
	- `NODE_ROLE=queue python -m src`
	- `NODE_ROLE=cache python -m src`

## Docker Compose

Dari root repositori:

`docker compose -f docker/docker-compose.yml up --build`

## Simulasi Partisi

- Blok target tertentu: `PARTITION_BLOCKLIST=lock-2:8001,lock-3:8002`
- Tingkat drop acak: `PARTITION_DROP_RATE=0.2`

## Benchmark

Jalankan locust dengan skenario bawaan:

`locust -f benchmarks/load_test_scenarios.py --headless -u 5 -r 1 -t 1m --csv=benchmarks/locust`


## Laporan

Isi [docs/report.md](docs/report.md) dan ekspor ke PDF sebagai `report.pdf`.

## Pengujian

- Unit: `pytest tests/unit`
- Integrasi: `pytest tests/integration`

## Struktur

- src/: implementasi inti
- tests/: unit, integration, dan performance tests
- docker/: aset container
- docs/: dokumentasi dan spesifikasi API
- benchmarks/: load test scenarios
