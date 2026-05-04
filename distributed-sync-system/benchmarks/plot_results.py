from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt


def read_locust_csv(path: Path) -> Dict[str, List[Tuple[float, float]]]:
    data: Dict[str, List[Tuple[float, float]]] = {}
    with path.open("r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            name = row.get("Name") or "unknown"
            rps = float(row.get("Requests/s") or 0)
            avg = float(row.get("Average Response Time") or 0)
            data.setdefault(name, []).append((rps, avg))
    return data


def plot_summary(data: Dict[str, List[Tuple[float, float]]], out_dir: Path) -> None:
    names = [name for name in data.keys() if name != "Aggregated"]
    avg_latency = [sum(item[1] for item in data[name]) / max(1, len(data[name])) for name in names]
    avg_rps = [sum(item[0] for item in data[name]) / max(1, len(data[name])) for name in names]

    out_dir.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(8, 4))
    plt.bar(names, avg_latency)
    plt.title("Average Latency (ms)")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig(out_dir / "latency.png")
    plt.close()

    plt.figure(figsize=(8, 4))
    plt.bar(names, avg_rps)
    plt.title("Average Throughput (req/s)")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig(out_dir / "throughput.png")
    plt.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot locust CSV results")
    parser.add_argument("--csv", required=True, help="Path to locust stats CSV")
    parser.add_argument("--out", default="benchmarks/results", help="Output directory for plots")
    args = parser.parse_args()

    data = read_locust_csv(Path(args.csv))
    plot_summary(data, Path(args.out))


if __name__ == "__main__":
    main()
