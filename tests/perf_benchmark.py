import concurrent.futures
import math
import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from server import serve
from client import ReplicatedCounterClient

def run_benchmark_config(targets, num_clients, total_requests=2000):
    reqs_per_client = total_requests // num_clients
    latencies = []

    def worker(client_idx):
        client = ReplicatedCounterClient(targets=targets, client_id=f"bench-client-{client_idx}")
        local_latencies = []
        for _ in range(reqs_per_client):
            start = time.perf_counter()
            client.incr("bench_cnt", delta=1)
            end = time.perf_counter()
            local_latencies.append((end - start) * 1000.0)  # ms
        return local_latencies

    start_total = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_clients) as executor:
        futures = [executor.submit(worker, i) for i in range(num_clients)]
        for f in concurrent.futures.as_completed(futures):
            latencies.extend(f.result())
    end_total = time.perf_counter()

    latencies.sort()
    n = len(latencies)
    median = latencies[n // 2]
    p95_idx = math.ceil(0.95 * n) - 1
    p95 = latencies[p95_idx]

    return median, p95, n

def main():
    print("Starting Task C4 Performance Benchmark...")

    s1, serv1 = serve(port=50501, node_id="bench-single")
    single_targets = ["localhost:50501"]


    ports = [50502, 50503, 50504]
    servers = []
    quorum_targets = []
    for i, p in enumerate(ports):
        s, _ = serve(port=p, node_id=f"bench-q-{i}")
        servers.append(s)
        quorum_targets.append(f"localhost:{p}")

    results = {}
    try:
        print("Benchmarking Single replica, 1 client...")
        results["s1"] = run_benchmark_config(single_targets, 1, 2000)

        print("Benchmarking Single replica, 16 clients...")
        results["s16"] = run_benchmark_config(single_targets, 16, 2000)

        print("Benchmarking Quorum (3 replicas), 1 client...")
        results["q1"] = run_benchmark_config(quorum_targets, 1, 2000)

        print("Benchmarking Quorum (3 replicas), 16 clients...")
        results["q16"] = run_benchmark_config(quorum_targets, 16, 2000)

    finally:
        s1.stop(0)
        for s in servers:
            s.stop(0)

    print("\n" + "="*70)
    print("Performance Evaluation Results (Task C4)")
    print("="*70)
    print(f"| {'Configuration':<30} | {'Median latency (ms)':<20} | {'p95 latency (ms)':<18} | {'Requests':<10} |")
    print(f"|{'-'*32}|{'-'*22}|{'-'*20}|{'-'*12}|")
    configs = [
        ("Single replica, 1 client", "s1"),
        ("Single replica, 16 clients", "s16"),
        ("Quorum (3 replicas), 1 client", "q1"),
        ("Quorum (3 replicas), 16 clients", "q16"),
    ]
    for label, key in configs:
        med, p95, reqs = results[key]
        print(f"| {label:<30} | {med:<20.3f} | {p95:<18.3f} | {reqs:<10} |")
    print("="*70)

if __name__ == "__main__":
    main()