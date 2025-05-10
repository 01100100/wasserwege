import requests
import statistics
import os
import json
from glob import glob
import time


def benchmark_gpx_processing(
    api_url, gpx_files, iterations=5, log_dir="benchmark_logs"
):
    """Benchmark GPX processing performance and log results."""

    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"benchmark_{int(time.time())}.json")

    results = {}

    for gpx_file in gpx_files:
        print(f"Benchmarking {os.path.basename(gpx_file)}...")

        timings = []
        crossing_counts = []

        for i in range(iterations):
            with open(gpx_file, "rb") as f:
                start_time = time.time()
                response = requests.post(f"{api_url}/process_gpx", files={"file": f})
                elapsed_time = time.time() - start_time

                if response.status_code == 200:
                    data = response.json()
                    timings.append(elapsed_time * 1000)  # Convert to milliseconds
                    crossing_counts.append(data["total_crossings"])
                    print(
                        f"  Run {i + 1}: {elapsed_time * 1000:.2f}ms, {data['total_crossings']} crossings"
                    )
                else:
                    print(f"  Run {i + 1}: Error - {response.status_code}")

        if timings:
            results[os.path.basename(gpx_file)] = {
                "min_time_ms": min(timings),
                "max_time_ms": max(timings),
                "avg_time_ms": statistics.mean(timings),
                "median_time_ms": statistics.median(timings),
                "crossing_count": crossing_counts[0],  # Should be same for all runs
            }

    # Print summary
    print("\nPerformance Summary:")
    print("=" * 80)
    print(
        f"{'File':<30} | {'Min (ms)':<10} | {'Max (ms)':<10} | {'Avg (ms)':<10} | {'Median (ms)':<10} | {'Crossings':<10}"
    )
    print("-" * 80)

    for filename, stats in results.items():
        print(
            f"{filename:<30} | {stats['min_time_ms']:<10.2f} | {stats['max_time_ms']:<10.2f} | {stats['avg_time_ms']:<10.2f} | {stats['median_time_ms']:<10.2f} | {stats['crossing_count']:<10}"
        )

    # Save results to log file
    with open(log_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"Benchmark results saved to {log_file}")
    return results


if __name__ == "__main__":
    api_url = "http://localhost:8000"
    gpx_files = glob("test_data/*.gpx")

    if not gpx_files:
        print("No GPX files found in test_data directory!")
    else:
        benchmark_gpx_processing(api_url, gpx_files)
