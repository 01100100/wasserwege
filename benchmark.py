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

        # Initialize metrics collection
        crossing_counts = []
        timings = []

        for i in range(iterations):
            with open(gpx_file, "rb") as f:
                start_time = time.time()
                response = requests.post(f"{api_url}/process_gpx", files={"file": f})
                end_time = time.time()

                round_trip_time = (
                    end_time - start_time
                ) * 1000  # Convert to milliseconds

                if response.status_code == 200:
                    data = response.json()
                    crossing_counts.append(len(data.get("crossings", [])))
                    timings.append(round_trip_time)

                    print(
                        f"  Run {i + 1}: Crossings: {len(data.get('crossings', []))}, Round trip: {round_trip_time:.2f}ms"
                    )
                else:
                    print(f"  Run {i + 1}: Error - {response.status_code}")

        if crossing_counts:
            # Store results for this file
            file_result = {
                "crossing_count": crossing_counts[0],  # Should be same for all runs
                "timings_ms": {
                    "min": min(timings),
                    "max": max(timings),
                    "avg": sum(timings) / len(timings),
                    "median": statistics.median(timings),
                },
            }

            results[os.path.basename(gpx_file)] = file_result

    # Save results to log file
    with open(log_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nBenchmark results saved to {log_file}")
    return results


if __name__ == "__main__":
    api_url = "http://localhost:8000"
    gpx_files = glob("test_data/gpx/*.gpx")

    if not gpx_files:
        print("No GPX files found in test_data/gpx directory!")
    else:
        print(f"Found {len(gpx_files)} GPX files to benchmark.")
        print(f"Files: {', '.join([os.path.basename(f) for f in gpx_files])}")
        benchmark_gpx_processing(api_url, gpx_files)
