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

    # For collecting aggregate statistics across all files
    all_round_trip = []
    all_server_processing = []
    all_gpx_parsing = []
    all_linestring_conversion = []
    all_intersection_finding = []
    total_crossings = 0
    total_files = 0

    for gpx_file in gpx_files:
        print(f"Benchmarking {os.path.basename(gpx_file)}...")

        # Initialize metrics collection
        total_timings = []
        round_trip_timings = []
        gpx_parsing_timings = []
        linestring_conversion_timings = []
        intersection_finding_timings = []
        crossing_counts = []

        for i in range(iterations):
            with open(gpx_file, "rb") as f:
                start_time = time.time()
                response = requests.post(f"{api_url}/process_gpx", files={"file": f})
                round_trip_time = time.time() - start_time

                if response.status_code == 200:
                    data = response.json()
                    # Collect all timing metrics
                    round_trip_timings.append(round_trip_time * 1000)  # Convert to ms
                    total_timings.append(data["processing_times_ms"]["total"])
                    gpx_parsing_timings.append(
                        data["processing_times_ms"]["gpx_parsing"]
                    )
                    linestring_conversion_timings.append(
                        data["processing_times_ms"]["linestring_conversion"]
                    )
                    intersection_finding_timings.append(
                        data["processing_times_ms"]["intersection_finding"]
                    )
                    crossing_counts.append(data["total_crossings"])

                    print(
                        f"  Run {i + 1}: "
                        f"Round trip: {round_trip_time * 1000:.2f}ms, "
                        f"Server processing: {data['processing_times_ms']['total']:.2f}ms, "
                        f"Crossings: {data['total_crossings']}"
                    )
                else:
                    print(f"  Run {i + 1}: Error - {response.status_code}")

        if total_timings:
            # Add to the aggregate data collectors
            all_round_trip.extend(round_trip_timings)
            all_server_processing.extend(total_timings)
            all_gpx_parsing.extend(gpx_parsing_timings)
            all_linestring_conversion.extend(linestring_conversion_timings)
            all_intersection_finding.extend(intersection_finding_timings)
            total_crossings += crossing_counts[0]
            total_files += 1

            results[os.path.basename(gpx_file)] = {
                "round_trip": {
                    "min_ms": min(round_trip_timings),
                    "max_ms": max(round_trip_timings),
                    "avg_ms": statistics.mean(round_trip_timings),
                    "median_ms": statistics.median(round_trip_timings),
                },
                "server_processing": {
                    "min_ms": min(total_timings),
                    "max_ms": max(total_timings),
                    "avg_ms": statistics.mean(total_timings),
                    "median_ms": statistics.median(total_timings),
                },
                "gpx_parsing": {
                    "min_ms": min(gpx_parsing_timings),
                    "max_ms": max(gpx_parsing_timings),
                    "avg_ms": statistics.mean(gpx_parsing_timings),
                    "median_ms": statistics.median(gpx_parsing_timings),
                },
                "linestring_conversion": {
                    "min_ms": min(linestring_conversion_timings),
                    "max_ms": max(linestring_conversion_timings),
                    "avg_ms": statistics.mean(linestring_conversion_timings),
                    "median_ms": statistics.median(linestring_conversion_timings),
                },
                "intersection_finding": {
                    "min_ms": min(intersection_finding_timings),
                    "max_ms": max(intersection_finding_timings),
                    "avg_ms": statistics.mean(intersection_finding_timings),
                    "median_ms": statistics.median(intersection_finding_timings),
                },
                "crossing_count": crossing_counts[0],  # Should be same for all runs
            }

    # Print summary with detailed breakdown
    print("\nPerformance Summary:")
    print("=" * 100)

    for filename, stats in results.items():
        print(f"\n🗺️ {filename} - {stats['crossing_count']} crossings")
        print("-" * 100)
        print(
            f"{'Metric':<25} | {'Min (ms)':<10} | {'Max (ms)':<10} | {'Avg (ms)':<10} | {'Median (ms)':<10}"
        )
        print("-" * 100)

        for metric in [
            "round_trip",
            "server_processing",
            "gpx_parsing",
            "linestring_conversion",
            "intersection_finding",
        ]:
            m = stats[metric]
            print(
                f"{metric.replace('_', ' ').title():<25} | "
                f"{m['min_ms']:<10.2f} | {m['max_ms']:<10.2f} | "
                f"{m['avg_ms']:<10.2f} | {m['median_ms']:<10.2f}"
            )

    # Add aggregate summary at the end
    if total_files > 0:
        print("\n📈 AGGREGATE SUMMARY ACROSS ALL FILES")
        print("=" * 100)
        print(f"Total Files: {total_files}, Total Crossings: {total_crossings}")
        print("-" * 100)
        print(
            f"{'Metric':<25} | {'Min (ms)':<10} | {'Max (ms)':<10} | {'Avg (ms)':<10} | {'Median (ms)':<10}"
        )
        print("-" * 100)

        # Print aggregate stats for each metric
        metrics_data = {
            "Round Trip": all_round_trip,
            "Server Processing": all_server_processing,
            "Gpx Parsing": all_gpx_parsing,
            "Linestring Conversion": all_linestring_conversion,
            "Intersection Finding": all_intersection_finding,
        }

        for metric_name, data in metrics_data.items():
            if data:  # Make sure we have data
                print(
                    f"{metric_name:<25} | "
                    f"{min(data):<10.2f} | {max(data):<10.2f} | "
                    f"{statistics.mean(data):<10.2f} | {statistics.median(data):<10.2f}"
                )

        # Add aggregate results to the JSON output
        results["_aggregate"] = {
            "total_files": total_files,
            "total_crossings": total_crossings,
            "round_trip": {
                "min_ms": min(all_round_trip) if all_round_trip else 0,
                "max_ms": max(all_round_trip) if all_round_trip else 0,
                "avg_ms": statistics.mean(all_round_trip) if all_round_trip else 0,
                "median_ms": statistics.median(all_round_trip) if all_round_trip else 0,
            },
            "server_processing": {
                "min_ms": min(all_server_processing) if all_server_processing else 0,
                "max_ms": max(all_server_processing) if all_server_processing else 0,
                "avg_ms": statistics.mean(all_server_processing)
                if all_server_processing
                else 0,
                "median_ms": statistics.median(all_server_processing)
                if all_server_processing
                else 0,
            },
            "gpx_parsing": {
                "min_ms": min(all_gpx_parsing) if all_gpx_parsing else 0,
                "max_ms": max(all_gpx_parsing) if all_gpx_parsing else 0,
                "avg_ms": statistics.mean(all_gpx_parsing) if all_gpx_parsing else 0,
                "median_ms": statistics.median(all_gpx_parsing)
                if all_gpx_parsing
                else 0,
            },
            "linestring_conversion": {
                "min_ms": min(all_linestring_conversion)
                if all_linestring_conversion
                else 0,
                "max_ms": max(all_linestring_conversion)
                if all_linestring_conversion
                else 0,
                "avg_ms": statistics.mean(all_linestring_conversion)
                if all_linestring_conversion
                else 0,
                "median_ms": statistics.median(all_linestring_conversion)
                if all_linestring_conversion
                else 0,
            },
            "intersection_finding": {
                "min_ms": min(all_intersection_finding)
                if all_intersection_finding
                else 0,
                "max_ms": max(all_intersection_finding)
                if all_intersection_finding
                else 0,
                "avg_ms": statistics.mean(all_intersection_finding)
                if all_intersection_finding
                else 0,
                "median_ms": statistics.median(all_intersection_finding)
                if all_intersection_finding
                else 0,
            },
        }

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
