import os
import duckdb
import time
import subprocess
from pathlib import Path


def run_dbt_model():
    """Run the dbt model to process waterways data"""
    print("Running dbt model to process waterways data...")

    # Change to the quelle directory
    os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "quelle"))

    # Run dbt model
    result = subprocess.run(
        ["dbt", "run", "--select", "example.waterways_with_names"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print("Error running dbt model:")
        print(result.stderr)
        raise RuntimeError("Failed to run dbt model")

    print("Successfully processed waterways data with dbt")

    # Change back to the original directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    return Path("data/processed/waterways_with_names.parquet").absolute()


def setup_waterways_database(parquet_file=None, db_path="data/pond.duckdb"):
    """Set up DuckDB with waterway data and spatial indexing"""

    # Run dbt model if parquet file not provided
    if parquet_file is None:
        parquet_file = run_dbt_model()

    print(f"Setting up DuckDB database from {parquet_file}")
    start_time = time.time()

    # Create database directory if needed
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect to DuckDB
    con = duckdb.connect(db_path)

    # Install and load spatial extension
    con.execute("INSTALL spatial; LOAD spatial;")

    # Create waterways table optimized for spatial queries
    print("Creating waterways table...")
    con.execute(
        """
    CREATE OR REPLACE TABLE waterways AS
    SELECT
        id,
        name,
        type,
        ST_GeomFromWKB(geometry) as geometry
    FROM read_parquet($1)
    WHERE geometry IS NOT NULL
    """,
        [str(parquet_file)],
    )

    # Add spatial index
    print("Creating spatial index (R-tree)...")
    con.execute("""
    CREATE INDEX IF NOT EXISTS waterways_spatial_idx ON waterways USING RTREE (geometry);
    """)

    # Get row count
    count = con.execute("SELECT COUNT(*) FROM waterways").fetchone()[0]
    print(f"Loaded {count} waterway features")

    # Analyze table for query optimization
    print("Analyzing table for query optimization...")
    con.execute("ANALYZE waterways")

    # Set pragmas for performance
    con.execute("PRAGMA memory_limit='8GB'")
    con.execute("PRAGMA threads=8")

    elapsed = time.time() - start_time
    print(f"Database setup completed in {elapsed:.2f} seconds")

    con.close()
    return db_path


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        parquet_file = sys.argv[1]
        setup_waterways_database(parquet_file)
    else:
        setup_waterways_database()

    print("Database ready for use!")
