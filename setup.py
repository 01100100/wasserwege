import os
import duckdb
import time


def setup_waterways_database(parquet_file, db_path="data/pond.duckdb"):
    """Set up DuckDB with waterway data and spatial indexing"""

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
    SELECT * FROM read_parquet($1)
    """,
        [parquet_file],
    )

    # Add spatial index
    print("Creating spatial index (R-tree)...")
    con.execute("""
    CREATE INDEX waterways_spatial_idx ON waterways USING RTREE (geometry);
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

    if len(sys.argv) != 2:
        print("Usage: python setup.py <waterways.parquet>")
        sys.exit(1)

    parquet_file = sys.argv[1]
    setup_waterways_database(parquet_file)
    print("Database ready for use!")
