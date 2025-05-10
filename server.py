from fastapi import FastAPI, File, UploadFile, HTTPException
import duckdb
import gpxpy
import json
import time
import uvicorn

app = FastAPI(
    title="Waterway Crossing API",
    description="Fast service to detect waterway crossings from GPX files",
)

DB_PATH = "data/pond.duckdb"

# Reuse connection for better performance
conn = duckdb.connect(DB_PATH, read_only=True)
conn.execute("LOAD spatial;")


@app.post("/process_gpx")
async def process_gpx(file: UploadFile = File(...)):
    """Process GPX file and find waterway intersections"""
    overall_start_time = time.time()

    # Read the uploaded file
    contents = await file.read()

    try:
        # Parse GPX
        parse_start_time = time.time()
        gpx = gpxpy.parse(contents.decode())
        parse_time_ms = round((time.time() - parse_start_time) * 1000, 2)

        # Extract linestring from GPX
        linestring_start_time = time.time()
        linestring = gpx_to_linestring(gpx)
        linestring_time_ms = round((time.time() - linestring_start_time) * 1000, 2)

        # Find intersections
        intersection_start_time = time.time()
        crossings = find_waterway_crossings(linestring)
        intersection_time_ms = round((time.time() - intersection_start_time) * 1000, 2)

        # Return results with timing
        overall_processing_time_ms = round((time.time() - overall_start_time) * 1000, 2)
        return {
            "processing_times_ms": {
                "total": overall_processing_time_ms,
                "gpx_parsing": parse_time_ms,
                "linestring_conversion": linestring_time_ms,
                "intersection_finding": intersection_time_ms,
            },
            "total_crossings": len(crossings),
            "crossings": crossings,
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def gpx_to_linestring(gpx):
    """Convert GPX to WKT linestring"""
    points = []
    for track in gpx.tracks:
        for segment in track.segments:
            for point in segment.points:
                points.append((point.longitude, point.latitude))

    if len(points) < 2:
        raise ValueError("GPX file must contain at least 2 points")

    # Create WKT linestring
    return f"LINESTRING({', '.join([f'{x} {y}' for x, y in points])})"


def find_waterway_crossings(linestring):
    """Find waterway crossings using spatial index"""

    # Use prepared statement for better performance
    # Query updated to use correct column names: w.geom, w.waterway_name, w.waterway_type
    # Also aliased CTE's geometry column to avoid ambiguity if needed, though w.geom and r.geom are distinct.
    result = conn.execute(
        """
        WITH route_geom_cte AS (
            SELECT ST_GeomFromText($1) as geom
        )
        SELECT
            w.id,
            w.waterway_name,
            w.waterway_type,
            ST_AsGeoJSON(ST_Intersection(w.geom, r.geom)) as intersection_geojson
        FROM waterways w, route_geom_cte r
        WHERE ST_Intersects(w.geom, r.geom)
    """,
        [linestring],
    ).fetchall()

    # Format results
    crossings = []
    # Updated loop variables to match the query's SELECT list
    for rid, rname, rtype, r_intersection_geojson in result:
        crossings.append(
            {
                "id": rid,
                "name": rname if rname else "Unnamed waterway",
                "type": rtype,
                "intersection": json.loads(r_intersection_geojson),
            }
        )

    return crossings


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    result = conn.execute("SELECT COUNT(*) FROM waterways").fetchone()
    waterway_count = result[0] if result else 0
    return {"status": "healthy", "database": DB_PATH, "waterway_count": waterway_count}


if __name__ == "__main__":
    print("🚀 Starting Wasserwege server at http://0.0.0.0:8000")
    print("📚 API docs at http://localhost:8000/docs")
    print("💧 Waterway database: " + DB_PATH)
    result = conn.execute("SELECT COUNT(*) FROM waterways").fetchone()
    waterway_count = result[0] if result else 0
    if waterway_count == 0:
        print("❌ No waterways found in the database. Please load the database first.")
        exit(1)
    else:
        print(f"🌊 Loaded {waterway_count} waterways")
    uvicorn.run(app, host="0.0.0.0", port=8000)
