from fastapi import FastAPI, File, UploadFile, HTTPException
import duckdb
import gpxpy
import json
import time

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
    start_time = time.time()

    # Read the uploaded file
    contents = await file.read()

    try:
        # Parse GPX
        gpx = gpxpy.parse(contents.decode())

        # Extract linestring from GPX
        linestring = gpx_to_linestring(gpx)

        # Find intersections
        crossings = find_waterway_crossings(linestring)

        # Return results with timing
        processing_time = time.time() - start_time
        return {
            "processing_time_ms": round(processing_time * 1000, 2),
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
    result = conn.execute(
        """
        WITH route AS (
            SELECT ST_GeomFromText($1) as geom
        )
        SELECT
            w.id,
            w.name,
            w.type,
            ST_AsGeoJSON(ST_Intersection(w.geometry, r.geom)) as intersection
        FROM waterways w, route r
        WHERE ST_Intersects(w.geometry, r.geom)
    """,
        [linestring],
    ).fetchall()

    # Format results
    crossings = []
    for id, name, waterway_type, intersection in result:
        crossings.append(
            {
                "id": id,
                "name": name if name else "Unnamed waterway",
                "type": waterway_type,
                "intersection": json.loads(intersection),
            }
        )

    return crossings


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    waterway_count = conn.execute("SELECT COUNT(*) FROM waterways").fetchone()[0]
    return {"status": "healthy", "database": DB_PATH, "waterway_count": waterway_count}
