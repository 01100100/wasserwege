from fastapi import FastAPI, File, UploadFile
import duckdb
import json
import time
import uvicorn
import io
import lxml.etree as lxml_ET

app = FastAPI(
    title="Waterway Intersection API",
    description="Optimized service to find waterways intersecting a GPX route",
)

DB_PATH = "data/pond.duckdb"

# Reuse connection for better performance
conn = duckdb.connect(DB_PATH, read_only=True)
conn.execute("LOAD spatial;")


def custom_parse_gpx(gpx_content):
    """Custom fast GPX parser using ElementTree with optimizations"""
    start_time = time.time()

    # Parse with lxml's faster parser
    parser = lxml_ET.XMLParser(remove_blank_text=True, recover=True)
    root = lxml_ET.parse(io.BytesIO(gpx_content), parser).getroot()
    # Define namespace for GPX 1.1
    ns = {"gpx": "http://www.topografix.com/GPX/1/1"}
    # Fast path using XPath directly
    points = [
        (float(pt.get("lon", 0)), float(pt.get("lat", 0)))
        for pt in root.xpath(".//gpx:trkpt", namespaces=ns)
        if pt.get("lat") and pt.get("lon")
    ]

    parse_time = time.time() - start_time
    return points, parse_time


@app.post("/process_gpx")
async def process_gpx(file: UploadFile = File(...)):
    """Process GPX file and find waterway intersections"""
    t0 = time.time()
    # Read the uploaded GPX file
    contents = await file.read()
    t1 = time.time()
    print(f"File read time: {t1 - t0:.2f} seconds")

    # Use the custom parser
    points, parse_time = custom_parse_gpx(contents)
    t2 = time.time()
    print(f"Custom GPX parsing time: {t2 - t1:.2f} seconds")

    if len(points) < 2:
        return {"error": "GPX file must contain at least 2 points"}

    # Create linestring
    linestring = f"LINESTRING({', '.join([f'{x} {y}' for x, y in points])})"
    t3 = time.time()
    print(f"GPX to LineString conversion time: {t3 - t2:.2f} seconds")

    # Query waterways intersecting the route
    query = """
        WITH route AS (
            SELECT ST_GeomFromText($1) AS geom,
                   ST_Envelope(ST_GeomFromText($1)) AS bbox
        )
        SELECT
            w.id,
            w.waterway_name,
            w.waterway_type,
            ST_AsGeoJSON(ST_Intersection(w.geom, r.geom)) AS intersection_geojson
        FROM waterways w, route r
        WHERE ST_Intersects(w.geom, r.bbox)  -- First filter with bounding box (uses R-Tree)
          AND ST_Intersects(w.geom, r.geom)  -- Then precise intersection
        ORDER BY ST_Length(ST_Intersection(w.geom, r.geom)) DESC  -- Sort by intersection length
    """

    try:
        results = conn.execute(query, [linestring]).fetchall()
        t4 = time.time()
        print(f"Query execution time: {t4 - t3:.2f} seconds")

        # Format results
        t5 = time.time()
        print("Formatting results...")
        crossings = [
            {
                "id": row[0],
                "name": row[1] or "Unnamed waterway",
                "type": row[2],
                "intersection": json.loads(row[3]),
            }
            for row in results
        ]
        t6 = time.time()
        print(f"Result formatting time: {t6 - t5:.2f} seconds")

        print(f"Processing time: {t6 - t0:.2f} seconds")

        return {"crossings": crossings}
    except Exception as e:
        return {"error": f"Query failed: {str(e)}", "linestring": linestring}


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
