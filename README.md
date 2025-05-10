# Wasserwege 🌊🚴‍♂️

Wasserwege is a high-performance service that allows users to upload a `GPX` file and (almost) instantly determine which waterways their route has crossed. It leverages spatial indexing, an incredible duck of a database and open data from OSM to provide blazing-fast results 🔥


## Features ⚡️

- Upload a `.gpx` file to detect intersected waterways.
- Supports GeoParquet for efficient waterway data storage.
- Blazing-fast spatial queries using DuckDB with R-tree indexing.
- RESTful API for seamless integration with other applications.
- Lightweight and optimized for performance.

## Powered By Open Data 🌎

The application relies on open data from [OpenStreetMap](https://www.openstreetmap.org/about) and tools like [ohsome-planet](https://github.com/GIScience/ohsome-planet) for waterway extraction.

## External Libraries and Resources

Wasserwege uses several external libraries and resources:

- **[DuckDB](https://duckdb.org/)** for in-process analytical database capabilities.
- **[FastAPI](https://fastapi.tiangolo.com/)** for building the RESTful API.
- **[gpxpy](https://github.com/tkrajina/gpxpy)** for parsing GPX files.
- **[pyarrow](https://arrow.apache.org/)** for handling GeoParquet files.
- **[osmium-tool](https://osmcode.org/osmium-tool/)** for converting OSM data to GeoJSON.

## Development

### Project Structure

The project is structured as follows:

- `setup.py`: Script to set up the DuckDB database with waterway data.
- `server.py`: FastAPI server to handle GPX uploads and return intersected waterways.
- `benchmark.py`: Script to benchmark the performance of the service.
- `data/`: Directory to store the DuckDB database and GeoParquet files.

### Setting Up the Database

To set up the database with waterway data, run the following command:

```bash
python setup.py <path_to_waterways.parquet>
```

This will create a DuckDB database with spatial indexing for fast queries.

### Running the Server

To start the FastAPI server, run:

```bash
python server.py
```

The server will be available at [`http://localhost:8000`](http://localhost:8000).

### Benchmarking

The `benchmark.py` script allows you to test the performance of the `/process_gpx` endpoint and logs the results for historical tracking.

#### Usage

Run the script as follows:

```bash
python benchmark.py
```

The script will:

1. Send multiple requests to the `/process_gpx` endpoint for each GPX file in the `test_data/` directory.
2. Measure the response time and log the results.
3. Save the benchmarking results as a JSON file in the `benchmark_logs/` directory, with a timestamped filename.

#### Example Output

The script prints a performance summary to the console, including the minimum, maximum, average, and median response times for each GPX file. The results are also saved in the `benchmark_logs/` directory for future reference.

## API Endpoints

### `/process_gpx`

- **Method**: POST
- **Description**: Upload a GPX file to detect intersected waterways.
- **Request**: Multipart form-data with a `file` field containing the GPX file.
- **Response**: JSON with the list of intersected waterways and processing time.

### `/health`

- **Method**: GET
- **Description**: Check the health of the service and database status.
- **Response**: JSON with the database status and waterway count.

## Deployment

### Local Deployment

1. Set up the database using `setup.py`.
2. Start the server using `server.py`.

### Production Deployment

For production, use a WSGI server like `uvicorn`:

```bash
uvicorn server:app --host 0.0.0.0 --port 8000 --workers 4
```

## Preparing German Waterway Data

To prepare the German waterway data, use the `prepare_german_waterways.py` script.

<script async id="asciicast-569727" src="https://asciinema.org/a/569727.js"></script>

The script automates the following steps:

1. Downloads the latest German OSM file from [Geofabrik](https://download.geofabrik.de/europe/germany-latest.osm.pbf).
2. Filters the OSM file for waterways using `osmium`.
3. Converts the filtered data into GeoParquet format using `ohsome-planet`.

### Requirements

- `curl` for downloading the OSM file.
- `osmium` for filtering the OSM file.
- `java` (version 21) and `ohsome-planet` for converting to GeoParquet.

### Usage

Run the script as follows:

```bash
python prepare_german_waterways.py
```

The script will:

1. Download the German OSM file to `data/osm/germany-latest.osm.pbf`.
2. Filter waterways into `data/osm/germany-waterways.osm.pbf`.
3. Convert the filtered data into GeoParquet format in `data/osm/out-germany/`.

The resulting GeoParquet files can be used for further geospatial analysis.

## Installing Dependencies

To prepare and process waterway data, you need to install the following tools:

### Osmium

Osmium is a versatile command-line tool for working with OpenStreetMap data. It is used to filter OSM files for specific tags, such as waterways.

It can be installed using the following methods:

1. **Homebrew (macOS)**:
   ```bash
   brew install osmium-tool
   ```
2. **Linux**:
   ```bash
   sudo apt-get install osmium-tool
   ```

Other installation methods are available on the [Osmium installation page](https://osmcode.org/osmium-tool/manual.html#installation).

### ohsome-planet

The ohsome-planet tool converts OSM PBF files into GeoParquet format for geospatial analysis.

#### Requirements

- Java 21 is required to run the tool.
- Maven is required to build the tool.

You can install it into a subdirectory of your project and build it using the following commands:

```bash
git clone --recurse-submodules https://github.com/GIScience/ohsome-planet.git
cd ohsome-planet
./mvnw clean package -DskipTests
```

Once these tools are installed, you can use the `prepare_german_waterways.py` script to process waterway data.

## Shoutouts

Thanks to the open-source community for providing the tools and data that make this project possible.
