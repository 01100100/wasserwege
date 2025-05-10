import os
import subprocess
from tqdm import tqdm
from enum import Enum


class Country(Enum):
    GERMANY = "germany"
    FRANCE = "france"
    ITALY = "italy"
    SPAIN = "spain"
    POLAND = "poland"
    NETHERLANDS = "netherlands"
    BELGIUM = "belgium"
    SWITZERLAND = "switzerland"
    AUSTRIA = "austria"
    DENMARK = "denmark"
    SWEDEN = "sweden"
    NORWAY = "norway"
    FINLAND = "finland"
    PORTUGAL = "portugal"
    CZECHIA = "czech-republic"
    HUNGARY = "hungary"
    SLOVAKIA = "slovakia"
    SLOVENIA = "slovenia"
    CROATIA = "croatia"
    LUXEMBOURG = "luxembourg"
    IRELAND = "ireland"
    UNITED_KINGDOM = "great-britain"


def download_osm_file(country: Country, output_dir="data/osm"):
    """Download the latest OSM file for a given country from Geofabrik."""
    os.makedirs(output_dir, exist_ok=True)
    osm_file = os.path.join(output_dir, f"{country.value}-latest.osm.pbf")

    if os.path.exists(osm_file):
        print(f"✅ {osm_file} already exists. Skipping download.")
        return osm_file

    print(f"🌍 Downloading {country.name} OSM file...")
    url = f"https://download.geofabrik.de/europe/{country.value}-latest.osm.pbf"
    subprocess.run(["curl", "-o", osm_file, url], check=True)
    print(f"✅ Download complete: {osm_file}")

    return osm_file


def filter_waterways(osm_file, output_dir="data/filtered"):
    """Filter the OSM file for waterways using osmium."""
    filtered_file = os.path.join(output_dir, "germany-waterways.osm.pbf")

    if not os.path.exists(filtered_file):
        print("🚰 Filtering waterways from OSM file...")
        with tqdm(total=100, desc="Filtering", unit="%") as pbar:
            subprocess.run(
                [
                    "osmium",
                    "tags-filter",
                    osm_file,
                    "w/waterway",
                    "-o",
                    filtered_file,
                    "--overwrite",
                ],
                check=True,
            )
            pbar.update(100)
    else:
        print("✅ Waterways file already exists. Skipping filtering.")

    return filtered_file


def convert_to_geoparquet(filtered_file, output_dir="data/parquet"):
    """Convert the filtered OSM file to GeoParquet using ohsome-planet."""
    geoparquet_dir = os.path.join(output_dir, "germany")

    if not os.path.exists(geoparquet_dir):
        print("📦 Converting waterways to GeoParquet format...")
        with tqdm(total=100, desc="Converting", unit="%") as pbar:
            subprocess.run(
                [
                    "java",
                    "-jar",
                    "ohsome-planet-cli/target/ohsome-planet.jar",
                    "contributions",
                    "--pbf",
                    filtered_file,
                    "--output",
                    geoparquet_dir,
                    "--overwrite",
                ],
                check=True,
            )
            pbar.update(100)
    else:
        print("✅ GeoParquet directory already exists. Skipping conversion.")

    return geoparquet_dir


if __name__ == "__main__":
    print("🚀 Starting waterway data preparation...")
    country = Country.GERMANY
    osm_file = download_osm_file(country)
    filtered_file = filter_waterways(osm_file)
    geoparquet_dir = convert_to_geoparquet(filtered_file)
    print(f"🎉 GeoParquet files are ready in {geoparquet_dir}.")
