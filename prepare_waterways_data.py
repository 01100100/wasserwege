import os
import subprocess
from tqdm import tqdm
from enum import Enum
import click
import sys


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


@click.command()
@click.option(
    "--country",
    type=click.Choice([c.name for c in Country], case_sensitive=False),
    default="GERMANY",
    help="Country to download OSM data for.",
)
@click.option("--overwrite", is_flag=True, help="Overwrite existing data at each step.")
@click.option(
    "--skip-dbt", is_flag=True, help="Skip running the dbt after data preparation."
)
def main(country, overwrite, skip_dbt):
    """Prepare waterway data for the specified country."""
    print("🚀 Starting waterway data preparation...")
    country_enum = Country[country.upper()]

    osm_file = download_osm_file(country_enum, overwrite=overwrite)
    filtered_file = filter_waterways(osm_file, overwrite=overwrite)
    geoparquet_dir = convert_to_geoparquet(filtered_file, overwrite=overwrite)

    print(f"🎉 GeoParquet files are ready in {geoparquet_dir}.")

    if not skip_dbt:
        run_dbt(overwrite)


def download_osm_file(country: Country, output_dir="data/raw", overwrite=False):
    """Download the latest OSM file for a given country from Geofabrik."""
    os.makedirs(output_dir, exist_ok=True)
    osm_file = os.path.join(output_dir, f"{country.value}-latest.osm.pbf")

    if os.path.exists(osm_file):
        if not overwrite:
            user_input = (
                input(
                    f"⚠️ {osm_file} already exists. Do you want to overwrite it? (y/n): "
                )
                .strip()
                .lower()
            )
            if user_input != "y":
                print(f"✅ {osm_file} already exists. Skipping download.")
                return osm_file

    print(f"🌍 Downloading {country.name} OSM file...")
    url = f"https://download.geofabrik.de/europe/{country.value}-latest.osm.pbf"
    subprocess.run(["curl", "-o", osm_file, url], check=True)
    print(f"✅ Download complete: {osm_file}")

    return osm_file


def validate_osm_file(osm_file):
    """Validate the OSM file using osmium."""
    try:
        subprocess.run(
            ["osmium", "fileinfo", osm_file], check=True, stdout=subprocess.DEVNULL
        )
        return True
    except subprocess.CalledProcessError:
        return False


def check_filtered_file(filtered_file):
    """Check if the filtered OSM file is valid."""
    try:
        subprocess.run(
            ["osmium", "fileinfo", filtered_file], check=True, stdout=subprocess.DEVNULL
        )
        return True
    except subprocess.CalledProcessError:
        return False


def filter_waterways(osm_file, output_dir="data/filtered", overwrite=False):
    """Filter the OSM file for waterways and relations using osmium."""
    filtered_file = os.path.join(output_dir, "germany-waterways.osm.pbf")

    if os.path.exists(filtered_file):
        if not overwrite:
            user_input = (
                input(
                    f"⚠️ {filtered_file} already exists. Do you want to overwrite it? (y/n): "
                )
                .strip()
                .lower()
            )
            if user_input != "y":
                print("✅ Filtered waterways file is valid. Skipping filtering.")
                return filtered_file

    print("🚰 Filtering waterways and relations from OSM file...")
    with tqdm(total=100, desc="Filtering", unit="%") as pbar:
        process = subprocess.Popen(
            [
                "osmium",
                "tags-filter",
                osm_file,
                "w/waterway",
                "r/type=waterway",
                "-o",
                filtered_file,
                "--overwrite",
                "--verbose",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        for line in process.stdout:
            print(line.strip())
            pbar.update(1)  # Simulate progress bar update

        process.wait()

        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, process.args)

    print("✅ Filtering complete and file is valid.")
    return filtered_file


def convert_to_geoparquet(filtered_file, output_dir="data/parquet", overwrite=False):
    """Convert the filtered OSM file to GeoParquet using ohsome-planet."""
    geoparquet_dir = os.path.join(output_dir, "germany")
    jar_path = "ohsome-planet/ohsome-planet-cli/target/ohsome-planet.jar"

    if not os.path.exists(jar_path):
        raise FileNotFoundError(
            f"❌ The JAR file {jar_path} is missing. Please build the ohsome-planet tool as described in its README."
        )

    if os.path.exists(geoparquet_dir):
        if not overwrite:
            user_input = (
                input(
                    f"⚠️ GeoParquet directory {geoparquet_dir} already exists. Do you want to overwrite it? (y/n): "
                )
                .strip()
                .lower()
            )
            if user_input != "y":
                print("✅ GeoParquet directory already exists. Skipping conversion.")
                return geoparquet_dir

    print("📦 Converting waterways to GeoParquet format...")
    with tqdm(total=100, desc="Converting", unit="%") as pbar:
        subprocess.run(
            [
                "java",
                "-jar",
                jar_path,
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

    print("✅ Conversion complete.")
    return geoparquet_dir


def run_dbt(overwrite=False):
    """Run dbt to process waterways data"""
    # Get the absolute path to the quelle directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    quelle_dir = os.path.join(current_dir, "quelle")

    if not os.path.exists(quelle_dir):
        print(f"❌ Could not find quelle directory at {quelle_dir}")
        return

    # Check if local duckdb database exists
    db_path = os.path.join(current_dir, "data", "pond.duckdb")

    if os.path.exists(db_path) and not overwrite:
        user_input = (
            input(
                f"⚠️ {db_path} already exists 🦆. Do you want to regenerate it? (y/n): "
            )
            .strip()
            .lower()
        )
        if user_input != "y":
            print("✅ Processed data already exists. Skipping dbt.")
            return

    print("🧮 Running dbt to transform waterways data...")

    # Save current directory to return to it later
    original_dir = os.getcwd()

    try:
        # Change to the quelle directory
        os.chdir(quelle_dir)

        # Run dbt build to execute the model and tests
        with tqdm(total=100, desc="Running dbt", unit="%") as pbar:
            process = subprocess.run(
                ["dbt", "build"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )

            for line in process.stdout.splitlines():
                print(f"  dbt: {line}")

            pbar.update(100)

        print("✅ dbt execution complete.")

        if os.path.exists(db_path):
            print("🐣 A Fresh db was born...")
        else:
            print(f"⚠️ Expected duckdb file not found at {db_path}")

    except subprocess.CalledProcessError as e:
        print(f"❌ Error running dbt: {e}")
        print(e.stdout)
    except Exception as e:
        print(f"❌ Unexpected error running dbt: {e}")
    finally:
        # Return to the original directory
        os.chdir(original_dir)


def check_osmium_installed():
    """Check if osmium is installed and accessible."""
    try:
        subprocess.run(
            ["osmium", "--version"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        raise FileNotFoundError(
            "❌ Osmium is not installed or not in the PATH. Please install Osmium and try again."
        )
    except subprocess.CalledProcessError:
        raise RuntimeError(
            "❌ Osmium is installed but not functioning correctly. Please check your installation."
        )


def check_dbt_installed():
    """Check if dbt is installed and accessible."""
    try:
        subprocess.run(
            ["dbt", "--version"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        raise FileNotFoundError(
            "❌ dbt is not installed or not in the PATH. Please install dbt and try again."
        )
    except subprocess.CalledProcessError:
        raise RuntimeError(
            "❌ dbt is installed but not functioning correctly. Please check your installation."
        )


# Call this function at the start of the script
if __name__ == "__main__":
    check_osmium_installed()
    try:
        check_dbt_installed()
    except Exception as e:
        print(f"Warning: {e}")
        print(
            "Continuing without dbt. The --skip-dbt flag will be automatically applied."
        )
        if "--skip-dbt" not in sys.argv:
            sys.argv.append("--skip-dbt")

    main()
