import os
import subprocess
from tqdm import tqdm
from enum import Enum
import click
import sys


class Country(Enum):
    EUROPE = "europe"
    AUSTRIA = "austria"
    BELGIUM = "belgium"
    CROATIA = "croatia"
    CZECHIA = "czech-republic"
    DENMARK = "denmark"
    FINLAND = "finland"
    FRANCE = "france"
    GERMANY = "germany"
    HUNGARY = "hungary"
    IRELAND = "ireland"
    ITALY = "italy"
    LUXEMBOURG = "luxembourg"
    NETHERLANDS = "netherlands"
    NORWAY = "norway"
    POLAND = "poland"
    PORTUGAL = "portugal"
    SLOVAKIA = "slovakia"
    SLOVENIA = "slovenia"
    SPAIN = "spain"
    SWEDEN = "sweden"
    SWITZERLAND = "switzerland"
    UNITED_KINGDOM = "great-britain"


@click.command()
@click.option(
    "--country",
    type=click.Choice([c.name for c in Country], case_sensitive=False),
    help="Country which to process OSM data 🌍",
    required=True,
)
@click.option(
    "--skip-dbt", is_flag=True, help="Skip running the dbt after data preparation."
)
def main(country, skip_dbt):
    """Prepare waterway data for the specified country."""
    print(f"🚀 Starting waterway data preparation for {country}...")
    country_enum = Country[country.upper()]

    osm_file = download_osm_file(country_enum)
    if not osm_file:
        print("🛑 Halting process as OSM file step was skipped or failed.")
        return

    filtered_file = filter_waterways(country_enum, osm_file)
    if not filtered_file:
        print("🛑 Halting process as filtering step was skipped or failed.")
        return

    geoparquet_dir = convert_to_geoparquet(country_enum, filtered_file)
    if not geoparquet_dir:
        print("🛑 Halting process as GeoParquet conversion was skipped or failed.")
        return

    print(f"🎉 GeoParquet files are ready in {geoparquet_dir}.")

    if not skip_dbt:
        run_dbt(country_enum)


def download_osm_file(country: Country, output_dir="data/raw"):
    """Download the latest OSM file for a given country from Geofabrik."""
    # Create country-specific subdirectory
    country_dir = os.path.join(output_dir, country.value)
    os.makedirs(country_dir, exist_ok=True)

    osm_file = os.path.join(country_dir, f"{country.value}-latest.osm.pbf")

    if os.path.exists(osm_file):
        if not click.confirm(
            f"⚠️ {osm_file} already exists. Do you want to overwrite it?",
            default=False,
        ):
            print(f"✅ {osm_file} already exists. Skipping download.")
            return osm_file
        else:
            print(
                f"ℹ️ {osm_file} already exists. Proceeding with overwrite as confirmed by user."
            )

    print(f"🌍 Downloading {country.name} OSM file...")
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


def filter_waterways(country: Country, osm_file, output_dir="data/filtered"):
    """Filter the OSM file for waterways and relations using osmium."""
    # Create country-specific subdirectory
    country_dir = os.path.join(output_dir, country.value)
    os.makedirs(country_dir, exist_ok=True)

    filtered_file = os.path.join(country_dir, f"{country.value}-waterways.osm.pbf")

    if os.path.exists(filtered_file):
        if not click.confirm(
            f"⚠️ Filtered file {filtered_file} already exists. Do you want to overwrite it?",
            default=False,
        ):
            print(
                f"✅ Filtered file {filtered_file} already exists. Skipping filtering."
            )
            return filtered_file
        else:
            print(
                f"ℹ️ Filtered file {filtered_file} already exists. Proceeding with overwrite as confirmed by user."
            )

    print(f"🚰 Filtering waterways and relations from {country.name} OSM file...")
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

        if process.stdout:
            for line in process.stdout:
                print(line.strip())
                pbar.update(1)
        else:
            print("Warning: No stdout from osmium process")

        process.wait()

        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, process.args)

    print("✅ Filtering complete and file is valid.")
    return filtered_file


def convert_to_geoparquet(country: Country, filtered_file, output_dir="data/parquet"):
    """Convert the filtered OSM file to GeoParquet using ohsome-planet."""
    # Create country-specific subdirectory
    country_dir = os.path.join(output_dir, country.value)
    os.makedirs(country_dir, exist_ok=True)

    geoparquet_dir = country_dir
    jar_path = "ohsome-planet/ohsome-planet-cli/target/ohsome-planet.jar"

    if not os.path.exists(jar_path):
        raise FileNotFoundError(
            f"❌ The JAR file {jar_path} is missing. Please build the ohsome-planet tool as described in its README."
        )

    if os.path.exists(geoparquet_dir):
        if not click.confirm(
            f"⚠️ GeoParquet directory {geoparquet_dir} already exists. Do you want to overwrite it?",
            default=False,
        ):
            print(
                f"✅ GeoParquet directory {geoparquet_dir} already exists. Skipping conversion."
            )
            return geoparquet_dir
        else:
            print(
                f"ℹ️ GeoParquet directory {geoparquet_dir} already exists. Proceeding with overwrite as confirmed by user."
            )

    print(f"📦 Converting {country.name} waterways to GeoParquet format...")
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


def run_dbt(country: Country):
    """Run dbt to process waterways data"""
    # Get the absolute path to the quelle directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    quelle_dir = os.path.join(current_dir, "quelle")

    if not os.path.exists(quelle_dir):
        print(f"❌ Could not find quelle directory at {quelle_dir}")
        return

    # Check if local duckdb database exists
    db_path = os.path.join(current_dir, "data", "pond.duckdb")

    if os.path.exists(db_path):
        if not click.confirm(
            f"⚠️ Processed data {db_path} already exists. Do you want to regenerate it?",
            default=False,
        ):
            print(f"✅ Processed data {db_path} already exists. Skipping dbt.")
            return
        else:
            print(
                f"ℹ️ Processed data {db_path} already exists. Proceeding with regeneration as confirmed by user."
            )

    print(f"🧮 Running dbt to transform {country.name} waterways data...")

    # Save current directory to return to it later
    original_dir = os.getcwd()

    try:
        # Change to the quelle directory
        os.chdir(quelle_dir)

        # Run dbt build to execute the model and tests
        with tqdm(total=100, desc="Running dbt", unit="%") as pbar:
            # Pass country as a variable to dbt
            process = subprocess.run(
                ["dbt", "build", "--vars", f"{{country: {country.value}}}", "--debug"],
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
