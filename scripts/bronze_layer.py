# First stage in Medallion Architecture: Ingest raw flight data from OpenSky API and 
# store it in the Bronze layer as JSON files.

import requests
import json
import logging
from datetime import datetime
from pathlib import Path

# Logging outputs correctly in Airflow logs for monitoring and debugging
logger = logging.getLogger(__name__)

# Source endpoint: Current state of all aircrafts being tracked globally by OpenSky Network API
URL = "https://opensky-network.org/api/states/all"

# Column reference for the raw state vector array returned by OpenSky
# The API returns a list of arrays — no column headers included
# This mapping serves as documentation for downstream Silver processing
OPENSKY_COLUMNS = [
    "icao24",         # Unique ICAO 24-bit aircraft identifier
    "callsign",       # Flight callsign (may be null)
    "origin_country", # Country of registration
    "time_position",  # Unix timestamp of last position update
    "last_contact",   # Unix timestamp of last signal received
    "longitude",      # WGS-84 longitude in decimal degrees
    "latitude",       # WGS-84 latitude in decimal degrees
    "baro_altitude",  # Barometric altitude in meters
    "on_ground",      # Boolean — True if aircraft is on ground
    "velocity",       # Ground speed in m/s
    "true_track",     # Track angle (degrees clockwise from north)
    "vertical_rate",  # Climb/descent rate in m/s
    "sensors",        # IDs of sensors that received signal
    "geo_altitude",   # Geometric altitude in meters
    "squawk",         # Transponder code
    "spi",            # Special purpose indicator
    "position_source" # 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM
]

def run_bronze_ingestion(**context):
    """
    Function running within Airflow to fetch flight data from OpenSky API.
    Raw response is persisted without transformation into the Bronze layer.
    Timestamp-based file naming ensures historical record is maintained.
    Metadata is captured using a Payload envelope that includes the column reference for downstream auditing.

    Input Parameters:
        - context:  Airflow's task context dictionary — it gets automatically injected 
                    when Airflow runs this function via a PythonOperator It carries metadata 
                    about the current DAG run, execution time, task instance etc. 
                    
                    Without it, you can't use context["ti"] to push an XCom value at the end.
    
    Raises:
        - requests.exceptions.RequestException: API call failed at network level
        - ValueError: API returned malformed or non-JSON response
        - Exception: Any unexpected error during ingestion
    """
    logger.info("Starting Bronze layer ingestion from OpenSky Network API...")

    try:
        logger.info("Sending GET request to: %s", URL)
        response = requests.get(URL, timeout=30) # API call, timeout prevents hanging/slow API response
        
        # Check if the request was successful, if not it will raise an HTTPError and
        # Airflow will mark the task as failed instead of silently continuing
        response.raise_for_status()

        logger.info(
            "API responded successfully — HTTP %s", 
            response.status_code
        )
    
    except requests.exceptions.RequestException as e:
        # ConnectionError, Timeout, TooManyRedirects, HTTPError etc.
        # The API was never successfully reached or returned an HTTP error
        logger.error("Request to source API failed: %s", e)
        raise

    try:
        raw_data = response.json() # parsing the JSON response

        # Raising ValueError if response is not valid JSON
        logger.info(
            "JSON parsed successfully — %d aircraft states received",
            len(raw_data.get("states", []) or [])
        )

    except ValueError as e:
        # API was reachable but returned malformed or non-JSON content
        # Could indicate API changes, maintenance pages, or rate limit responses
        logger.error("Invalid JSON received from source API: %s", e)
        raise

    try:
        # Every API call produces a uniquely named file based on the current timestamp. Re-running the pipeline
        # will create a new file without overwriting previous data, allowing us to maintain a historical record 
        # of all ingested flight data.
        ingestion_timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        logger.info("Building metadata envelope — timestamp: %s", ingestion_timestamp)

        # Wrapping the raw payload with ingestion metadata
        payload = {
            "ingestion_metadata": {
                "ingested_at": ingestion_timestamp,
                "source_url": URL,
                "record_count": len(raw_data.get("states", []) or []),
                "api_timestamp": raw_data.get("time"),
                "dag_run_id": context.get("run_id", "manual")
            },
            "columns": OPENSKY_COLUMNS,  # Column reference for downstream tasks
            "data": raw_data             # Raw API response — untouched
        }

        # Persisting raw data as json files in bronze layer with timestamp-based naming
        bronze_path = Path(
            f"/opt/airflow/data/bronze/flights_{ingestion_timestamp}.json"
        )

        bronze_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Bronze storage path resolved: %s", bronze_path)

        with open(bronze_path, "w") as f: 
            json.dump(raw_data, f, indent = 2) 

        logger.info(
            "Bronze file written successfully — path: %s | records: %d",
            bronze_path,
            payload["ingestion_metadata"]["record_count"]
        )

        # 'ti' = Task Instance, used by Airflow tasks to pass data b/w each other
        # The downstream silver task will call the xcom_pull to know which bronze file was created to be processed
        context["ti"].xcom_push(key="bronze_file", value=str(bronze_path))
        
        logger.info(
            "XCom push successful — key: 'bronze_file' | value: %s", 
            bronze_path
        )

    except Exception as e:
        # File I/O errors, XCom failures, metadata build errors etc.
        # Ensureing no silent failures reach downstream Silver tasks
        logger.error("Unexpected bronze ingestion error: %s", e)
        raise
