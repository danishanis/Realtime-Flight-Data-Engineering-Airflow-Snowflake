# First stage in Medallion Architecture: Ingest raw flight data from OpenSky API and 
# store it in the Bronze layer as JSON files.

import requests
import json
from datetime import datetime
from pathlib import Path

# Source endpoint: Current state of all aircrafts being tracked globally by OpenSky Network API
URL = "https://opensky-network.org/api/states/all"

def run_bronze_ingestion(**context):
    """
    Function running within Airflow to fetch flight data from OpenSky API

    Input Parameters:
        - context:  Airflow's task context dictionary — it gets automatically injected 
                    when Airflow runs this function via a PythonOperator It carries metadata 
                    about the current DAG run, execution time, task instance etc. 
                    
                    Without it, you can't use context["ti"] to push an XCom value at the end.
    """
    response = requests.get(URL, timeout=30) # API call
    response.raise_for_status() # Check if the request was successful, if not it will raise an HTTPError

    data = response.json()

    # Every API call produces a uniquely named file based on the current timestamp. Re-running the pipeline
    # will create a new file without overwriting previous data, allowing us to maintain a historical record 
    # of all ingested flight data.
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    path = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json")

    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(data, f)

    # 'ti' = Task Instance, used by Airflow tasks to pass data b/w each other
    # The downstream silver task will call the xcom_pull to know which bronze file was created to be processed
    context["ti"].xcom_push(key="bronze_file", value=str(path))
