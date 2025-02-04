# [START get_flights]
# [START import_module]

import json
import pandas
import datetime as dt
import textwrap3 as tw
from pyopensky.rest import REST
from pyopensky.config import opensky_config_dir
import time
# Can use the 'traffic' module to download and interact with the aircraft database?
# Maybe should download one time and store on postgresql

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task


# [END import_module]

# [START instantiate_dag]

with DAG(
    "get_flights_dag",
    default_args={"retries": 2},
    description="Get all state vectors from opensky network.",
    schedule="@continuous",
    start_date=dt.datetime(2025,1,25),
    catchup=False,
    tags=["Flights"],
    max_active_runs=1,
    max_active_tasks=1
) as dag:
    
    # [END instantiate_dag]
    # [START extract_function]
    def extract(**kwargs):
        # rest = REST()
        ti = kwargs['ti']
        # state_vectors = rest.states(bounds=(42, 53, 83, 141))
        state_vectors = pandas.DataFrame(
            {
                "id":["e69045", "b34125", "fa7723"], 
                "altitude":[4300, 2000, 1890],
                "longitude":[60, 50, 15],
                "latitude":[35, 40, 33]
            }
        )
        ti.xcom_push('flights', state_vectors) # Make flight data available to load task
    
    def load_to_warehouse(**kwargs):
        ti = kwargs['ti']
        flights = ti.xcom_pull(task_ids='extract', key='flights')
        print(flights)

    def delay_10(**kwargs):
        time.sleep(10)

    # [START main_flow]
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_to_warehouse
    )

    delay_task = PythonOperator(
        task_id="delay",
        python_callable=delay_10
    )

    extract_task.doc_md = tw.dedent(
        """ /
        ### Extract Task
        A simple task which gets all of the current state vectors from the opensky network.
        """ 
    )

    extract_task >> load_task >> delay_task
    # [END main_flow]