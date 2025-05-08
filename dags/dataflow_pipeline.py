import json
from airflow.sdk import dag, task

@dag(
    schedule=None
)
def dataflow_pipeline():
    @task()
    def extract():
        data_string = '{"dataPoint": "e-5-3", "value": 134}'

        return json.loads(data_string)
    
    @task()
    def load(data: dict):
        print(f"Data is: {data}")

    data = extract()
    load(data)

dataflow_pipeline()
