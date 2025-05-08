from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Union
import os
import json
import subprocess

app = FastAPI()

TEMP_DIR = "/opt/airflow/temp"
OUTPUT_DIR = "/opt/airflow/output"

class DataPoint(BaseModel):
    dataPoint: str
    value: Union[int, str]

@app.post("/data")
async def receive_data(data_points: List[DataPoint]):
    os.makedirs(TEMP_DIR, exist_ok=True)
    data_file = os.path.join(TEMP_DIR, "incoming_data.json")
    
    with open(data_file, "w") as f:
        json.dump([dp.model_dump() for dp in data_points], f)
    
    # Trigger the DAG using Airflow CLI
    try:
        subprocess.run(["airflow", "dags", "unpause", "dataflow_pipeline"])
        subprocess.run(["airflow", "dags", "trigger", "dataflow_pipeline"])
        return {"message": "Data received and processing started"}
    except subprocess.CalledProcessError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger DAG: {e.stderr}"
        )

@app.get("/download/{file_type}")
async def download_latest_file(file_type: str):
    if file_type not in ["xlsx", "pdf"]:
        raise HTTPException(status_code=400, detail="Invalid file type. Use 'xlsx' or 'pdf'")
    
    files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith(f".{file_type}")]
    
    if not files:
        raise HTTPException(status_code=404, detail=f"No {file_type} files found")
    
    # Get the latest file
    latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(OUTPUT_DIR, x)))
    file_path = os.path.join(OUTPUT_DIR, latest_file)
    
    return FileResponse(file_path, filename=latest_file) 