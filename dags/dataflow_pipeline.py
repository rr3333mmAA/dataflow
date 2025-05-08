import json
from datetime import datetime
import pandas as pd
from airflow.decorators import dag, task
import os
from fpdf import FPDF

TEMP_DIR = "/opt/airflow/temp"
OUTPUT_DIR = "/opt/airflow/output"

@dag(
    schedule=None
)
def dataflow_pipeline():
    @task()
    def extract():
        os.makedirs(TEMP_DIR, exist_ok=True)
        data_file = os.path.join(TEMP_DIR, "incoming_data.json")
        
        with open(data_file, "r") as f:
            data = json.load(f)
        return data
    
    @task()
    def transform(data: list):
        transformed_data = []
        for item in data:
            entity, category, subcategory = item["dataPoint"].split("-")
            
            value = item["value"]
            value_type = "numeric" if isinstance(value, int) else "text"
            
            transformed_data.append({
                "entity": entity,
                "category": category,
                "subcategory": subcategory,
                "value": value,
                "value_type": value_type
            })
        
        return transformed_data
    
    @task()
    def save_files(data: list):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        df = pd.DataFrame(data)
        
        # Save Excel file
        excel_path = os.path.join(OUTPUT_DIR, f"output_{timestamp}.xlsx")
        df.to_excel(excel_path, index=False)
        
        # Save PDF file
        pdf_path = os.path.join(OUTPUT_DIR, f"output_{timestamp}.pdf")
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=12)
        
        # Add headers
        cols = df.columns
        col_width = pdf.w / len(cols)
        for col in cols:
            pdf.cell(col_width, 10, str(col), 1)
        pdf.ln()
        
        # Add rows
        for _, row in df.iterrows():
            for item in row:
                pdf.cell(col_width, 10, str(item), 1)
            pdf.ln()
        
        pdf.output(pdf_path)
        
        return {"excel": excel_path, "pdf": pdf_path}

    data = extract()
    transformed_data = transform(data)
    save_files(transformed_data)

dataflow_pipeline()
