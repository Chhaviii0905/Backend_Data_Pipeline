# pipeline-service/services/ingestion.py
import requests
import dlt
from dlt.destinations import postgres
import os
from datetime import datetime

FLASK_URL = os.environ.get("FLASK_URL", "http://mock-server:5000/api/customers")

def fetch_customers():
    """Fetch all customers from Flask API, handling pagination"""
    customers = []
    page = 1
    limit = 10

    while True:
        resp = requests.get(FLASK_URL, params={"page": page, "limit": limit})
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            break
        # Convert date strings to datetime objects
        for c in data:
            c["date_of_birth"] = datetime.strptime(c["date_of_birth"], "%Y-%m-%d").date()
            c["created_at"] = datetime.strptime(c["created_at"], "%Y-%m-%dT%H:%M:%S")
        customers.extend(data)
        page += 1

    return customers

def ingest_customers():
    """Ingest customers using dlt"""
    #Get database URL
    db_url = os.environ.get("DATABASE_URL", "postgresql://postgres:Chhavi@postgres:5432/customer_db")

    #Create dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="customer_pipeline",
        destination=postgres(db_url),
        dataset_name="public"
    )

    customers = fetch_customers()
    print(f"Fetched {len(customers)} customers")

    #Load data
    load_info = pipeline.run(
        customers,
        table_name="customers",
        write_disposition="merge",
        primary_key="customer_id"
    )
    print(f"Load info: {load_info}")

    return len(customers), load_info