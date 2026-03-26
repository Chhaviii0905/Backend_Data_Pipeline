from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from database import engine, Base, get_db
from models.customer import Customer
import requests
from datetime import datetime
from services.ingestion import ingest_customers

app = FastAPI(title="Customer Ingestion Pipeline")

# Create tables automatically - commented out to let DLT create the table
# Base.metadata.create_all(bind=engine)

FLASK_URL = "http://mock-server:5000/api/customers"

@app.get("/api/health")
def health():
    return {"status": "pipeline running"}

# ----------------- Ingestion -----------------
@app.post("/api/ingest")
def ingest():
    try:
        records_processed, load_info = ingest_customers()
        return {"status": "success", "records_processed": records_processed, "load_info": str(load_info)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ----------------- Customers -----------------
@app.get("/api/customers")
def get_customers(page: int = 1, limit: int = 10, db: Session = Depends(get_db)):
    query = db.query(Customer)
    total = query.count()
    start = (page - 1) * limit
    customers = query.offset(start).limit(limit).all()
    data = [c.__dict__.copy() for c in customers]
    for d in data:
        d.pop("_sa_instance_state", None)
    return {"data": data, "total": total, "page": page, "limit": limit}

@app.get("/api/customers/{customer_id}")
def get_customer(customer_id: str, db: Session = Depends(get_db)):
    customer = db.query(Customer).filter(Customer.customer_id == customer_id).first()
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    data = customer.__dict__.copy()
    data.pop("_sa_instance_state", None)
    return data