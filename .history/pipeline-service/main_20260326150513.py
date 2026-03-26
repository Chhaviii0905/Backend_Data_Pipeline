from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import engine, Base, get_db
from models.customer import Customer
import requests
from datetime import datetime
from services.ingestion import ingest_customers

app = FastAPI(title="Customer Ingestion Pipeline")

#Create tables automatically
FLASK_URL = "http://mock-server:5000/api/customers"

@app.get("/api/health")
def health():
    return {"status": "pipeline running"}

#Ingestion
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
    try:
        # Use raw SQL to avoid model issues
        result = db.execute(text("SELECT COUNT(*) FROM customers")).scalar()
        total = result if result else 0
        
        start = (page - 1) * limit
        customers = db.execute(text(f"SELECT customer_id, first_name, last_name, email, phone, address, date_of_birth, account_balance, created_at FROM customers ORDER BY customer_id LIMIT {limit} OFFSET {start}")).fetchall()
        
        data = []
        for c in customers:
            data.append({
                "customer_id": c[0],
                "first_name": c[1],
                "last_name": c[2],
                "email": c[3],
                "phone": c[4],
                "address": c[5],
                "date_of_birth": str(c[6]) if c[6] else None,
                "account_balance": float(c[7]) if c[7] else None,
                "created_at": str(c[8]) if c[8] else None
            })
        
        return {"data": data, "total": total, "page": page, "limit": limit}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/customers/{customer_id}")
def get_customer(customer_id: str, db: Session = Depends(get_db)):
    try:
        customer = db.execute(text("SELECT customer_id, first_name, last_name, email, phone, address, date_of_birth, account_balance, created_at FROM customers WHERE customer_id = :id"), {"id": customer_id}).fetchone()
        if not customer:
            raise HTTPException(status_code=404, detail="Customer not found")
        
        data = {
            "customer_id": customer[0],
            "first_name": customer[1],
            "last_name": customer[2],
            "email": customer[3],
            "phone": customer[4],
            "address": customer[5],
            "date_of_birth": str(customer[6]) if customer[6] else None,
            "account_balance": float(customer[7]) if customer[7] else None,
            "created_at": str(customer[8]) if customer[8] else None
        }
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))