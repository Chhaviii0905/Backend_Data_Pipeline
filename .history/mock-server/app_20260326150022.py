from flask import Flask, jsonify, request
import json
import os

app = Flask(__name__)

DATA_FILE = os.path.join(os.path.dirname(__file__), "data/customers.json")

def load_customers():
    with open(DATA_FILE, "r") as f:
        return json.load(f)

@app.route("/api/health")
def health():
    return jsonify({"status": "ok"}), 200

@app.route("/api/customers", methods=["GET"])
def get_customers():
    customers = load_customers()
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 10))

    start = (page - 1) * limit
    end = start + limit

    return jsonify({
        "data": customers[start:end],
        "total": len(customers),
        "page": page,
        "limit": limit
    })

@app.route("/api/customers/<customer_id>", methods=["GET"])
def get_customer(customer_id):
    customers = load_customers()
    for c in customers:
        if c["customer_id"] == customer_id:
            return jsonify(c)

    return jsonify({"error": "Customer not found"}), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)