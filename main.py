import pandas as pd
import os
import smtplib
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# =========================
# FASTAPI APP
# =========================

app = FastAPI(title="Loan Risk Monitoring API")

# =========================
# CONFIG
# =========================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")

# =========================
# NORMALIZE COLUMN NAMES
# =========================

def normalize_columns(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df

# =========================
# LOAD CSV FILES
# =========================

agreement = normalize_columns(pd.read_csv(os.path.join(BASE_DIR, "agreement_details.csv")))
product = normalize_columns(pd.read_csv(os.path.join(BASE_DIR, "product_details.csv")))
dealer = normalize_columns(pd.read_csv(os.path.join(BASE_DIR, "dealer_details.csv")))
employee = normalize_columns(pd.read_csv(os.path.join(BASE_DIR, "employee_details.csv")))
bounce = normalize_columns(pd.read_csv(os.path.join(BASE_DIR, "bounce_details.csv")))
payment = normalize_columns(pd.read_csv(os.path.join(BASE_DIR, "payment_details.csv")))

print("✓ CSV files loaded")

# =========================
# REQUEST MODEL
# =========================

class AgreementQuery(BaseModel):
    agreement_no: int

# =========================
# SAFE MERGE
# =========================

def safe_merge(left_df, right_df, left_key, right_key):

    if left_key not in left_df.columns or right_key not in right_df.columns:
        return left_df

    return left_df.merge(
        right_df,
        left_on=left_key,
        right_on=right_key,
        how="left"
    )

# =========================
# MASTER API
# =========================

@app.post("/get_master")
def get_master(query: AgreementQuery):

    if "agreement_no" not in agreement.columns:
        return {"error": "agreement_no column missing"}

    a = agreement[agreement["agreement_no"] == query.agreement_no]

    if a.empty:
        return JSONResponse(
            status_code=404,
            content={"error": "Agreement not found"}
        )

    m = a.copy()

    m = safe_merge(m, product, "product_id", "product_id")
    m = safe_merge(m, dealer, "dealer_id", "dealer_id")
    m = safe_merge(m, employee, "employee_id", "employee_id")

    return m.to_dict(orient="records")[0]

# =========================
# BOUNCE API
# =========================

@app.post("/get_bounce")
def get_bounce(query: AgreementQuery):

    if "agreement_no" not in bounce.columns:
        return {"agreement_no": query.agreement_no, "bounce_count": 0}

    count = len(bounce[bounce["agreement_no"] == query.agreement_no])

    return {
        "agreement_no": query.agreement_no,
        "bounce_count": int(count)
    }

# =========================
# DPD API
# =========================

@app.post("/get_dpd")
def get_dpd(query: AgreementQuery):

    if "agreement_no" not in payment.columns:
        return {"agreement_no": query.agreement_no, "dpd": 0}

    p = payment[payment["agreement_no"] == query.agreement_no]

    if p.empty:
        return {"agreement_no": query.agreement_no, "dpd": 0}

    row = p.iloc[0]

    due = pd.to_datetime(row.get("due_
