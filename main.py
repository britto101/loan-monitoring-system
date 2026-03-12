import pandas as pd
import os
import smtplib
import uvicorn
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
# CONFIG & ENV
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")

# =========================
# ROBUST DATA LOADER
# =========================
def load_and_clean_csv(file_name):
    path = os.path.join(BASE_DIR, file_name)
    if not os.path.exists(path):
        print(f"⚠️ Warning: {file_name} not found!")
        return pd.DataFrame()
    
    df = pd.read_csv(path)
    # Clean column names: remove spaces and convert to lowercase
    df.columns = df.columns.str.strip().str.lower()
    return df

# Initial Load
agreement = load_and_clean_csv("agreement_details.csv")
product = load_and_clean_csv("product_details.csv")
dealer = load_and_clean_csv("dealer_details.csv")
employee = load_and_clean_csv("employee_details.csv")
bounce = load_and_clean_csv("bounce_details.csv")
payment = load_and_clean_csv("payment_details.csv")

print("✓ CSV files loaded and columns cleaned")

# =========================
# REQUEST MODEL
# =========================
class AgreementQuery(BaseModel):
    agreement_no: int

# =========================
# SAFE MERGE
# =========================
def safe_merge(left_df, right_df, left_key, right_key):
    if left_df.empty or right_df.empty:
        return left_df
    if left_key not in left_df.columns or right_key not in right_df.columns:
        return left_df

    return left_df.merge(right_df, left_on=left_key, right_on=right_key, how="left")

# =========================
# API ENDPOINTS
# =========================

@app.get("/")
def home():
    return {"service": "Loan Risk Monitoring API", "status": "running"}

@app.post("/get_master")
def get_master(query: AgreementQuery):
    a = agreement[agreement["agreement_no"] == query.agreement_no]
    if a.empty:
        return JSONResponse(status_code=404, content={"error": "Agreement not found"})

    m = a.copy()
    m = safe_merge(m, product, "product_id", "product_id")
    m = safe_merge(m, dealer, "dealer_id", "dealer_id")
    m = safe_merge(m, employee, "employee_id", "employee_id")
    return m.to_dict(orient="records")[0]

@app.post("/get_bounce")
def get_bounce(query: AgreementQuery):
    count = len(bounce[bounce["agreement_no"] == query.agreement_no]) if not bounce.empty else 0
    return {"agreement_no": query.agreement_no, "bounce_count": int(count)}

@app.post("/get_dpd")
def get_dpd(query: AgreementQuery):
    p = payment[payment["agreement_no"] == query.agreement_no]
    if p.empty:
        return {"agreement_no": query.agreement_no, "dpd": 0}

    row = p.iloc[0]
    due = pd.to_datetime(row["due_date"])
    paid = pd.to_datetime(row["payment_date"])
    dpd = (paid - due).days
    return {"agreement_no": query.agreement_no, "dpd": int(dpd)}

# =========================
# RISK ENGINE & EMAIL
# =========================

def send_via_gmail(body, csv_path=None):
    if not EMAIL_USER or not EMAIL_PASS or not EMAIL_TO:
        print("Email credentials not configured")
        return

    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_TO
    msg["Subject"] = f"Daily Risk Alert - {pd.Timestamp.now().strftime('%Y-%m-%d')}"
    msg.attach(MIMEText(body, "plain"))

    if csv_path and os.path.exists(csv_path):
        with open(csv_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(csv_path)}")
            msg.attach(part)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)
        print("✓ Email sent")
    except Exception as e:
        print("Email error:", e)

@app.get("/run-risk")
def trigger_risk():
    print("\nRunning Risk Analysis...")
    results = []

    if agreement.empty:
        return {"error": "No agreement data available"}

    for ag in agreement["agreement_no"]:
        # Get bounce count safely
        b_count = len(bounce[bounce["agreement_no"] == ag]) if not bounce.empty else 0
        
        # Get DPD safely
        p = payment[payment["agreement_no"] == ag] if not payment.empty else pd.DataFrame()
        dpd = 0
        if not p.empty:
            row = p.iloc[0]
            dpd = (pd.to_datetime(row["payment_date"]) - pd.to_datetime(row["due_date"])).days

        # Risk Logic
        if dpd > 10 or b_count >= 2:
            risk = "HIGH RISK" if dpd > 30 else "MEDIUM RISK"
            action = "Legal Notice Triggered" if dpd > 30 else "Reminder Mail Triggered"
            
            results.append({
                "agreement_no": ag,
                "dpd": dpd,
                "bounce_count": b_count,
                "risk_level": risk,
                "action_taken": action
            })

    output_path = os.path.join(BASE_DIR, "daily_risk_output.csv")
    if results:
        df_out = pd.DataFrame(results)
        df_out.to_csv(output_path, index=False)
        
        summary = f"Daily Risk Report\nDate: {pd.Timestamp.now()}\nTotal Risky Agreements: {len(results)}\n"
        send_via_gmail(summary, output_path)

    return {"message": "Analysis completed", "risky_count": len(results)}

# =========================
# RUNNER (For Local/Render)
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
