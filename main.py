import pandas as pd
import os
import uvicorn
import requests  # Added for SendGrid API
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import base64

# =========================
# FASTAPI APP
# =========================
app = FastAPI(title="Loan Risk Monitoring API")

# =========================
# CONFIG & ENV
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# On Render, set EMAIL_PASS to your SendGrid API Key (starts with SG.)
# Set EMAIL_USER to your verified sender email address in SendGrid
EMAIL_PASS = os.getenv("EMAIL_PASS") 
EMAIL_USER = os.getenv("EMAIL_USER") 
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
    if left_df.empty or right_df.empty: return left_df
    if left_key not in left_df.columns or right_key not in right_df.columns: return left_df
    return left_df.merge(right_df, left_on=left_key, right_on=right_key, how="left")

# =========================
# EMAIL VIA SENDGRID API (Replaces SMTP)
# =========================
def send_via_sendgrid(body, csv_path=None):
    if not EMAIL_PASS or not EMAIL_USER or not EMAIL_TO:
        print("❌ Email configuration missing (EMAIL_PASS/USER/TO)")
        return

    url = "https://api.sendgrid.com/v3/mail/send"
    
    message_data = {
        "personalizations": [{"to": [{"email": EMAIL_TO}]}],
        "from": {"email": EMAIL_USER}, # Must be your SendGrid verified sender
        "subject": f"Daily Risk Alert - {pd.Timestamp.now().strftime('%Y-%m-%d')}",
        "content": [{"type": "text/plain", "value": body}]
    }

    if csv_path and os.path.exists(csv_path):
        with open(csv_path, "rb") as f:
            encoded_file = base64.b64encode(f.read()).decode()
            message_data["attachments"] = [{
                "content": encoded_file,
                "filename": os.path.basename(csv_path),
                "type": "text/csv",
                "disposition": "attachment"
            }]

    headers = {
        "Authorization": f"Bearer {EMAIL_PASS}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, headers=headers, json=message_data)
        if response.status_code in [200, 201, 202]:
            print("✓ Email sent successfully via SendGrid API")
        else:
            print(f"❌ SendGrid Error: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Request Error: {e}")

# =========================
# API ENDPOINTS
# =========================

@app.get("/")
def home():
    return {"service": "Loan Risk Monitoring API", "status": "running"}

@app.get("/run-risk")
def trigger_risk():
    print("\nRunning Risk Analysis...")
    results = []

    if agreement.empty:
        return {"error": "No agreement data available"}

    for ag in agreement["agreement_no"]:
        b_count = len(bounce[bounce["agreement_no"] == ag]) if not bounce.empty else 0
        p = payment[payment["agreement_no"] == ag] if not payment.empty else pd.DataFrame()
        
        dpd = 0
        if not p.empty:
            row = p.iloc[0]
            # Fixed date parsing warning
            due_date = pd.to_datetime(row["due_date"], dayfirst=True, errors='coerce')
            pay_date = pd.to_datetime(row["payment_date"], dayfirst=True, errors='coerce')
            if pd.notnull(due_date) and pd.notnull(pay_date):
                dpd = (pay_date - due_date).days

        if dpd > 10 or b_count >= 2:
            risk = "HIGH RISK" if dpd > 30 else "MEDIUM RISK"
            action = "Legal Notice Triggered" if dpd > 30 else "Reminder Mail Triggered"
            results.append({
                "agreement_no": ag, "dpd": dpd, "bounce_count": b_count,
                "risk_level": risk, "action_taken": action
            })

    output_path = os.path.join(BASE_DIR, "daily_risk_output.csv")
    if results:
        df_out = pd.DataFrame(results)
        df_out.to_csv(output_path, index=False)
        summary = f"Daily Risk Report\nDate: {pd.Timestamp.now()}\nTotal Risky Agreements: {len(results)}\n"
        send_via_sendgrid(summary, output_path)

    return {"message": "Analysis completed", "risky_count": len(results)}

# Add standard API endpoints (get_master, etc) as needed...

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
