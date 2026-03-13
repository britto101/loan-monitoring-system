import pandas as pd
import os
import base64
from datetime import datetime
from pytz import timezone
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# SENDGRID IMPORT
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition

# =========================
# FASTAPI APP
# =========================

app = FastAPI(title="Loan Risk Monitoring API")

# =========================
# CONFIG & PATHS
# =========================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LAST_RUN_FILE = os.path.join(BASE_DIR, "last_run.txt")

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_TO = os.getenv("EMAIL_TO")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")

# =========================
# LOAD CSV FILES
# =========================

try:
    agreement = pd.read_csv(os.path.join(BASE_DIR, "agreement_details.csv"))
    product = pd.read_csv(os.path.join(BASE_DIR, "product_details.csv"))
    dealer = pd.read_csv(os.path.join(BASE_DIR, "dealer_details.csv"))
    employee = pd.read_csv(os.path.join(BASE_DIR, "employee_details.csv"))
    bounce = pd.read_csv(os.path.join(BASE_DIR, "bounce_details.csv"))
    payment = pd.read_csv(os.path.join(BASE_DIR, "payment_details.csv"))

    # Normalize column names
    for df in [agreement, product, dealer, employee, bounce, payment]:
        df.columns = df.columns.str.lower().str.strip()
    print("✓ CSV files loaded")
except Exception as e:
    print(f"Error loading CSVs: {e}")

# =========================
# REQUEST MODEL & HELPERS
# =========================

class AgreementQuery(BaseModel):
    agreement_no: int

def safe_merge(left_df, right_df, left_key, right_key):
    if left_key not in left_df.columns or right_key not in right_df.columns:
        return left_df
    return left_df.merge(right_df, left_on=left_key, right_on=right_key, how="left")

# =========================
# CORE LOGIC: SHOULD JOB RUN?
# =========================

def should_run_job():
    """
    Checks if the current time is within the window (08:00 - 08:05)
    and verifies if it has already run today.
    """
    ist = timezone("Asia/Kolkata")
    now = datetime.now(ist)
    # Target Window: 08:00 AM to 08:05 AM IST
    if now.hour == 8 and 0 <= now.minute <= 5:
        today = now.strftime("%Y-%m-%d")

        # Check if we already succeeded today
        if os.path.exists(LAST_RUN_FILE):
            with open(LAST_RUN_FILE, "r") as f:
                last_date = f.read().strip()
                if last_date == today:
                    return False # Already ran today

        # Write today's date to file to mark success
        with open(LAST_RUN_FILE, "w") as f:
            f.write(today)
        return True

    return False

# =========================
# EMAIL & RISK ENGINE
# =========================

def send_via_sendgrid(body, csv_path=None):
    if not SENDGRID_API_KEY:
        print("SendGrid API key not configured")
        return

    message = Mail(
        from_email=EMAIL_USER,
        to_emails=EMAIL_TO,
        subject="Daily Risk Alert (Automated)",
        plain_text_content=body
    )

    if csv_path and os.path.exists(csv_path):
        with open(csv_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode()
            attachment = Attachment(
                FileContent(encoded),
                FileName(os.path.basename(csv_path)),
                FileType("text/csv"),
                Disposition("attachment")
            )
            message.attachment = attachment

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        sg.send(message)
        print("✓ Email sent via SendGrid")
    except Exception as e:
        print("SendGrid error:", e)

def run_risk_analysis():
    print("\nRunning Risk Analysis...")
    results = []

    for ag in agreement["agreement_no"]:
        bounce_count = len(bounce[bounce["agreement_no"] == ag])
        p = payment[payment["agreement_no"] == ag]
        dpd = 0

        if not p.empty:
            row = p.iloc[0]
            due = pd.to_datetime(row["due_date"])
            paid = pd.to_datetime(row["payment_date"])
            dpd = (paid - due).days

        if dpd > 10 or bounce_count >= 2:
            risk = "HIGH RISK" if dpd > 30 else "MEDIUM RISK"
            action = "Legal Notice Triggered" if dpd > 30 else "Reminder Mail Triggered"
            results.append({
                "agreement_no": ag,
                "DPD": dpd,
                "Bounce": bounce_count,
                "Risk": risk,
                "Action": action
            })

    output_path = os.path.join(BASE_DIR, "daily_risk_output.csv")
    if results:
        df = pd.DataFrame(results)
        df.to_csv(output_path, index=False)
        body = f"Daily Risk Report\nDate: {pd.Timestamp.now()}\nTotal Risky Agreements: {len(results)}"
        send_via_sendgrid(body, output_path)
    
    return len(results)

# =========================
# ENDPOINTS
# =========================

@app.get("/run-risk")
def trigger_risk():
    """
    This endpoint is called by UptimeRobot every 5 mins.
    It checks the time and runs the analysis only in the target window.
    """
    if should_run_job():
        count = run_risk_analysis()
        return {
            "status": "success",
            "message": "Risk analysis executed and email sent.",
            "risky_agreements": count
        }

    return {
        "status": "idle",
        "message": "Not within scheduling window or already completed for today."
    }

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

@app.get("/")
def home():
    return {"service": "Loan Risk Monitoring API", "status": "running"}

