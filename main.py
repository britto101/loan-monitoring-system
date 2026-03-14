import pandas as pd
import os
import base64
from datetime import datetime
from pytz import timezone
from fastapi import FastAPI, Request
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition

app = FastAPI(title="Loan Risk Monitor")

# CONFIG - Environment Variables
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LAST_RUN_FILE = os.path.join(BASE_DIR, "last_run.txt")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_TO = os.getenv("EMAIL_TO")

def should_run_now():
    """Checks if it is the 11:00 AM window and if we haven't run today."""
    ist = timezone("Asia/Kolkata")
    now = datetime.now(ist)
    
    # Target Window: 11:00 AM to 11:20 AM IST
    if now.hour == 11 and 0 <= now.minute <= 20:
        today = now.strftime("%Y-%m-%d")
        
        if os.path.exists(LAST_RUN_FILE):
            try:
                with open(LAST_RUN_FILE, "r") as f:
                    if f.read().strip() == today:
                        return False 
            except:
                pass
        
        with open(LAST_RUN_FILE, "w") as f:
            f.write(today)
        return True
    return False

def run_risk_analysis():
    """Core logic to process CSVs and send email."""
    try:
        agreement = pd.read_csv(os.path.join(BASE_DIR, "agreement_details.csv"))
        bounce = pd.read_csv(os.path.join(BASE_DIR, "bounce_details.csv"))
        payment = pd.read_csv(os.path.join(BASE_DIR, "payment_details.csv"))
        
        results = []
        for ag in agreement["agreement_no"]:
            b_count = len(bounce[bounce["agreement_no"] == ag])
            p = payment[payment["agreement_no"] == ag]
            dpd = 0
            
            if not p.empty:
                due = pd.to_datetime(p.iloc[0]["due_date"])
                paid = pd.to_datetime(p.iloc[0]["payment_date"])
                dpd = (paid - due).days

            if dpd > 10 or b_count >= 2:
                results.append({
                    "agreement_no": ag, 
                    "DPD": dpd, 
                    "Bounce": b_count, 
                    "Risk": "HIGH" if dpd > 30 else "MEDIUM"
                })

        if not results:
            print("No risky agreements found.")
            return 0

        df = pd.DataFrame(results)
        csv_path = os.path.join(BASE_DIR, "daily_report.csv")
        df.to_csv(csv_path, index=False)

        message = Mail(
            from_email=EMAIL_USER,
            to_emails=EMAIL_TO,
            subject=f"Loan Risk Report: {datetime.now(timezone('Asia/Kolkata')).strftime('%d-%m-%Y')}",
            plain_text_content=f"Found {len(results)} risky accounts."
        )

        with open(csv_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode()
            message.attachment = Attachment(
                FileContent(encoded),
                FileName("risk_report.csv"),
                FileType("text/csv"),
                Disposition("attachment")
            )
        
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        sg.send(message)
        return len(results)
    except Exception as e:
        print(f"Error: {e}")
        return 0

# --- ROUTES ---

@app.api_route("/", methods=["GET", "HEAD"])
def health_check(request: Request):
    """
    Automatic trigger for UptimeRobot. 
    Supports HEAD and GET. Only runs at 11:00 AM IST.
    """
    if should_run_now():
        count = run_risk_analysis()
        return {"status": "executed", "records_found": count}
    
    return {"status": "monitoring", "message": "App is awake. Waiting for 11:00 AM IST."}

@app.get("/force-trigger")
def force_trigger():
    """
    Manual trigger. Bypass clock and last_run file.
    Use this to test or send report immediately.
    """
    count = run_risk_analysis()
    return {
        "status": "manual_execution_success", 
        "records_sent": count,
        "message": "The email has been triggered regardless of the time."
    }
