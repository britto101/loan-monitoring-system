import pandas as pd
import os
import base64
from datetime import datetime
from pytz import timezone
from fastapi import FastAPI
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition

app = FastAPI(title="Loan Risk Monitor")

# CONFIG - Render uses environment variables for security
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LAST_RUN_FILE = os.path.join(BASE_DIR, "last_run.txt")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_TO = os.getenv("EMAIL_TO")

def should_run_now():
    """Checks if it is the 8:00 AM IST window and if we haven't run today."""
    ist = timezone("Asia/Kolkata")
    now = datetime.now(ist)
    
    # Target: 08:00 AM to 08:15 AM IST
    # We allow a 15-min window in case Render takes a moment to wake up
    if now.hour == 8 and 0 <= now.minute <= 15:
        today = now.strftime("%Y-%m-%d")
        
        # Check if the file exists and has today's date
        if os.path.exists(LAST_RUN_FILE):
            with open(LAST_RUN_FILE, "r") as f:
                if f.read().strip() == today:
                    return False # Already sent today
        
        # Mark as run by writing today's date
        with open(LAST_RUN_FILE, "w") as f:
            f.write(today)
        return True
    return False

def run_risk_analysis():
    """The core logic to process CSVs and send the email."""
    try:
        # Load the CSV files from the repository folder
        agreement = pd.read_csv(os.path.join(BASE_DIR, "agreement_details.csv"))
        bounce = pd.read_csv(os.path.join(BASE_DIR, "bounce_details.csv"))
        payment = pd.read_csv(os.path.join(BASE_DIR, "payment_details.csv"))
        
        results = []
        for ag in agreement["agreement_no"]:
            b_count = len(bounce[bounce["agreement_no"] == ag])
            p = payment[payment["agreement_no"] == ag]
            dpd = 0
            
            if not p.empty:
                # Calculate Days Past Due
                due = pd.to_datetime(p.iloc[0]["due_date"])
                paid = pd.to_datetime(p.iloc[0]["payment_date"])
                dpd = (paid - due).days

            # Logic: 10+ days late OR 2+ bounces
            if dpd > 10 or b_count >= 2:
                results.append({
                    "agreement_no": ag, 
                    "DPD": dpd, 
                    "Bounce": b_count, 
                    "Risk": "HIGH" if dpd > 30 else "MEDIUM"
                })

        if not results:
            return 0

        # Create the report CSV
        df = pd.DataFrame(results)
        csv_path = os.path.join(BASE_DIR, "daily_report.csv")
        df.to_csv(csv_path, index=False)

        # Prepare SendGrid Email
        message = Mail(
            from_email=EMAIL_USER,
            to_emails=EMAIL_TO,
            subject=f"Risk Report - {datetime.now().strftime('%Y-%m-%d')}",
            plain_text_content=f"Analysis complete. Found {len(results)} risky agreements."
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
        print(f"Error during analysis: {e}")
        return 0

@app.get("/")
def health_check():
    """
    UptimeRobot hits this. 
    It keeps the app awake and triggers the logic if the time is right.
    """
    if should_run_now():
        count = run_risk_analysis()
        return {"status": "success", "message": "Risk report sent", "count": count}
    
    return {"status": "active", "message": "Monitoring... Waiting for 08:00 AM IST"}
