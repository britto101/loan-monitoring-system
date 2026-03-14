import pandas as pd
import os
import base64
from datetime import datetime
from pytz import timezone
from fastapi import FastAPI, Request, BackgroundTasks
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition

app = FastAPI(title="Loan Risk Monitor")

# CONFIG
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LAST_RUN_FILE = os.path.join(BASE_DIR, "last_run.txt")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_TO = os.getenv("EMAIL_TO")

def should_run_now():
    ist = timezone("Asia/Kolkata")
    now = datetime.now(ist)
    if now.hour == 11 and 0 <= now.minute <= 20:
        today = now.strftime("%Y-%m-%d")
        if os.path.exists(LAST_RUN_FILE):
            with open(LAST_RUN_FILE, "r") as f:
                if f.read().strip() == today:
                    return False
        with open(LAST_RUN_FILE, "w") as f:
            f.write(today)
        return True
    return False

def run_risk_analysis():
    """Logic remains the same, but added better error logging."""
    try:
        agreement = pd.read_csv(os.path.join(BASE_DIR, "agreement_details.csv"))
        bounce = pd.read_csv(os.path.join(BASE_DIR, "bounce_details.csv"))
        payment = pd.read_csv(os.path.join(BASE_DIR, "payment_details.csv"))
        
        # Clean column names
        for df_obj in [agreement, bounce, payment]:
            df_obj.columns = df_obj.columns.str.strip().str.replace('﻿', '')

        results = []
        for ag in agreement["agreement_no"]:
            b_count = len(bounce[bounce["agreement_no"] == ag])
            p = payment[payment["agreement_no"] == ag]
            dpd = 0
            if not p.empty:
                due = pd.to_datetime(p.iloc[0]["due_date"], dayfirst=True)
                paid = pd.to_datetime(p.iloc[0]["payment_date"], dayfirst=True)
                dpd = (paid - due).days
            if dpd > 10 or b_count >= 2:
                results.append({"agreement_no": ag, "DPD": dpd, "Bounce": b_count, "Risk": "HIGH" if dpd > 30 else "MEDIUM"})

        if results:
            df = pd.DataFrame(results)
            csv_path = os.path.join(BASE_DIR, "daily_report.csv")
            df.to_csv(csv_path, index=False)
            
            message = Mail(
                from_email=EMAIL_USER,
                to_emails=EMAIL_TO,
                subject=f"Loan Risk Report: {datetime.now(timezone('Asia/Kolkata')).strftime('%d-%m-%Y')}",
                plain_text_content=f"Automation successful. Found {len(results)} risky accounts."
            )
            with open(csv_path, "rb") as f:
                encoded = base64.b64encode(f.read()).decode()
                message.attachment = Attachment(FileContent(encoded), FileName("risk_report.csv"), FileType("text/csv"), Disposition("attachment"))
            
            sg = SendGridAPIClient(SENDGRID_API_KEY)
            response = sg.send(message)
            print(f"SUCCESS: Email sent. Status Code: {response.status_code}")
        else:
            print("INFO: No risky accounts found today.")
    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")

@app.api_route("/", methods=["GET", "HEAD"])
async def health_check(background_tasks: BackgroundTasks):
    """
    UptimeRobot hits this. 
    If it's time to run, we hand the email task to the 'background' 
    so it finishes even if UptimeRobot closes the connection.
    """
    if should_run_now():
        background_tasks.add_task(run_risk_analysis)
        return {"status": "task_queued"}
    return {"status": "monitoring"}

@app.get("/force-trigger")
async def force_trigger(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_risk_analysis)
    return {"status": "manual_force_queued"}
