import os
import pandas as pd
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition
import base64

app = FastAPI()

# -----------------------------
# Environment Variables
# -----------------------------

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
EMAIL_FROM = os.getenv("EMAIL_USER")
EMAIL_TO = os.getenv("EMAIL_TO")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# -----------------------------
# Load CSV Files
# -----------------------------

def load_data():
    try:
        agreement = pd.read_csv("agreement.csv")
    except:
        agreement = pd.DataFrame()

    try:
        bounce = pd.read_csv("bounce.csv")
    except:
        bounce = pd.DataFrame()

    try:
        payment = pd.read_csv("payment.csv")
    except:
        payment = pd.DataFrame()

    return agreement, bounce, payment


# -----------------------------
# Send Email using SendGrid
# -----------------------------

def send_via_sendgrid(summary, file_path):

    with open(file_path, "rb") as f:
        data = f.read()

    encoded = base64.b64encode(data).decode()

    attachment = Attachment(
        FileContent(encoded),
        FileName("daily_risk_report.csv"),
        FileType("text/csv"),
        Disposition("attachment")
    )

    message = Mail(
        from_email=EMAIL_FROM,
        to_emails=EMAIL_TO,
        subject="Daily Risk Analysis Report",
        plain_text_content=summary
    )

    message.attachment = attachment

    sg = SendGridAPIClient(SENDGRID_API_KEY)
    sg.send(message)

    print("Email sent successfully")


# -----------------------------
# Risk Analysis Logic
# -----------------------------

def run_risk_analysis():

    print(f"\n[{pd.Timestamp.now()}] Starting Risk Analysis...")

    agreement, bounce, payment = load_data()

    results = []

    if agreement.empty:
        print("No agreement data found.")
        return 0

    for ag in agreement["agreement_no"]:

        b_count = len(bounce[bounce["agreement_no"] == ag]) if not bounce.empty else 0

        p = payment[payment["agreement_no"] == ag] if not payment.empty else pd.DataFrame()

        dpd = 0

        if not p.empty:

            row = p.iloc[0]

            due_date = pd.to_datetime(row["due_date"], errors="coerce")
            pay_date = pd.to_datetime(row["payment_date"], errors="coerce")

            if pd.notnull(due_date) and pd.notnull(pay_date):
                dpd = (pay_date - due_date).days

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

    if results:

        output_file = os.path.join(BASE_DIR, "daily_risk_output.csv")

        df = pd.DataFrame(results)
        df.to_csv(output_file, index=False)

        summary = f"""
Daily Risk Report

Date: {pd.Timestamp.now()}

Total Risky Agreements: {len(results)}
"""

        send_via_sendgrid(summary, output_file)

        print(f"Report sent for {len(results)} risky agreements")

    else:
        print("No risky agreements found")

    return len(results)


# -----------------------------
# Scheduler Setup
# -----------------------------

scheduler = BackgroundScheduler(timezone="Asia/Kolkata")

scheduler.add_job(
    run_risk_analysis,
    CronTrigger(hour=10, minute=0),
    id="daily_risk_job",
    replace_existing=True
)

# -----------------------------
# FastAPI Lifecycle
# -----------------------------

@app.on_event("startup")
def start_scheduler():
    scheduler.start()
    print("Scheduler started → Mail will trigger daily at 10 AM IST")


@app.on_event("shutdown")
def shutdown_scheduler():
    scheduler.shutdown()


# -----------------------------
# API Endpoints
# -----------------------------

@app.get("/")
def home():
    return {"message": "Risk Automation Server Running"}


@app.get("/run-risk")
def trigger_risk_manually():
    count = run_risk_analysis()
    return {
        "message": "Manual analysis completed",
        "risky_count": count
    }
