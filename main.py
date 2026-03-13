import os
import pandas as pd
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone

app = FastAPI()

# Base directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load Data
agreement = pd.read_csv("agreement.csv")
bounce = pd.read_csv("bounce.csv")
payment = pd.read_csv("payment.csv")

# Dummy email function
def send_via_sendgrid(summary, file):
    print("Email sent with report:", file)


# --- RISK ANALYSIS FUNCTION ---
def run_risk_analysis():

    print(f"\n[{pd.Timestamp.now()}] Starting Risk Analysis...")

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

            due_date = pd.to_datetime(row["due_date"], dayfirst=True, errors='coerce')
            pay_date = pd.to_datetime(row["payment_date"], dayfirst=True, errors='coerce')

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

        output_path = os.path.join(BASE_DIR, "daily_risk_output.csv")

        df_out = pd.DataFrame(results)

        df_out.to_csv(output_path, index=False)

        summary = f"""
Daily Risk Report
Date: {pd.Timestamp.now()}
Total Risky Agreements: {len(results)}
"""

        send_via_sendgrid(summary, output_path)

        print("Report generated.")

    else:
        print("No risky agreements found.")

    return len(results)


# --- SCHEDULER ---

ist_tz = timezone("Asia/Kolkata")

scheduler = BackgroundScheduler(timezone=ist_tz)

scheduler.add_job(
    run_risk_analysis,
    CronTrigger(hour=10, minute=0),
    id="daily_risk_job",
    replace_existing=True
)


@app.on_event("startup")
def startup():

    scheduler.start()

    print("Scheduler started. Running daily at 10:00 AM IST.")


@app.on_event("shutdown")
def shutdown():

    scheduler.shutdown()


# --- MANUAL ENDPOINT ---

@app.get("/run-risk")
def trigger_risk():

    count = run_risk_analysis()

    return {
        "message": "Manual analysis completed",
        "risky_count": count
    }
