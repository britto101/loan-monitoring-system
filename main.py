import os
import pandas as pd
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone

app = FastAPI()

# --- 1. CORE RISK LOGIC (STANDALONE FUNCTION) ---
def run_risk_analysis():
    """
    Main logic that can be called by both the Scheduler and the API.
    """
    print(f"\n[{pd.Timestamp.now()}] Starting Risk Analysis...")
    results = []

    # Ensure data is available (assuming 'agreement', 'bounce', 'payment' are globally accessible)
    if agreement.empty:
        print("Skipping: No agreement data found.")
        return 0

    for ag in agreement["agreement_no"]:
        # Calculate Bounces
        b_count = len(bounce[bounce["agreement_no"] == ag]) if not bounce.empty else 0
        
        # Calculate DPD (Days Past Due)
        p = payment[payment["agreement_no"] == ag] if not payment.empty else pd.DataFrame()
        dpd = 0
        if not p.empty:
            row = p.iloc[0]
            due_date = pd.to_datetime(row["due_date"], dayfirst=True, errors='coerce')
            pay_date = pd.to_datetime(row["payment_date"], dayfirst=True, errors='coerce')
            if pd.notnull(due_date) and pd.notnull(pay_date):
                dpd = (pay_date - due_date).days

        # Risk Classification
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

    # File Generation and Emailing
    if results:
        output_path = os.path.join(BASE_DIR, "daily_risk_output.csv")
        df_out = pd.DataFrame(results)
        df_out.to_csv(output_path, index=False)
        
        summary = (f"Daily Risk Report\n"
                   f"Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}\n"
                   f"Total Risky Agreements: {len(results)}\n")
        
        send_via_sendgrid(summary, output_path)
        print(f"Automation Success: Sent report for {len(results)} agreements.")
    else:
        print("No risky agreements found today.")
    
    return len(results)

# --- 2. AUTOMATION SCHEDULER ---
scheduler = BackgroundScheduler()
ist_tz = timezone('Asia/Kolkata')

# Change hour and minute to your preferred IST time (e.g., 10:00 AM)
scheduler.add_job(
    run_risk_analysis, 
    trigger=CronTrigger(hour=10, minute=0, timezone=ist_tz),
    id="daily_risk_job",
    replace_existing=True
)

# Start/Stop Scheduler with FastAPI Lifecycle
@app.on_event("startup")
def startup_event():
    scheduler.start()
    print("Scheduler started: Auto-risk check set for 10:00 AM IST daily.")

@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()

# --- 3. MANUAL ENDPOINT ---
@app.get("/run-risk")
def trigger_risk_manually():
    """Manual trigger if you want to run the analysis instantly via browser."""
    count = run_risk_analysis()
    return {"message": "Manual analysis completed", "risky_count": count}
