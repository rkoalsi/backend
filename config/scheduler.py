# scheduler.py
from datetime import datetime, timedelta
import logging, os, smtplib

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from dotenv import load_dotenv
from email.mime.text import MIMEText

load_dotenv()  # ensure .env is loaded (if needed for the MONGO_URI)

RESET_EMAIL_SENDER = os.getenv("RESET_EMAIL_SENDER")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = os.getenv("SMTP_PORT")
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
# ----------------------------------------------------------------------
# 1) Configure job stores
# ----------------------------------------------------------------------
# APScheduler >= 4.0 has built-in MongoDB support using "type": "mongodb".
jobstores = {
    "default": {
        "type": "mongodb",
        # Here, "host" can be a full MongoDB URI (including credentials, if needed).
        "host": os.getenv("MONGO_URI"),  # e.g. "mongodb://user:pass@mongo:27017"
        "port": 27017,
        "database": os.getenv("DB_NAME"),  # adjust as needed
        "collection": "scheduled_jobs",  # adjust as needed
    }
}

# Configure the scheduler to store jobs in Mongo and operate in UTC.
scheduler = AsyncIOScheduler(jobstores=jobstores)


def _job_event_listener(event):
    if event.exception:
        logging.error(f"Job {event.job_id} raised an exception!")
    else:
        logging.info(f"Job {event.job_id} completed successfully!")


def scheduler_startup():
    logging.info("Starting APScheduler...")
    scheduler.start()
    scheduler.add_listener(_job_event_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    logging.info("Scheduler started.")


def scheduler_shutdown():
    logging.info("Shutting down APScheduler...")
    scheduler.shutdown()


# Helper function to send reset email
def send_overdue_email(obj: dict):
    to = obj.get("to")
    invoice_number = obj.get("invoice_number")
    created_at = obj.get("created_at")
    due_date = obj.get("due_date")
    customer_name = obj.get("customer_name")
    total = obj.get("total")
    balance = obj.get("balance")
    salesperson_name = obj.get("salesperson_name")
    subject = f"Payment Collection Reminder - {invoice_number}"
    body = f"""
    Hi {salesperson_name},

    This is a Payment Collection Reminder. The following Invoice is Overdue:

    Invoice Number: {invoice_number}
    Invoice Created At: {created_at}
    Invoice Due Date: {due_date}
    Customer Name: {customer_name}
    Total Amount: {total}
    Balance Amount: {balance}

    Thanks,
    Pupscribe Team
    """
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = RESET_EMAIL_SENDER
    msg["To"] = to
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(RESET_EMAIL_SENDER, [to], msg.as_string())
    except Exception as e:
        print(f"Error sending email: {e}")
        raise Exception("Failed to send reset email")


def email_salesperson(obj: dict):
    send_overdue_email(obj)


def schedule_job(param: str, delay_seconds: int = 30) -> str:
    """
    Schedule a one-time job to run my_task(param) delay_seconds from *local* system time.
    If your machine is set to IST locally, it uses IST.
    If your server is UTC, it uses UTC there.
    """
    # 2) Use datetime.now() so it's the local machine time
    run_time = datetime.now() + timedelta(seconds=delay_seconds)
    job_id = f"job_{param}_{datetime.now().timestamp()}"
    # 3) (Optional) Add misfire_grace_time if your system might lag
    # misfire_grace_time means "still run the job if it started slightly late."
    scheduler.add_job(
        func=email_salesperson,
        trigger=DateTrigger(run_date=run_time),
        args=[param],
        id=job_id,
        replace_existing=True,
        misfire_grace_time=60,  # e.g. 1 minute grace
    )
    logging.info(
        f"Scheduled job {job_id} at {run_time} (local system time) with param='{param}'"
    )
    return job_id
