# scheduler.py
from datetime import datetime
import logging, os, smtplib

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from dotenv import load_dotenv
from .whatsapp import send_whatsapp

load_dotenv()  # ensure .env is loaded

RESET_EMAIL_SENDER = os.getenv("RESET_EMAIL_SENDER")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

# Job stores configuration remains the same
jobstores = {
    "default": {
        "type": "mongodb",
        "host": os.getenv("MONGO_URI"),
        "port": 27017,
        "database": os.getenv("DB_NAME"),
        "collection": "scheduled_jobs",
    }
}

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


# Helper function to send overdue msg
def send_overdue_msg(obj: dict):
    to = obj.get("to")
    invoice_number = obj.get("invoice_number")
    created_at = obj.get("created_at")
    due_date = obj.get("due_date")
    customer_name = obj.get("customer_name")
    total = obj.get("total")
    balance = obj.get("balance")
    salesperson_name = obj.get("salesperson_name")
    email_type = obj.get("type")
    template_doc = {
        "name": (
            "payment_reminder"
            if email_type == "one_week_before"
            else "payment_reminder_due"
        ),
        "language": "en_US",
    }
    params = {
        "name": salesperson_name,
        "invoice_number": invoice_number,
        "invoice_date": created_at,
        "invoice_due_date": due_date,
        "customer_name": customer_name,
        "amount": total,
        "balance": balance,
    }
    try:
        send_whatsapp(to, template_doc, params)
    except Exception as e:
        logging.error(f"Error sending msg to {to}: {e}")
        raise Exception("Failed to send overdue msg")


def notify_salesperson(obj: dict):
    send_overdue_msg(obj)


def schedule_job(email_params: dict, run_date: datetime, job_suffix: str) -> str:
    """
    Schedule a one-time job to send an email at a specific run_date.

    :param email_params: Dictionary containing email details.
    :param run_date: The datetime when the job should run.
    :param job_suffix: Suffix to identify the type of job (e.g., 'one_week_before', 'due_date').
    :return: The job ID.
    """
    invoice_id = email_params.get("invoice_id")
    job_id = f"job_{invoice_id}_{job_suffix}"

    # Remove existing job with the same ID to prevent duplicates
    (
        scheduler.remove_job(job_id, jobstore="default")
        if scheduler.get_job(job_id)
        else None
    )

    scheduler.add_job(
        func=notify_salesperson,
        trigger=DateTrigger(run_date=run_date),
        args=[email_params],
        id=job_id,
        replace_existing=True,
        misfire_grace_time=60,  # 1 minute grace
    )
    logging.info(
        f"Scheduled job {job_id} at {run_date} (UTC) with params='{email_params}'"
    )
    return job_id


def remove_scheduled_jobs(invoice_id: str):
    """
    Remove all scheduled jobs related to a specific invoice.

    :param invoice_id: The ID of the invoice.
    """
    for job in scheduler.get_jobs(jobstore="default"):
        if job.id.startswith(f"job_{invoice_id}_"):
            scheduler.remove_job(job.id, jobstore="default")
            logging.info(f"Removed scheduled job {job.id}")
