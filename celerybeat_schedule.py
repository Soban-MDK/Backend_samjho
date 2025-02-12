from celery.schedules import crontab
from celery_config import celery  # Import Celery instance

celery.conf.beat_schedule = {
    "run_daily_pipeline": {
        "task": "tasks.full_pipeline",  # Use correct task name
        "schedule": crontab(hour=13, minute=0),  # Runs every day at 13:00 IST
    }
}
