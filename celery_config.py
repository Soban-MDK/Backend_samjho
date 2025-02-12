from celery import Celery
from celery.schedules import crontab
import os

# Set the default Django settings module
os.environ.setdefault('CELERY_CONFIG_MODULE', 'celery_config')

# Initialize Celery app
celery = Celery(
    'backend_samjho',
    broker='redis://localhost:6379/1',
    backend='redis://localhost:6379/1',
    include=['tasks']
)

# Configure Celery
celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Kolkata',  # Replace with your timezone
    enable_utc=False,
    task_always_eager=False,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_track_started=True
)

# Add beat schedule directly in the config
celery.conf.beat_schedule = {
    "run_daily_pipeline": {
        "task": "tasks.full_pipeline",
        "schedule": crontab(hour=18, minute=43)
    }
}