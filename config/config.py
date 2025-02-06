import os
from dotenv import load_dotenv

load_dotenv()

# Remote Database
REMOTE_DB_HOST = os.getenv("REMOTE_DB_HOST")
REMOTE_DB_PORT = os.getenv("REMOTE_DB_PORT")
REMOTE_DB_USER = os.getenv("REMOTE_DB_USER")
REMOTE_DB_NAME = os.getenv("REMOTE_DB_NAME")
REMOTE_DB_PASS = os.getenv("REMOTE_DB_PASSWORD")

# Local Database
LOCAL_DB_HOST = os.getenv("LOCAL_DB_HOST")
LOCAL_DB_PORT = os.getenv("LOCAL_DB_PORT")
LOCAL_DB_USER = os.getenv("LOCAL_DB_USER")
LOCAL_DB_NAME = os.getenv("LOCAL_DB_NAME")
LOCAL_DB_PASS = os.getenv("LOCAL_DB_PASSWORD")