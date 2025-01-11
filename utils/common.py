import os
from datetime import datetime
import psycopg2
from psycopg2.extras import DictCursor
from flask import current_app

# ANSI color codes
YELLOW = '\033[93m'
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
CYAN = '\033[96m'
ORANGE = '\033[38;5;208m'
RESET = '\033[0m'

def log_message(color, tag, message):
    """Log a message with timestamp and PID"""
    timestamp = datetime.now().strftime("%Y:%m:%d-%H:%M:%S")
    worker_id = os.environ.get('GUNICORN_WORKER_ID', 'MAIN')
    print(f"{color}[{timestamp}] [{tag}-{os.getpid()}] {message}{RESET}", flush=True)

def get_db_connection():
    """Get a database connection"""
    try:
        # Get DATABASE_URL from current app config
        DATABASE_URL = current_app.config.get('DATABASE_URL')
        if not DATABASE_URL:
            log_message(RED, "ERROR", "DATABASE_URL not set in app config")
            return None
            
        conn = psycopg2.connect(DATABASE_URL)
        conn.cursor_factory = DictCursor
        return conn
    except Exception as e:
        log_message(RED, "ERROR", f"Database connection error: {str(e)}")
        return None 