import os
from datetime import datetime
import psycopg2
from psycopg2.extras import DictCursor
from flask import current_app
import math

# ANSI color codes
YELLOW = '\033[93m'
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
CYAN = '\033[96m'
ORANGE = '\033[38;5;208m'
RESET = '\033[0m'

# Debug levels
DEBUG_LEVEL = 1  # 1 = critical/important, 2 = normal, 3 = verbose/detailed

# Get the absolute path of the directory containing server.py
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def log_message(color, tag, message, level=2):
    """Log a message with timestamp and PID"""
    # Skip messages with level higher than DEBUG_LEVEL
    if level > DEBUG_LEVEL:
        return
        
    timestamp = datetime.now().strftime("%Y:%m:%d-%H:%M:%S")
    worker_id = os.environ.get('GUNICORN_WORKER_ID', 'MAIN')
    print(f"{color}[{timestamp}] [{tag}-{os.getpid()}] {message}{RESET}", flush=True)

def get_db_connection():
    """Get a database connection"""
    try:
        # Try to get DATABASE_URL from app config first
        DATABASE_URL = current_app.config.get('DATABASE_URL')
        
        # If not in app config, try environment variable
        if not DATABASE_URL:
            DATABASE_URL = os.environ.get('DATABASE_URL')
            
        if not DATABASE_URL:
            log_message(RED, "ERROR", "DATABASE_URL not set in app config or environment", level=1)
            return None
            
        conn = psycopg2.connect(DATABASE_URL)
        conn.cursor_factory = DictCursor
        return conn
    except Exception as e:
        log_message(RED, "ERROR", f"Database connection error: {str(e)}", level=1)
        return None

def get_ring_materials():
    """Get ring materials from CSV file"""
    rm = {}
    try:
        with open('data/ring_materials.csv', 'r') as f:
            next(f)  # Skip header
            for line in f:
                mat, ab, rt, cond, val = line.strip().split(',')
                rm[mat] = {
                    'ring_types': [x.strip() for x in rt.split('/')],
                    'abbreviation': ab,
                    'conditions': cond,
                    'value': val
                }
    except Exception as e:
        log_message(RED, "ERROR", f"Error loading ring materials: {str(e)}", level=3)
    return rm 