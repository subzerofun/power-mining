import os
import sys
import json
import time
import signal
import psutil
import zmq
import subprocess
import threading
from datetime import datetime
import tempfile

# ANSI color codes
YELLOW = '\033[93m'
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
ORANGE = '\033[38;5;208m'
RESET = '\033[0m'

# Constants
STATUS_RECEIVE_PORT = 5557  # Port to receive updates from update_live_web.py
STATUS_PUBLISH_PORT = 5558  # Port to publish status to web workers
PID_FILE = os.path.join(tempfile.gettempdir(), 'update_live_web.pid')
DAEMON_PID_FILE = os.path.join(tempfile.gettempdir(), 'update_daemon.pid')

# Global state
updater_process = None
current_status = {"state": "offline", "last_db_update": None, "daemon_pid": os.getpid()}
running = True

def log_message(color, tag, message):
    """Log a message with timestamp and PID"""
    timestamp = datetime.now().strftime("%Y:%m:%d-%H:%M:%S")
    print(f"{color}[{timestamp}] [DAEMON-{os.getpid()}] [{tag}] {message}{RESET}", flush=True)

def write_daemon_pid():
    """Write daemon PID to file"""
    try:
        with open(DAEMON_PID_FILE, 'w') as f:
            f.write(str(os.getpid()))
    except Exception as e:
        log_message(RED, "ERROR", f"Failed to write daemon PID file: {e}")

def kill_updater_process():
    """Kill the update_live_web.py process if running"""
    global updater_process
    if updater_process:
        try:
            p = psutil.Process(updater_process.pid)
            if "update_live_web.py" in p.cmdline():
                for c in p.children(recursive=True):
                    try: c.kill()
                    except: pass
                updater_process.terminate()
                try: updater_process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    updater_process.kill()
            updater_process = None
        except: pass

def start_updater():
    """Start update_live_web.py process"""
    global updater_process, current_status
    
    try:
        # Check if already running
        if updater_process and updater_process.poll() is None:
            return
            
        # Start new process
        updater_process = subprocess.Popen(
            [sys.executable, "update_live_web.py", "--auto"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=False
        )
        
        # Write PID to file
        with open(PID_FILE, 'w') as f:
            f.write(str(updater_process.pid))
        
        log_message(GREEN, "MONITOR", f"Started update_live_web.py with PID: {updater_process.pid}")
        current_status["updater_pid"] = updater_process.pid
        
        # Start output handling threads
        threading.Thread(target=handle_output, args=(updater_process.stdout,), daemon=True).start()
        threading.Thread(target=handle_output, args=(updater_process.stderr,), daemon=True).start()
        
    except Exception as e:
        log_message(RED, "ERROR", f"Failed to start updater: {e}")
        current_status["state"] = "error"

def handle_output(pipe):
    """Handle output from update_live_web.py"""
    try:
        while True:
            line = pipe.readline()
            if not line:
                break
            try:
                decoded = line.decode('utf-8', errors='replace').strip()
                if decoded:
                    print(f"{ORANGE}{decoded}{RESET}", flush=True)
            except Exception as e:
                log_message(RED, "ERROR", f"Error decoding output: {e}")
    except Exception as e:
        log_message(RED, "ERROR", f"Error in output handler: {e}")

def cleanup():
    """Cleanup function for exit"""
    global running
    running = False
    kill_updater_process()
    if os.path.exists(PID_FILE):
        try: os.remove(PID_FILE)
        except: pass
    if os.path.exists(DAEMON_PID_FILE):
        try: os.remove(DAEMON_PID_FILE)
        except: pass

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    log_message(YELLOW, "SHUTDOWN", "Received shutdown signal")
    cleanup()
    sys.exit(0)

def main():
    global running, current_status
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Write daemon PID
    write_daemon_pid()
    
    # Setup ZMQ
    context = zmq.Context()
    
    # Socket to receive status from update_live_web.py
    status_receiver = context.socket(zmq.SUB)
    status_receiver.setsockopt(zmq.RCVTIMEO, 1000)  # 1 second timeout
    status_receiver.setsockopt(zmq.LINGER, 0)       # Don't wait on close
    status_receiver.setsockopt_string(zmq.SUBSCRIBE, "")
    status_receiver.connect(f"tcp://localhost:{STATUS_RECEIVE_PORT}")
    
    # Socket to publish status to web workers
    status_publisher = context.socket(zmq.PUB)
    status_publisher.setsockopt(zmq.LINGER, 0)  # Don't wait on close
    status_publisher.bind(f"tcp://*:{STATUS_PUBLISH_PORT}")
    
    # Small delay to allow publisher to fully bind
    time.sleep(0.2)
    
    log_message(RED, "ZMQ-DEBUG", f"Daemon started. PID: {os.getpid()}")
    log_message(RED, "ZMQ-DEBUG", f"Receiving status on port {STATUS_RECEIVE_PORT}")
    log_message(RED, "ZMQ-DEBUG", f"Publishing status on port {STATUS_PUBLISH_PORT}")
    
    # Start initial updater process
    start_updater()
    
    # Send initial status to ensure workers get it
    current_status["daemon_pid"] = os.getpid()
    status_publisher.send_string(json.dumps(current_status))
    log_message(RED, "ZMQ-DEBUG", f"Sent initial status: {current_status['state']}")
    
    last_status_time = time.time()
    
    while running:
        try:
            # Check for status updates from update_live_web.py
            if status_receiver.poll(100):
                message = status_receiver.recv_string()
                try:
                    status = json.loads(message)
                    current_status.update(status)
                    current_status["daemon_pid"] = os.getpid()
                    last_status_time = time.time()
                    
                    # Forward status to web workers with daemon info
                    status_publisher.send_string(json.dumps(current_status))
                    log_message(RED, "ZMQ-DEBUG", f"Received status from updater and forwarded to workers: {current_status['state']}")
                except Exception as e:
                    log_message(RED, "ERROR", f"Failed to process status: {e}")
            
            # Periodically resend status even if no updates
            if time.time() - last_status_time > 5:  # Every 5 seconds
                status_publisher.send_string(json.dumps(current_status))
                log_message(RED, "ZMQ-DEBUG", f"Sent periodic status update: {current_status['state']}")
                last_status_time = time.time()
            
            time.sleep(0.1)
            
        except Exception as e:
            log_message(RED, "ERROR", f"Main loop error: {e}")
            time.sleep(1)
    
    # Cleanup
    status_receiver.close()
    status_publisher.close()
    context.term()

if __name__ == "__main__":
    try:
        main()
    finally:
        cleanup() 