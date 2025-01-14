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
import argparse

# ANSI color codes
YELLOW = '\033[93m'
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
ORANGE = '\033[38;5;208m'
RESET = '\033[0m'

# Constants
STATUS_RECEIVE_PORT = 5557  # Port to receive updates from update_live.py
STATUS_PUBLISH_PORT = 5558  # Port to publish status to web workers
PID_FILE = os.path.join(tempfile.gettempdir(), 'update_live.pid')
DAEMON_PID_FILE = os.path.join(tempfile.gettempdir(), 'update_daemon.pid')

# ZMQ connection settings
ZMQ_HOST = os.environ.get('ZMQ_HOST', 'localhost')  # Allow configuration via environment

# Global state
updater_process = None
current_status = {"state": "offline", "last_db_update": None, "daemon_pid": os.getpid()}
running = True

# Debug levels
DEBUG_LEVEL = 1  # 0 = silent, 1 = critical/important, 2 = normal, 3 = verbose/detailed

def log_message(color, tag, message, level=1):
    """Log a message with timestamp and PID"""
    # Skip messages if debug level is 0 or message level is higher than DEBUG_LEVEL
    if DEBUG_LEVEL == 0 or level > DEBUG_LEVEL:
        return

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
    """Kill the update_live.py process if running"""
    global updater_process
    if updater_process:
        try:
            p = psutil.Process(updater_process.pid)
            if "update_live.py" in p.cmdline():
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
    """Find or start the update_live.py process"""
    global updater_process, current_status
    
    try:
        # Try to find existing process multiple times with delays
        for attempt in range(3):  # Try 3 times
            log_message(YELLOW, "MONITOR", f"Searching for update processes (attempt {attempt + 1}/3)...", level=1)
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = " ".join(proc.cmdline())
                    name = proc.name()
                    
                    # Log any process containing "update"
                    if "update" in name.lower() or "update" in cmdline.lower():
                        log_message(BLUE, "MONITOR", f"Found process: PID={proc.pid}, Name={name}, Cmdline={cmdline}", level=1)
                    
                    # Check for our specific process
                    if "update_live.py" in cmdline.replace("\\", "/") or name in ["update", "update-1"]:
                        updater_process = proc
                        log_message(GREEN, "MONITOR", f"Found existing update process with PID: {proc.pid} (name: {name}, cmdline: {cmdline})", level=1)
                        current_status["updater_pid"] = proc.pid
                        return True
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
            
            if attempt < 2:  # Don't sleep on last attempt
                log_message(YELLOW, "MONITOR", f"No suitable update process found, waiting 10 seconds...", level=1)
                time.sleep(10)  # Wait 10 seconds between attempts
        
        # If still not found after retries, start new process
        log_message(YELLOW, "MONITOR", "No update_live.py found after waiting, starting new process...", level=1)
        updater_process = subprocess.Popen(
            [sys.executable, "update_live.py", "--auto"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=False
        )
        
        # Write PID to file
        with open(PID_FILE, 'w') as f:
            f.write(str(updater_process.pid))
        
        log_message(GREEN, "MONITOR", f"Started update_live.py with PID: {updater_process.pid}", level=1)
        current_status["updater_pid"] = updater_process.pid
        
        # Start output handling threads
        threading.Thread(target=handle_output, args=(updater_process.stdout,), daemon=True).start()
        threading.Thread(target=handle_output, args=(updater_process.stderr,), daemon=True).start()
        
        return True
        
    except Exception as e:
        log_message(RED, "ERROR", f"Error in start_updater: {e}", level=1)
        current_status["state"] = "error"
        return False

def handle_output(pipe):
    """Handle output from update_live.py"""
    try:
        while True:
            line = pipe.readline()
            if not line:
                break
            try:
                # Only show update_live.py output in debug level 3
                if DEBUG_LEVEL >= 3:
                    decoded = line.decode('utf-8', errors='replace').strip()
                    if decoded:
                        print(f"{ORANGE}{decoded}{RESET}", flush=True)
            except Exception as e:
                log_message(RED, "ERROR", f"Error decoding output: {e}", level=1)
    except Exception as e:
        log_message(RED, "ERROR", f"Error in output handler: {e}", level=1)

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
    log_message(YELLOW, "SHUTDOWN", "Received shutdown signal", level=1)
    cleanup()
    sys.exit(0)

def main():
    global running, current_status, DEBUG_LEVEL
    
    # Add argument parsing
    parser = argparse.ArgumentParser(description='EDDN Update Daemon')
    parser.add_argument('--debug-level', type=int, choices=[0, 1, 2, 3], default=1, 
                       help='Debug level (0=silent, 1=critical, 2=normal, 3=verbose)')
    args = parser.parse_args()
    
    # Set DEBUG_LEVEL from argument
    DEBUG_LEVEL = args.debug_level
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Write daemon PID
    write_daemon_pid()
    
    # Setup ZMQ
    context = zmq.Context()
    
    # Socket to receive status from update_live.py
    status_receiver = context.socket(zmq.SUB)
    status_receiver.setsockopt(zmq.RCVTIMEO, 1000)  # 1 second timeout
    status_receiver.setsockopt(zmq.LINGER, 0)       # Don't wait on close
    status_receiver.setsockopt_string(zmq.SUBSCRIBE, "")
    status_receiver.connect("tcp://127.0.0.1:5557")  # Connect to update_live.py publisher
    
    # Socket to publish status to web workers
    status_publisher = context.socket(zmq.PUB)
    status_publisher.setsockopt(zmq.LINGER, 0)  # Don't wait on close
    status_publisher.bind("tcp://0.0.0.0:5558")  # Bind for web workers to connect to
    
    log_message(RED, "ZMQ-DEBUG", f"Connecting to update_live.py on localhost:{STATUS_RECEIVE_PORT}", level=1)
    log_message(RED, "ZMQ-DEBUG", f"Publishing to web workers on port {STATUS_PUBLISH_PORT}", level=1)
    
    # Small delay to allow publisher to fully bind
    time.sleep(0.2)
    
    log_message(RED, "ZMQ-DEBUG", f"Daemon started. PID: {os.getpid()}", level=1)
    log_message(RED, "ZMQ-DEBUG", f"Receiving status on port {STATUS_RECEIVE_PORT}", level=2)
    log_message(RED, "ZMQ-DEBUG", f"Publishing status on port {STATUS_PUBLISH_PORT}", level=2)
    
    # Wait for ZMQ messages first
    log_message(YELLOW, "MONITOR", "Waiting for ZMQ messages from existing update process...", level=1)
    zmq_wait_start = time.time()
    has_received_status = False
    
    while time.time() - zmq_wait_start < 60:  # Wait up to 30 seconds for ZMQ messages
        if status_receiver.poll(1000):  # Check every second
            try:
                message = status_receiver.recv_string()
                status = json.loads(message)
                current_status.update(status)
                current_status["daemon_pid"] = os.getpid()
                has_received_status = True
                log_message(GREEN, "MONITOR", f"Received ZMQ message from existing update process: {status.get('state')}", level=1)
                break
            except Exception as e:
                log_message(RED, "ERROR", f"Error receiving ZMQ message: {e}", level=1)
        
        log_message(YELLOW, "MONITOR", "No ZMQ messages yet, still waiting...", level=2)
    
    # Only try to start our own process if we haven't received any ZMQ messages
    if not has_received_status:
        log_message(YELLOW, "MONITOR", "No ZMQ messages received, checking for existing process...", level=1)
        if not start_updater():
            log_message(YELLOW, "MONITOR", "No existing process found, will start new one in 3 minutes...", level=1)
            current_status["state"] = "starting"
    
    # Send initial status to ensure workers get it
    current_status["daemon_pid"] = os.getpid()
    status_publisher.send_string(json.dumps(current_status))
    log_message(RED, "ZMQ-DEBUG", f"Sent initial status: {current_status['state']}", level=1)
    
    last_status_time = time.time()
    startup_time = time.time()
    
    while running:
        try:
            # Check for status updates from update_live.py
            if status_receiver.poll(100):
                message = status_receiver.recv_string()
                try:
                    status = json.loads(message)
                    current_status.update(status)
                    current_status["daemon_pid"] = os.getpid()
                    last_status_time = time.time()
                    has_received_status = True
                    
                    # Forward status to web workers with daemon info
                    status_publisher.send_string(json.dumps(current_status))
                    log_message(RED, "ZMQ-DEBUG", f"Received status from updater and forwarded to workers: {current_status['state']}", level=2)
                except Exception as e:
                    log_message(RED, "ERROR", f"Failed to process status: {e}", level=1)
            
            # Only try to start process if we haven't received any status for a while
            if not has_received_status and time.time() - startup_time > 180:  # 3 minutes
                log_message(YELLOW, "MONITOR", "No status received for 3 minutes, checking for existing process...", level=1)
                if start_updater():
                    startup_time = time.time()  # Reset timer if process started
            
            # Periodically resend status even if no updates
            if time.time() - last_status_time > 5:  # Every 5 seconds
                status_publisher.send_string(json.dumps(current_status))
                log_message(RED, "ZMQ-DEBUG", f"Sent periodic status update: {current_status['state']}", level=2)
                last_status_time = time.time()
            
            time.sleep(0.1)
            
        except Exception as e:
            log_message(RED, "ERROR", f"Main loop error: {e}", level=1)
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