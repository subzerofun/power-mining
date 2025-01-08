import os
import sys
import json
import time
import signal
import psutil
import asyncio
import threading
import subprocess
import io
from datetime import datetime

# ANSI color codes
YELLOW = '\033[93m'
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
ORANGE = '\033[38;5;208m'
RESET = '\033[0m'

# Global variables for processes
updater_process = None
daily_process = None
live_update_requested = False

# Global state for EDDN status
eddn_status = {
    "state": None,
    "last_db_update": None
}

# Global state for daily update status
daily_status = {
    "state": "offline",
    "progress": 0,
    "total": 0,
    "message": "",
    "last_update": None
}

def kill_updater_process():
    """Forcefully kill the updater process and all its children"""
    global updater_process
    if updater_process:
        try:
            process = psutil.Process(updater_process.pid)
            children = process.children(recursive=True)
            for child in children:
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
                    
            if os.name == 'nt':
                updater_process.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                updater_process.terminate()
            
            try:
                updater_process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                if os.name == 'nt':
                    os.kill(updater_process.pid, signal.SIGTERM)
                else:
                    updater_process.kill()
                    
            updater_process = None
        except (psutil.NoSuchProcess, ProcessLookupError):
            pass
        except Exception as e:
            print(f"Error killing updater process: {e}", file=sys.stderr)

def kill_daily_process():
    """Forcefully kill the daily update process"""
    global daily_process, daily_status
    if daily_process:
        try:
            process = psutil.Process(daily_process.pid)
            children = process.children(recursive=True)
            for child in children:
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
                    
            if os.name == 'nt':
                daily_process.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                daily_process.terminate()
            
            try:
                daily_process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                if os.name == 'nt':
                    os.kill(daily_process.pid, signal.SIGTERM)
                else:
                    daily_process.kill()
                    
            daily_process = None
            daily_status["state"] = "offline"
        except (psutil.NoSuchProcess, ProcessLookupError):
            pass
        except Exception as e:
            print(f"Error killing daily process: {e}", file=sys.stderr)

def stop_updater():
    """Stop the EDDN updater process"""
    global eddn_status
    eddn_status["state"] = "offline"
    kill_updater_process()

def stop_daily_update():
    """Stop the daily update process"""
    kill_daily_process()

def handle_output(line):
    """Handle output from update_live.py and update status"""
    global eddn_status
    line = line.strip()
    
    if "[INIT]" in line or "[STOPPING]" in line or "[TERMINATED]" in line:
        print(f"{YELLOW}{line}{RESET}", flush=True)
    else:
        print(f"{BLUE}{line}{RESET}", flush=True)
    
    if "[INIT]" in line:
        eddn_status["state"] = "starting"
    elif "Loaded" in line and "commodities from CSV" in line:
        eddn_status["state"] = "starting"
    elif "Listening to EDDN" in line:
        eddn_status["state"] = "running"
    elif "[DATABASE] Writing to Database starting..." in line:
        eddn_status["state"] = "updating"
        eddn_status["last_db_update"] = datetime.now().isoformat()
        eddn_status["update_start_time"] = time.time()
    elif "[DATABASE] Writing to Database finished." in line or "Writing to Database finished. Updated" in line:
        if "update_start_time" in eddn_status:
            elapsed = time.time() - eddn_status["update_start_time"]
            if elapsed < 1:
                time.sleep(1 - elapsed)
            del eddn_status["update_start_time"]
        eddn_status["state"] = "running"
    elif "[STOPPING]" in line or "[TERMINATED]" in line:
        eddn_status["state"] = "offline"
        print(f"{YELLOW}[STATUS] EDDN updater stopped{RESET}", flush=True)
    elif "Error:" in line:
        eddn_status["state"] = "error"
        print(f"{YELLOW}[STATUS] EDDN updater encountered an error{RESET}", flush=True)

def clean_ansi_codes(message):
    """Clean ANSI escape codes from a message"""
    for code in ['\033[93m', '\033[94m', '\033[92m', '\033[91m', '\033[0m']:
        message = message.replace(code, '')
    return message.strip()

def handle_daily_output(line):
    """Handle output from update_daily.py and update status"""
    global daily_status, eddn_status, updater_process, live_update_requested
    line = line.strip()
    
    if "[COMPLETED]" in line:
        print(f"{YELLOW}[DEBUG] Received completion signal{RESET}", flush=True)
        daily_status["state"] = "updated"
        daily_status["last_update"] = datetime.now().strftime("%Y-%m-%d")
        if updater_process is None and live_update_requested:
            print(f"{YELLOW}[DEBUG] Live update was requested and no updater running{RESET}", flush=True)
            print(f"{YELLOW}Daily update completed. Starting live EDDN updates...{RESET}")
            eddn_status["state"] = "starting"
            start_updater()
            time.sleep(0.5)
        else:
            print(f"{YELLOW}[DEBUG] Skipping live update start: updater_process={updater_process}, live_update_requested={live_update_requested}{RESET}", flush=True)
        return
    
    # For progress updates, print on same line
    if "[STATUS]" in line and any(x in line for x in ["Downloading", "Extracting", "Processing entries"]):
        print(f"\r{YELLOW}{line}{RESET}", end="", flush=True)
    else:
        print(f"{YELLOW}{line}{RESET}", flush=True)
    
    if "[STATUS]" in line:
        if "Downloading" in line:
            daily_status["state"] = "downloading"
            try:
                daily_status["progress"] = int(line.split("%")[0].split()[-1])
            except:
                daily_status["progress"] = 0
        elif "Extracting" in line:
            daily_status["state"] = "extracting"
            try:
                daily_status["progress"] = int(line.split("%")[0].split()[-1])
            except:
                daily_status["progress"] = 0
        elif "Processing entries" in line:
            daily_status["state"] = "processing"
            try:
                parts = line.split("(")[1].split(")")[0].split("/")
                current = int(parts[0])
                total = int(parts[1])
                daily_status["progress"] = current
                daily_status["total"] = total
            except:
                pass
        elif "Updated:" in line:
            daily_status["state"] = "updated"
            daily_status["message"] = clean_ansi_codes(line.split("[STATUS]")[1])
            daily_status["last_update"] = datetime.now().isoformat()
        elif "error" in line.lower():
            daily_status["state"] = "error"
            daily_status["message"] = clean_ansi_codes(line.split("[STATUS]")[1])
        elif "Saving batch" in line or "Batch saved" in line:
            daily_status["message"] = clean_ansi_codes(line.split("[STATUS]")[1])

def start_daily_update():
    """Start the daily update process"""
    global daily_process, daily_status
    
    daily_status["state"] = "starting"
    daily_status["progress"] = 0
    daily_status["total"] = 0
    daily_status["message"] = "Starting daily update..."
    
    def handle_output_stream(pipe, stream_name):
        try:
            print(f"{YELLOW}[DEBUG] Starting {stream_name} stream handler{RESET}", flush=True)
            with io.TextIOWrapper(pipe, encoding='utf-8', errors='replace') as text_pipe:
                while True:
                    line = text_pipe.readline()
                    if not line:
                        break
                    if line.strip():
                        handle_daily_output(line.strip())
        except Exception as e:
            print(f"Error in daily update {stream_name} stream: {e}", file=sys.stderr)
    
    try:
        print(f"{YELLOW}[DEBUG] Starting daily update process{RESET}", flush=True)
        daily_process = subprocess.Popen(
            [sys.executable, "update_daily.py", "--auto", "--fast"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=False,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
        )
        
        stdout_thread = threading.Thread(target=handle_output_stream, args=(daily_process.stdout, "stdout"), daemon=True)
        stderr_thread = threading.Thread(target=handle_output_stream, args=(daily_process.stderr, "stderr"), daemon=True)
        
        stdout_thread.start()
        stderr_thread.start()
        
        print(f"{YELLOW}[DEBUG] Daily update process started with PID {daily_process.pid}{RESET}", flush=True)
        return daily_process
        
    except Exception as e:
        print(f"Error starting daily update: {e}", file=sys.stderr)
        daily_status["state"] = "error"
        daily_status["message"] = str(e)
        return None

async def handle_websocket(websocket):
    """Handle WebSocket connections and send status updates"""
    try:
        # Ensure JSON directory exists
        json_dir = 'json'
        status_file = os.path.join(json_dir, 'daily_update_status.json')
        if not os.path.exists(json_dir):
            os.makedirs(json_dir)
        
        while True:
            # Read or create the daily update status file
            try:
                if not os.path.exists(status_file):
                    # Create default status
                    default_status = {
                        "last_update": datetime.now().strftime("%Y-%m-%d"),
                        "last_file": None,
                        "completed": False,
                        "error": None,
                        "processed_entries": 0,
                        "total_entries": 0
                    }
                    with open(status_file, 'w') as f:
                        json.dump(default_status, f, indent=2)
                
                with open(status_file, 'r') as f:
                    daily_status_file = json.load(f)
                    if daily_status_file.get("last_update"):
                        daily_status["last_update"] = daily_status_file["last_update"]
            except Exception as e:
                print(f"Error handling status file: {e}", file=sys.stderr)
            
            await websocket.send(json.dumps({
                "eddn": eddn_status,
                "daily": daily_status
            }))
            await asyncio.sleep(0.1)
    except Exception as e:
        pass

def start_updater():
    """Start the EDDN updater process"""
    global updater_process, eddn_status
    
    eddn_status["state"] = "starting"
    
    def handle_output_stream(pipe, color):
        try:
            with io.TextIOWrapper(pipe, encoding='utf-8', errors='replace') as text_pipe:
                while True:
                    line = text_pipe.readline()
                    if not line:
                        break
                    if line.strip():
                        handle_output(line.strip())
        except Exception as e:
            print(f"Error in output stream: {e}", file=sys.stderr)
    
    try:
        updater_process = subprocess.Popen(
            [sys.executable, "update_live.py", "--auto"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=False,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
        )
        
        print(f"{YELLOW}[STATUS] Starting EDDN Live Update (PID: {updater_process.pid}){RESET}", flush=True)
        
        threading.Thread(target=handle_output_stream, args=(updater_process.stdout, BLUE), daemon=True).start()
        threading.Thread(target=handle_output_stream, args=(updater_process.stderr, BLUE), daemon=True).start()
        
        time.sleep(0.5)
        
        if updater_process.poll() is None:
            eddn_status["state"] = "starting"
        else:
            eddn_status["state"] = "error"
            print(f"{YELLOW}[ERROR] EDDN updater failed to start{RESET}", file=sys.stderr)
        
    except Exception as e:
        print(f"Error starting updater: {e}", file=sys.stderr)
        eddn_status["state"] = "error" 