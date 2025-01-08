import os
import sys
import sqlite3
import argparse
import requests
import bz2
from datetime import datetime, timedelta, timezone
import csv
import time
import msgspec  # Much faster than orjson
import json
import io

# ANSI color codes
YELLOW = '\033[93m'
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
ORANGE = '\033[38;5;208m'  # Add orange color code
RESET = '\033[0m'

# Constants
DB_PATH = "systems.db"
COMMODITIES_CSV = os.path.join("data", "commodities_mining.csv")
JSON_DIR = "json"
BASE_URL = "https://edgalaxydata.space/EDDN"
STATUS_FILE = os.path.join(JSON_DIR, "daily_update_status.json")

# Status reporting
daily_status = {
    "state": "offline",  # offline, downloading, extracting, deleting, processing
    "progress": 0,
    "total": 0,
    "message": "",
    "last_line": ""  # Track the last line for progress updates
}

def update_status(state, progress=None, total=None, message=""):
    """Update the status of the daily update process"""
    # Update internal status (no ANSI codes)
    daily_status["state"] = state
    if progress is not None:
        daily_status["progress"] = progress
    if total is not None:
        daily_status["total"] = total
    if message:
        # Clean message of any ANSI codes before storing
        clean_message = message
        for code in [ORANGE, BLUE, GREEN, RED, RESET]:
            clean_message = clean_message.replace(code, "")
        daily_status["message"] = clean_message.strip()
    
    # Format the console output (with colors)
    if state in ["downloading", "extracting"]:
        # Add newline if the last message wasn't a progress message
        if daily_status["last_line"] != "progress":
            print("", flush=True)  # Add empty line
        print(f"\r{ORANGE}[STATUS] {progress}% {state.capitalize()}{RESET}", end="", flush=True)
        daily_status["last_line"] = "progress"
    elif state == "processing" and progress is not None and total is not None:
        # Add newline if the last message wasn't a progress message
        if daily_status["last_line"] != "progress":
            print("", flush=True)  # Add empty line
        print(f"\r{ORANGE}[STATUS] Processing entries ({min(progress, total)}/{total}){RESET}", end="", flush=True)
        daily_status["last_line"] = "progress"
    elif message:
        # Always start message on a new line if we were showing progress
        if daily_status["last_line"] == "progress":
            print("", flush=True)  # Add empty line
        print(f"{ORANGE}[STATUS] {message}{RESET}", flush=True)
        daily_status["last_line"] = "message"

# Define the message structure for validation and faster processing
class EDDNMessage(msgspec.Struct):
    """Structure for EDDN messages"""
    schemaRef: str = msgspec.field(name="$schemaRef")
    message: dict

def load_commodity_map():
    """Load commodity mapping from CSV"""
    print(f"{BLUE}[INIT] Loading commodity mapping from {COMMODITIES_CSV}{RESET}", flush=True)
    commodity_map = {}
    with open(COMMODITIES_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            eddn_id = row["id"].strip()
            local_name = row["name"].strip()
            # Special case: store as "Void Opal" but handle both forms
            if local_name == "Void Opals":
                local_name = "Void Opal"
            commodity_map[eddn_id] = local_name
    print(f"{GREEN}[INIT] Loaded {len(commodity_map)} commodities from CSV{RESET}", flush=True)
    return commodity_map

def find_latest_file():
    """Find the latest commodity file from the past 7 days"""
    print(f"{BLUE}[SEARCH] Looking for latest commodity file...{RESET}", flush=True)
    
    # Get today's date in UTC
    today = datetime.now(timezone.utc)
    
    def check_file_status(url, check_modified=True):
        """Check if file exists and optionally check modification time"""
        try:
            head_response = requests.head(url)
            if head_response.status_code != 200:
                return None, None
            
            # Only check modification time for root files if requested
            if check_modified:
                last_modified = head_response.headers.get('last-modified')
                if last_modified:
                    last_mod_time = datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S GMT')
                    now = datetime.now(timezone.utc)
                    time_diff = now - last_mod_time.replace(tzinfo=timezone.utc)
                    
                    # If file was modified in the last 10 minutes, consider it unstable
                    if time_diff.total_seconds() < 600:  # 10 minutes
                        print(f"{YELLOW}[WARNING] File at {url} was modified recently ({time_diff.total_seconds():.0f} seconds ago){RESET}", flush=True)
                        return None, "recent"
            
            return head_response, None
            
        except Exception as e:
            print(f"{RED}[ERROR] Failed to check file status: {str(e)}{RESET}", flush=True)
            return None, str(e)
    
    # First check root level for newest file
    for i in range(7):
        date = today - timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        
        # Check for local files first
        local_jsonl = os.path.join(JSON_DIR, f"Commodity-{date_str}.jsonl")
        local_bz2 = os.path.join(JSON_DIR, f"Commodity-{date_str}.jsonl.bz2")
        
        # Check if we have a local file
        if os.path.exists(local_jsonl):
            print(f"{GREEN}[FOUND] Found local file {local_jsonl}{RESET}", flush=True)
            return local_jsonl, None, date_str
        elif os.path.exists(local_bz2):
            print(f"{GREEN}[FOUND] Found local compressed file {local_bz2}{RESET}", flush=True)
            return local_jsonl, local_bz2, date_str
            
        # Check root level of server
        root_jsonl_url = f"{BASE_URL}/Commodity-{date_str}.jsonl"
        root_bz2_url = f"{BASE_URL}/Commodity-{date_str}.jsonl.bz2"
        
        # First check for compressed file
        head_response, status = check_file_status(root_bz2_url, check_modified=True)  # Check modification time for root files
        if head_response:
            print(f"{GREEN}[FOUND] Found compressed file at server root: {root_bz2_url}{RESET}", flush=True)
            return local_jsonl, root_bz2_url, date_str
        elif status == "recent":
            # If most recent file is being modified, skip to subfolder check
            print(f"{YELLOW}[STATUS] Most recent file is being updated, checking archive folders...{RESET}", flush=True)
            break
            
        # Then check for uncompressed file
        head_response, status = check_file_status(root_jsonl_url, check_modified=True)  # Check modification time for root files
        if head_response:
            print(f"{GREEN}[FOUND] Found uncompressed file at server root: {root_jsonl_url}{RESET}", flush=True)
            return local_jsonl, root_jsonl_url, date_str
        elif status == "recent":
            # If most recent file is being modified, skip to subfolder check
            print(f"{YELLOW}[STATUS] Most recent file is being updated, checking archive folders...{RESET}", flush=True)
            break
    
    # If no files found in root or most recent file is being modified, check subfolders
    print(f"{YELLOW}[SEARCH] Checking monthly folders...{RESET}", flush=True)
    
    # Check subfolders without checking modification time
    for i in range(7):
        date = today - timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        year_month = date.strftime("%Y-%m")
        
        # Check for local files first
        local_jsonl = os.path.join(JSON_DIR, f"Commodity-{date_str}.jsonl")
        local_bz2 = os.path.join(JSON_DIR, f"Commodity-{date_str}.jsonl.bz2")
        
        if os.path.exists(local_jsonl):
            print(f"{GREEN}[FOUND] Found local file in archive: {local_jsonl}{RESET}", flush=True)
            return local_jsonl, None, date_str
        elif os.path.exists(local_bz2):
            print(f"{GREEN}[FOUND] Found local compressed archive file: {local_bz2}{RESET}", flush=True)
            return local_jsonl, local_bz2, date_str
        
        # Check server subfolder without checking modification time
        subfolder_url = f"{BASE_URL}/{year_month}/Commodity-{date_str}.jsonl.bz2"
        head_response, status = check_file_status(subfolder_url, check_modified=False)  # Don't check modification time for archive files
        if head_response:
            print(f"{GREEN}[FOUND] Found file in server archive: {subfolder_url}{RESET}", flush=True)
            return local_jsonl, subfolder_url, date_str
    
    print(f"{RED}[ERROR] No commodity files found for the past 7 days{RESET}", flush=True)
    return None, None, None

def download_and_extract(url, jsonl_path, bz2_path):
    """Download and extract the bz2 file"""
    if not os.path.exists(JSON_DIR):
        os.makedirs(JSON_DIR)
    
    update_status("downloading", 0, message="Starting download...")
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    
    # Check if this is a compressed file
    is_compressed = url.endswith('.bz2')
    target_path = bz2_path if is_compressed else jsonl_path
    
    # Download the file
    downloaded = 0
    with open(target_path, 'wb') as f:
        if total_size == 0:
            f.write(response.content)
        else:
            for data in response.iter_content(chunk_size=8192):
                downloaded += len(data)
                f.write(data)
                percent = min(100, int(100 * downloaded / total_size))
                update_status("downloading", percent, message=f"{percent}% Downloading...")
    
    # If compressed, extract it
    if is_compressed:
        update_status("extracting", 0, message="Starting extraction...")
        
        # Get compressed size for percentage calculation
        compressed_size = os.path.getsize(bz2_path)
        
        # Delete old JSONL files before extraction
        for file in os.listdir(JSON_DIR):
            if file.endswith('.jsonl'):
                try:
                    os.remove(os.path.join(JSON_DIR, file))
                except Exception as e:
                    print(f"Error deleting old file {file}: {e}", file=sys.stderr)
        
        # Extract new file
        extracted = 0
        with bz2.open(bz2_path, 'rb') as source, open(jsonl_path, 'wb') as target:
            for data in iter(lambda: source.read(8192), b''):
                extracted += len(data)
                target.write(data)
                percent = min(100, int(100 * extracted / compressed_size))
                update_status("extracting", percent, message=f"{percent}% Extracting...")
        
        # Delete the bz2 file
        try:
            os.remove(bz2_path)
        except Exception as e:
            print(f"Error deleting bz2 file: {e}", file=sys.stderr)
        
        update_status("processing", message="File extracted and ready for processing")
    else:
        update_status("processing", message="File downloaded and ready for processing")
    
    return jsonl_path

def process_jsonl(jsonl_path, commodity_map, auto_commit=False, fast_mode=False):
    """Process the JSONL file and update the database"""
    update_status("processing", message="Counting valid entries...")
    print("", flush=True)  # Add empty line
    
    # Extract date from filename
    date_str = os.path.basename(jsonl_path).split('-')[1].split('.')[0]
    
    # Count valid entries for progress
    total_lines = 0
    schema_ref = "https://eddn.edcd.io/schemas/commodity/3"
    decoder = msgspec.json.Decoder(EDDNMessage)
    
    with open(jsonl_path, 'rb') as f:
        for line in f:
            try:
                entry = decoder.decode(line)
                if entry.schemaRef.lower() == schema_ref.lower():
                    total_lines += 1
            except:
                continue
    
    update_status("processing", 0, total_lines, f"Found {total_lines} valid entries to process")
    print("", flush=True)  # Add empty line
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if fast_mode:
        _process_fast_mode(conn, cursor, jsonl_path, total_lines, commodity_map, date_str)
    else:
        _process_normal_mode(conn, cursor, jsonl_path, total_lines, commodity_map, auto_commit)
    
    conn.close()

def _process_fast_mode(conn, cursor, jsonl_path, total_lines, commodity_map, date_str):
    """Fast processing mode - just delete everything and resave"""
    processed_entries = 0
    stations_updated = 0
    total_commodities = 0
    missing_market_ids = 0
    missing_station_ids = 0
    
    # Load current status
    status_data = load_status()
    
    # Check if we need to resume (only if we have processed entries but haven't completed)
    if status_data["processed_entries"] > 0 and status_data["processed_entries"] < status_data["total_entries"]:
        resume_from = max(0, status_data["processed_entries"] - 1000)  # Roll back 1000 entries for safety
        print(f"{YELLOW}[STATUS] Previous update incomplete. Processed {status_data['processed_entries']}/{status_data['total_entries']}{RESET}", flush=True)
        print("", flush=True)  # Add empty line between messages
    else:
        resume_from = 0
    
    # Enable WAL mode and set pragmas for better write performance
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA temp_store=MEMORY")
    cursor.execute("PRAGMA cache_size=10000")
    
    # First pass: collect all station names from the new file
    if resume_from == 0:
        update_status("processing", message="Scanning file for stations...")
        print("", flush=True)  # Add empty line
        stations_to_update = {}  # Change to dict to track latest entry per marketId
        schema_ref = "https://eddn.edcd.io/schemas/commodity/3"
        decoder = msgspec.json.Decoder(EDDNMessage)
        
        # Get current update times for all stations
        station_update_times = {}
        cursor.execute("SELECT station_id, update_time FROM stations WHERE update_time IS NOT NULL")
        for row in cursor.fetchall():
            if row[1]:  # Only store if update_time is not null
                try:
                    station_update_times[row[0]] = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S+00")
                except ValueError:
                    pass  # Skip if we can't parse the date
        
        with open(jsonl_path, 'rb') as f:
            for line in f:
                try:
                    entry = decoder.decode(line)
                    if entry.schemaRef.lower() != schema_ref.lower():
                        continue
                    
                    message = entry.message
                    station_name = message.get("stationName")
                    market_id = message.get("marketId")
                    timestamp = message.get("timestamp")
                    
                    # Skip Fleet Carriers
                    if message.get("stationType") == "FleetCarrier" or \
                       (message.get("economies") and message["economies"][0].get("name") == "Carrier"):
                        continue
                    
                    if not station_name or not market_id or not timestamp:
                        continue
                        
                    # Convert timestamp for comparison
                    try:
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        update_time = dt.strftime("%Y-%m-%d %H:%M:%S+00")
                        entry_dt = datetime.strptime(update_time, "%Y-%m-%d %H:%M:%S+00")
                        
                        # Check if this entry is newer than what we have
                        current_dt = station_update_times.get(market_id)
                        if current_dt and current_dt >= entry_dt:
                            continue  # Skip if we have newer data
                        
                        # Only keep the latest entry for each marketId
                        if market_id not in stations_to_update or update_time > stations_to_update[market_id][1]:
                            stations_to_update[market_id] = (station_name, update_time)
                    except (ValueError, AttributeError):
                        continue
                except:
                    continue
        
        if stations_to_update:
            update_status("deleting", message=f"Deleting commodities from {len(stations_to_update)} stations...")
            print("", flush=True)  # Add empty line
            
            # Group stations by whether they need updating based on timestamp
            stations_to_delete = []
            for market_id, (station_name, new_time) in stations_to_update.items():
                try:
                    current_dt = station_update_times.get(market_id)
                    new_dt = datetime.strptime(new_time, "%Y-%m-%d %H:%M:%S+00")
                    if not current_dt or new_dt > current_dt:
                        stations_to_delete.append(market_id)
                except (ValueError, AttributeError):
                    continue
            
            # Delete in larger batches for better performance
            batch_size = 1000
            for i in range(0, len(stations_to_delete), batch_size):
                batch = stations_to_delete[i:i + batch_size]
                placeholders = ','.join('?' * len(batch))
                cursor.execute(f"""
                    DELETE FROM station_commodities 
                    WHERE station_id IN ({placeholders})
                """, batch)
                conn.commit()  # Commit after each batch
                
                # Show progress
                processed = min(i + batch_size, len(stations_to_delete))
                print(f"{GREEN}[STATUS] Cleared commodities from {processed} of {len(stations_to_delete)} stations{RESET}", flush=True)
            
            print(f"{GREEN}[STATUS] Cleared commodities from {len(stations_to_delete)} stations with newer data{RESET}", flush=True)
            print("", flush=True)  # Add empty line after delete message
    
    # Process in chunks
    commodities_by_station = {}  # Store commodities per station
    schema_ref = "https://eddn.edcd.io/schemas/commodity/3"
    
    # Stats for final summary
    skipped_carriers = 0
    new_stations = 0
    
    # Create a decoder for faster processing
    decoder = msgspec.json.Decoder(EDDNMessage)
    
    with open(jsonl_path, 'rb') as f:
        for line in f:
            processed_entries += 1
            
            # Skip entries until we reach the resume point
            if processed_entries < resume_from:
                continue
                
            if processed_entries % 1000 == 0:  # Only update display every 1000 entries
                update_status("processing", processed_entries, total_lines)
                print("", flush=True)  # Add empty line
            
            try:
                entry = decoder.decode(line)
                if entry.schemaRef.lower() != schema_ref.lower():
                    continue
                
                message = entry.message
                station_name = message.get("stationName")
                system_name = message.get("systemName")
                market_id = message.get("marketId")
                timestamp = message.get("timestamp")
                
                # Check if this is a Fleet Carrier
                if message.get("stationType") == "FleetCarrier" or \
                   (message.get("economies") and message["economies"][0].get("name") == "Carrier"):
                    skipped_carriers += 1
                    continue
                
                if not station_name or not system_name or not market_id or not timestamp:
                    continue
                
                # Convert timestamp
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    update_time = dt.strftime("%Y-%m-%d %H:%M:%S+00")
                except (ValueError, AttributeError):
                    continue
                
                # First, get the system id64
                cursor.execute("SELECT id64 FROM systems WHERE name = ?", (system_name,))
                system_row = cursor.fetchone()
                if not system_row:
                    continue
                
                system_id64 = system_row[0]
                
                # Only process if this is the latest entry for this market_id
                if market_id not in stations_to_update or update_time != stations_to_update[market_id][1]:
                    continue
                
                # Initialize commodities list for this station if not exists
                if market_id not in commodities_by_station:
                    commodities_by_station[market_id] = {}  # Change to dict to prevent duplicates
                
                # Process commodities for this station
                for commodity in message.get("commodities", []):
                    name = commodity.get("name", "").lower()
                    if not name or name not in commodity_map:
                        continue
                    
                    sell_price = commodity.get("sellPrice", 0)
                    if sell_price <= 0:
                        continue
                    
                    demand = commodity.get("demand", 0)
                    # Store directly in dict to prevent duplicates
                    commodities_by_station[market_id][commodity_map[name]] = (
                        system_id64, market_id, station_name,
                        commodity_map[name], sell_price, demand
                    )
                
                # Save every 500 stations
                if len(commodities_by_station) >= 500:
                    stations_updated += len(commodities_by_station)
                    batch_commodities = sum(len(comms) for comms in commodities_by_station.values())
                    total_commodities += batch_commodities
                    
                    # Show batch save status
                    update_status("processing", processed_entries, total_lines, 
                                f"Saving batch: {batch_commodities} commodities for {len(commodities_by_station)} stations...")
                    print("", flush=True)  # Add empty line
                    
                    # Insert commodities for each station
                    for market_id, commodities in commodities_by_station.items():
                        # Use a single INSERT statement with all values
                        values = list(commodities.values())
                        if values:  # Only insert if we have values
                            cursor.executemany("""
                                INSERT INTO station_commodities 
                                    (system_id64, station_id, station_name, commodity_name, sell_price, demand)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, values)
                            # Update the station's update_time
                            cursor.execute("""
                                UPDATE stations 
                                SET update_time = ? 
                                WHERE station_id = ?
                            """, (stations_to_update[market_id][1], market_id))
                            conn.commit()  # Commit after each station
                    
                    # Show completion
                    update_status("processing", processed_entries, total_lines, "Batch saved successfully")
                    print("", flush=True)  # Add empty line
                    
                    commodities_by_station.clear()
                
            except msgspec.ValidationError:
                continue
            except Exception as ex:
                continue
    
    # Print summary stats
    if skipped_carriers > 0 or new_stations > 0:
        print(f"{YELLOW}[DEBUG] {skipped_carriers} Skipped Fleet Carriers{RESET}", flush=True)
        print(f"{YELLOW}[DEBUG] {new_stations} New Stations saved{RESET}", flush=True)
        print("", flush=True)  # Add empty line
    
    # Print final debug counts
    if missing_market_ids > 0 or missing_station_ids > 0:
        print(f"{YELLOW}[DEBUG] Summary:{RESET}", flush=True)
        print(f"{YELLOW}[DEBUG] Entries without marketId: {missing_market_ids}{RESET}", flush=True)
        print(f"{YELLOW}[DEBUG] Entries without station_id: {missing_station_ids}{RESET}", flush=True)
        print("", flush=True)  # Add empty line
    
    # Save any remaining data
    if commodities_by_station:
        stations_updated += len(commodities_by_station)
        batch_commodities = sum(len(comms) for comms in commodities_by_station.values())
        total_commodities += batch_commodities
        
        # Show final batch save status
        update_status("processing", processed_entries, total_lines, 
                     f"Saving final batch: {batch_commodities} commodities for {len(commodities_by_station)} stations...")
        print("", flush=True)  # Add empty line
        
        # Insert remaining commodities for each station
        for market_id, commodities in commodities_by_station.items():
            # Use a single INSERT statement with all values
            values = list(commodities.values())
            if values:  # Only insert if we have values
                cursor.executemany("""
                    INSERT INTO station_commodities 
                        (system_id64, station_id, station_name, commodity_name, sell_price, demand)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, values)
                # Update the station's update_time
                cursor.execute("""
                    UPDATE stations 
                    SET update_time = ? 
                    WHERE station_id = ?
                """, (stations_to_update[market_id][1], market_id))
                conn.commit()  # Commit after each station
        
        # Show completion
        update_status("processing", processed_entries, total_lines, "Final batch saved successfully")
        print("", flush=True)  # Add empty line
    
    # Final status update using the date from the file
    if processed_entries >= total_lines:
        # Extract full date from filename (YYYY-MM-DD)
        file_date = date_str if len(date_str) == 10 else "-".join(os.path.basename(jsonl_path).split("-")[1:4]).split(".")[0]
        
        # Always save the final count
        status_data.update({
            "processed_entries": processed_entries,
            "total_entries": total_lines,
            "completed": True,
            "last_update": file_date,  # Use full YYYY-MM-DD date
            "last_file": jsonl_path
        })
        save_status(status_data)
        update_status("updated", processed_entries, total_lines, f"EDDN data: {file_date}")
        print("", flush=True)  # Add empty line
        print(f"\n[COMPLETED] Daily update finished successfully", flush=True)
    else:
        # Extract full date from filename (YYYY-MM-DD)
        file_date = date_str if len(date_str) == 10 else "-".join(os.path.basename(jsonl_path).split("-")[1:4]).split(".")[0]
        
        # Always save the final count
        status_data.update({
            "processed_entries": processed_entries,
            "total_entries": total_lines,
            "completed": False,
            "last_update": file_date,  # Use full YYYY-MM-DD date
            "last_file": jsonl_path
        })
        save_status(status_data)
        update_status("updated", processed_entries, total_lines, f"INCOMPLETE: {file_date}")
        print("", flush=True)  # Add empty line
        print(f"\n[STATUS] Incomplete: processed {processed_entries}/{total_lines} entries", flush=True)

def _process_normal_mode(conn, cursor, jsonl_path, total_lines, commodity_map, auto_commit):
    """Normal processing mode with full change tracking"""
    # Stats for progress reporting
    processed_entries = 0
    stations_found = 0
    stations_with_changes = 0
    total_stations = 0
    total_updated = 0
    total_added = 0
    total_deleted = 0
    
    # Batch processing
    pending_changes = []
    schema_ref = "https://eddn.edcd.io/schemas/commodity/3"
    
    update_status("processing", 0, total_lines, "Reading entries and checking for changes...")
     
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            processed_entries += 1
            if processed_entries % 1000 == 0:  # Only update every 1000 entries
                update_status("processing", processed_entries, total_lines, 
                            f"Processed {processed_entries}/{total_lines} entries - Found {stations_found} stations, {stations_with_changes} with changes")
            
            try:
                entry = msgspec.json.decode(line)
                if entry.schemaRef.lower() != schema_ref.lower():
                    continue
                
                message = entry.message
                market_id = message.get("marketId")
                station_name = message.get("stationName")
                timestamp = message.get("timestamp")
                
                if not market_id and not station_name:
                    continue
                
                # Convert timestamp
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    update_time = dt.strftime("%Y-%m-%d %H:%M:%S+00")
                except (ValueError, AttributeError):
                    update_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")
                
                # Try to find station by marketId first
                if market_id:
                    cursor.execute("SELECT station_id FROM stations WHERE station_name = ?", (station_name,))
                    station_row = cursor.fetchone()
                    
                    # TEMPORARY FIX - REMOVE ONCE DATABASE IS CORRECTED
                    if station_row and station_row[0] is None:  # Found station but station_id is NULL
                        cursor.execute("""
                            UPDATE stations 
                            SET station_id = ? 
                            WHERE station_name = ?
                        """, (market_id, station_name))
                        conn.commit()
                        station_row = (market_id,)  # Use the marketId as station_id
                
                # If not found by marketId, try by station name
                if not station_row and station_name:
                    cursor.execute("SELECT station_id FROM stations WHERE station_name = ?", (station_name,))
                    station_row = cursor.fetchone()
                
                if not station_row:
                    continue
                
                stations_found += 1
                station_id = station_row[0]
                
                # Get current commodities for this station
                cursor.execute("""
                    SELECT commodity_name, sell_price, demand
                    FROM station_commodities
                    WHERE station_id = ?
                """, (station_id,))
                
                old_commodities = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
                new_commodities = {}
                
                # Process ALL commodities that match our mining list
                for commodity in message.get("commodities", []):
                    eddn_name = commodity.get("name", "").lower()
                    if eddn_name not in commodity_map:
                        continue
                    
                    local_name = commodity_map[eddn_name]
                    sell_price = commodity.get("sellPrice", 0)
                    demand = commodity.get("demand", 0)
                    new_commodities[local_name] = (sell_price, demand)  # Store ALL matching commodities
                
                # Compare and record changes
                updated_list = []
                added_list = []
                deleted_list = []
                
                # Check for updates and deletions
                for name, (old_price, old_demand) in old_commodities.items():
                    if name not in new_commodities:
                        deleted_list.append((name, old_price, old_demand))
                    else:
                        new_price, new_demand = new_commodities[name]
                        if new_price != old_price or new_demand != old_demand:
                            updated_list.append((name, old_price, old_demand, new_price, new_demand))
                
                # Check for additions
                for name, (price, demand) in new_commodities.items():
                    if name not in old_commodities:
                        added_list.append((name, price, demand))
                
                if updated_list or added_list or deleted_list:
                    stations_with_changes += 1
                    
                    pending_changes.append({
                        "station_id": station_id,
                        "station_name": station_name,
                        "new_data": new_commodities,
                        "update_time": update_time
                    })
                    
                    total_stations += 1
                    total_updated += len(updated_list)
                    total_added += len(added_list)
                    total_deleted += len(deleted_list)
                    
                    # Write to database every 50 stations with changes
                    if len(pending_changes) >= 50:
                        update_status("processing", processed_entries, total_lines, f"Writing batch of {len(pending_changes)} stations...")
                        for change in pending_changes:
                            _update_station(cursor, change)
                        conn.commit()
                        update_status("processing", processed_entries, total_lines, "Changes committed to database")
                        pending_changes = []
                
            except msgspec.ValidationError:
                continue
            except Exception as e:
                continue
    
    # Write any remaining changes
    if pending_changes:
        update_status("processing", total_lines, total_lines, f"Writing final batch of {len(pending_changes)} stations...")
        for change in pending_changes:
            _update_station(cursor, change)
        conn.commit()
        update_status("processing", total_lines, total_lines, "Changes committed to database")
    
    # Print final summary
    if total_stations > 0:
        update_status("processing", total_lines, total_lines, 
                     f"Daily Update Complete - {total_stations} stations updated: {total_updated} updated, {total_added} added, {total_deleted} deleted")
    else:
        update_status("processing", total_lines, total_lines, "Daily Update Complete - No changes were made to the database")

def _update_station(cursor, change):
    """Update a single station in the database"""
    station_id = change["station_id"]
    station_name = change["station_name"]
    
    # Get system_id64 from stations table
    cursor.execute("""
        SELECT system_id64
        FROM stations
        WHERE station_name = ? AND station_id = ?
    """, (station_name, station_id))
    row = cursor.fetchone()
    if not row:
        print(f"[ERROR] Could not find system_id64 for station {station_name}", flush=True)
        return
    system_id = row[0]
    
    # Delete old commodities
    cursor.execute("DELETE FROM station_commodities WHERE system_id64 = ? AND station_name = ?", 
                  (system_id, station_name))
    
    # Insert ALL commodities
    cursor.executemany("""
        INSERT INTO station_commodities 
            (system_id64, station_id, station_name, commodity_name, sell_price, demand)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [(system_id, station_id, station_name, commodity_name, data[0], data[1]) 
          for commodity_name, data in change["new_data"].items()])
    
    # Update station timestamp
    cursor.execute("""
        UPDATE stations
        SET update_time = ?
        WHERE system_id64 = ? AND station_id = ?
    """, (change["update_time"], system_id, station_id))

def flush_commodities_to_db(conn, commodity_buffer, reverse_map, auto_commit=False, fast_mode=False):
    """
    Compares old DB data vs new EDDN data for each station.
    Returns (stations_updated, commodities_updated) tuple.
    """
    if not commodity_buffer:
        return 0, 0

    cursor = conn.cursor()
    changes = {}
    total_commodities = 0
    stations_processed = 0
    total_stations = len(commodity_buffer)

    for station_name, new_map in commodity_buffer.items():
        stations_processed += 1
        if stations_processed % 10 == 0:
            print(f"[PROCESS] {stations_processed} of {total_stations} entries processed", flush=True)

        # Step 1: lookup station in 'stations'
        cursor.execute("""
            SELECT system_id64, station_id
              FROM stations
             WHERE station_name = ?
        """, (station_name,))
        row = cursor.fetchone()
        if not row:
            continue
        system_id64, st_id = row

        if fast_mode:
            # In fast mode, just store the new data for update
            changes[station_name] = {
                "system_id64": system_id64,
                "station_id": st_id,
                "station_name": station_name,
                "new_data": new_map
            }
            total_commodities += len(new_map)
            print(f"[FOUND] [station_id: {st_id}] Changes in {station_name}: {len(new_map)} entries", flush=True)
        else:
            # Normal mode with full comparison
            cursor.execute("""
                SELECT commodity_name, sell_price, demand
                  FROM station_commodities
                 WHERE system_id64 = ?
                   AND station_name = ?
            """, (system_id64, station_name))
            old_rows = cursor.fetchall()
            old_map = {r[0]: (r[1], r[2]) for r in old_rows}

            added_list = []
            updated_list = []
            deleted_list = []

            for old_name, (old_sell, old_demand) in old_map.items():
                if old_name not in new_map:
                    old_id = reverse_map.get(old_name, "???")
                    deleted_list.append((old_id, old_name, old_sell, old_demand))
                else:
                    (new_id, new_sell, new_demand) = new_map[old_name]
                    if new_sell != old_sell or new_demand != old_demand:
                        updated_list.append((new_id, old_name, old_sell, old_demand, new_sell, new_demand))

            for local_name, (eddn_id, s_price, dem) in new_map.items():
                if local_name not in old_map:
                    added_list.append((eddn_id, local_name, s_price, dem))

            if added_list or updated_list or deleted_list:
                print(f"[FOUND] [station_id: {st_id}] Changes in {station_name}: {len(updated_list)} updated, {len(added_list)} added, {len(deleted_list)} deleted", flush=True)
                changes[station_name] = {
                    "system_id64": system_id64,
                    "station_id": st_id,
                    "station_name": station_name,
                    "new_data": new_map
                }
                total_commodities += len(new_map)

    if not changes:
        print("\nNo commodity changes to commit.")
        commodity_buffer.clear()
        return 0, 0

    # Step 4: commit changes
    should_commit = auto_commit
    if not auto_commit:
        ans = input("Commit these commodity changes? [Y/N] ").strip().lower()
        should_commit = ans == "y"

    stations_updated = 0
    if should_commit:
        try:
            print(f"[DATABASE] Writing {len(changes)} station updates...", flush=True)
            stations_processed = 0
            
            for station_name, info in changes.items():
                _update_station(cursor, info)
                stations_processed += 1
                if stations_processed % 10 == 0:
                    print(f"[DATABASE] {stations_processed} of {len(changes)} entries saved", flush=True)
                stations_updated += 1

            conn.commit()
            print("[DATABASE] All station updates saved", flush=True)
            print(f"[SUCCESS] {stations_updated} entries in DB updated", flush=True)
            
        except Exception as ex:
            conn.rollback()
            print(f"Error while committing commodity changes: {ex}", file=sys.stderr)
            return 0, 0
    else:
        conn.rollback()
        if not auto_commit:
            print("Commodity changes rolled back.")

    cursor.close()
    commodity_buffer.clear()
    return stations_updated, total_commodities

def load_status():
    """Load the status from daily_update_status.json"""
    try:
        # Ensure JSON directory exists
        if not os.path.exists(JSON_DIR):
            os.makedirs(JSON_DIR)
        
        # Create default status with today's date in YYYY-MM-DD format
        today = datetime.now(timezone.utc)
        default_status = {
            "last_update": today.strftime("%Y-%m-%d"),
            "last_file": None,
            "completed": False,
            "error": None,
            "processed_entries": 0,
            "total_entries": 0
        }
        
        # Create file if it doesn't exist
        if not os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, 'w') as f:
                json.dump(default_status, f, indent=2)
            return default_status
            
        # Read existing file
        with open(STATUS_FILE, 'r') as f:
            return json.load(f)
            
    except Exception as e:
        print(f"Error handling status file: {e}", file=sys.stderr)
        return default_status

def save_status(status_data):
    """Save the status to daily_update_status.json"""
    try:
        # Ensure JSON directory exists
        if not os.path.exists(JSON_DIR):
            os.makedirs(JSON_DIR)
            
        with open(STATUS_FILE, 'w') as f:
            json.dump(status_data, f, indent=2)
    except Exception as e:
        print(f"Error saving status file: {e}", file=sys.stderr)

def handle_output_stream(pipe, stream_name):
    try:
        with io.TextIOWrapper(pipe, encoding='utf-8', errors='replace') as text_pipe:
            while True:
                line = text_pipe.readline()
                if not line:
                    break
                if line.strip():  # Only process non-empty lines
                    handle_daily_output(line.strip())
    except Exception as e:
        print(f"Error in daily update {stream_name} stream: {e}", file=sys.stderr)

def try_download_recent_file():
    """Try to download the most recent available file from the past 7 days"""
    today = datetime.now(timezone.utc)
    
    # Try each of the past 7 days
    for i in range(7):
        date = today - timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        year_month = date.strftime("%Y-%m")
        jsonl_path = os.path.join(JSON_DIR, f"Commodity-{date_str}.jsonl")
        remote_url = f"{BASE_URL}/{year_month}/Commodity-{date_str}.jsonl.bz2"
        
        print(f"{ORANGE}[STATUS] Checking for data from {date_str}{RESET}", flush=True)
        
        # Check if file exists on server
        response = requests.head(remote_url)
        if response.status_code == 200:
            print(f"{ORANGE}[STATUS] Found data for {date_str}, downloading...{RESET}", flush=True)
            bz2_path = jsonl_path + '.bz2'
            return download_and_extract(remote_url, jsonl_path, bz2_path), date_str
    
    return None, None

def main():
    parser = argparse.ArgumentParser(description='Daily EDDN data updater for Power Mining')
    parser.add_argument('--auto', action='store_true', help='Automatically commit changes without asking')
    parser.add_argument('--fast', action='store_true', help='Use fast mode without detailed change tracking')
    parser.add_argument('--force', action='store_true', help='Force update even if already completed today')
    args = parser.parse_args()
    
    try:
        # Load current status
        status_data = load_status()
        
        # Get current PID
        current_pid = os.getpid()
        update_status("processing", message=f"Starting Daily EDDN Update (PID: {current_pid})")
        print("", flush=True)  # Add empty line after status message
        
        # Load commodity mapping
        commodity_map = load_commodity_map()
        
        # Find latest file
        jsonl_path, source, date_str = find_latest_file()
        
        # Check if we've already completed this update
        if not args.force and status_data["completed"] and status_data["last_file"] == jsonl_path:
            print(f"{GREEN}[STATUS] Already completed update of {date_str} (last file: {status_data['last_file']}){RESET}", flush=True)
            status_msg = f"EDDN data: {date_str}"
            update_status("updated", message=status_msg)
            print(f"\n[COMPLETED] Daily update already done", flush=True)
            return 0
        
        # If we have a local file but it's incomplete
        if jsonl_path and os.path.exists(jsonl_path) and not status_data["completed"]:
            print(f"{YELLOW}[STATUS] Found incomplete update for {date_str}, resuming...{RESET}", flush=True)
            print(f"{YELLOW}[STATUS] Processed {status_data['processed_entries']}/{status_data['total_entries']} entries{RESET}", flush=True)
            process_jsonl(jsonl_path, commodity_map, args.auto, args.fast)
            return 0
            
        if not jsonl_path and date_str:  # Data is already up to date
            if not args.force and status_data["completed"]:
                # Only skip if completed is true
                status_msg = f"EDDN data: {date_str}"
                update_status("updated", message=status_msg)
                print(f"\n[COMPLETED] Daily update already done", flush=True)
                return 0
            else:
                # If not completed or force flag is set, run the update
                print(f"{ORANGE}[STATUS] Running update for {date_str} (completed={status_data['completed']}){RESET}", flush=True)
                
                # Find the most recent JSONL file in the json directory
                existing_files = [f for f in os.listdir(JSON_DIR) if f.endswith('.jsonl')]
                if existing_files:
                    # Sort files by date in filename
                    latest_file = sorted(existing_files, reverse=True)[0]
                    jsonl_path = os.path.join(JSON_DIR, latest_file)
                    print(f"{ORANGE}[STATUS] Using existing file: {jsonl_path}{RESET}", flush=True)
                    
                    # Process the file
                    process_jsonl(jsonl_path, commodity_map, args.auto, args.fast)
                    return 0
        
        if not jsonl_path:
            # First check for any existing files in the json directory
            existing_files = [f for f in os.listdir(JSON_DIR) if f.endswith('.jsonl')]
            if existing_files:
                # Sort files by date in filename
                latest_file = sorted(existing_files, reverse=True)[0]
                jsonl_path = os.path.join(JSON_DIR, latest_file)
                print(f"{ORANGE}[STATUS] Found existing file: {jsonl_path}{RESET}", flush=True)
                
                # Process the file
                process_jsonl(jsonl_path, commodity_map, args.auto, args.fast)
                return 0
            
            # If no local files found, try to download recent data
            print(f"{ORANGE}[STATUS] No local files found. Checking server for recent data...{RESET}", flush=True)
            jsonl_path, found_date = try_download_recent_file()
            if not jsonl_path:
                error_msg = "No commodity files found on server for the past 7 days"
                update_status("error", message=error_msg)
                status_data.update({
                    "last_update": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "last_file": None,
                    "completed": False,
                    "error": error_msg
                })
                save_status(status_data)
                return 1
        
        # Handle the file based on what we found
        if source is None and os.path.exists(jsonl_path):
            # We have the uncompressed file already
            if not args.auto:
                print(f"{ORANGE}[PROMPT] JSONL file already exists. Update again? (y/n):{RESET}", flush=True)
                response = input().lower()
                if response != 'y':
                    update_status("offline", message="Update cancelled")
                    
                    # Update status file
                    status_data.update({
                        "last_update": date_str,  # Just store YYYY-MM-DD
                        "last_file": None,
                        "completed": False,
                        "error": "Update cancelled by user"
                    })
                    save_status(status_data)
                    return 1
        elif source and source.endswith('.jsonl'):
            # We need to download an uncompressed file
            print(f"{ORANGE}[STATUS] Downloading uncompressed file from server...{RESET}", flush=True)
            jsonl_path = download_and_extract(source, jsonl_path, None)
        elif source and source.endswith('.bz2'):
            # We need to download and extract a compressed file
            print(f"{ORANGE}[STATUS] Downloading compressed file from server...{RESET}", flush=True)
            bz2_path = jsonl_path + '.bz2'
            jsonl_path = download_and_extract(source, jsonl_path, bz2_path)
        
        # Process the file
        process_jsonl(jsonl_path, commodity_map, args.auto, args.fast)
        
        return 0
        
    except KeyboardInterrupt:
        error_msg = "Daily update cancelled by user"
        update_status("offline", message=error_msg)
        
        # Update status file with interruption
        status_data.update({
            "last_update": datetime.now(timezone.utc).isoformat(),
            "last_file": jsonl_path if 'jsonl_path' in locals() else None,
            "completed": False,
            "error": error_msg
        })
        save_status(status_data)
        return 1
        
    except Exception as ex:
        # Clean error message (without ANSI codes)
        error_msg = str(ex)
        update_status("error", message=error_msg)
        
        # Update status file with error
        status_data.update({
            "last_update": datetime.now(timezone.utc).isoformat(),
            "last_file": jsonl_path if 'jsonl_path' in locals() else None,
            "completed": False,
            "error": error_msg
        })
        save_status(status_data)
        return 1
        
    finally:
        sys.stdout.write('\n')  # Ensure we're on a new line
        sys.stdout.flush()

if __name__ == '__main__':
    sys.exit(main()) 