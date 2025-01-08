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
        print(f"\r{ORANGE}[STATUS] {progress}% {state.capitalize()}{RESET}", end="", flush=True)
    elif state == "processing" and progress is not None and total is not None:
        print(f"\r{ORANGE}[STATUS] Processing entries ({min(progress, total)}/{total}){RESET}", end="", flush=True)
    elif message:
        print(f"\n{ORANGE}[STATUS] {message}{RESET}", flush=True)

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
    
    # Check if we already have today's data
    today_str = today.strftime("%Y-%m-%d")
    
    # Check if we have a local file first (try both naming patterns)
    local_file_patterns = [
        os.path.join(JSON_DIR, f"commodities_{today_str}.jsonl"),
        os.path.join(JSON_DIR, f"Commodity-{today_str}.jsonl")
    ]
    
    for local_file in local_file_patterns:
        if os.path.exists(local_file):
            print(f"{GREEN}[STATUS] Database already up to date with {today_str} data{RESET}", flush=True)
            return None, None, today_str
    
    # Look for files from the past 7 days
    for i in range(7):
        date = today - timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        
        # Check for local files first (try both naming patterns)
        local_file_patterns = [
            os.path.join(JSON_DIR, f"commodities_{date_str}.jsonl"),
            os.path.join(JSON_DIR, f"Commodity-{date_str}.jsonl")
        ]
        
        for local_file in local_file_patterns:
            if os.path.exists(local_file):
                return local_file, None, date_str
            
            local_bz2 = local_file + '.bz2'
            if os.path.exists(local_bz2):
                return local_file, local_bz2, date_str
        
        # Check remote URL
        remote_url = f"{BASE_URL}/commodities_{date_str}.jsonl.bz2"
        response = requests.head(remote_url)
        if response.status_code == 200:
            return local_file_patterns[0], remote_url, date_str
            
    return None, None, None

def download_and_extract(url, jsonl_path, bz2_path):
    """Download and extract the bz2 file"""
    if not os.path.exists(JSON_DIR):
        os.makedirs(JSON_DIR)
    
    update_status("downloading", 0, message="Starting download...")
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    
    with open(bz2_path, 'wb') as f:
        if total_size == 0:
            f.write(response.content)
        else:
            downloaded = 0
            for data in response.iter_content(chunk_size=8192):
                downloaded += len(data)
                f.write(data)
                percent = min(100, int(100 * downloaded / total_size))
                update_status("downloading", percent)
    
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
    with bz2.open(bz2_path, 'rb') as source, open(jsonl_path, 'wb') as target:
        decompressor = bz2.BZ2Decompressor()
        extracted = 0
        for data in iter(lambda: source.read(8192), b''):
            extracted += len(data)
            target.write(data)
            percent = min(100, int(100 * extracted / compressed_size))
            update_status("extracting", percent)
    
    # Delete the bz2 file
    try:
        os.remove(bz2_path)
    except Exception as e:
        print(f"Error deleting bz2 file: {e}", file=sys.stderr)
    
    update_status("processing", message="File extracted and ready for processing")
    return jsonl_path

def process_jsonl(jsonl_path, commodity_map, auto_commit=False, fast_mode=False):
    """Process the JSONL file and update the database"""
    update_status("processing", message="Counting valid entries...")
    
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
    
    # Load current status
    status_data = load_status()
    
    # Check if we need to resume (only if we have processed entries but haven't completed)
    if status_data["processed_entries"] > 0 and status_data["processed_entries"] < status_data["total_entries"]:
        resume_from = max(0, status_data["processed_entries"] - 1000)  # Roll back 1000 entries for safety
        print(f"{YELLOW}[STATUS] Previous update incomplete. Processed {status_data['processed_entries']}/{status_data['total_entries']}{RESET}", flush=True)
        print("")  # Add empty line between messages
    else:
        resume_from = 0
    
    # Enable WAL mode and set pragmas for better write performance
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA temp_store=MEMORY")
    cursor.execute("PRAGMA cache_size=10000")
    
    # Only delete if this is a fresh start (not resuming)
    if resume_from == 0:
        update_status("deleting", message="Deleting existing commodities...")
        cursor.execute("DELETE FROM station_commodities")
        conn.commit()
        print("", flush=True)  # Add empty line after delete message
    else:
        print(f"{YELLOW}[STATUS] Resuming from entry {resume_from} (rolling back 1000 entries for safety){RESET}", flush=True)
        print(f"{YELLOW}[STATUS] Keeping existing data and continuing from last position{RESET}", flush=True)
    
    # Process in chunks
    commodities_by_station = {}  # Store commodities per station
    updated_stations = {}  # Store update_time per station
    schema_ref = "https://eddn.edcd.io/schemas/commodity/3"
    
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
            
            try:
                entry = decoder.decode(line)
                if entry.schemaRef.lower() != schema_ref.lower():
                    continue
                
                message = entry.message
                station_name = message.get("stationName")
                system_name = message.get("systemName")  # Add system name check
                market_id = message.get("marketId")      # Add market ID check
                timestamp = message.get("timestamp")
                
                if not station_name or not system_name or not timestamp:  # Require both station and system names
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
                
                # Now get station info using system_id64
                if market_id:
                    # Try by market ID first
                    cursor.execute("""
                        SELECT station_id 
                        FROM stations
                        WHERE system_id64 = ? AND station_id = ?
                    """, (system_id64, market_id))
                else:
                    # Try by station name
                    cursor.execute("""
                        SELECT station_id
                        FROM stations
                        WHERE system_id64 = ? AND station_name = ?
                    """, (system_id64, station_name))
                
                row = cursor.fetchone()
                if not row:
                    continue
                
                station_id = row[0]
                
                # Initialize commodities list for this station if not exists
                station_key = f"{system_id64}:{station_name}"  # Use composite key to ensure uniqueness
                if station_key not in commodities_by_station:
                    commodities_by_station[station_key] = []
                updated_stations[station_key] = (update_time, system_id64, station_id, station_name)
                
                # Process commodities for this station
                for commodity in message.get("commodities", []):
                    name = commodity.get("name", "").lower()
                    if not name or name not in commodity_map:
                        continue
                    
                    sell_price = commodity.get("sellPrice", 0)
                    if sell_price <= 0:
                        continue
                    
                    demand = commodity.get("demand", 0)
                    commodities_by_station[station_key].append((
                        system_id64, station_id, station_name,
                        commodity_map[name], sell_price, demand
                    ))
                
                # Save every 500 stations
                if len(commodities_by_station) >= 500:
                    stations_updated += len(commodities_by_station)
                    batch_commodities = sum(len(comms) for comms in commodities_by_station.values())
                    total_commodities += batch_commodities
                    
                    # Show batch save status
                    update_status("processing", processed_entries, total_lines, 
                                f"Saving batch: {batch_commodities} commodities for {len(commodities_by_station)} stations...")
                    
                    # Insert commodities for each station
                    for station_key, commodities in commodities_by_station.items():
                        cursor.executemany("""
                            INSERT INTO station_commodities 
                                (system_id64, station_id, station_name, commodity_name, sell_price, demand)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, commodities)
                    
                    # Update timestamps
                    for station_key, (timestamp, sys_id, st_id, st_name) in updated_stations.items():
                        cursor.execute("""
                            UPDATE stations 
                            SET update_time = ? 
                            WHERE system_id64 = ? AND station_id = ? AND station_name = ?
                        """, (timestamp, sys_id, st_id, st_name))
                    
                    conn.commit()
                    
                    # Show completion
                    update_status("processing", processed_entries, total_lines, "Batch saved successfully")
                    
                    commodities_by_station.clear()
                    updated_stations.clear()
                
            except msgspec.ValidationError:
                continue
            except Exception as ex:
                continue
    
    # Save any remaining data
    if commodities_by_station:
        stations_updated += len(commodities_by_station)
        batch_commodities = sum(len(comms) for comms in commodities_by_station.values())
        total_commodities += batch_commodities
        
        # Show final batch save status
        update_status("processing", processed_entries, total_lines, 
                     f"Saving final batch: {batch_commodities} commodities for {len(commodities_by_station)} stations...")
        
        # Insert remaining commodities for each station
        for station_key, commodities in commodities_by_station.items():
            cursor.executemany("""
                INSERT INTO station_commodities 
                    (system_id64, station_id, station_name, commodity_name, sell_price, demand)
                VALUES (?, ?, ?, ?, ?, ?)
            """, commodities)
        
        # Update remaining timestamps
        for station_key, (timestamp, sys_id, st_id, st_name) in updated_stations.items():
            cursor.execute("""
                UPDATE stations 
                SET update_time = ? 
                WHERE system_id64 = ? AND station_id = ? AND station_name = ?
            """, (timestamp, sys_id, st_id, st_name))
        
        conn.commit()
        
        # Show completion
        update_status("processing", processed_entries, total_lines, "Final batch saved successfully")
    
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
                    
                    # No need for additional status updates here - process_jsonl handles it all
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
        elif os.path.exists(source):
            # We have a local bz2 file that needs extraction
            update_status("extracting", 0, message=f"Extracting existing {source}")
            with bz2.open(source, 'rb') as source_file, open(jsonl_path, 'wb') as target:
                target.write(source_file.read())
            os.remove(source)  # Delete the bz2 file after extraction
            update_status("processing", message=f"Extracted to {jsonl_path}")
        else:
            # We need to download the file
            bz2_path = jsonl_path + '.bz2'
            jsonl_path = download_and_extract(source, jsonl_path, bz2_path)
        
        # Process the file
        process_jsonl(jsonl_path, commodity_map, args.auto, args.fast)
        
        # No need for additional status updates here - process_jsonl handles it all
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