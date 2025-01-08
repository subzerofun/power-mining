import os
import sys
import sqlite3
import json
import zlib
import time
import signal
import argparse
from datetime import datetime, timezone
import csv
import msgspec

# ANSI color codes
YELLOW = '\033[93m'
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
RESET = '\033[0m'

# Constants
DB_PATH = "systems.db"
COMMODITIES_CSV = os.path.join("data", "commodities_mining.csv")

# Global state
running = True
commodity_buffer = {}
commodity_map = {}
reverse_map = {}

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global running
    print(f"\n{YELLOW}[STOPPING] Received signal to stop{RESET}", flush=True)
    running = False

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def load_commodity_map():
    """Load commodity mapping from CSV"""
    print(f"{BLUE}[INIT] Loading commodity mapping from {COMMODITIES_CSV}{RESET}", flush=True)
    commodity_map = {}
    reverse_map = {}
    with open(COMMODITIES_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            eddn_id = row["id"].strip()
            local_name = row["name"].strip()
            # Special case: store as "Void Opal" but handle both forms
            if local_name == "Void Opals":
                local_name = "Void Opal"
            commodity_map[eddn_id] = local_name
            reverse_map[local_name] = eddn_id
    print(f"{GREEN}[INIT] Loaded {len(commodity_map)} commodities from CSV{RESET}", flush=True)
    return commodity_map, reverse_map

def flush_commodities_to_db(conn, commodity_buffer, auto_commit=False):
    """Write buffered commodities to database"""
    if not commodity_buffer:
        return 0, 0

    cursor = conn.cursor()
    total_commodities = 0
    stations_processed = 0
    total_stations = len(commodity_buffer)

    try:
        print(f"{YELLOW}[DATABASE] Writing to Database starting...{RESET}", flush=True)
        
        # Enable WAL mode and set pragmas for better write performance
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA temp_store=MEMORY")
        cursor.execute("PRAGMA cache_size=10000")
        
        # Process each station's commodities
        for station_name, new_map in commodity_buffer.items():
            stations_processed += 1
            
            # Get station info
            cursor.execute("""
                SELECT system_id64, station_id
                FROM stations
                WHERE station_name = ?
            """, (station_name,))
            row = cursor.fetchone()
            if not row:
                continue
                
            system_id64, station_id = row
            
            # Delete existing commodities
            cursor.execute("""
                DELETE FROM station_commodities 
                WHERE system_id64 = ? AND station_name = ?
            """, (system_id64, station_name))
            
            # Insert new commodities
            cursor.executemany("""
                INSERT INTO station_commodities 
                    (system_id64, station_id, station_name, commodity_name, sell_price, demand)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [(system_id64, station_id, station_name, commodity_name, data[0], data[1]) 
                  for commodity_name, data in new_map.items()])
            
            # Update station timestamp
            cursor.execute("""
                UPDATE stations
                SET update_time = ?
                WHERE system_id64 = ? AND station_id = ?
            """, (datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00"), system_id64, station_id))
            
            total_commodities += len(new_map)
            
            # Commit every 10 stations
            if stations_processed % 10 == 0:
                conn.commit()
                print(f"{YELLOW}[DATABASE] Processed {stations_processed} of {total_stations} stations{RESET}", flush=True)

        # Final commit
        conn.commit()
        print(f"{GREEN}[DATABASE] Writing to Database finished. Updated {stations_processed} stations with {total_commodities} commodities{RESET}", flush=True)
        
    except Exception as e:
        print(f"{RED}[ERROR] Database error: {str(e)}{RESET}", file=sys.stderr)
        conn.rollback()
        return 0, 0
        
    finally:
        cursor.close()
        commodity_buffer.clear()
        
    return stations_processed, total_commodities

def process_message(message, commodity_map):
    """Process a single EDDN message"""
    try:
        # Skip fleet carriers
        if message.get("stationType") == "FleetCarrier" or \
           (message.get("economies") and message["economies"][0].get("name") == "Carrier"):
            return None, None
            
        station_name = message.get("stationName")
        if not station_name:
            return None, None
            
        # Process commodities
        station_commodities = {}
        for commodity in message.get("commodities", []):
            name = commodity.get("name", "").lower()
            if not name or name not in commodity_map:
                continue
                
            sell_price = commodity.get("sellPrice", 0)
            if sell_price <= 0:
                continue
                
            demand = commodity.get("demand", 0)
            station_commodities[commodity_map[name]] = (sell_price, demand)
            
        if station_commodities:
            return station_name, station_commodities
            
    except Exception as e:
        print(f"{RED}[ERROR] Error processing message: {str(e)}{RESET}", file=sys.stderr)
        
    return None, None

def main():
    """Main function"""
    global running, commodity_buffer, commodity_map, reverse_map
    
    parser = argparse.ArgumentParser(description='EDDN Live Update Service')
    parser.add_argument('--auto', action='store_true', help='Automatically commit changes')
    args = parser.parse_args()
    
    try:
        # Load commodity mapping
        commodity_map, reverse_map = load_commodity_map()
        
        # Connect to database
        conn = sqlite3.connect(DB_PATH)
        
        # Create message decoder
        decoder = msgspec.json.Decoder()
        
        print(f"{GREEN}[CONNECTED] Connected to EDDN{RESET}", flush=True)
        
        last_flush = time.time()
        messages_processed = 0
        
        while running:
            try:
                # Process messages from stdin
                line = sys.stdin.readline()
                if not line:
                    break
                    
                # Parse message
                data = decoder.decode(line.encode())
                if not isinstance(data, dict):
                    continue
                    
                # Check schema
                schema = data.get("$schemaRef", "")
                if "commodity/3" not in schema.lower():
                    continue
                    
                # Process message
                station_name, commodities = process_message(data.get("message", {}), commodity_map)
                if station_name and commodities:
                    commodity_buffer[station_name] = commodities
                    messages_processed += 1
                    
                    # Print status every 100 messages
                    if messages_processed % 100 == 0:
                        print(f"{YELLOW}[STATUS] Processed {messages_processed} messages{RESET}", flush=True)
                    
                    # Flush to database every 5 minutes or 1000 messages
                    if (time.time() - last_flush >= 300) or len(commodity_buffer) >= 1000:
                        print(f"{YELLOW}[UPDATING] Writing {len(commodity_buffer)} stations to database...{RESET}", flush=True)
                        stations, commodities = flush_commodities_to_db(conn, commodity_buffer, args.auto)
                        if stations > 0:
                            print(f"{GREEN}[UPDATED] Wrote {commodities} commodities for {stations} stations{RESET}", flush=True)
                        last_flush = time.time()
                        
            except Exception as e:
                print(f"{RED}[ERROR] Error processing message: {str(e)}{RESET}", file=sys.stderr)
                continue
                
        # Final flush on exit
        if commodity_buffer:
            print(f"{YELLOW}[STATUS] Final database update...{RESET}", flush=True)
            stations, commodities = flush_commodities_to_db(conn, commodity_buffer, args.auto)
            if stations > 0:
                print(f"{GREEN}[UPDATED] Wrote {commodities} commodities for {stations} stations{RESET}", flush=True)
                
    except Exception as e:
        print(f"{RED}[ERROR] Fatal error: {str(e)}{RESET}", file=sys.stderr)
        return 1
        
    finally:
        print(f"{YELLOW}[TERMINATED] EDDN updater stopped{RESET}", flush=True)
        
    return 0

if __name__ == '__main__':
    sys.exit(main()) 