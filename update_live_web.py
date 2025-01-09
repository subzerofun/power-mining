import os
import sys
import json
import zlib
import time
import signal
import argparse
from datetime import datetime, timezone
import csv
import msgspec
import psycopg2
import zmq
from psycopg2.extras import DictCursor

# ANSI color codes
YELLOW = '\033[93m'
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
RESET = '\033[0m'

# Constants
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://wlwrgnlxqabffodl:ageujqzrvdqoqfid@138.199.149.152:8001/elyztbqfgbdsjtnl')
COMMODITIES_CSV = os.path.join("data", "commodities_mining.csv")
EDDN_RELAY = "tcp://eddn.edcd.io:9500"

# How often (in seconds) to flush changes to DB
DB_UPDATE_INTERVAL = 10

# Debug flag for detailed commodity changes
DEBUG = False

# Global state
running = True
commodity_buffer = {}
commodity_map = {}
reverse_map = {}

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global running
    print(f"\n{YELLOW}[STOPPING] EDDN Update Service{RESET}", flush=True)
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
    print(f"{GREEN}[INIT] Loaded {len(commodity_map)} commodities from CSV (mapping EDDN ID -> local name){RESET}", flush=True)
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
        
        # Process each station's commodities
        for station_name, new_map in commodity_buffer.items():
            stations_processed += 1
            
            # Get station info
            cursor.execute("""
                SELECT system_id64, station_id
                FROM stations
                WHERE station_name = %s
            """, (station_name,))
            row = cursor.fetchone()
            if not row:
                continue
                
            system_id64, station_id = row
            
            # Delete existing commodities
            cursor.execute("""
                DELETE FROM station_commodities 
                WHERE system_id64 = %s AND station_name = %s
            """, (system_id64, station_name))
            
            # Insert new commodities
            cursor.executemany("""
                INSERT INTO station_commodities 
                    (system_id64, station_id, station_name, commodity_name, sell_price, demand)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, [(system_id64, station_id, station_name, commodity_name, data[0], data[1]) 
                  for commodity_name, data in new_map.items()])
            
            # Update station timestamp
            cursor.execute("""
                UPDATE stations
                SET update_time = %s
                WHERE system_id64 = %s AND station_id = %s
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
            if DEBUG:
                print(f"{YELLOW}[DEBUG] Skipped Fleet Carrier Data: {message.get('stationName')}{RESET}", flush=True)
            return None, None
            
        station_name = message.get("stationName")
        market_id = message.get("marketId")
        
        if market_id is None and DEBUG:
            print(f"{YELLOW}[DEBUG] Live update without marketId: {station_name}{RESET}", flush=True)
        
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
            station_commodities[commodity_map[name]] = (sell_price, demand, market_id)
            
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
        print(f"{BLUE}[INIT] Starting Live EDDN Update every {DB_UPDATE_INTERVAL} seconds{RESET}", flush=True)
        
        # Load commodity mapping
        commodity_map, reverse_map = load_commodity_map()
        
        # Connect to database
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False  # We'll manage transactions manually
        
        # Create message decoder
        decoder = msgspec.json.Decoder()
        
        # Connect to EDDN
        context = zmq.Context()
        subscriber = context.socket(zmq.SUB)
        subscriber.connect(EDDN_RELAY)
        subscriber.setsockopt_string(zmq.SUBSCRIBE, "")  # subscribe to all messages
        
        print(f"{GREEN}[CONNECTED] Listening to EDDN. Flush changes every {DB_UPDATE_INTERVAL}s. (Press Ctrl+C to stop){RESET}", flush=True)
        print(f"{BLUE}Mode: {'automatic' if args.auto else 'manual'}{RESET}", flush=True)
        
        last_flush = time.time()
        messages_processed = 0
        
        while running:
            try:
                # Get message from EDDN
                raw_msg = subscriber.recv()
                message = zlib.decompress(raw_msg)
                data = decoder.decode(message)
                
                # Check schema
                schema = data.get("$schemaRef", "").lower()
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
                    
                    # Flush to database every DB_UPDATE_INTERVAL seconds
                    current_time = time.time()
                    if current_time - last_flush >= DB_UPDATE_INTERVAL:
                        print(f"{YELLOW}[DATABASE] Writing to Database starting...{RESET}", flush=True)
                        stations, commodities = flush_commodities_to_db(conn, commodity_buffer)
                        if stations > 0:
                            print(f"{GREEN}[DATABASE] Writing to Database finished. Updated {stations} stations with {commodities} commodities{RESET}", flush=True)
                        last_flush = current_time
                        
            except Exception as e:
                print(f"{RED}[ERROR] Error processing message: {str(e)}{RESET}", file=sys.stderr)
                continue
                    
        # Final flush on exit
        if commodity_buffer:
            print(f"{YELLOW}[DATABASE] Writing to Database starting...{RESET}", flush=True)
            stations, commodities = flush_commodities_to_db(conn, commodity_buffer)
            if stations > 0:
                print(f"{GREEN}[DATABASE] Writing to Database finished. Updated {stations} stations with {commodities} commodities{RESET}", flush=True)
                
    except Exception as e:
        print(f"{RED}[ERROR] Fatal error: {str(e)}{RESET}", file=sys.stderr)
        return 1
        
    finally:
        if 'conn' in locals():
            conn.close()
        print(f"{YELLOW}[TERMINATED] EDDN Update Service{RESET}", flush=True)
        
    return 0

if __name__ == '__main__':
    sys.exit(main()) 