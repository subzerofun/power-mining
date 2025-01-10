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
import atexit
import platform

# ANSI color codes
YELLOW = '\033[93m'
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
RESET = '\033[0m'

# Check if we're on Windows
USE_COLORS = platform.system() != "Windows"

def get_timestamp():
    """Get current timestamp in YYYY:MM:DD-HH:MM:SS format"""
    return datetime.now().strftime("%Y:%m:%d-%H:%M:%S")

def log_message(color, tag, message):
    """Log a message with timestamp and color"""
    timestamp = datetime.now().strftime("%Y:%m:%d-%H:%M:%S")
    if USE_COLORS:
        print(f"{color}[{timestamp}] [{tag}] {message}{RESET}", flush=True)
    else:
        print(f"[{timestamp}] [{tag}] {message}", flush=True)  # No colors on Windows

# Constants
DATABASE_URL = None  # Will be set from args or env in main()
STATUS_PORT = int(os.getenv('STATUS_PORT', '5557'))

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

# ZMQ setup
zmq_context = zmq.Context()
status_publisher = zmq_context.socket(zmq.PUB)
try:
    status_publisher.bind(f"tcp://*:{STATUS_PORT}")
except Exception as e:
    log_message(RED, "ERROR", f"Failed to bind to status port: {e}")
    # Don't exit, just continue without status updates

def publish_status(state, last_db_update=None):
    """Publish status update via ZMQ"""
    try:
        status = {
            "state": state,
            "last_db_update": last_db_update.isoformat() if last_db_update else None
        }
        log_message(BLUE, "STATUS", f"Publishing state: {state}")
        status_publisher.send_string(json.dumps(status))
    except Exception as e:
        log_message(RED, "ERROR", f"Failed to publish status: {e}")
        import traceback
        log_message(RED, "ERROR", f"Status traceback: {traceback.format_exc()}")

def cleanup():
    """Cleanup function to be called on exit"""
    try:
        status_publisher.close()
        zmq_context.term()
    except:
        pass

# Register cleanup
atexit.register(cleanup)

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global running
    log_message(YELLOW, "STOPPING", "EDDN Update Service")
    publish_status("offline")
    running = False

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def load_commodity_map():
    """Load commodity mapping from CSV"""
    log_message(BLUE, "INIT", f"Loading commodity mapping from {COMMODITIES_CSV}")
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
    log_message(GREEN, "INIT", f"Loaded {len(commodity_map)} commodities from CSV (mapping EDDN ID -> local name)")
    return commodity_map, reverse_map

def flush_commodities_to_db(conn, commodity_buffer, auto_commit=False):
    """Write buffered commodities to database"""
    if not commodity_buffer:
        log_message(YELLOW, "DATABASE", "No commodities in buffer to write")
        return 0, 0

    cursor = conn.cursor()
    total_commodities = 0
    stations_processed = 0
    total_stations = len(commodity_buffer)

    try:
        log_message(YELLOW, "DATABASE", f"Writing to Database starting... ({total_stations} stations to process)")
        
        # Process each station's commodities
        for station_name, new_map in commodity_buffer.items():
            try:
                stations_processed += 1
                
                # Get station info
                cursor.execute("""
                    SELECT system_id64, station_id
                    FROM stations
                    WHERE station_name = %s
                """, (station_name,))
                row = cursor.fetchone()
                if not row:
                    log_message(RED, "ERROR", f"Station not found in database: {station_name}")
                    continue
                    
                system_id64, station_id = row
                log_message(BLUE, "DATABASE", f"Processing station {station_name} ({len(new_map)} commodities)")
                
                # Delete existing commodities
                try:
                    cursor.execute("""
                        DELETE FROM station_commodities 
                        WHERE system_id64 = %s AND station_name = %s
                    """, (system_id64, station_name))
                    log_message(BLUE, "DATABASE", f"Deleted existing commodities for {station_name}")
                except Exception as e:
                    log_message(RED, "ERROR", f"Failed to delete existing commodities for {station_name}: {str(e)}")
                    continue
                
                # Insert new commodities
                try:
                    cursor.executemany("""
                        INSERT INTO station_commodities 
                            (system_id64, station_id, station_name, commodity_name, sell_price, demand)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (system_id64, station_name, commodity_name) 
                        DO UPDATE SET 
                            sell_price = EXCLUDED.sell_price,
                            demand = EXCLUDED.demand
                    """, [(system_id64, station_id, station_name, commodity_name, data[0], data[1]) 
                          for commodity_name, data in new_map.items()])
                    log_message(BLUE, "DATABASE", f"Inserted {len(new_map)} commodities for {station_name}")
                except Exception as e:
                    log_message(RED, "ERROR", f"Failed to insert commodities for {station_name}: {str(e)}")
                    continue
                
                # Update station timestamp
                try:
                    cursor.execute("""
                        UPDATE stations
                        SET update_time = %s
                        WHERE system_id64 = %s AND station_id = %s
                    """, (datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00"), system_id64, station_id))
                    log_message(BLUE, "DATABASE", f"Updated timestamp for {station_name}")
                except Exception as e:
                    log_message(RED, "ERROR", f"Failed to update timestamp for {station_name}: {str(e)}")
                
                total_commodities += len(new_map)
                
                # Log progress every 10 stations
                if stations_processed % 10 == 0:
                    conn.commit()
                    log_message(BLUE, "DATABASE", f"Progress: {stations_processed}/{total_stations} stations processed")

            except Exception as e:
                log_message(RED, "ERROR", f"Failed to process station {station_name}: {str(e)}")
                continue

        # Final commit
        conn.commit()
        log_message(GREEN, "DATABASE", f"✓ Successfully updated {stations_processed} stations with {total_commodities} commodities")
        
    except Exception as e:
        log_message(RED, "ERROR", f"Database error: {str(e)}")
        conn.rollback()
        return 0, 0
        
    finally:
        cursor.close()
        commodity_buffer.clear()
        
    return stations_processed, total_commodities

def handle_journal_message(message):
    """Process power data from journal messages"""
    event = message.get("event", "")
    if event not in ("FSDJump", "Location"):
        return

    system_name = message.get("StarSystem", "")
    system_id64 = message.get("SystemAddress")
    if not system_name or not system_id64:
        return

    powers = message.get("Powers", [])
    power_state = message.get("PowerplayState", "")

    controlling_power = None
    if isinstance(powers, list) and len(powers) == 1:
        controlling_power = powers[0]
    elif isinstance(powers, str):
        controlling_power = powers

    if controlling_power is None:
        return

    # Log the power info we received from EDDN
    log_message(BLUE, "POWER", f"EDDN Power Info - System: {system_name}, Power: {controlling_power}, State: {power_state}")

    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            cur = conn.cursor()
            # First check if power or state has changed
            cur.execute("""
                SELECT controlling_power, power_state
                FROM systems
                WHERE id64 = %s AND name = %s
            """, (system_id64, system_name))
            row = cur.fetchone()
            if row:
                old_power, old_state = row
                if old_power == controlling_power and old_state == power_state:
                    log_message(BLUE, "POWER", f"No change needed for {system_name} (already {controlling_power}, {power_state})")
                    return  # No change needed
                
            # Only update if different
            cur.execute("""
                UPDATE systems 
                SET controlling_power = %s,
                    power_state = %s
                WHERE id64 = %s AND name = %s
            """, (controlling_power, power_state, system_id64, system_name))
            
            if cur.rowcount > 0:
                log_message(GREEN, "POWER", f"✓ Updated power status for {system_name}: {controlling_power} ({power_state})")
            conn.commit()
    except Exception as e:
        log_message(RED, "ERROR", f"Failed to update power status: {e}")

def process_message(message, commodity_map):
    """Process a single EDDN message"""
    try:
        schema_ref = message.get("$schemaRef", "").lower()
        
        # Handle journal messages for power data
        if "journal" in schema_ref:
            handle_journal_message(message)
            return None, None
            
        # Skip if not a commodity message (check for exact schema)
        if "commodity" not in schema_ref:
            return None, None
            
        # Skip fleet carriers
        if message.get("stationType") == "FleetCarrier" or \
           (message.get("economies") and message["economies"][0].get("name") == "Carrier"):
            log_message(YELLOW, "DEBUG", f"Skipped Fleet Carrier Data: {message.get('stationName')}")
            return None, None
            
        station_name = message.get("stationName")
        system_name = message.get("systemName", "Unknown")
        market_id = message.get("marketId")
        
        log_message(BLUE, "DEBUG", f"Processing message for {station_name} in {system_name}")
        
        if market_id is None:
            log_message(YELLOW, "DEBUG", f"Live update without marketId: {station_name} in system {system_name}")
        
        if not station_name:
            log_message(YELLOW, "DEBUG", "Message missing station name")
            return None, None
            
        # Process commodities
        station_commodities = {}
        commodities = message.get("commodities", [])
        log_message(BLUE, "DEBUG", f"Found {len(commodities)} commodities at {station_name} in {system_name}")
        
        for commodity in commodities:
            name = commodity.get("name", "").lower()
            if not name:
                log_message(YELLOW, "DEBUG", "Commodity missing name")
                continue
                
            if name not in commodity_map:
                log_message(YELLOW, "DEBUG", f"Skipping unknown commodity: {name}")
                continue
                
            sell_price = commodity.get("sellPrice", 0)
            if sell_price <= 0:
                log_message(YELLOW, "DEBUG", f"Skipping {name} - no sell price")
                continue
                
            demand = commodity.get("demand", 0)
            station_commodities[commodity_map[name]] = (sell_price, demand, market_id)
            log_message(GREEN, "COMMODITY", f"✓ {commodity_map[name]} at {station_name}: {sell_price:,} cr (demand: {demand:,})")
            
        if station_commodities:
            log_message(GREEN, "COMMODITY", f"Added {len(station_commodities)} mining commodities to buffer for {station_name}")
            # Publish status update to indicate activity
            publish_status("running", datetime.now(timezone.utc))
            return station_name, station_commodities
        else:
            log_message(YELLOW, "COMMODITY", f"No relevant commodities found at {station_name}")
            
    except Exception as e:
        log_message(RED, "ERROR", f"Error processing message: {str(e)}")
        import traceback
        log_message(RED, "ERROR", f"Traceback: {traceback.format_exc()}")
        
    return None, None

def main():
    """Main function"""
    global running, commodity_buffer, commodity_map, reverse_map, DATABASE_URL
    
    parser = argparse.ArgumentParser(description='EDDN Live Update Service')
    parser.add_argument('--auto', action='store_true', help='Automatically commit changes')
    parser.add_argument('--db', help='Database URL (e.g. postgresql://user:pass@host:port/dbname)')
    args = parser.parse_args()
    
    # Set DATABASE_URL from argument or environment variable
    DATABASE_URL = args.db or os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        log_message(RED, "ERROR", "Database URL must be provided via --db argument or DATABASE_URL environment variable")
        return 1
    
    try:
        log_message(BLUE, "INIT", f"Starting Live EDDN Update every {DB_UPDATE_INTERVAL} seconds")
        
        publish_status("starting")
        
        # Load commodity mapping
        commodity_map, reverse_map = load_commodity_map()
        
        # Parse database URL for logging
        from urllib.parse import urlparse
        db_url = urlparse(DATABASE_URL)
        log_message(BLUE, "DATABASE", f"Connecting to database: {db_url.hostname}:{db_url.port}/{db_url.path[1:]}")
        
        # Connect to database with simple configuration
        conn_start = time.time()
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = False
            
            conn_time = time.time() - conn_start
            log_message(GREEN, "DATABASE", f"Connected to database in {conn_time:.2f}s")
            
            # Log connection info
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            cursor.execute("SHOW server_version")
            server_version = cursor.fetchone()[0]
            cursor.execute("SHOW max_connections")
            max_connections = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM pg_stat_activity")
            current_connections = cursor.fetchone()[0]
            
            log_message(BLUE, "DATABASE", f"PostgreSQL version: {version}")
            log_message(BLUE, "DATABASE", f"Server version: {server_version}")
            log_message(BLUE, "DATABASE", f"Connections: {current_connections}/{max_connections}")
            
            # Test tables
            cursor.execute("SELECT COUNT(*) FROM systems")
            systems_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM stations")
            stations_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM station_commodities")
            commodities_count = cursor.fetchone()[0]
            
            log_message(BLUE, "DATABASE", f"Database contains: {systems_count} systems, {stations_count} stations, {commodities_count} commodity records")
            
            cursor.close()
        except Exception as e:
            log_message(RED, "ERROR", f"Database connection failed: {str(e)}")
            raise
        
        publish_status("running")
        
        # Create message decoder
        decoder = msgspec.json.Decoder()
        
        # Connect to EDDN
        context = zmq.Context()
        subscriber = context.socket(zmq.SUB)
        subscriber.setsockopt(zmq.RCVTIMEO, 10000)  # 10 second timeout
        subscriber.connect(EDDN_RELAY)
        subscriber.setsockopt_string(zmq.SUBSCRIBE, "")  # subscribe to all messages
        
        log_message(GREEN, "CONNECTED", f"Listening to EDDN. Flush changes every {DB_UPDATE_INTERVAL}s. (Press Ctrl+C to stop)")
        log_message(BLUE, "MODE", "automatic" if args.auto else "manual")
        
        last_flush = time.time()
        last_message = time.time()
        messages_processed = 0
        total_messages = 0
        commodity_messages = 0
        db_operations = 0
        db_errors = 0
        
        while running:
            try:
                # Get message from EDDN
                try:
                    raw_msg = subscriber.recv()
                    last_message = time.time()
                    total_messages += 1
                    if total_messages % 100 == 0:
                        log_message(BLUE, "STATUS", f"Received {total_messages} total messages ({commodity_messages} commodity messages)")
                except zmq.error.Again:
                    continue  # Timeout, continue loop
                
                message = zlib.decompress(raw_msg)
                data = decoder.decode(message)
                
                # Check schema
                schema = data.get("$schemaRef", "").lower()
                if "https://eddn.edcd.io/schemas/commodity/3" in schema.lower():
                    commodity_messages += 1
                    log_message(BLUE, "DEBUG", f"Processing commodity message {commodity_messages}")
                    log_message(BLUE, "DEBUG", f"Message schema: {schema}")
                else:
                    continue
                    
                # Process message
                station_name, commodities = process_message(data.get("message", {}), commodity_map)
                if station_name and commodities:
                    commodity_buffer[station_name] = commodities
                    messages_processed += 1
                    log_message(BLUE, "DEBUG", f"Buffer now contains {len(commodity_buffer)} stations")
                    
                    # Print status every 100 messages
                    if messages_processed % 100 == 0:
                        log_message(YELLOW, "STATUS", f"Processed {messages_processed} commodity messages")
                    
                    # Flush to database every DB_UPDATE_INTERVAL seconds
                    current_time = time.time()
                    if current_time - last_flush >= DB_UPDATE_INTERVAL:
                        log_message(YELLOW, "DEBUG", f"Time since last flush: {current_time - last_flush:.1f}s")
                        if commodity_buffer:
                            log_message(YELLOW, "DATABASE", f"Writing to Database starting... ({len(commodity_buffer)} stations in buffer)")
                            for station, commodities in commodity_buffer.items():
                                log_message(BLUE, "DATABASE", f"Station {station}: {len(commodities)} commodities buffered")
                            publish_status("updating", datetime.now(timezone.utc))
                            stations, commodities = flush_commodities_to_db(conn, commodity_buffer)
                            if stations > 0:
                                db_operations += 1
                                log_message(GREEN, "DATABASE", f"✓ Successfully updated {stations} stations with {commodities} commodities")
                            else:
                                db_errors += 1
                                log_message(RED, "ERROR", "No stations were updated")
                            publish_status("running", datetime.now(timezone.utc))
                        else:
                            log_message(BLUE, "DATABASE", "No commodities in buffer to write")
                        last_flush = current_time
                
            except Exception as e:
                log_message(RED, "ERROR", f"Error processing message: {str(e)}")
                publish_status("error")
                continue
                    
        # Final flush on exit
        if commodity_buffer:
            log_message(YELLOW, "DATABASE", "Writing to Database starting...")
            publish_status("updating", datetime.now(timezone.utc))
            stations, commodities = flush_commodities_to_db(conn, commodity_buffer)
            if stations > 0:
                log_message(GREEN, "DATABASE", f"[DATABASE] Writing to Database finished. Updated {stations} stations, {commodities} commodities")
            publish_status("running", datetime.now(timezone.utc))
                
    except Exception as e:
        log_message(RED, "ERROR", f"Fatal error: {str(e)}")
        publish_status("error")
        return 1
        
    finally:
        if 'conn' in locals():
            conn.close()
        publish_status("offline")
        log_message(YELLOW, "TERMINATED", "EDDN Update Service")
        
    return 0

if __name__ == '__main__':
    sys.exit(main()) 