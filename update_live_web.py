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

# ANSI color codes
YELLOW = '\033[93m'
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
RESET = '\033[0m'

def get_timestamp():
    """Get current timestamp in YYYY:MM:DD-HH:MM:SS format"""
    return datetime.now().strftime("%Y:%m:%d-%H:%M:%S")

def log_message(color, tag, message):
    """Log a message with timestamp and color"""
    timestamp = datetime.now().strftime("%Y:%m:%d-%H:%M:%S")
    print(f"[{timestamp}] [{tag}] {message}", flush=True)  # Always flush

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
    status_publisher.connect(f"tcp://localhost:{STATUS_PORT}")
except Exception as e:
    log_message(RED, "ERROR", f"Failed to connect to status port: {e}")
    # Don't exit, just continue without status updates

def publish_status(state, last_db_update=None):
    """Publish status update via ZMQ"""
    try:
        status = {
            "state": state,
            "last_db_update": last_db_update.isoformat() if last_db_update else None
        }
        status_publisher.send_string(json.dumps(status))
    except Exception as e:
        log_message(RED, "ERROR", f"Failed to publish status: {e}")

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

def flush_commodities_to_db(conn, commodity_buffer):
    """Flush commodity updates to database"""
    if not commodity_buffer:
        log_message(BLUE, "DEBUG", "No commodities to flush")
        return 0, 0

    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        stations_processed = 0
        total_commodities = 0
        stations_not_found = []
        
        log_message(BLUE, "DATABASE", f"Starting to process {len(commodity_buffer)} stations")
        
        for station_name, commodities in commodity_buffer.items():
            try:
                # Get station info
                query_start = time.time()
                log_message(BLUE, "DATABASE", f"Looking up station: {station_name}")
                cursor.execute("""
                    SELECT s.id64 as system_id64, st.station_id, s.name as system_name
                    FROM stations st
                    JOIN systems s ON s.id64 = st.system_id64
                    WHERE st.station_name = %s
                """, (station_name,))
                query_time = time.time() - query_start
                log_message(BLUE, "DATABASE", f"Station lookup completed in {query_time:.3f}s")
                
                station = cursor.fetchone()
                if not station:
                    stations_not_found.append(station_name)
                    log_message(RED, "ERROR", f"Station not found in database: {station_name}")
                    continue
                
                system_id64 = station['system_id64']
                station_id = station['station_id']
                system_name = station['system_name']
                
                log_message(BLUE, "DATABASE", f"Processing station {station_name} in system {system_name} (ID: {system_id64})")
                
                # Delete existing commodities
                delete_start = time.time()
                cursor.execute("""
                    DELETE FROM station_commodities 
                    WHERE system_id64 = %s AND station_id = %s
                """, (system_id64, station_id))
                
                deleted_rows = cursor.rowcount
                delete_time = time.time() - delete_start
                log_message(BLUE, "DATABASE", f"Deleted {deleted_rows} existing commodities for {station_name} in {delete_time:.3f}s")
                
                # Insert new commodities
                insert_start = time.time()
                commodities_added = 0
                for commodity_name, (sell_price, demand, market_id) in commodities.items():
                    cursor.execute("""
                        INSERT INTO station_commodities 
                        (system_id64, station_id, station_name, commodity_name, sell_price, demand)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (system_id64, station_id, station_name, commodity_name, sell_price, demand))
                    commodities_added += 1
                    total_commodities += 1
                
                insert_time = time.time() - insert_start
                log_message(BLUE, "DATABASE", f"Inserted {commodities_added} commodities in {insert_time:.3f}s")
                
                # Update station timestamp
                update_start = time.time()
                cursor.execute("""
                    UPDATE stations
                    SET update_time = %s
                    WHERE system_id64 = %s AND station_id = %s
                """, (datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00"), system_id64, station_id))
                update_time = time.time() - update_start
                
                stations_processed += 1
                log_message(GREEN, "DATABASE", f"Successfully updated {station_name} with {len(commodities)} commodities (Delete: {delete_time:.3f}s, Insert: {insert_time:.3f}s, Update: {update_time:.3f}s)")
                
                # Commit every 10 stations
                if stations_processed % 10 == 0:
                    commit_start = time.time()
                    conn.commit()
                    commit_time = time.time() - commit_start
                    log_message(BLUE, "DATABASE", f"Committed changes for {stations_processed} of {len(commodity_buffer)} stations in {commit_time:.3f}s")
            
            except Exception as e:
                log_message(RED, "ERROR", f"Database error processing station {station_name}: {str(e)}")
                conn.rollback()
                continue
        
        # Final commit
        if stations_processed % 10 != 0:  # Only if we haven't just committed
            commit_start = time.time()
            conn.commit()
            commit_time = time.time() - commit_start
            log_message(BLUE, "DATABASE", f"Final commit completed in {commit_time:.3f}s")
        
        # Summary log
        if stations_processed > 0:
            log_message(GREEN, "DATABASE", f"Successfully processed {stations_processed} stations with {total_commodities} commodities")
        if stations_not_found:
            log_message(RED, "ERROR", f"Stations not found in database: {', '.join(stations_not_found)}")
        
    except Exception as e:
        log_message(RED, "ERROR", f"Fatal database error: {str(e)}")
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
                    return  # No change needed
                
            # Only update if different
            cur.execute("""
                UPDATE systems 
                SET controlling_power = %s,
                    power_state = %s
                WHERE id64 = %s AND name = %s
            """, (controlling_power, power_state, system_id64, system_name))
            
            if cur.rowcount > 0:
                log_message(GREEN, "POWER", f"Updated power status for {system_name}: {controlling_power} ({power_state})")
            conn.commit()
    except Exception as e:
        log_message(RED, "ERROR", f"Failed to update power status: {e}")

def process_message(message, commodity_map):
    """Process a single EDDN message"""
    try:
        schema_ref = message.get("$schemaRef", "").lower()
        
        # Skip if not a commodity message
        if "commodity" not in schema_ref:
            return None, None
            
        # Handle journal messages for power data
        if "journal" in schema_ref:
            handle_journal_message(message)
            return None, None
            
        # Skip fleet carriers
        if message.get("stationType") == "FleetCarrier" or \
           (message.get("economies") and message["economies"][0].get("name") == "Carrier"):
            log_message(YELLOW, "DEBUG", f"Skipped Fleet Carrier Data: {message.get('stationName')}")
            return None, None
            
        station_name = message.get("stationName")
        system_name = message.get("systemName", "Unknown")
        market_id = message.get("marketId")
        
        if market_id is None:
            log_message(YELLOW, "DEBUG", f"Live update without marketId: {station_name} in system {system_name}")
        
        if not station_name:
            return None, None
            
        # Process commodities
        station_commodities = {}
        commodities = message.get("commodities", [])
        log_message(BLUE, "DEBUG", f"Processing {len(commodities)} commodities from {station_name} in system {system_name}")
        
        for commodity in commodities:
            name = commodity.get("name", "").lower()
            if not name:
                continue
                
            if name not in commodity_map:
                continue
                
            sell_price = commodity.get("sellPrice", 0)
            if sell_price <= 0:
                continue
                
            demand = commodity.get("demand", 0)
            station_commodities[commodity_map[name]] = (sell_price, demand, market_id)
            log_message(BLUE, "DEBUG", f"Found commodity {commodity_map[name]} at {station_name} (price: {sell_price}, demand: {demand})")
            
        if station_commodities:
            log_message(GREEN, "DEBUG", f"Found {len(station_commodities)} relevant commodities for {station_name} in {system_name}")
            # Publish status update to indicate activity
            publish_status("running", datetime.now(timezone.utc))
            return station_name, station_commodities
        else:
            log_message(YELLOW, "DEBUG", f"No relevant commodities found for {station_name} in {system_name}")
            
    except Exception as e:
        log_message(RED, "ERROR", f"Error processing message: {str(e)}")
        publish_status("error")
        
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
                # Check database connection every minute
                if time.time() - last_flush >= 60:
                    try:
                        cursor = conn.cursor()
                        cursor.execute("SELECT 1")
                        cursor.close()
                        log_message(BLUE, "DATABASE", f"Connection check OK. Operations: {db_operations}, Errors: {db_errors}")
                    except Exception as e:
                        log_message(RED, "ERROR", f"Database connection lost: {str(e)}")
                        # Try to reconnect
                        try:
                            conn.close()
                        except:
                            pass
                        conn = psycopg2.connect(DATABASE_URL)
                        conn.autocommit = False
                        log_message(GREEN, "DATABASE", "Reconnected to database")
                
                # Check if we need to reconnect
                current_time = time.time()
                if current_time - last_message > 60:  # No messages for 1 minute
                    log_message(YELLOW, "WARN", "No messages received for 1 minute, reconnecting to EDDN...")
                    subscriber.disconnect(EDDN_RELAY)
                    subscriber.connect(EDDN_RELAY)
                    last_message = current_time
                
                # Get message from EDDN
                try:
                    raw_msg = subscriber.recv()
                    last_message = time.time()
                    total_messages += 1
                    if total_messages % 100 == 0:
                        log_message(BLUE, "DEBUG", f"Received {total_messages} total messages ({commodity_messages} commodity messages)")
                except zmq.error.Again:
                    continue  # Timeout, continue loop
                
                message = zlib.decompress(raw_msg)
                data = decoder.decode(message)
                
                # Check schema
                schema = data.get("$schemaRef", "").lower()
                if "commodity/3" in schema.lower():
                    commodity_messages += 1
                    log_message(BLUE, "DEBUG", f"Processing commodity message {commodity_messages}")
                else:
                    continue
                    
                # Process message
                station_name, commodities = process_message(data.get("message", {}), commodity_map)
                if station_name and commodities:
                    commodity_buffer[station_name] = commodities
                    messages_processed += 1
                    log_message(BLUE, "DEBUG", f"Added {len(commodities)} commodities for {station_name}")
                    
                    # Print status every 100 messages
                    if messages_processed % 100 == 0:
                        log_message(YELLOW, "STATUS", f"Processed {messages_processed} commodity messages")
                    
                    # Flush to database every DB_UPDATE_INTERVAL seconds
                    current_time = time.time()
                    if current_time - last_flush >= DB_UPDATE_INTERVAL:
                        if commodity_buffer:
                            log_message(YELLOW, "DATABASE", f"Writing to Database starting... (Total ops: {db_operations})")
                            publish_status("updating", datetime.now(timezone.utc))
                            op_start = time.time()
                            stations, commodities = flush_commodities_to_db(conn, commodity_buffer)
                            op_time = time.time() - op_start
                            if stations > 0:
                                db_operations += 1
                                log_message(GREEN, "DATABASE", f"Database write completed in {op_time:.2f}s. Updated {stations} stations, {commodities} commodities")
                            else:
                                db_errors += 1
                            publish_status("running", datetime.now(timezone.utc))
                        else:
                            log_message(BLUE, "DATABASE", f"No new data to write (Total ops: {db_operations}, Errors: {db_errors})")
                        last_flush = current_time
                
            except zmq.ZMQError as e:
                log_message(RED, "ERROR", f"ZMQ error: {str(e)}")
                time.sleep(1)  # Wait before retrying
                continue
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