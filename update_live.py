import os
import sys
import json
import zlib
import time
import signal
import argparse
from datetime import datetime, timezone, timedelta
import csv
import msgspec
import psycopg2
import zmq
from psycopg2.extras import DictCursor
import atexit
import platform

# ANSI color codes
YELLOW = '\033[93m'  # Default color
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
CYAN = '\033[96m'  # For database operations
ORANGE = '\033[38;5;208m'
MAGENTA = '\033[95m'  # For system state tracking
RESET = '\033[0m'

# Debug levels
DEBUG_LEVEL = 2  # 1 = critical/important, 2 = normal, 3 = verbose/detailed

# Constants
DATABASE_URL = None  # Will be set from args or env in main()
STATUS_PORT = int(os.getenv('STATUS_PORT', '5557'))

COMMODITIES_CSV = os.path.join("data", "commodities_mining.csv")
EDDN_RELAY = "tcp://eddn.edcd.io:9500"

# How often (in seconds) to flush changes to DB
DB_UPDATE_INTERVAL = 20

# Debug flag for detailed commodity changes
DEBUG = False

# Global state
running = True
commodity_buffer = {}
commodity_map = {}
reverse_map = {}


# Global commodity ID mapping
def get_commodity_ids(conn):
    """Load commodity ID mapping from database"""
    cursor = conn.cursor()
    cursor.execute("SELECT commodity_name, commodity_id FROM commodity_types")
    return {name: id for name, id in cursor}

def get_timestamp():
    """Get current timestamp in YYYY:MM:DD-HH:MM:SS format"""
    return datetime.now().strftime("%Y:%m:%d-%H:%M:%S")

def log_message(tag, message, level=2):
    """Log a message with timestamp and PID"""
    # Skip messages with level higher than DEBUG_LEVEL
    if DEBUG_LEVEL == 0 or level > DEBUG_LEVEL:
        return
        
    timestamp = datetime.now().strftime("%Y:%m:%d-%H:%M:%S")
    color = YELLOW  # Default color
    
    if tag == "STATUS":
        color = RED
    elif tag == "DATABASE":
        color = CYAN
    elif tag == "ERROR":
        color = RED
    
    print(f"{color}[{timestamp}] [{os.getpid()}] [{tag}] {message}{RESET}", flush=True)

# ZMQ setup
zmq_context = zmq.Context()
status_publisher = zmq_context.socket(zmq.PUB)

# Bind to all interfaces and log the actual binding address
try:
    bind_address = f"tcp://0.0.0.0:{STATUS_PORT}"
    status_publisher.bind(bind_address)
    log_message("STATUS", f"Successfully bound ZMQ publisher to {bind_address}", level=1)
    
    # Get actual endpoint details
    endpoint = status_publisher.getsockopt(zmq.LAST_ENDPOINT).decode()
    log_message("STATUS", f"Actual ZMQ endpoint: {endpoint}", level=1)
    
    # Log network interfaces for debugging
    import socket
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    log_message("STATUS", f"Container hostname: {hostname}, IP: {ip_address}", level=1)
except Exception as e:
    log_message("ERROR", f"Failed to bind to status port {bind_address}: {e}", level=1)
    # Don't exit, just continue without status updates

def publish_status(state, last_db_update=None):
    """Publish status update via ZMQ"""
    try:
        status = {
            "state": state,
            "last_db_update": last_db_update.isoformat() if last_db_update else None,
            "pid": os.getpid()  # Add PID to status messages
        }
        status_json = json.dumps(status)
        log_message("STATUS", f"Publishing state: {state} (message size: {len(status_json)} bytes)", level=2)
        status_publisher.send_string(status_json)
        log_message("STATUS", f"Successfully published state: {state}", level=2)
    except Exception as e:
        log_message("ERROR", f"Failed to publish status: {e}", level=1)
        import traceback
        log_message("ERROR", f"Status traceback: {traceback.format_exc()}", level=1)

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
    log_message("STOPPING", "EDDN Update Service")
    publish_status("offline")
    running = False

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def load_commodity_map():
    """Load commodity mapping from CSV"""
    log_message("INIT", f"Loading commodity mapping from {COMMODITIES_CSV}", level=1)
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
    log_message("INIT", f"Loaded {len(commodity_map)} commodities from CSV (mapping EDDN ID -> local name)", level=2)
    return commodity_map, reverse_map

def flush_commodities_to_db(conn, commodity_buffer, auto_commit=False):
    """Write buffered commodities to database"""
    if not commodity_buffer:
        log_message("DATABASE", "No commodities in buffer to write", level=2)
        return 0, 0

    cursor = conn.cursor()
    total_commodities = 0
    stations_processed = 0
    total_stations = len(commodity_buffer)

    try:
        log_message("DATABASE", f"Writing to Database starting... ({total_stations} stations to process)", level=1)
        
        # Process each station's commodities
        for (system_id64, station_name), (new_map, eddn_timestamp) in commodity_buffer.items():
            try:
                stations_processed += 1
                
                # Start transaction for this station
                cursor.execute("BEGIN")
                
                # Get station info using both system_id64 and station_name with row lock
                cursor.execute("""
                    SELECT station_id
                    FROM stations
                    WHERE system_id64 = %s AND station_name = %s
                    FOR UPDATE
                """, (system_id64, station_name))
                row = cursor.fetchone()
                if not row:
                    log_message("ERROR", f"Station not found in database: {station_name} in system {system_id64}", level=1)
                    cursor.execute("ROLLBACK")
                    continue
                    
                station_id = row[0]
                log_message("DATABASE", f"Processing station {station_name} ({len(new_map)} commodities)", level=2)
                
                # Delete existing commodities using proper primary key
                try:
                    cursor.execute("""
                        DELETE FROM station_commodities_mapped 
                        WHERE system_id64 = %s AND station_name = %s
                    """, (system_id64, station_name))
                    rows_deleted = cursor.rowcount
                    log_message("DATABASE", f"Deleted {rows_deleted} existing commodities for {station_name}", level=2)
                except Exception as e:
                    log_message("ERROR", f"Failed to delete existing commodities for {station_name}: {str(e)}", level=1)
                    cursor.execute("ROLLBACK")
                    continue
                
                # Insert new commodities with improved error handling
                try:
                    # Get commodity ID mapping once
                    commodity_ids = get_commodity_ids(conn)
                    
                    # In the insert section, change only the data preparation:
                    commodity_data = [(system_id64, station_id, station_name, commodity_ids[commodity_name], data[0], data[1]) 
                                    for commodity_name, data in new_map.items()]
                    
                    # Validate data before insert
                    for data in commodity_data:
                        if None in data:
                            raise ValueError(f"Invalid commodity data: {data}")
                    
                    cursor.executemany("""
                        INSERT INTO station_commodities_mapped 
                            (system_id64, station_id, station_name, commodity_id, sell_price, demand)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (system_id64, station_id, commodity_id) 
                        DO UPDATE SET 
                            station_name = EXCLUDED.station_name,
                            sell_price = EXCLUDED.sell_price,
                            demand = EXCLUDED.demand
                    """, commodity_data)
                    rows_affected = cursor.rowcount

                    log_message("DATABASE", f"Inserted/Updated {rows_affected} commodities for {station_name} (expected {len(new_map)})", level=2)
                    
                    # Verify no duplicates were created
                    cursor.execute("""
                        SELECT COUNT(*), COUNT(DISTINCT (system_id64, station_id, commodity_id))
                        FROM station_commodities_mapped
                        WHERE system_id64 = %s AND station_id = %s
                    """, (system_id64, station_id))
                    total, distinct = cursor.fetchone()
                    if total != distinct:
                        raise Exception(f"Duplicate entries detected: {total} total vs {distinct} distinct")
                        
                except Exception as e:
                    log_message("ERROR", f"Failed to insert commodities for {station_name}: {str(e)}", level=1)
                    cursor.execute("ROLLBACK")
                    continue
                
                # Update station timestamp using EDDN timestamp
                try:
                    # Parse EDDN timestamp (format: "2025-01-11T01:19:39Z")
                    # Convert to database format (timestamp without time zone)
                    try:
                        dt = datetime.strptime(eddn_timestamp, "%Y-%m-%dT%H:%M:%SZ")
                        db_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
                        log_message("DEBUG", f"Converting EDDN timestamp '{eddn_timestamp}' to DB format '{db_timestamp}'", level=3)
                    except ValueError as e:
                        log_message("ERROR", f"Failed to parse EDDN timestamp '{eddn_timestamp}': {str(e)}", level=1)
                        cursor.execute("ROLLBACK")
                        continue

                    cursor.execute("""
                        UPDATE stations
                        SET update_time = %s
                        WHERE system_id64 = %s AND station_id = %s
                        RETURNING update_time
                    """, (db_timestamp, system_id64, station_id))
                    
                    rows_updated = cursor.rowcount
                    if rows_updated == 0:
                        log_message("ERROR", f"Failed to update timestamp for {station_name} - no rows affected (timestamp: {db_timestamp})", level=1)
                        cursor.execute("ROLLBACK")
                        continue
                    else:
                        updated_time = cursor.fetchone()[0]
                        log_message("DATABASE", f"Updated timestamp for {station_name} from EDDN time '{eddn_timestamp}' to DB time '{updated_time}' (rows affected: {rows_updated})", level=2)
                except Exception as e:
                    log_message("ERROR", f"Failed to update timestamp for {station_name}: {str(e)}", level=1)
                    cursor.execute("ROLLBACK")
                    continue
                
                # Commit transaction for this station
                cursor.execute("COMMIT")
                total_commodities += len(new_map)
                
                # Log progress every 10 stations
                if stations_processed % 10 == 0:
                    log_message("DATABASE", f"Progress: {stations_processed}/{total_stations} stations processed", level=2)

            except Exception as e:
                log_message("ERROR", f"Failed to process station {station_name}: {str(e)}", level=1)
                try:
                    cursor.execute("ROLLBACK")
                except:
                    pass
                continue

        log_message("DATABASE", f"✓ Successfully updated {stations_processed} stations with {total_commodities} commodities", level=1)
        
    except Exception as e:
        log_message("ERROR", f"Database error: {str(e)}", level=1)
        try:
            cursor.execute("ROLLBACK")
        except:
            pass
        return 0, 0
        
    finally:
        cursor.close()
        commodity_buffer.clear()
        
    return stations_processed, total_commodities

def handle_power_data(message):
    """Process all power data from FSDJump events"""
    # Only process FSDJump events
    event = message.get("event", "")
    if event != "FSDJump":
        return

    # Log that we found a FSDJump event
    log_message("POWER-DEBUG", ORANGE + f"Processing {event} event", level=2)

    # Get system info
    system_name = message.get("StarSystem", "")
    system_id64 = message.get("SystemAddress")
    if not system_name or not system_id64:
        log_message("POWER-DEBUG", GREEN + f"Missing system info - Name: {system_name}, ID64: {system_id64}", level=2)
        return

    # Get current values from database first
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT controlling_power, power_state, powers_acquiring
                FROM systems
                WHERE id64 = %s AND name = %s
            """, (system_id64, system_name))
            row = cur.fetchone()
            
            if not row:
                log_message("POWER-DEBUG", GREEN + f"System not found in database: {system_name}", level=2)
                return
                
            current_power, current_state, current_powers = row
            # Convert current_powers from JSONB to list if not None, otherwise empty list
            current_powers = current_powers if current_powers else []
            
            # Only update fields that are actually present in the message
            has_controlling_power = "ControllingPower" in message
            has_power_state = "PowerplayState" in message
            has_powers = "Powers" in message
            
            # Use current values if fields are not in message
            controlling_power = message.get("ControllingPower", current_power)
            power_state = message.get("PowerplayState", current_state)
            powers = message.get("Powers", current_powers)
            
            # Validate powers is a list
            if isinstance(powers, str):
                powers = [powers]
            elif not isinstance(powers, list):
                log_message("POWER-DEBUG", GREEN + f"Powers has unexpected type: {type(powers)}", level=2)
                powers = current_powers

            # Filter out controlling power from powers array if it exists there
            if controlling_power and controlling_power in powers:
                powers = [p for p in powers if p != controlling_power]
                log_message("POWER-DEBUG", GREEN + f"Removed controlling power {controlling_power} from powers_acquiring array", level=2)

            # Log the power data we found
            log_message("POWER-DEBUG", GREEN + "Power data found:", level=2)
            log_message("POWER-DEBUG", GREEN + f"  System: {system_name} (ID64: {system_id64})", level=2)
            log_message("POWER-DEBUG", GREEN + f"  Controlling Power: {controlling_power} {'(from message)' if has_controlling_power else '(unchanged)'}", level=2)
            log_message("POWER-DEBUG", GREEN + f"  Power State: {power_state} {'(from message)' if has_power_state else '(unchanged)'}", level=2)
            log_message("POWER-DEBUG", GREEN + f"  Powers Acquiring: {powers} {'(from message)' if has_powers else '(unchanged)'}", level=2)

            # Check if there are actual changes, considering NULL values
            power_changed = has_controlling_power and current_power != controlling_power and not (current_power is None and controlling_power is None)
            state_changed = has_power_state and current_state != power_state and not (current_state is None and power_state is None)
            powers_changed = has_powers and sorted(current_powers) != sorted(powers)
            
            # Log what changed
            changes = []
            if power_changed:
                changes.append(f"controlling_power: {current_power} -> {controlling_power}")
            if state_changed:
                changes.append(f"power_state: {current_state} -> {power_state}")
            if powers_changed:
                changes.append(f"powers_acquiring: {current_powers} -> {powers}")
            
            # Get current timestamp and adjust it by subtracting one hour
            # current_timestamp = datetime.now(timezone.utc) - timedelta(hours=1)
            current_timestamp = datetime.now(timezone.utc)
            
            # Always update controlling_power and power_state
            update_fields = ["controlling_power = %s", "power_state = %s", "last_updated = %s"]
            params = [controlling_power, power_state, current_timestamp]
            
            # Only conditionally include powers_acquiring if it changed
            if powers_changed:
                update_fields.append("powers_acquiring = %s::jsonb")
                params.append(json.dumps(powers))
                
            # Always update
            query = f"""
                UPDATE systems 
                SET {', '.join(update_fields)}
                WHERE id64 = %s AND name = %s
                RETURNING id64, last_updated
            """
            params.extend([system_id64, system_name])
            cur.execute(query, params)

            if cur.rowcount > 0:
                result = cur.fetchone()
                updated_timestamp = result[1] if result and len(result) > 1 else None
                
                if changes:
                    log_message("POWER-DEBUG", GREEN + f"✓ Updated power status for {system_name}: {', '.join(changes)}", level=1)
                else:
                    log_message("POWER-DEBUG", GREEN + f"✓ Refreshed power status for {system_name} (no changes detected)", level=1)
                
                # Log the timestamp update
                if updated_timestamp:
                    log_message("POWER-DEBUG", GREEN + f"System {system_name} last_updated timestamp set to: {updated_timestamp}", level=2)
                
                conn.commit()
            else:
                log_message("POWER-DEBUG", GREEN + f"No rows updated for system {system_name}", level=1)
                
    except Exception as e:
        log_message("POWER-DEBUG", GREEN + f"Failed to update power status: {e}", level=1)
        import traceback
        log_message("POWER-DEBUG", GREEN + f"Traceback: {traceback.format_exc()}", level=1)

def handle_system_state(data):
    """Handle system state updates from FSDJump events"""
    try:
        if 'SystemFaction' not in data or 'Factions' not in data:
            return
            
        system_faction = data['SystemFaction'].get('Name')
        if not system_faction:
            return
            
        log_message("STATE", MAGENTA + f"Detecting controlling faction: {system_faction}", level=2)
        
        # Find the controlling faction in the factions list
        current_state = None
        for faction in data['Factions']:
            if faction['Name'] == system_faction:
                if 'ActiveStates' in faction and faction['ActiveStates']:
                    # Take the first active state
                    current_state = faction['ActiveStates'][0]['State']
                    log_message("STATE", MAGENTA + f"Detecting state: {current_state}", level=2)
                break
        
        if current_state is None:
            log_message("STATE", MAGENTA + "No active state found for controlling faction", level=2)
            return
            
        # Check current state in database
        with psycopg2.connect(DATABASE_URL) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT system_state 
                FROM systems 
                WHERE id64 = %s
            """, (data['SystemAddress'],))
            
            row = cursor.fetchone()
            if not row:
                log_message("STATE", MAGENTA + f"System {data['StarSystem']} not found in database", level=2)
                return
                
            db_state = row[0]
            log_message("STATE", MAGENTA + f"Comparing state: DB='{db_state}' vs Current='{current_state}'", level=2)
            
            # Update if states differ
            if db_state != current_state:
                log_message("STATE", MAGENTA + f"Updating state from '{db_state}' to '{current_state}'", level=2)
                cursor.execute("""
                    UPDATE systems 
                    SET system_state = %s 
                    WHERE id64 = %s
                """, (current_state, data['SystemAddress']))
                conn.commit()
                log_message("STATE", MAGENTA + f"Writing to database: system_state = '{current_state}'", level=2)
            else:
                log_message("STATE", MAGENTA + "Disregarding state update - no change", level=2)
                
    except Exception as e:
        log_message("STATE", MAGENTA + f"Error updating system state: {str(e)}", level=1)
        import traceback
        log_message("STATE", MAGENTA + f"Traceback: {traceback.format_exc()}", level=1)

def handle_colony_ship_event(message, event_type):
    """Process colony ship events (Docked and FSSSignalDiscovered)"""
    try:
        # Check if this is a colony ship event
        is_colony_ship = False
        
        if event_type == 'Docked':
            station_name = message.get('StationName', '')
            is_colony_ship = station_name == 'System Colonisation Ship'
        elif event_type == 'FSSSignalDiscovered':
            signal_name = message.get('SignalName', '')
            is_colony_ship = signal_name == 'System Colonisation Ship'
        
        if not is_colony_ship:
            return
        
        # Extract common fields
        system_id64 = message.get('SystemAddress')
        if not system_id64:
            log_message("COLONY", f"Missing SystemAddress in {event_type} event", level=1)
            return
        
        # Get system name from database
        system_name = None
        try:
            with psycopg2.connect(DATABASE_URL) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT name
                    FROM systems
                    WHERE id64 = %s
                """, (system_id64,))
                row = cursor.fetchone()
                if row:
                    system_name = row[0]
        except Exception as e:
            log_message("ERROR", f"Failed to get system name for ID64 {system_id64}: {str(e)}", level=1)
        
        # Initialize database fields
        station_id = None
        station_name = None
        station_type = None
        station_faction = None
        station_government = None
        economy = None
        economies = None
        landing_pads = None
        signal_type = None
        
        if event_type == 'Docked':
            # Extract Docked event specific fields
            station_name = message.get('StationName')
            station_type = message.get('StationType')
            station_id = message.get('MarketID')
            station_faction = message.get('StationFaction')
            station_government = message.get('StationGovernment_Localised')
            economy = message.get('StationEconomy_Localised')
            economies = message.get('StationEconomies')
            landing_pads = message.get('LandingPads')
            
            log_message("COLONY", f"✓ Docked at colony ship in {system_name or 'Unknown System'} (ID64: {system_id64})", level=1)
        elif event_type == 'FSSSignalDiscovered':
            # Extract FSSSignalDiscovered event specific fields
            station_name = message.get('SignalName')
            signal_type = message.get('SignalType')
            
            log_message("COLONY", f"✓ Discovered colony ship in {system_name or 'Unknown System'} (ID64: {system_id64})", level=1)
        
        # Save to database
        save_colony_ship_to_db(
            system_id64=system_id64,
            station_id=station_id,
            station_name=station_name,
            station_type=station_type,
            station_faction=station_faction,
            station_government=station_government,
            economy=economy,
            economies=economies,
            landing_pads=landing_pads,
            signal_type=signal_type
        )
        
    except Exception as e:
        log_message("ERROR", f"Error processing colony ship event: {str(e)}", level=1)
        import traceback
        log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=1)

def save_colony_ship_to_db(system_id64, station_name, station_id=None, station_type=None, 
                          station_faction=None, station_government=None, economy=None, 
                          economies=None, landing_pads=None, signal_type=None):
    """Save colony ship information to the database"""
    try:
        if not system_id64 or not station_name:
            log_message("ERROR", "Missing required fields for colony ship database entry", level=1)
            return False
        
        # Convert JSON fields to strings if they're not None
        station_faction_json = json.dumps(station_faction) if station_faction else None
        economies_json = json.dumps(economies) if economies else None
        landing_pads_json = json.dumps(landing_pads) if landing_pads else None
        
        # Start transaction
        with psycopg2.connect(DATABASE_URL) as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN")
            
            # Check if this colony ship already exists in the database
            cursor.execute("""
                SELECT id, first_seen
                FROM colony_systems
                WHERE system_id64 = %s AND station_name = %s
            """, (system_id64, station_name))
            
            existing_record = cursor.fetchone()
            
            if existing_record:
                # Update existing record
                colony_id, first_seen = existing_record
                log_message("COLONY", f"Updating existing colony ship record (ID: {colony_id})", level=2)
                
                # Update with new information, preserving first_seen
                cursor.execute("""
                    UPDATE colony_systems
                    SET 
                        station_id = COALESCE(%s, station_id),
                        station_type = COALESCE(%s, station_type),
                        station_faction = COALESCE(%s::jsonb, station_faction),
                        station_government = COALESCE(%s, station_government),
                        economy = COALESCE(%s, economy),
                        economies = COALESCE(%s::jsonb, economies),
                        landing_pads = COALESCE(%s::jsonb, landing_pads),
                        signal_type = COALESCE(%s, signal_type),
                        last_updated = NOW()
                    WHERE id = %s
                    RETURNING id
                """, (
                    station_id, station_type, station_faction_json, station_government,
                    economy, economies_json, landing_pads_json, signal_type, colony_id
                ))
                
                if cursor.rowcount == 0:
                    log_message("ERROR", f"Failed to update colony ship record", level=1)
                    cursor.execute("ROLLBACK")
                    return False
                
                log_message("COLONY", f"✓ Updated colony ship record in database", level=1)
            else:
                # Insert new record
                log_message("COLONY", f"Creating new colony ship record", level=2)
                
                cursor.execute("""
                    INSERT INTO colony_systems (
                        system_id64, station_id, station_name, station_type,
                        station_faction, station_government, economy,
                        economies, landing_pads, signal_type,
                        first_seen, last_updated
                    ) VALUES (
                        %s, %s, %s, %s, 
                        %s::jsonb, %s, %s, 
                        %s::jsonb, %s::jsonb, %s,
                        NOW(), NOW()
                    )
                    RETURNING id
                """, (
                    system_id64, station_id, station_name, station_type,
                    station_faction_json, station_government, economy,
                    economies_json, landing_pads_json, signal_type
                ))
                
                new_id = cursor.fetchone()[0]
                log_message("COLONY", f"✓ Created new colony ship record in database (ID: {new_id})", level=1)
            
            # Commit transaction
            conn.commit()
            return True
            
    except Exception as e:
        log_message("ERROR", f"Database error saving colony ship: {str(e)}", level=1)
        import traceback
        log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=1)
        try:
            conn.rollback()
        except:
            pass
        return False

def process_journal_message(message):
    """Process journal messages for power data"""
    try:
        # Get the inner message object
        msg_data = message.get("message")
        if not msg_data:
            log_message("POWER-DEBUG", GREEN + "Missing message field", level=1)
            return False
            
        # Check for FSDJump event and process power data
        message_type = msg_data.get("event")
        if message_type == 'FSDJump':
            #log_message("POWER-DEBUG", GREEN + f"Processing {message_type} event", level=1)
            handle_power_data(msg_data)
            handle_system_state(msg_data)
            return True
        # Process colony ship events
        elif message_type == 'Docked' or message_type == 'FSSSignalDiscovered':
            handle_colony_ship_event(msg_data, message_type)
            return True
            
        return False
    except Exception as e:
        log_message("POWER-DEBUG", GREEN + f"Error processing journal message: {str(e)}", level=1)
        import traceback
        log_message("POWER-DEBUG", GREEN + f"Traceback: {traceback.format_exc()}", level=1)
        return False

def process_message(message, commodity_map):
    """Process a single EDDN message"""
    try:
        # Check schema type and get inner message
        schema_ref = message.get("$schemaRef", "").lower()
        msg_data = message.get("message", {})
            
        # Continue with existing commodity processing
        if message.get("stationType") == "FleetCarrier" or \
           (message.get("economies") and message["economies"][0].get("name") == "Carrier"):
            log_message("DEBUG", f"Skipped Fleet Carrier Data: {message.get('stationName')}", level=2)
            return None, None
            
        station_name = message.get("stationName")
        system_name = message.get("systemName", "Unknown")
        market_id = message.get("marketId")
        timestamp = message.get("timestamp")
        
        log_message("DEBUG", f"Processing station {station_name} in {system_name} (timestamp: {timestamp})", level=2)
        
        if not timestamp:
            log_message("ERROR", "Message missing timestamp", level=1)
            return None, None
            
        if market_id is None:
            log_message("DEBUG", f"Live update without marketId: {station_name} in system {system_name}", level=2)
        
        if not station_name or not system_name:
            log_message("DEBUG", "Message missing station name or system name", level=2)
            return None, None
            
        # Get system_id64 from systems table
        try:
            with psycopg2.connect(DATABASE_URL) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT id64
                    FROM systems
                    WHERE name = %s
                """, (system_name,))
                row = cursor.fetchone()
                if not row:
                    log_message("ERROR", f"System not found in database: {system_name}", level=1)
                    return None, None
                system_id64 = row[0]
        except Exception as e:
            log_message("ERROR", f"Failed to get system_id64 for {system_name}: {str(e)}", level=1)
            return None, None
            
        # Process commodities
        station_commodities = {}
        commodities = message.get("commodities", [])
        log_message("DEBUG", f"Found {len(commodities)} commodities", level=3)
        
        for commodity in commodities:
            name = commodity.get("name", "").lower()
            if not name:
                continue
                
            if name not in commodity_map:
                continue  # Skip logging unknown commodities
                
            sell_price = commodity.get("sellPrice", 0)
            if sell_price <= 0:
                continue
                
            demand = commodity.get("demand", 0)
            log_message("DEBUG", f"Processing commodity: {name} (price: {sell_price}, demand: {demand})", level=3)
            station_commodities[commodity_map[name]] = (sell_price, demand, market_id)
            log_message("COMMODITY", f"✓ {commodity_map[name]} at {station_name}: {sell_price:,} cr (demand: {demand:,})", level=3)
            
        if station_commodities:
            log_message("COMMODITY", f"Added {len(station_commodities)} mining commodities to buffer for {station_name}", level=2)
            # Publish status update to indicate activity
            publish_status("running", datetime.now(timezone.utc))
            # Store timestamp and system_id64 with commodities
            return (system_id64, station_name), (station_commodities, timestamp)
        else:
            log_message("DEBUG", f"No relevant commodities found at {station_name}", level=2)
            
    except Exception as e:
        log_message("ERROR", f"Error processing message: {str(e)}", level=1)
        import traceback
        log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=1)
        
    return None, None

def main():
    """Main function"""
    global running, commodity_buffer, commodity_map, reverse_map, DATABASE_URL
    
    parser = argparse.ArgumentParser(description='EDDN Live Update Service')
    parser.add_argument('--auto', action='store_true', help='Automatically commit changes')
    parser.add_argument('--db', help='Database URL (e.g. postgresql://user:pass@host:port/dbname)')
    parser.add_argument('--debug-level', type=int, choices=[0, 1, 2, 3], default=1, help='Debug level (0=silent, 1=critical, 2=normal, 3=verbose)')
    args = parser.parse_args()
    
    # Set DEBUG_LEVEL from argument
    global DEBUG_LEVEL
    DEBUG_LEVEL = args.debug_level
    
    # Set DATABASE_URL from argument or environment variable
    DATABASE_URL = args.db or os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        log_message("ERROR", "Database URL must be provided via --db argument or DATABASE_URL environment variable", level=1)
        return 1
    
    try:
        log_message("INIT", f"Starting Live EDDN Update every {DB_UPDATE_INTERVAL} seconds", level=1)
        
        publish_status("starting")
        
        # Load commodity mapping
        commodity_map, reverse_map = load_commodity_map()
        
        # Parse database URL for logging
        from urllib.parse import urlparse
        db_url = urlparse(DATABASE_URL)
        log_message("DATABASE", f"Connecting to database: {db_url.hostname}:{db_url.port}/{db_url.path[1:]}", level=1)
        
        # Connect to database with simple configuration
        conn_start = time.time()
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = False
            
            conn_time = time.time() - conn_start
            log_message("DATABASE", f"Connected to database in {conn_time:.2f}s", level=1)
            
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
            
            log_message("DATABASE", f"PostgreSQL version: {version}", level=2)
            log_message("DATABASE", f"Server version: {server_version}", level=2)
            log_message("DATABASE", f"Connections: {current_connections}/{max_connections}", level=2)
            
            # Test tables
            cursor.execute("SELECT COUNT(*) FROM systems")
            systems_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM stations")
            stations_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM station_commodities")
            commodities_count = cursor.fetchone()[0]
            
            log_message("DATABASE", f"Database contains: {systems_count} systems, {stations_count} stations, {commodities_count} commodity records", level=2)
            
            cursor.close()
        except Exception as e:
            log_message("ERROR", f"Database connection failed: {str(e)}", level=1)
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
        
        log_message("CONNECTED", f"Listening to EDDN. Flush changes every {DB_UPDATE_INTERVAL}s. (Press Ctrl+C to stop)", level=1)
        log_message("MODE", "automatic" if args.auto else "manual", level=1)
        
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
                        log_message("STATUS", f"Received {total_messages} total messages ({commodity_messages} commodity messages)", level=2)
                except zmq.error.Again:
                    continue  # Timeout, continue loop
                
                message = zlib.decompress(raw_msg)
                data = decoder.decode(message)
                
                # Route messages based on schema
                schema_ref = data.get("$schemaRef", "").lower()
                msg_data = data.get("message", {})

                # Handle journal messages first
                if "journal" in schema_ref:
                    #log_message("JOURNAL", BLUE + f"Processing Journal message", level=2)
                    #log_message("JOURNAL", BLUE + f"Journal Message schema: {schema_ref}", level=2)
                    process_journal_message(data)
                    continue

                # Continue with commodity processing
                if "commodity" in schema_ref:
                    commodity_messages += 1
                    log_message("DEBUG", f"Processing commodity message {commodity_messages}", level=2)
                    log_message("DEBUG", f"Commodity Message schema: {schema_ref}", level=2)
                else:
                    continue

                # Process commodity message
                station_name, commodities = process_message(data.get("message", {}), commodity_map)
                if station_name and commodities:
                    commodity_buffer[station_name] = commodities
                    messages_processed += 1
                    log_message("DEBUG", f"Buffer now contains {len(commodity_buffer)} stations", level=2)
                    
                    # Print status every 100 messages
                    if messages_processed % 100 == 0:
                        log_message("STATUS", f"Processed {messages_processed} commodity messages", level=2)
                    
                    # Flush to database every DB_UPDATE_INTERVAL seconds
                    current_time = time.time()
                    if current_time - last_flush >= DB_UPDATE_INTERVAL:
                        log_message("DEBUG", f"Time since last flush: {current_time - last_flush:.1f}s", level=2)
                        if commodity_buffer:
                            log_message("DATABASE", f"Writing to Database starting... ({len(commodity_buffer)} stations in buffer)", level=2)
                            for station, commodities in commodity_buffer.items():
                                log_message("DATABASE", f"Station {station}: {len(commodities)} commodities buffered", level=2)
                            publish_status("updating", datetime.now(timezone.utc))
                            stations, commodities = flush_commodities_to_db(conn, commodity_buffer)
                            if stations > 0:
                                db_operations += 1
                                log_message("DATABASE", f"✓ Successfully updated {stations} stations with {commodities} commodities", level=1)
                            else:
                                db_errors += 1
                                log_message("ERROR", "No stations were updated", level=1)
                            publish_status("running", datetime.now(timezone.utc))
                        else:
                            log_message("DATABASE", "No commodities in buffer to write", level=2)
                        last_flush = current_time
                
            except Exception as e:
                log_message("ERROR", f"Error processing message: {str(e)}", level=1)
                publish_status("error")
                continue
                    
        # Final flush on exit
        if commodity_buffer:
            log_message("DATABASE", "Writing to Database starting...", level=2)
            publish_status("updating", datetime.now(timezone.utc))
            stations, commodities = flush_commodities_to_db(conn, commodity_buffer)
            if stations > 0:
                log_message("DATABASE", f"[DATABASE] Writing to Database finished. Updated {stations} stations, {commodities} commodities", level=1)
            publish_status("running", datetime.now(timezone.utc))
                
    except Exception as e:
        log_message("ERROR", f"Fatal error: {str(e)}", level=1)
        publish_status("error")
        return 1
        
    finally:
        if 'conn' in locals():
            conn.close()
        publish_status("offline")
        log_message("TERMINATED", "EDDN Update Service", level=1)
        
    return 0

if __name__ == '__main__':
    sys.exit(main()) 