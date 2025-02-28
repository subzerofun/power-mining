#!/usr/bin/env python3
"""
EDDN Update Service - Elite Dangerous Database Updater

This script connects to the Elite Dangerous Data Network (EDDN),
processes incoming data, and updates a PostgreSQL database with
commodity prices, system states, and power play information.
"""

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
import traceback

# ANSI color codes
YELLOW = '\033[93m'
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
CYAN = '\033[96m'
ORANGE = '\033[38;5;208m'
MAGENTA = '\033[95m'
WHITE = '\033[97m'
RESET = '\033[0m'


class EDDNUpdater:
    """
    Elite Dangerous Data Network (EDDN) updater service.
    
    Listens to the EDDN relay for journal and commodity events,
    processes them, and updates the database with new information.
    """
    
    def __init__(self, database_url=None, debug_level=1, auto_commit=False):
        """
        Initialize the EDDN updater service.
        
        Args:
            database_url (str): PostgreSQL connection string
            debug_level (int): Logging verbosity (0-3)
            auto_commit (bool): Whether to automatically commit changes
        """
        # Configuration
        self.database_url = database_url
        self.debug_level = debug_level
        self.auto_commit = auto_commit
        self.running = True
        self.db_conn = None
        
        # Constants
        self.STATUS_PORT = int(os.getenv('STATUS_PORT', '5557'))
        self.EVENT_PORT = int(os.getenv('EVENT_PORT', '5559'))
        self.COMMODITIES_CSV = os.path.join("data", "commodities_mining.csv")
        self.EDDN_RELAY = "tcp://eddn.edcd.io:9500"
        self.DB_UPDATE_INTERVAL = 20
        
        # State
        self.commodity_buffer = {}
        self.commodity_map = {}
        self.reverse_map = {}
        
        # Initialize ZMQ context
        self.zmq_context = None
        self.status_publisher = None
        self.event_publisher = None
        
        # Initialize message decoder
        self.decoder = msgspec.json.Decoder()
        
        # Register signal handlers and cleanup
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        atexit.register(self._cleanup)
    
    def start(self):
        """
        Start the EDDN updater service.
        
        1. Initialize ZMQ
        2. Connect to database
        3. Load commodity mappings
        4. Connect to EDDN
        5. Process messages in main loop
        """
        self.log_message("STARTING", "EDDN Update Service", level=1)
        
        try:
            # Set up ZMQ communication
            self._setup_zmq()
            self.publish_status("starting")
            
            # Connect to database
            self._connect_database()
            
            # Load commodity mappings
            self._load_commodity_map()
            
            # Connect to EDDN
            subscriber = self._connect_eddn()
            
            # Main processing loop
            self.publish_status("running")
            self.log_message("CONNECTED", f"Listening to EDDN. Flush changes every {self.DB_UPDATE_INTERVAL}s.", level=1)
            self.log_message("MODE", "automatic" if self.auto_commit else "manual", level=1)
            
            last_flush = time.time()
            messages_processed = 0
            total_messages = 0
            commodity_messages = 0
            
            while self.running:
                try:
                    # Receive message from EDDN
                    try:
                        raw_msg = subscriber.recv()
                        total_messages += 1
                        if total_messages % 100 == 0:
                            self.log_message("STATUS", f"Received {total_messages} total messages ({commodity_messages} commodity)", level=2)
                    except zmq.error.Again:
                        continue  # Timeout, continue loop
                    
                    # Decompress and decode message
                    message = zlib.decompress(raw_msg)
                    data = self.decoder.decode(message)
                    
                    # Get schema reference and message data
                    schema_ref = data.get("$schemaRef", "").lower()
                    msg_data = data.get("message", {})
                    
                    # Process message based on schema
                    if "journal" in schema_ref:
                        # Handle journal events
                        self.process_journal_message(data)
                    elif "commodity" in schema_ref:
                        # Handle commodity events
                        commodity_messages += 1
                        self.log_message("DEBUG", f"Processing commodity message {commodity_messages}", level=2)
                        self.log_message("DEBUG", f"Commodity Message schema: {schema_ref}", level=2)
                        
                        # Process commodity message
                        station_key, commodities = self.process_commodity_message(msg_data)
                        if station_key and commodities:
                            self.commodity_buffer[station_key] = commodities
                            messages_processed += 1
                            
                            # Flush to database periodically
                            current_time = time.time()
                            if current_time - last_flush >= self.DB_UPDATE_INTERVAL:
                                self.flush_commodities_to_db()
                                last_flush = current_time
                    
                except Exception as e:
                    self.log_message("ERROR", f"Error processing message: {str(e)}", level=1)
                    self.log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=3)
                    self.publish_status("error")
                    continue
            
            # Final flush on exit
            if self.commodity_buffer:
                self.flush_commodities_to_db()
                
        except Exception as e:
            self.log_message("ERROR", f"Fatal error: {str(e)}", level=1)
            self.log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=1)
            self.publish_status("error")
            return 1
            
        finally:
            # Ensure we clean up
            self._cleanup()
            
        return 0
    
    def stop(self):
        """Stop the EDDN updater service."""
        self.running = False
        self.log_message("STOPPING", "EDDN Update Service", level=1)
        self.publish_status("offline")
    
    def _connect_database(self):
        """Establish connection to the PostgreSQL database."""
        try:
            # Parse database URL for logging
            from urllib.parse import urlparse
            db_url = urlparse(self.database_url)
            self.log_message("DATABASE", f"Connecting to database: {db_url.hostname}:{db_url.port}/{db_url.path[1:]}", level=1)
            
            # Connect to database
            conn_start = time.time()
            self.db_conn = psycopg2.connect(self.database_url)
            self.db_conn.autocommit = False
            
            conn_time = time.time() - conn_start
            self.log_message("DATABASE", f"Connected to database in {conn_time:.2f}s", level=1)
            
            # Test database connection
            self._test_database_connection()
            
            return True
        except Exception as e:
            self.log_message("ERROR", f"Database connection failed: {str(e)}", level=1)
            raise
    
    def _test_database_connection(self):
        """Test database connection and log information."""
        try:
            cursor = self.db_conn.cursor()
            
            # Get PostgreSQL version
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            cursor.execute("SHOW server_version")
            server_version = cursor.fetchone()[0]
            cursor.execute("SHOW max_connections")
            max_connections = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM pg_stat_activity")
            current_connections = cursor.fetchone()[0]
            
            self.log_message("DATABASE", f"PostgreSQL version: {version}", level=2)
            self.log_message("DATABASE", f"Server version: {server_version}", level=2)
            self.log_message("DATABASE", f"Connections: {current_connections}/{max_connections}", level=2)
            
            # Test tables
            cursor.execute("SELECT COUNT(*) FROM systems")
            systems_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM stations")
            stations_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM station_commodities_mapped")
            commodities_count = cursor.fetchone()[0]
            
            self.log_message("DATABASE", f"Database contains: {systems_count} systems, {stations_count} stations, {commodities_count} commodity records", level=2)
            
            cursor.close()
        except Exception as e:
            self.log_message("ERROR", f"Database test failed: {str(e)}", level=1)
            self.log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=3)
    
    def _setup_zmq(self):
        """Set up ZMQ publishers for status and events."""
        self.zmq_context = zmq.Context()
        
        # Set up status publisher (for daemon)
        self.status_publisher = self.zmq_context.socket(zmq.PUB)
        bind_address = f"tcp://0.0.0.0:{self.STATUS_PORT}"
        
        try:
            self.status_publisher.bind(bind_address)
            self.log_message("STATUS", f"Successfully bound ZMQ status publisher to {bind_address}", level=1)
            
            # Get actual endpoint details
            endpoint = self.status_publisher.getsockopt(zmq.LAST_ENDPOINT).decode()
            self.log_message("STATUS", f"Actual ZMQ status endpoint: {endpoint}", level=1)
        except Exception as e:
            self.log_message("ERROR", f"Failed to bind to status port {bind_address}: {e}", level=1)
        
        # Set up event publisher (for other components)
        self.event_publisher = self.zmq_context.socket(zmq.PUB)
        event_bind_address = f"tcp://0.0.0.0:{self.EVENT_PORT}"
        
        try:
            self.event_publisher.bind(event_bind_address)
            self.log_message("STATUS", f"Successfully bound ZMQ event publisher to {event_bind_address}", level=1)
            
            # Get actual endpoint details
            event_endpoint = self.event_publisher.getsockopt(zmq.LAST_ENDPOINT).decode()
            self.log_message("STATUS", f"Actual ZMQ event endpoint: {event_endpoint}", level=1)
        except Exception as e:
            self.log_message("ERROR", f"Failed to bind to event port {event_bind_address}: {e}", level=1)
        
        # Log network interfaces for debugging
        try:
            import socket
            hostname = socket.gethostname()
            ip_address = socket.gethostbyname(hostname)
            self.log_message("STATUS", f"Container hostname: {hostname}, IP: {ip_address}", level=1)
        except Exception as e:
            self.log_message("ERROR", f"Failed to get network info: {e}", level=1)
    
    def _connect_eddn(self):
        """Connect to the EDDN relay."""
        context = zmq.Context()
        subscriber = context.socket(zmq.SUB)
        subscriber.setsockopt(zmq.RCVTIMEO, 10000)  # 10 second timeout
        subscriber.connect(self.EDDN_RELAY)
        subscriber.setsockopt_string(zmq.SUBSCRIBE, "")  # subscribe to all messages
        
        self.log_message("INIT", f"Connected to EDDN relay: {self.EDDN_RELAY}", level=1)
        return subscriber
    
    def _load_commodity_map(self):
        """Load commodity mapping from CSV file."""
        self.log_message("INIT", f"Loading commodity mapping from {self.COMMODITIES_CSV}", level=1)
        self.commodity_map = {}
        self.reverse_map = {}
        
        try:
            with open(self.COMMODITIES_CSV, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    eddn_id = row["id"].strip().lower()
                    local_name = row["name"].strip()
                    # Special case: store as "Void Opal" but handle both forms
                    if local_name == "Void Opals":
                        local_name = "Void Opal"
                    self.commodity_map[eddn_id] = local_name
                    self.reverse_map[local_name] = eddn_id
                    
            self.log_message("INIT", f"Loaded {len(self.commodity_map)} commodities from CSV", level=2)
        except Exception as e:
            self.log_message("ERROR", f"Failed to load commodity map: {str(e)}", level=1)
            raise
    
    def _get_commodity_ids(self):
        """Load commodity ID mapping from database."""
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT commodity_name, commodity_id FROM commodity_types")
        return {name: id for name, id in cursor}
    
    def publish_status(self, state, last_db_update=None):
        """
        Publish status update via ZMQ.
        
        Args:
            state (str): Current state ("starting", "running", "updating", "error", "offline")
            last_db_update (datetime, optional): Timestamp of last database update
        """
        if not self.status_publisher:
            return
            
        try:
            status = {
                "state": state,
                "last_db_update": last_db_update.isoformat() if last_db_update else None,
                "pid": os.getpid()
            }
            status_json = json.dumps(status)
            self.log_message("STATUS", f"Publishing state: {state}", level=2)
            self.status_publisher.send_string(status_json)
        except Exception as e:
            self.log_message("ERROR", f"Failed to publish status: {e}", level=1)
    
    def publish_event(self, event_type, event_data):
        """
        Publish event update via ZMQ.
        
        Args:
            event_type (str): Type of event ("journal", "commodity", "system", "mining")
            event_data (dict): Event data to publish
        """
        if not self.event_publisher:
            return
            
        try:
            event = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "type": event_type,
                "data": event_data
            }
            event_json = json.dumps(event)
            self.event_publisher.send_string(event_json)
            self.log_message("EVENT", f"Published {event_type} event", level=2)
        except Exception as e:
            self.log_message("ERROR", f"Failed to publish event: {e}", level=1)
    
    def process_journal_message(self, message):
        """
        Process journal messages for system data.
        
        Args: message (dict): EDDN journal message        
        Returns: bool: True if message was processed successfully
        """
        try:
            # Get the inner message object
            msg_data = message.get("message")
            if not msg_data:
                self.log_message("ERROR", "Missing message field in journal data", level=1)
                return False
                
            # Get event type and process accordingly
            message_type = msg_data.get("event")
            if not message_type:
                self.log_message("ERROR", "Missing event type in journal data", level=1)
                return False

            # Process power and system state data for FSDJump events
            if message_type == 'FSDJump':
                # Process power data
                self.handle_power_data(msg_data)
                
                # Process system state
                self.handle_system_state(msg_data)
            # Process colony ship events
            elif message_type == 'Docked' or message_type == 'FSSSignalDiscovered':
                self.handle_colony_ship_event(msg_data, message_type)
            # Process SAASignalsFound events
            elif message_type == 'SAASignalsFound':
                self.handle_saa_signals_event(msg_data)

            # Always process mining events and publish journal events
            self.process_mining_events(msg_data)
            
            return True
        except Exception as e:
            self.log_message("ERROR", f"Error processing journal message: {str(e)}", level=1)
            self.log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=3)
            return False
    
    def handle_power_data(self, message):
        """
        Process power data from FSDJump events.
        
        Args:
            message (dict): FSDJump event data
        """
        # Extract system info
        system_name = message.get("StarSystem", "")
        system_id64 = message.get("SystemAddress")
        
        if not system_name or not system_id64:
            self.log_message("POWER", f"Missing system info - Name: {system_name}, ID64: {system_id64}", level=2)
            return
        
        try:
            # Get current values from database
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    SELECT controlling_power, power_state, powers_acquiring
                    FROM systems
                    WHERE id64 = %s
                """, (system_id64,))
                
                row = cur.fetchone()
                if not row:
                    self.log_message("POWER", f"System not found in database: {system_name}", level=2)
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
                
                # Always log the current power state
                power_status = f"Current power state for {system_name}: "
                if current_power:
                    power_status += f"Controlled by {current_power}"
                    if current_state:
                        power_status += f" ({current_state})"
                else:
                    power_status += "No controlling power"
                
                if current_powers and len(current_powers) > 0:
                    power_status += f", Powers acquiring: {', '.join(current_powers)}"
                
                self.log_message("POWER", power_status, level=1)
                
                # Always publish power event, even if no changes
                self.publish_event('power', {
                    'system_id64': system_id64,
                    'system_name': system_name,
                    'controlling_power': controlling_power,
                    'power_state': power_state,
                    'powers_acquiring': powers,
                    'current_state': {
                        'controlling_power': current_power,
                        'power_state': current_state,
                        'powers_acquiring': current_powers
                    }
                })

                # Validate powers is a list
                if isinstance(powers, str):
                    powers = [powers]
                elif not isinstance(powers, list):
                    self.log_message("POWER", f"Powers has unexpected type: {type(powers)}", level=2)
                    powers = current_powers
                
                # Filter out controlling power from powers array if it exists there
                if controlling_power and controlling_power in powers:
                    self.log_message("POWER", f"System {system_name} - Current controlling power: {controlling_power}", level=1)
                    self.log_message("POWER", f"Current powers_acquiring array: {powers}", level=1)
                    powers = [p for p in powers if p != controlling_power]
                    self.log_message("POWER", f"Removed controlling power {controlling_power} from powers_acquiring array", level=1)
                    self.log_message("POWER", f"New powers_acquiring array: {powers}", level=1)
                
                # Check if there are actual changes, considering NULL values
                power_changed = has_controlling_power and current_power != controlling_power and not (current_power is None and controlling_power is None)
                state_changed = has_power_state and current_state != power_state and not (current_state is None and power_state is None)
                powers_changed = has_powers and sorted(current_powers) != sorted(powers)
                
                # Log detailed changes when they occur
                if power_changed:
                    self.log_message("POWER", f"Power change detected for {system_name}: Controlling power changing from {current_power or 'None'} to {controlling_power or 'None'}", level=1)
                
                if state_changed:
                    self.log_message("POWER", f"State change detected for {system_name}: Power state changing from {current_state or 'None'} to {power_state or 'None'}", level=1)
                
                if powers_changed:
                    self.log_message("POWER", f"Powers acquiring change detected for {system_name}: From {', '.join(current_powers) if current_powers else 'None'} to {', '.join(powers) if powers else 'None'}", level=1)
                
                # Always update controlling_power and power_state, even if they haven't changed
                should_update = True
                
                # Log what changed
                changes = []
                if power_changed:
                    changes.append(f"controlling_power: {current_power} → {controlling_power}")
                if state_changed:
                    changes.append(f"power_state: {current_state} → {power_state}")
                if powers_changed:
                    changes.append(f"powers_acquiring: {current_powers} → {powers}")
                
                # Start a transaction
                cur.execute("BEGIN")
                
                # Log database operation start
                self.log_message("DATABASE", f"Starting database update for system {system_name} (ID64: {system_id64})", level=2)
                
                # Get current timestamp and adjust it by subtracting one hour
                #current_timestamp = datetime.now() - timedelta(hours=1)
                
                # Build UPDATE query - always include controlling_power and power_state
                update_fields = ["controlling_power = %s", "power_state = %s", "last_updated = %s"]
                params = [controlling_power, power_state, current_timestamp]
                
                # Only conditionally include powers_acquiring
                if powers_changed:
                    update_fields.append("powers_acquiring = %s::jsonb")
                    params.append(json.dumps(powers))
                
                # Always update
                query = f"""
                    UPDATE systems 
                    SET {', '.join(update_fields)}
                    WHERE id64 = %s
                    RETURNING id64, last_updated  -- Return ID and timestamp to verify update
                """
                params.append(system_id64)
                
                self.log_message("DATABASE", f"Executing query: {query.replace('%s', '?')}", level=3)
                cur.execute(query, params)
                
                # Verify update was successful
                if cur.rowcount == 0:
                    self.log_message("ERROR", f"Failed to update power data for {system_name}", level=1)
                    self.log_message("DATABASE", f"Database update failed for system {system_name}", level=1)
                    cur.execute("ROLLBACK")
                    return
                
                # Get the updated timestamp
                result = cur.fetchone()
                updated_timestamp = result[1] if result and len(result) > 1 else None
                
                # Commit transaction and log changes
                self.db_conn.commit()
                self.log_message("DATABASE", f"✓ Successfully committed database update for system {system_name}", level=1)
                
                # If no changes were detected but we still updated, log it
                if not changes:
                    self.log_message("POWER", f"✓ Refreshed power status for {system_name} (no changes detected)", level=1)
                else:
                    self.log_message("POWER", f"✓ Updated power status for {system_name}: {', '.join(changes)}", level=1)
                
                # Log the timestamp update
                if updated_timestamp:
                    self.log_message("POWER", f"System {system_name} last_updated timestamp set to: {updated_timestamp} (timezone adjusted)", level=1)
                else:
                    self.log_message("POWER", f"System {system_name} last_updated timestamp was updated (timezone adjusted)", level=1)
                
                # Log the new power state after update
                new_power_status = f"New power state for {system_name}: "
                if controlling_power:
                    new_power_status += f"Controlled by {controlling_power}"
                    if power_state:
                        new_power_status += f" ({power_state})"
                else:
                    new_power_status += "No controlling power"
                
                if powers and len(powers) > 0:
                    new_power_status += f", Powers acquiring: {', '.join(powers)}"
                
                self.log_message("POWER", new_power_status, level=1)
                
                # Publish event to notify other components
                self.publish_event('power', {
                    'system_id64': system_id64,
                    'system_name': system_name,
                    'changes': changes,
                    'controlling_power': controlling_power,
                    'power_state': power_state,
                    'powers_acquiring': powers
                })
        except Exception as e:
            self.log_message("ERROR", f"Failed to update power status: {e}", level=1)
            self.log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=3)
            try:
                self.db_conn.rollback()
            except:
                pass
    
    def handle_system_state(self, data):
        """
        Handle system state updates from FSDJump events.
        
        Args:
            data (dict): FSDJump event data
        """
        try:
            if 'SystemFaction' not in data or 'Factions' not in data:
                return
                
            system_faction = data['SystemFaction'].get('Name')
            if not system_faction:
                return
                
            self.log_message("STATE", f"Detecting controlling faction: {system_faction}", level=2)
            
            # System info
            system_name = data.get('StarSystem')
            system_id64 = data.get('SystemAddress')
            
            if not system_name or not system_id64:
                self.log_message("STATE", f"Missing system info in event data", level=2)
                return
            
            # Find the controlling faction in the factions list
            current_state = None
            for faction in data['Factions']:
                if faction['Name'] == system_faction:
                    if 'ActiveStates' in faction and faction['ActiveStates']:
                        # Take the first active state
                        current_state = faction['ActiveStates'][0]['State']
                        self.log_message("STATE", f"Detecting state: {current_state}", level=2)
                    break
            
            # Publish state update immediately
            self.publish_event('system', {
                'system_id64': system_id64,
                'system_name': system_name,
                'changes': [f"system_state: {current_state}"],
                'faction': system_faction,
                'immediate': True
            })

            if current_state is None:
                self.log_message("STATE", "No active state found for controlling faction", level=2)
                return
                
            # Update database
            with self.db_conn.cursor() as cursor:
                # Check current state in database
                cursor.execute("BEGIN")
                cursor.execute("""
                    SELECT system_state 
                    FROM systems 
                    WHERE id64 = %s
                """, (system_id64,))
                
                row = cursor.fetchone()
                if not row:
                    self.log_message("STATE", f"System {system_name} not found in database", level=2)
                    cursor.execute("ROLLBACK")
                    return
                    
                db_state = row[0]
                self.log_message("STATE", f"Comparing state: DB='{db_state}' vs Current='{current_state}'", level=2)
                
                # Update if states differ
                if db_state != current_state:
                    self.log_message("STATE", f"Updating state from '{db_state}' to '{current_state}'", level=2)
                    cursor.execute("""
                        UPDATE systems 
                        SET system_state = %s 
                        WHERE id64 = %s
                        RETURNING id64
                    """, (current_state, system_id64))
                    
                    # Verify update was successful
                    if cursor.rowcount == 0:
                        self.log_message("ERROR", f"Failed to update system state for {system_name}", level=1)
                        cursor.execute("ROLLBACK")
                        return
                    
                    # Commit transaction and log changes
                    self.db_conn.commit()
                    self.log_message("STATE", f"✓ Updated system state for {system_name} from '{db_state}' to '{current_state}'", level=1)
                    
                    # Publish final state update
                    self.publish_event('system', {
                        'system_id64': system_id64,
                        'system_name': system_name,
                        'changes': [f"system_state: {db_state} → {current_state}"]
                    })
                else:
                    self.log_message("STATE", "Disregarding state update - no change", level=2)
                    cursor.execute("ROLLBACK")
                    
        except Exception as e:
            self.log_message("ERROR", f"Error updating system state: {str(e)}", level=1)
            self.log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=3)
            try:
                self.db_conn.rollback()
            except:
                pass
    
    def process_mining_events(self, event_data):
        """
        Process mining related events for testing.
        Recursively searches through all event data for mining-related terms.
        """
        mining_keywords = [
            'mining', 'asteroid', 'prospector', 'refined', 'ring', 'laser', 'subsurface',
            'deposit', 'motherlode', 'seismic', 'charge', 'crack', 'fragment', 'chunk',
            'mineral', 'resource', 'extraction', 'pristine', 'depleted', 'reserves'
        ]
        
        def search_dict_for_mining(data, path=""):
            """Recursively search dictionary for mining-related terms."""
            found_terms = []
            if isinstance(data, dict):
                for key, value in data.items():
                    new_path = f"{path}.{key}" if path else key
                    # Check the key itself
                    if any(keyword in key.lower() for keyword in mining_keywords):
                        found_terms.append((new_path, key))
                    # Check the value
                    if isinstance(value, (dict, list)):
                        found_terms.extend(search_dict_for_mining(value, new_path))
                    elif isinstance(value, str) and any(keyword in value.lower() for keyword in mining_keywords):
                        found_terms.append((new_path, value))
            elif isinstance(data, list):
                for i, item in enumerate(data):
                    new_path = f"{path}[{i}]"
                    found_terms.extend(search_dict_for_mining(item, new_path))
            return found_terms

        # Get event type
        event_type = event_data.get('event', 'Unknown')
        
        # Always publish journal events
        self.publish_event('journal', {
            'event_type': event_type,
            'data': event_data
        })

        # Check for mining-specific events first
        mining_events = [
            'SupercruiseExit', 'SAASignalsFound', 'ProspectedAsteroid', 
            'AsteroidCracked', 'Cargo', 'MiningRefined', 'MarketSell'
        ]
        
        is_mining_event = event_type in mining_events
        mining_terms = search_dict_for_mining(event_data)
        
        if is_mining_event or mining_terms:
            # Log the event in red
            #self.log_message("MINING", f"Detected mining event: {event_type}", level=2, color=RED)
            #if mining_terms:
                #self.log_message("MINING", f"Found mining terms: {mining_terms}", level=3, color=RED)
            
            # Publish the event via ZMQ
            self.publish_event('mining', {
                'event_type': event_type,
                'timestamp': event_data.get('timestamp'),
                'mining_terms': mining_terms,
                'data': event_data
            })
            
            return True
        return False
    
    def process_commodity_message(self, message):
        """
        Process a single EDDN commodity message.
        
        Args:
            message (dict): EDDN commodity message
            
        Returns:
            tuple: (station_key, commodities) or (None, None) if no relevant data
        """
        try:
            # Skip Fleet Carrier data
            if message.get("stationType") == "FleetCarrier" or \
               (message.get("economies") and message["economies"][0].get("name") == "Carrier"):
                self.log_message("DEBUG", f"Skipped Fleet Carrier Data: {message.get('stationName')}", level=2)
                return None, None
                
            station_name = message.get("stationName")
            system_name = message.get("systemName", "Unknown")
            market_id = message.get("marketId")
            timestamp = message.get("timestamp")
            
            self.log_message("DEBUG", f"Processing station {station_name} in {system_name} (timestamp: {timestamp})", level=2)
            
            if not timestamp:
                self.log_message("ERROR", "Message missing timestamp", level=1)
                return None, None
                
            if market_id is None:
                self.log_message("DEBUG", f"Live update without marketId: {station_name} in system {system_name}", level=2)
            
            if not station_name or not system_name:
                self.log_message("DEBUG", "Message missing station name or system name", level=2)
                return None, None
                
            # Get system_id64 from systems table
            try:
                with self.db_conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id64
                        FROM systems
                        WHERE name = %s
                    """, (system_name,))
                    row = cursor.fetchone()
                    if not row:
                        self.log_message("ERROR", f"System not found in database: {system_name}", level=1)
                        return None, None
                    system_id64 = row[0]
            except Exception as e:
                self.log_message("ERROR", f"Failed to get system_id64 for {system_name}: {str(e)}", level=1)
                return None, None
                
            # Process commodities
            station_commodities = {}
            commodities = message.get("commodities", [])
            self.log_message("DEBUG", f"Found {len(commodities)} commodities", level=3)
            
            for commodity in commodities:
                name = commodity.get("name", "").lower()
                if not name:
                    continue
                    
                if name not in self.commodity_map:
                    continue  # Skip logging unknown commodities
                    
                sell_price = commodity.get("sellPrice", 0)
                if sell_price <= 0:
                    continue
                    
                demand = commodity.get("demand", 0)
                self.log_message("DEBUG", f"Processing commodity: {name} (price: {sell_price}, demand: {demand})", level=3)
                station_commodities[self.commodity_map[name]] = (sell_price, demand)
                self.log_message("COMMODITY", f"✓ {self.commodity_map[name]} at {station_name}: {sell_price:,} cr (demand: {demand:,})", level=3)
                
            if station_commodities:
                self.log_message("COMMODITY", f"Added {len(station_commodities)} mining commodities to buffer for {station_name}", level=2)
                # Publish status update to indicate activity
                self.publish_status("running", datetime.now(timezone.utc))
                # Store timestamp and system_id64 with commodities
                return (system_id64, station_name), (station_commodities, timestamp)
            else:
                self.log_message("DEBUG", f"No relevant commodities found at {station_name}", level=2)
                
        except Exception as e:
            self.log_message("ERROR", f"Error processing message: {str(e)}", level=1)
            self.log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=3)
            
        return None, None
    
    def flush_commodities_to_db(self):
        """
        Write buffered commodities to database.
        
        Returns:
            tuple: (stations_processed, total_commodities)
        """
        if not self.commodity_buffer:
            self.log_message("DATABASE", "No commodities in buffer to write", level=2)
            return 0, 0
        
        total_commodities = 0
        stations_processed = 0
        total_stations = len(self.commodity_buffer)
        
        try:
            self.log_message("DATABASE", f"Writing to Database starting... ({total_stations} stations to process)", level=1)
            self.publish_status("updating", datetime.now(timezone.utc))
            
            # Process each station's commodities
            for (system_id64, station_name), (new_map, eddn_timestamp) in self.commodity_buffer.items():
                try:
                    with self.db_conn.cursor() as cursor:
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
                            self.log_message("ERROR", f"Station not found in database: {station_name} in system {system_id64}", level=1)
                            cursor.execute("ROLLBACK")
                            continue
                            
                        station_id = row[0]
                        self.log_message("DATABASE", f"Processing station {station_name} ({len(new_map)} commodities)", level=2)
                        
                        # Delete existing commodities using proper primary key
                        try:
                            cursor.execute("""
                                DELETE FROM station_commodities_mapped 
                                WHERE system_id64 = %s AND station_name = %s
                            """, (system_id64, station_name))
                            rows_deleted = cursor.rowcount
                            self.log_message("DATABASE", f"Deleted {rows_deleted} existing commodities for {station_name}", level=2)
                        except Exception as e:
                            self.log_message("ERROR", f"Failed to delete existing commodities for {station_name}: {str(e)}", level=1)
                            cursor.execute("ROLLBACK")
                            continue
                        
                        # Insert new commodities with improved error handling
                        try:
                            # Get commodity ID mapping once
                            commodity_ids = self._get_commodity_ids()
                            
                            # Prepare data for insert
                            commodity_data = []
                            for commodity_name, data in new_map.items():
                                # Skip if commodity not found in mapping
                                if commodity_name not in commodity_ids:
                                    self.log_message("ERROR", f"Commodity not found in ID mapping: {commodity_name}", level=2)
                                    continue
                                    
                                commodity_id = commodity_ids[commodity_name]
                                sell_price, demand = data
                                
                                # Add to insert data
                                commodity_data.append((system_id64, station_id, station_name, commodity_id, sell_price, demand))
                            
                            # Validate data before insert
                            for data in commodity_data:
                                if None in data:
                                    raise ValueError(f"Invalid commodity data: {data}")
                            
                            # Execute the insert
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
                            
                            self.log_message("DATABASE", f"Inserted/Updated {rows_affected} commodities for {station_name} (expected {len(new_map)})", level=2)
                            
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
                            self.log_message("ERROR", f"Failed to insert commodities for {station_name}: {str(e)}", level=1)
                            self.log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=3)
                            cursor.execute("ROLLBACK")
                            continue
                        
                        # Update station timestamp using EDDN timestamp
                        try:
                            # Parse EDDN timestamp (format: "2025-01-11T01:19:39Z")
                            # Convert to database format (timestamp without time zone)
                            try:
                                dt = datetime.strptime(eddn_timestamp, "%Y-%m-%dT%H:%M:%SZ")
                                db_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
                                self.log_message("DEBUG", f"Converting EDDN timestamp '{eddn_timestamp}' to DB format '{db_timestamp}'", level=3)
                            except ValueError as e:
                                self.log_message("ERROR", f"Failed to parse EDDN timestamp '{eddn_timestamp}': {str(e)}", level=1)
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
                                self.log_message("ERROR", f"Failed to update timestamp for {station_name} - no rows affected", level=1)
                                cursor.execute("ROLLBACK")
                                continue
                            else:
                                updated_time = cursor.fetchone()[0]
                                self.log_message("DATABASE", f"Updated timestamp for {station_name} from EDDN time '{eddn_timestamp}' to DB time '{updated_time}'", level=2)
                        except Exception as e:
                            self.log_message("ERROR", f"Failed to update timestamp for {station_name}: {str(e)}", level=1)
                            cursor.execute("ROLLBACK")
                            continue
                        
                        # Commit transaction for this station
                        cursor.execute("COMMIT")
                        stations_processed += 1
                        total_commodities += len(new_map)
                        
                        # Get system name for event publishing
                        cursor.execute("""
                            SELECT name
                            FROM systems
                            WHERE id64 = %s
                        """, (system_id64,))
                        system_name = cursor.fetchone()[0]
                        
                        # Publish event
                        self.publish_event('commodity', {
                            'system_id64': system_id64,
                            'system_name': system_name,
                            'station_name': station_name,
                            'summary': {
                                'updated': len(new_map),
                                'added': len(new_map) - rows_deleted if rows_deleted > 0 else len(new_map),
                                'removed': rows_deleted
                            }
                        })
                        
                        # Log progress every 10 stations
                        if stations_processed % 10 == 0:
                            self.log_message("DATABASE", f"Progress: {stations_processed}/{total_stations} stations processed", level=2)
                
                except Exception as e:
                    self.log_message("ERROR", f"Failed to process station {station_name}: {str(e)}", level=1)
                    self.log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=3)
                    try:
                        self.db_conn.rollback()
                    except:
                        pass
                    continue
            
            self.log_message("DATABASE", f"✓ Successfully updated {stations_processed} stations with {total_commodities} commodities", level=1)
            
        except Exception as e:
            self.log_message("ERROR", f"Database error: {str(e)}", level=1)
            self.log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=3)
            try:
                self.db_conn.rollback()
            except:
                pass
            return 0, 0
            
        finally:
            self.commodity_buffer.clear()
            self.publish_status("running", datetime.now(timezone.utc))
            
        return stations_processed, total_commodities
    
    def log_message(self, tag, message, level=2, color=None):
        """
        Log a message with the specified tag and level.
        
        Args:
            tag (str): Message tag (e.g., "INFO", "ERROR")
            message (str): The message to log
            level (int): Message importance level (1=high, 3=low)
            color (str, optional): ANSI color code to use
        """
        if level > self.debug_level:
            return
            
        timestamp = datetime.now().strftime("%Y:%m:%d-%H:%M:%S")
        
        # Assign color based on tag if not explicitly provided
        if color is None:
            if tag == "ERROR":
                color = RED
            elif tag == "DATABASE":
                color = CYAN
            elif tag == "ERROR":
                color = RED
            elif tag == "POWER":
                color = GREEN
            elif tag == "STATE":
                color = MAGENTA
            elif tag == "MINING":
                color = RED
            elif tag == "COMMODITY":
                color = BLUE
            elif tag == "COLONY":
                color = ORANGE
            elif tag == "HEMATITE":
                color = RED
            else:
                color = WHITE
        
        print(f"{color}[{timestamp}] [{os.getpid()}] [{tag}] {message}{RESET}", flush=True)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.log_message("STOPPING", "EDDN Update Service (received signal)", level=1)
        self.stop()
    
    def _cleanup(self):
        """Cleanup function to be called on exit."""
        try:
            if self.db_conn:
                self.db_conn.close()
                self.db_conn = None
                
            if self.status_publisher:
                self.status_publisher.close()
                self.status_publisher = None
                
            if self.event_publisher:
                self.event_publisher.close()
                self.event_publisher = None
                
            if self.zmq_context:
                self.zmq_context.term()
                self.zmq_context = None
                
            self.log_message("TERMINATED", "EDDN Update Service cleaned up", level=1)
        except:
            pass
    
    def handle_colony_ship_event(self, message, event_type):
        """
        Process colony ship events (Docked and FSSSignalDiscovered).
        
        Args:
            message (dict): Event data
            event_type (str): Type of event ('Docked' or 'FSSSignalDiscovered')
        """
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
                self.log_message("COLONY", f"Missing SystemAddress in {event_type} event", level=1)
                return
            
            # Get system name from database
            system_name = None
            try:
                with self.db_conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT name
                        FROM systems
                        WHERE id64 = %s
                    """, (system_id64,))
                    row = cursor.fetchone()
                    if row:
                        system_name = row[0]
            except Exception as e:
                self.log_message("ERROR", f"Failed to get system name for ID64 {system_id64}: {str(e)}", level=1)
            
            # Process event based on type
            colony_data = {
                'system_id64': system_id64,
                'system_name': system_name or "Unknown System",
                'event_type': event_type,
                'timestamp': message.get('timestamp')
            }
            
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
                
                colony_data.update({
                    'station_name': station_name,
                    'station_type': station_type,
                    'station_id': station_id,
                    'station_faction': station_faction,
                    'station_government': station_government,
                    'economy': economy,
                    'economies': economies,
                    'landing_pads': landing_pads
                })
                self.log_message("COLONY", f"✓ Docked at colony ship in {system_name or 'Unknown System'} (ID64: {system_id64})", level=1, color=ORANGE)
            elif event_type == 'FSSSignalDiscovered':
                # Extract FSSSignalDiscovered event specific fields
                station_name = message.get('SignalName')
                signal_type = message.get('SignalType')
                
                colony_data.update({
                    'station_name': station_name,
                    'signal_type': signal_type
                })
                self.log_message("COLONY", f"✓ Discovered colony ship in {system_name or 'Unknown System'} (ID64: {system_id64})", level=1, color=ORANGE)
            
            # Publish colony event
            self.publish_event('colony', colony_data)
            
            # Save to database
            self.save_colony_ship_to_db(
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
            self.log_message("ERROR", f"Error processing colony ship event: {str(e)}", level=1)
            self.log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=3)
    
    def save_colony_ship_to_db(self, system_id64, station_name, station_id=None, station_type=None, 
                              station_faction=None, station_government=None, economy=None, 
                              economies=None, landing_pads=None, signal_type=None):
        """
        Save colony ship information to the database.
        
        Args:
            system_id64 (int): System Address (ID64)
            station_name (str): Name of the station
            station_id (int, optional): Market ID
            station_type (str, optional): Type of station
            station_faction (dict, optional): Station faction data
            station_government (str, optional): Station government
            economy (str, optional): Primary economy
            economies (list, optional): List of economies
            landing_pads (dict, optional): Landing pad information
            signal_type (str, optional): Signal type for FSSSignalDiscovered events
        """
        try:
            if not system_id64 or not station_name:
                self.log_message("ERROR", "Missing required fields for colony ship database entry", level=1)
                return False
            
            # Convert JSON fields to strings if they're not None
            station_faction_json = json.dumps(station_faction) if station_faction else None
            economies_json = json.dumps(economies) if economies else None
            landing_pads_json = json.dumps(landing_pads) if landing_pads else None
            
            # Start transaction
            with self.db_conn.cursor() as cursor:
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
                    self.log_message("COLONY", f"Updating existing colony ship record (ID: {colony_id})", level=2)
                    
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
                        self.log_message("ERROR", f"Failed to update colony ship record", level=1)
                        cursor.execute("ROLLBACK")
                        return False
                    
                    self.log_message("COLONY", f"✓ Updated colony ship record in database", level=1)
                else:
                    # Insert new record
                    self.log_message("COLONY", f"Creating new colony ship record", level=2)
                    
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
                    self.log_message("COLONY", f"✓ Created new colony ship record in database (ID: {new_id})", level=1)
                
                # Commit transaction
                self.db_conn.commit()
                return True
                
        except Exception as e:
            self.log_message("ERROR", f"Database error saving colony ship: {str(e)}", level=1)
            self.log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=3)
            try:
                self.db_conn.rollback()
            except:
                pass
            return False

    def handle_saa_signals_event(self, message):
        """
        Process SAASignalsFound events and save Haematite signals to the database.
        
        Args:
            message (dict): The SAASignalsFound event message
        """
        try:
            # Extract required fields
            timestamp = message.get("timestamp")
            body_name = message.get("BodyName")
            system_id64 = message.get("SystemAddress")
            body_id = message.get("BodyID")
            signals = message.get("Signals", [])
            
            # Validate required fields
            if not all([timestamp, body_name, system_id64, body_id, signals]):
                self.log_message("ERROR", f"Missing required fields in SAASignalsFound event: {message}", level=1)
                return
            
            # Check if any of the signals are Haematite/Hematite (with various spellings)
            hematite_signals = []
            for signal in signals:
                signal_type = signal.get("Type", "")
                if signal_type.lower() in ["haematite", "hematite", "hamaetite", "hemaetite"]:
                    hematite_signals.append(signal)
            
            # Only proceed if we found Haematite signals
            if not hematite_signals:
                self.log_message("INFO", f"No Haematite signals found in SAASignalsFound event for {body_name}", level=3)
                return
            
            # Get system name from database
            system_name = None
            try:
                with self.db_conn.cursor() as cursor:
                    cursor.execute("SELECT name FROM systems WHERE id64 = %s", (system_id64,))
                    result = cursor.fetchone()
                    if result:
                        system_name = result[0]
            except Exception as e:
                self.log_message("ERROR", f"Error fetching system name: {str(e)}", level=1)
            
            if not system_name:
                system_name = f"Unknown System ({system_id64})"
            
            # Save each Haematite signal to the database
            for signal in hematite_signals:
                signal_type = signal.get("Type", "")
                signal_count = signal.get("Count", 0)
                
                # Save to database
                self.save_saa_signals_to_db(
                    timestamp=timestamp,
                    body_name=body_name,
                    system_id64=system_id64,
                    body_id=body_id,
                    mineral_type=signal_type,
                    signal_count=signal_count
                )
            
            # Publish event to websocket
            hematite_count = sum(signal.get("Count", 0) for signal in hematite_signals)
            self.log_message("HEMATITE", f"Found {hematite_count} Haematite signals on {body_name} in {system_name}", level=1, color=RED)
            
            self.publish_event("hematite", {
                "system_name": system_name,
                "system_id64": system_id64,
                "body_name": body_name,
                "body_id": body_id,
                "hematite_count": hematite_count,
                "signals": signals  # Include all signals for display
            })
            
        except Exception as e:
            self.log_message("ERROR", f"Error processing SAASignalsFound event: {str(e)}", level=1)
            self.log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=3)

    def save_saa_signals_to_db(self, timestamp, body_name, system_id64, body_id, mineral_type, signal_count, ring_name=None, ring_type=None, reserve_level=None):
        """
        Save Haematite signals to the database.
        
        Args:
            timestamp (str): Event timestamp
            body_name (str): Name of the celestial body
            system_id64 (int): System ID64
            body_id (int): Body ID
            mineral_type (str): Type of mineral (Haematite/Hematite)
            signal_count (int): Number of signals
            ring_name (str, optional): Name of the ring if available
            ring_type (str, optional): Type of ring if available
            reserve_level (str, optional): Reserve level if available
        """
        try:
            # Standardize the mineral type to handle different spellings
            if mineral_type.lower() in ["haematite", "hematite", "hamaetite", "hemaetite"]:
                mineral_type = "Haematite"  # Use standard spelling
            
            # Check if this signal already exists in the database
            with self.db_conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, signal_count, first_seen, last_updated 
                    FROM haematite_signals 
                    WHERE system_id64 = %s AND body_name = %s AND 
                          (ring_name = %s OR (ring_name IS NULL AND %s IS NULL)) AND 
                          mineral_type = %s
                    """,
                    (system_id64, body_name, ring_name, ring_name, mineral_type)
                )
                result = cursor.fetchone()
                
                if result:
                    # Signal exists, update it if count has changed
                    signal_id, existing_count, first_seen, last_updated = result
                    
                    if existing_count != signal_count:
                        cursor.execute(
                            """
                            UPDATE haematite_signals 
                            SET signal_count = %s, 
                                last_updated = NOW(),
                                reserve_level = COALESCE(%s, reserve_level),
                                ring_type = COALESCE(%s, ring_type)
                            WHERE id = %s
                            """,
                            (signal_count, reserve_level, ring_type, signal_id)
                        )
                        self.log_message("HEMATITE", f"Updated Haematite signal count for {body_name} from {existing_count} to {signal_count}", level=2)
                    else:
                        # Only update last_updated if it's been more than a day
                        last_update_age = datetime.now() - last_updated
                        if last_update_age.days > 0:
                            cursor.execute(
                                "UPDATE haematite_signals SET last_updated = NOW() WHERE id = %s",
                                (signal_id,)
                            )
                else:
                    # New signal, insert it
                    cursor.execute(
                        """
                        INSERT INTO haematite_signals 
                        (system_id64, body_name, ring_name, ring_type, mineral_type, signal_count, reserve_level, first_seen, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                        """,
                        (system_id64, body_name, ring_name, ring_type, mineral_type, signal_count, reserve_level)
                    )
                    self.log_message("HEMATITE", f"Added new Haematite signal for {body_name} with count {signal_count}", level=2)
                
                # Commit the transaction if auto_commit is disabled
                if not self.auto_commit:
                    self.db_conn.commit()
                    
        except Exception as e:
            self.log_message("ERROR", f"Error saving Haematite signal to database: {str(e)}", level=1)
            self.log_message("ERROR", f"Traceback: {traceback.format_exc()}", level=3)
            
            # Rollback the transaction if auto_commit is disabled
            if not self.auto_commit:
                self.db_conn.rollback()


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='EDDN Live Update Service')
    parser.add_argument('--auto', action='store_true', help='Automatically commit changes')
    parser.add_argument('--db', help='Database URL (e.g. postgresql://user:pass@host:port/dbname)')
    parser.add_argument('--debug-level', type=int, choices=[0, 1, 2, 3], default=1, 
                      help='Debug level (0=silent, 1=critical, 2=normal, 3=verbose)')
    args = parser.parse_args()
    
    # Set DATABASE_URL from argument or environment variable
    database_url = args.db or os.getenv('DATABASE_URL')
    if not database_url:
        print(f"{RED}[ERROR] Database URL must be provided via --db argument or DATABASE_URL environment variable{RESET}")
        return 1
    
    # Create and start updater
    updater = EDDNUpdater(
        database_url=database_url,
        debug_level=args.debug_level,
        auto_commit=args.auto
    )
    
    return updater.start()


if __name__ == '__main__':
    sys.exit(main())
