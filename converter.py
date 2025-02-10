import sqlite3
import json
import ijson  # For streaming JSON parsing
from pathlib import Path
import sys
import argparse
from typing import Generator, Dict, Any
import math
from tqdm import tqdm
import time
from decimal import Decimal
import zlib  # Built-in compression
from rich.live import Live
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn
from rich.panel import Panel
from rich.layout import Layout
from rich.console import Console
from rich.text import Text
from rich.console import Group  # Add Group import
from rich.table import Table

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

TOTAL_ENTRIES = 24400  # Total number of entries in the file

MINERALS = {
    'Alexandrite', 'Bauxite', 'Benitoite', 'Bertrandite', 'Bromellite',
    'Coltan', 'Cryolite', 'Gallite', 'Goslarite', 'Grandidierite',
    'Indite', 'Jadeite', 'Lepidolite', 'Lithium Hydroxide',
    'Low Temperature Diamonds', 'LowTemperatureDiamond', 'Methane Clathrate',
    'Methanol Monohydrate Crystals', 'Moissanite', 'Monazite',
    'Musgravite', 'Painite', 'Pyrophyllite', 'Rhodplumsite',
    'Rutile', 'Serendibite', 'Taaffeite', 'Uraninite', 'Void Opal'
}

METALS = {
    'Aluminium', 'Beryllium', 'Bismuth', 'Cobalt', 'Copper',
    'Gallium', 'Gold', 'Hafnium 178', 'Indium', 'Lanthanum',
    'Lithium', 'Osmium', 'Palladium', 'Platinum', 'Praseodymium',
    'Samarium', 'Silver', 'Tantalum', 'Thallium', 'Thorium',
    'Titanium', 'Uranium'
}

# Combined set for checking both minerals and metals
MINERAL_SIGNALS = MINERALS | METALS

def calculate_distance(x: float, y: float, z: float, origin_x: float = 0, origin_y: float = 0, origin_z: float = 0) -> float:
    """Calculate distance between two points in 3D space."""
    return math.sqrt((x - origin_x)**2 + (y - origin_y)**2 + (z - origin_z)**2)

def create_database(db_path: str):
    """Create the SQLite database schema."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Create commodity types table first
    c.execute('''CREATE TABLE IF NOT EXISTS commodity_types (
        commodity_id INTEGER PRIMARY KEY,
        commodity_name TEXT UNIQUE
    )''')

    # Main systems table with frequently searched fields
    c.execute('''CREATE TABLE IF NOT EXISTS systems (
        id64 INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        x REAL,
        y REAL,
        z REAL,
        distance_from_sol REAL,
        controlling_power TEXT,
        power_state TEXT,
        powers_acquiring JSON,
        UNIQUE(name)
    )''')
    
    # Stations table for quick access to station data
    c.execute('''CREATE TABLE IF NOT EXISTS stations (
        system_id64 INTEGER,
        station_id INTEGER,
        body TEXT,
        station_name TEXT,
        station_type TEXT,
        primary_economy TEXT,
        landing_pad_size TEXT,
        distance_to_arrival REAL,
        update_time TEXT,
        PRIMARY KEY (system_id64, station_id),
        FOREIGN KEY(system_id64) REFERENCES systems(id64)
    )''')
    
    # Table for mineral signals in rings
    c.execute('''CREATE TABLE IF NOT EXISTS mineral_signals (
        system_id64 INTEGER,
        body_name TEXT,
        ring_name TEXT,
        mineral_type TEXT,
        signal_count INTEGER,
        reserve_level TEXT,
        ring_type TEXT,
        FOREIGN KEY(system_id64) REFERENCES systems(id64)
    )''')
    
    # Create mapped commodities table
    c.execute('''CREATE TABLE IF NOT EXISTS station_commodities_mapped (
        system_id64 INTEGER,
        station_id INTEGER,
        station_name TEXT,
        commodity_id INTEGER,
        sell_price INTEGER,
        demand INTEGER,
        PRIMARY KEY (system_id64, station_id, commodity_id),
        UNIQUE (system_id64, station_id, commodity_id),
        FOREIGN KEY(system_id64) REFERENCES systems(id64),
        FOREIGN KEY(system_id64, station_id) REFERENCES stations(system_id64, station_id),
        FOREIGN KEY(commodity_id) REFERENCES commodity_types(commodity_id)
    )''')
    
    # Create view for backward compatibility
    c.execute('''CREATE VIEW IF NOT EXISTS station_commodities AS
        SELECT sc.system_id64, sc.station_id, sc.station_name, ct.commodity_name, sc.sell_price, sc.demand
        FROM station_commodities_mapped sc
        JOIN commodity_types ct ON sc.commodity_id = ct.commodity_id
    ''')
    
    # Create indices for common searches
    c.execute('CREATE INDEX IF NOT EXISTS idx_controlling_power ON systems(controlling_power)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_coordinates ON systems(x, y, z)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_distance ON systems(distance_from_sol)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_mineral_type ON mineral_signals(mineral_type)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_commodity_search ON station_commodities_mapped(commodity_id, sell_price, demand)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_ring_search ON mineral_signals(ring_type, reserve_level)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_station_search ON stations(landing_pad_size, station_type)')
    
    conn.commit()
    return conn

def extract_mineral_signals(body: Dict) -> list:
    """Extract mineral signals from a body's rings."""
    signals = []
    if 'rings' in body:
        for ring in body['rings']:
            ring_type = ring.get('type', 'Unknown')
            
            # First add the ring itself, regardless of signals
            signals.append({
                'body_name': body['name'],
                'ring_name': ring['name'],
                'mineral_type': None,
                'signal_count': 0,
                'reserve_level': body.get('reserveLevel', 'Unknown'),
                'ring_type': ring_type
            })
            
            # Then add any hotspot signals if they exist
            if 'signals' in ring and 'signals' in ring['signals']:
                for mineral, count in ring['signals']['signals'].items():
                    if mineral in MINERAL_SIGNALS:
                        # Convert LowTemperatureDiamond to Low Temperature Diamonds
                        mineral_name = "Low Temperature Diamonds" if mineral == "LowTemperatureDiamond" else mineral
                        signals.append({
                            'body_name': body['name'],
                            'ring_name': ring['name'],
                            'mineral_type': mineral_name,
                            'signal_count': count,
                            'reserve_level': body.get('reserveLevel', 'Unknown'),
                            'ring_type': ring_type
                        })
    return signals

def get_or_create_commodity_id(conn, commodity_name: str) -> int:
    """Get the ID for a commodity, creating it if it doesn't exist."""
    c = conn.cursor()
    
    # Try to get existing ID
    c.execute('SELECT commodity_id FROM commodity_types WHERE commodity_name = ?', (commodity_name,))
    result = c.fetchone()
    
    if result:
        return result[0]
    
    # Create new entry if it doesn't exist
    c.execute('INSERT INTO commodity_types (commodity_name) VALUES (?)', (commodity_name,))
    conn.commit()
    return c.lastrowid

def extract_station_commodities(conn, station: Dict) -> list:
    """Extract relevant commodity data from a station."""
    commodities = []
    if 'market' in station and 'commodities' in station['market']:
        for commodity in station['market']['commodities']:
            # Get commodity name directly - no need to convert
            commodity_name = commodity['name']
            #if commodity_name == 'Low Temperature Diamonds':
            #    commodity_name = 'LowTemperatureDiamond'
            
            if commodity_name in MINERAL_SIGNALS:
                # Get or create commodity ID
                commodity_id = get_or_create_commodity_id(conn, commodity_name)
                
                commodities.append({
                    'station_name': station['name'],
                    'commodity_id': commodity_id,  # Store ID instead of name
                    'sell_price': commodity['sellPrice'],
                    'demand': commodity['demand']
                })
    return commodities

def process_json_stream(json_file: str) -> Generator[Dict[Any, Any], None, None]:
    """Stream the JSON file one system at a time to avoid memory issues."""
    with open(json_file, 'rb') as file:
        # Use float parsing directly to avoid Decimal conversion
        parser = ijson.items(file, 'item', use_float=True)
        for system in parser:
            yield system

def count_systems(json_file: str) -> int:
    """Quickly count the number of systems in the JSON file using ijson."""
    console = Console()
    print("Counting systems...")  # Use regular print for immediate output
    count = 0
    with open(json_file, 'rb') as file:
        # Use items() to get each system directly
        for _ in ijson.items(file, 'item', use_float=True):
            count += 1
            sys.stdout.write(f"\rFound {count:,} systems")
            sys.stdout.flush()
    print("\nDone counting!")
    return count

def convert_json_to_sqlite(json_file: str, db_file: str, max_distance: float, exclude_carriers: bool = False, compression: str = 'none', trim_entries: bool = False, totalentries: int = None):
    """Convert the large JSON file to SQLite database."""
    # First count total systems
    if totalentries is None:
        total_entries = count_systems(json_file)
    else:
        print(f"Using provided total of {totalentries:,} systems")
        total_entries = totalentries
    
    conn = create_database(db_file)
    c = conn.cursor()
    
    try:
        # Initialize counters
        processed = 0
        skipped_distance = 0
        skipped_carriers_stations = 0
        skipped_no_market = 0
        system_stations = 0  # Running total of system-level stations
        body_stations = 0    # Running total of body-level stations
        total_root_stations_processed = 0  # Total stations seen at system level
        total_body_stations_processed = 0  # Total stations seen at body level
        total_stations = 0   # Running total of all valid stations
        trimmed_outfitting = 0
        trimmed_shipyard = 0
        start_time = time.time()
        
        # Begin transaction
        c.execute('BEGIN')
        
        console = Console()
        progress = Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(bar_width=40, complete_style="green", finished_style="green", pulse_style="green"),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            "[cyan]{task.completed:>7,}[/]/[cyan]{task.total:,}[/] systems",
            "•",
            TimeRemainingColumn(),
            console=console,
            expand=False  # Don't expand to full width
        )
        
        # Add main task with proper description
        task = progress.add_task(
            f"[cyan]Processing systems within {max_distance:,.0f} ly of Sol[/]",
            total=total_entries
        )
        
        def get_renderable():
            elapsed = time.time() - start_time
            speed = processed / elapsed if elapsed > 0 else 0
            
            stats = Text()
            stats.append(f"Speed: {speed:.1f} systems/s\n\n", style="bold green")
            
            stats.append("Station Counts:\n", style="bold magenta")
            stats.append(f"Station entries: {system_stations:,}\n")
            stats.append(f"Body station entries: {body_stations:,}\n")
            stats.append(f"Total stations: {total_stations:,}\n")
            
            stats.append("\nSkipped Entries:\n", style="bold yellow")
            stats.append(f"Distance skipped: {skipped_distance:,}\n")
            if exclude_carriers:
                stats.append(f"Carriers skipped: {skipped_carriers_stations:,}\n")
            stats.append(f"No market: {skipped_no_market:,}\n")
            if trim_entries:
                stats.append(f"Trimmed shipyard: {trimmed_shipyard:,}\n")
                stats.append(f"Trimmed outfitting: {trimmed_outfitting:,}\n")
            
            return Group(progress, stats)
        
        with Live(get_renderable(), console=console, refresh_per_second=4, transient=True) as live:
            for system in process_json_stream(json_file):
                # Calculate distance from Sol
                coords = system.get('coords', {})
                x = float(coords.get('x', 0))
                y = float(coords.get('y', 0))
                z = float(coords.get('z', 0))
                distance = calculate_distance(x, y, z)
                
                # Skip systems outside the specified radius
                if distance > max_distance:
                    skipped_distance += 1
                    progress.update(task, advance=1)
                    live.update(get_renderable())
                    continue
                
                # Process stations from both system level and bodies
                all_stations = []
                
                # Add system-level stations with market
                if 'stations' in system:
                    total_root_stations_processed += len(system['stations'])
                    valid_stations = [(station, None) for station in system['stations'] if 'market' in station]
                    all_stations.extend(valid_stations)
                    system_stations += len(valid_stations)  # Add to running total
                    skipped_no_market += len([s for s in system['stations'] if 'market' not in s])
                
                # Add stations from bodies with market
                if 'bodies' in system:
                    for body in system['bodies']:
                        if 'stations' in body:
                            total_body_stations_processed += len(body['stations'])
                            valid_stations = [(station, body['name']) for station in body['stations'] if 'market' in station]
                            all_stations.extend(valid_stations)
                            body_stations += len(valid_stations)  # Add to running total
                            skipped_no_market += len([s for s in body['stations'] if 'market' not in s])
                
                # Filter out carriers if requested
                if exclude_carriers:
                    original_count = len(all_stations)
                    filtered_stations = [station_info for station_info in all_stations 
                                      if not (station_info[0].get('type') == 'Drake-Class Carrier' or 'carrierName' in station_info[0])]
                    carriers_removed = original_count - len(filtered_stations)
                    skipped_carriers_stations += carriers_removed
                    
                    # Adjust station counts for removed carriers
                    system_stations -= len([s for s in all_stations if s[1] is None and 
                                         (s[0].get('type') == 'Drake-Class Carrier' or 'carrierName' in s[0])])
                    body_stations -= len([s for s in all_stations if s[1] is not None and 
                                       (s[0].get('type') == 'Drake-Class Carrier' or 'carrierName' in s[0])])
                    all_stations = filtered_stations
                
                # Remove shipyard and outfitting if requested
                if trim_entries:
                    for station_info in all_stations:
                        station = station_info[0]
                        if 'shipyard' in station:
                            del station['shipyard']
                            trimmed_shipyard += 1
                        if 'outfitting' in station:
                            del station['outfitting']
                            trimmed_outfitting += 1
                
                # Create stations table entries
                for station, body_name in all_stations:
                    market_update_time = station['market'].get('updateTime')
                    
                    c.execute('''
                        INSERT OR REPLACE INTO stations 
                        (system_id64, station_id, body, station_name, station_type, primary_economy, landing_pad_size, distance_to_arrival, update_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        system['id64'],
                        station.get('id'),
                        body_name,
                        station.get('name'),
                        station.get('type'),
                        station.get('primaryEconomy'),
                        'L' if station.get('landingPads', {}).get('large', 0) > 0 
                        else 'M' if station.get('landingPads', {}).get('medium', 0) > 0
                        else 'S' if station.get('landingPads', {}).get('small', 0) > 0
                        else 'Unknown',
                        float(station.get('distanceToArrival', 0)),
                        market_update_time
                    ))
                
                # Insert system data
                c.execute('''
                    INSERT OR REPLACE INTO systems 
                    (id64, name, x, y, z, distance_from_sol, controlling_power, power_state, powers_acquiring)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    system['id64'],
                    system['name'],
                    x, y, z,
                    distance,
                    system.get('controllingPower'),
                    system.get('powerState'),
                    json.dumps([p for p in system.get('powers', []) if p != system.get('controllingPower')])
                ))
                
                # Process mineral signals from bodies
                if 'bodies' in system:
                    for body in system['bodies']:
                        for signal in extract_mineral_signals(body):
                            c.execute('''
                                INSERT INTO mineral_signals 
                                (system_id64, body_name, ring_name, mineral_type, signal_count, reserve_level, ring_type)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                system['id64'],
                                signal['body_name'],
                                signal['ring_name'],
                                signal['mineral_type'],
                                signal['signal_count'],
                                signal['reserve_level'],
                                signal['ring_type']
                            ))
                
                # Process station commodities
                for station, body_name in all_stations:
                    if station.get('type') == 'Drake-Class Carrier' or 'carrierName' in station:
                        continue
                    for commodity in extract_station_commodities(conn, station):  # Pass conn to the function
                        c.execute('''
                            INSERT INTO station_commodities_mapped 
                            (system_id64, station_id, station_name, commodity_id, sell_price, demand)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (
                            system['id64'],
                            station.get('id'),
                            commodity['station_name'],
                            commodity['commodity_id'],  # Use ID instead of name
                            commodity['sell_price'],
                            commodity['demand']
                        ))
                
                # Update total stations
                total_stations = system_stations + body_stations
                
                processed += 1
                progress.update(task, advance=1)
                live.update(get_renderable())
                
                if processed % 5000 == 0:
                    conn.commit()
                    c.execute('BEGIN')
        
        # Final commit
        conn.commit()
        
        # Create summary table
        table = Table(title="Processing Summary", show_header=True, header_style="bold magenta")
        table.add_column("Category", style="cyan")
        table.add_column("Count", justify="right", style="green")
        
        # Add station statistics
        table.add_row("Station entries", f"{system_stations:,}")
        table.add_row("Body station entries", f"{body_stations:,}")
        table.add_row("Total stations", f"{total_stations:,}")
        
        # Add skipped entries
        table.add_row("Distance skipped", f"{skipped_distance:,}")
        if exclude_carriers:
            table.add_row("Carriers skipped", f"{skipped_carriers_stations:,}")
        table.add_row("No market", f"{skipped_no_market:,}")
        
        if trim_entries:
            table.add_row("Trimmed shipyard", f"{trimmed_shipyard:,}")
            table.add_row("Trimmed outfitting", f"{trimmed_outfitting:,}")
        
        # Add performance metrics
        total_time = time.time() - start_time
        table.add_row("Total time", f"{total_time:.1f} seconds")
        table.add_row("Average speed", f"{processed/total_time:.1f} systems/s")
        
        # Print final summary
        console = Console()
        console.print("\n[bold green]Conversion complete![/]")
        console.print(table)
        
    except Exception as e:
        console = Console()
        console.print(f"\n[bold red]Error during conversion:[/] {str(e)}")
        conn.rollback()
    finally:
        conn.close()

def add_system_state(state_file: str, db_file: str):
    """Add system state data to the database."""
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    
    try:
        # Check if columns exist, add if not
        c.execute("PRAGMA table_info(systems)")
        columns = [col[1] for col in c.fetchall()]
        
        if 'system_state' not in columns:
            c.execute('ALTER TABLE systems ADD COLUMN system_state TEXT')
            conn.commit()
            
        if 'population' not in columns:
            c.execute('ALTER TABLE systems ADD COLUMN population INTEGER')
            conn.commit()
        
        # First count total entries
        console = Console()
        console.print("[cyan]Counting systems in state file...[/]")
        total_entries = 0
        with open(state_file, 'rb') as file:
            for _ in ijson.items(file, 'item', use_float=True):
                total_entries += 1
                sys.stdout.write(f"\rFound {total_entries:,} systems")
                sys.stdout.flush()
        console.print(f"\n[green]Found {total_entries:,} total systems[/]")
        
        # Initialize counters
        processed = 0
        matches_found = 0
        state_counts = {}
        start_time = time.time()
        
        # Create progress display
        progress = Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(bar_width=40, complete_style="green", finished_style="green", pulse_style="green"),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            "[cyan]{task.completed:>7,}[/]/[cyan]{task.total:,}[/] systems",
            "•",
            TimeRemainingColumn(),
            console=console,
            expand=False
        )
        
        task = progress.add_task("[cyan]Processing system states[/]", total=total_entries)
        
        def get_renderable():
            elapsed = time.time() - start_time
            speed = processed / elapsed if elapsed > 0 else 0
            
            stats = Text()
            stats.append(f"Speed: {speed:.1f} systems/s\n", style="bold green")
            stats.append(f"Matches found: {matches_found:,}\n\n", style="bold cyan")
            
            stats.append("System States:\n", style="bold magenta")
            # Sort states with None at the end
            sorted_states = sorted(state_counts.items(), key=lambda x: (x[0] is None, x[0] or ""))
            for state, count in sorted_states:
                if state is None:
                    stats.append(f"None: {count:,}\n")
                else:
                    stats.append(f"{state}: {count:,}\n")
            
            return Group(progress, stats)
        
        with Live(get_renderable(), console=console, refresh_per_second=4, transient=True) as live:
            # Begin transaction
            c.execute('BEGIN')
            
            # Process each system
            with open(state_file, 'rb') as file:
                for system in ijson.items(file, 'item', use_float=True):
                    id64 = system.get('id64')
                    state = system.get('state')
                    population = system.get('population')
                    
                    # Update state counts
                    state_counts[state] = state_counts.get(state, 0) + 1
                    
                    # Try to update matching system
                    c.execute('UPDATE systems SET system_state = ?, population = ? WHERE id64 = ?', 
                            (state, population, id64))
                    if c.rowcount > 0:
                        matches_found += 1
                    
                    processed += 1
                    progress.update(task, advance=1)
                    live.update(get_renderable())
                    
                    if processed % 5000 == 0:
                        conn.commit()
                        c.execute('BEGIN')
            
            # Final commit
            conn.commit()
        
        # Create summary table
        table = Table(title="System State Processing Summary", show_header=True, header_style="bold magenta")
        table.add_column("Category", style="cyan")
        table.add_column("Count", justify="right", style="green")
        
        # Add processing statistics
        table.add_row("Total systems processed", f"{processed:,}")
        table.add_row("Matches found", f"{matches_found:,}")
        
        # Add state counts
        table.add_section()
        # Sort states with None at the end
        sorted_states = sorted(state_counts.items(), key=lambda x: (x[0] is None, x[0] or ""))
        for state, count in sorted_states:
            table.add_row("State: " + (state if state is not None else "None"), f"{count:,}")
        
        # Add performance metrics
        table.add_section()
        total_time = time.time() - start_time
        table.add_row("Total time", f"{total_time:.1f} seconds")
        table.add_row("Average speed", f"{processed/total_time:.1f} systems/s")
        
        # Print final summary
        console.print("\n[bold green]System state processing complete![/]")
        console.print(table)
        
    except Exception as e:
        console = Console()
        console.print(f"\n[bold red]Error during system state processing:[/] {str(e)}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert Elite Dangerous JSON data to SQLite database')
    parser.add_argument('--spansh', help='Path to the Spansh JSON data file')
    parser.add_argument('--sqdb', help='Path to the SQLite database file')
    parser.add_argument('--systemstate', help='Path to the system state JSON file')
    parser.add_argument('--max-distance', type=str,
                      help='Maximum distance from Sol in light years')
    parser.add_argument('--exclude-carriers', action='store_true',
                      help='Exclude Drake-Class Carriers from the database')
    parser.add_argument('-c', '--compression', 
                      choices=['none', 'zlib', 'zstandard', 'lz4'],
                      default='none',
                      help='Compression method for JSON data (default: none)')
    parser.add_argument('--trim-entries', action='store_true',
                      help='Remove shipyard and outfitting data from stations')
    parser.add_argument('--totalentries', type=int,
                      help='Skip counting step and use this total number of systems')
    
    args = parser.parse_args()
    
    # Validate arguments based on scenario
    if args.spansh:
        # Initial conversion scenario
        if not Path(args.spansh).exists():
            print(f"Input file {args.spansh} does not exist!")
            sys.exit(1)
        if not args.sqdb:
            print("SQLite database path (--sqdb) is required!")
            sys.exit(1)
        if args.max_distance is None:
            print("Maximum distance (--max-distance) is required for Spansh conversion!")
            sys.exit(1)
        try:
            max_distance = float(args.max_distance)
        except ValueError:
            print("Max distance must be a number in light years")
            sys.exit(1)
            
        # Do initial conversion
        convert_json_to_sqlite(args.spansh, args.sqdb, max_distance, 
                             args.exclude_carriers, args.compression, args.trim_entries,
                             args.totalentries)
        
        # If systemstate is provided, add it after conversion
        if args.systemstate:
            if not Path(args.systemstate).exists():
                print(f"System state file {args.systemstate} does not exist!")
                sys.exit(1)
            add_system_state(args.systemstate, args.sqdb)
            
    elif args.systemstate:
        # System state only scenario
        if not args.sqdb:
            print("SQLite database path (--sqdb) is required!")
            sys.exit(1)
        if not Path(args.systemstate).exists():
            print(f"System state file {args.systemstate} does not exist!")
            sys.exit(1)
        add_system_state(args.systemstate, args.sqdb)
        
    else:
        print("Either --spansh or --systemstate with --sqdb must be provided!")
        sys.exit(1) 