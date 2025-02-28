import sqlite3
import json
import ijson  # For streaming JSON parsing
from pathlib import Path
import sys
import argparse
from typing import Generator, Dict, Any, Optional, List, Set, Tuple
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
from rich.console import Group
from rich.table import Table
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# Reuse mineral and metal sets from converter.py for consistency
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

# Set of carrier types to skip in both Spansh and EDSM data
CARRIER_TYPES = {'Drake-Class Carrier', 'Fleet Carrier'}

def calculate_distance(x: float, y: float, z: float, origin_x: float = 0, origin_y: float = 0, origin_z: float = 0) -> float:
    """Calculate distance between two points in 3D space."""
    return math.sqrt((x - origin_x)**2 + (y - origin_y)**2 + (z - origin_z)**2)

def clean_text(value):
    """Clean and decode text values, handling encoding issues"""
    if isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            try:
                return value.decode('latin1')
            except UnicodeDecodeError:
                return value.decode('utf-8', errors='replace')
    return value

def clean_row(row):
    """Clean all text values in a row.
    Let PostgreSQL handle JSON conversion for JSONB fields."""
    return tuple(clean_text(value) if isinstance(value, bytes) else value for value in row)

class StationProcessor:
    def __init__(self, db_url: str, max_distance: float, args):
        """Initialize the station processor with database connection and parameters."""
        self.db_url = db_url
        self.max_distance = max_distance
        self.args = args
        self.console = Console()
        
        # Spansh processing counters
        self.processed_systems = 0
        self.skipped_distance = 0
        self.spansh_processed_stations = 0  # Renamed from processed_stations
        self.missing_stations = 0
        self.missing_commodities = 0
        self.skipped_carriers = 0
        
        # EDSM processing counters
        self.edsm_processed_stations = 0  # New counter for EDSM
        self.matched_stations = 0
        self.updated_economies = 0
        self.updated_types = 0
        self.total_missing_economies = 0
        self.total_missing_types = 0
        
        self.start_time = time.time()
        
        # Sets to track existing data (fast in-memory lookups)
        self.existing_station_ids: Set[Tuple[int, int]] = set()  # (system_id64, station_id) pairs
        self.stations_without_economy: Set[int] = set()  # station_ids that need economy updates
        self.stations_without_type: Set[int] = set()  # station_ids that need type updates
        self.existing_commodities: Dict[str, int] = {}  # commodity_name -> commodity_id
        self.commodity_id_counter = 1
        
        # Add new counters for has_market tracking
        self.spansh_missing_market = 0
        self.spansh_filled_market = 0
        self.spansh_updated_economies = 0
        self.edsm_missing_market = 0
        self.edsm_filled_market = 0
        
        # Add dictionary to track market status in memory
        self.station_market_status: Dict[Tuple[int, int], bool] = {}  # (system_id64, station_id) -> has_market
        
        # Initialize database connections
        self.init_databases()

    def init_databases(self):
        """Initialize database connections and load existing data."""
        # Parse PostgreSQL connection string
        self.pg_conn = psycopg2.connect(self.db_url)
        self.pg_cur = self.pg_conn.cursor()
        
        # Connect to SQLite database (create if doesn't exist)
        if self.args.sqdb:
            output_path = Path(self.args.sqdb)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Just connect to database (creates it if it doesn't exist)
            self.sqlite_conn = sqlite3.connect(str(output_path))
            self.sqlite_cur = self.sqlite_conn.cursor()
            
            # Always create schema if tables don't exist
            self.create_sqlite_schema()
            
            if output_path.exists():
                self.console.print(f"[yellow]Using existing SQLite database: {self.args.sqdb}[/]")
            else:
                self.console.print(f"[yellow]Created new SQLite database: {self.args.sqdb}[/]")
        else:
            # If we're only generating SQL dump, use memory database
            self.sqlite_conn = sqlite3.connect(':memory:')
            self.sqlite_cur = self.sqlite_conn.cursor()
            self.create_sqlite_schema()  # Always create schema for memory database
        
        # Now load existing data into memory
        self.load_existing_data()

    def create_sqlite_schema(self):
        """Create SQLite schema matching the PostgreSQL structure."""
        # Create systems table
        self.sqlite_cur.execute('''CREATE TABLE IF NOT EXISTS systems (
            id64 INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            x REAL,
            y REAL,
            z REAL,
            distance_from_sol REAL,
            primary_economy TEXT,
            secondary_economy TEXT,
            security TEXT,
            controlling_power TEXT,
            power_state TEXT,
            powers_acquiring TEXT,  -- Stored as JSON text in SQLite, will be JSONB in PostgreSQL
            UNIQUE(name)
        )''')
        
        # Create stations table
        self.sqlite_cur.execute('''CREATE TABLE IF NOT EXISTS stations (
            system_id64 INTEGER,
            station_id INTEGER,
            body TEXT,
            station_name TEXT,
            station_type TEXT,
            primary_economy TEXT,
            economies TEXT,  -- Stored as JSON text in SQLite, will be JSONB in PostgreSQL
            landing_pad_size TEXT,
            distance_to_arrival REAL,
            update_time TEXT,
            has_market BOOLEAN,
            PRIMARY KEY (system_id64, station_id),
            FOREIGN KEY(system_id64) REFERENCES systems(id64)
        )''')
        
        # Create commodity types table
        self.sqlite_cur.execute('''CREATE TABLE IF NOT EXISTS commodity_types (
            commodity_id INTEGER PRIMARY KEY,
            commodity_name TEXT UNIQUE
        )''')
        
        # Create station commodities mapped table
        self.sqlite_cur.execute('''CREATE TABLE IF NOT EXISTS station_commodities_mapped (
            system_id64 INTEGER,
            station_id INTEGER,
            station_name TEXT,
            commodity_id INTEGER,
            sell_price INTEGER,
            demand INTEGER,
            PRIMARY KEY (system_id64, station_id, commodity_id),
            FOREIGN KEY(system_id64) REFERENCES systems(id64),
            FOREIGN KEY(system_id64, station_id) REFERENCES stations(system_id64, station_id),
            FOREIGN KEY(commodity_id) REFERENCES commodity_types(commodity_id)
        )''')
        
        # Create necessary indices
        self.sqlite_cur.execute('CREATE INDEX IF NOT EXISTS idx_stations_system ON stations(system_id64)')
        self.sqlite_cur.execute('CREATE INDEX IF NOT EXISTS idx_commodities_station ON station_commodities_mapped(system_id64, station_id)')
        
        self.sqlite_conn.commit()

    def load_existing_data(self):
        """Load existing station and commodity data from PostgreSQL and SQLite into memory."""
        if self.args.migrate:
            # Only load PostgreSQL data if we're in migration mode
            self.console.print("[yellow]Loading existing station data from PostgreSQL...[/]")
            self.pg_cur.execute('SELECT system_id64, station_id, primary_economy, station_type FROM stations')
            pg_count = 0
            for row in self.pg_cur.fetchall():
                system_id64, station_id, primary_economy, station_type = row
                self.existing_station_ids.add((system_id64, station_id))
                pg_count += 1
                if primary_economy is None:  # If primary_economy is NULL
                    self.stations_without_economy.add(station_id)
                    self.total_missing_economies += 1
                if station_type is None:  # If station_type is NULL
                    self.stations_without_type.add(station_id)
                    self.total_missing_types += 1
            
            self.console.print(f"[green]Loaded {pg_count:,} station IDs from PostgreSQL")
        
        # If we're skipping Spansh and have an existing SQLite file, load those stations too
        sqlite_count = 0
        if self.args.skip_spansh and self.args.sqdb and Path(self.args.sqdb).exists():
            self.console.print("[yellow]Loading existing station data from SQLite...[/]")
            try:
                self.sqlite_cur.execute('SELECT system_id64, station_id FROM stations')
                for row in self.sqlite_cur.fetchall():
                    station_key = (row[0], row[1])
                    if station_key not in self.existing_station_ids:
                        self.existing_station_ids.add(station_key)
                        sqlite_count += 1
                
                self.console.print(f"[green]Loaded {sqlite_count:,} additional station IDs from SQLite")
            except sqlite3.OperationalError as e:
                if "no such table" in str(e):
                    self.console.print("[yellow]No existing stations table found in SQLite database[/]")
                else:
                    raise
        
        total_stations = len(self.existing_station_ids)
        self.console.print(f"[green]Total unique station IDs loaded: {total_stations:,}")
        if self.args.migrate:
            self.console.print(f"[yellow]Found {self.total_missing_economies:,} stations without economy")
            self.console.print(f"[yellow]Found {self.total_missing_types:,} stations without type")
        
        # Load existing commodities and their IDs
        self.console.print("[yellow]Loading commodity types from PostgreSQL...[/]")
        self.pg_cur.execute('SELECT commodity_id, commodity_name FROM commodity_types')
        commodity_rows = self.pg_cur.fetchall()
        
        # Store in memory
        for row in commodity_rows:
            self.existing_commodities[row[1]] = row[0]
            self.commodity_id_counter = max(self.commodity_id_counter, row[0] + 1)
        
        # Copy to SQLite
        self.console.print("[yellow]Copying commodity types to SQLite...[/]")
        for commodity_id, commodity_name in commodity_rows:
            self.sqlite_cur.execute(
                'INSERT OR IGNORE INTO commodity_types (commodity_id, commodity_name) VALUES (?, ?)',
                (commodity_id, commodity_name)
            )
        self.sqlite_conn.commit()
        
        self.console.print(f"[green]Loaded and copied {len(commodity_rows):,} commodity types[/]")

    def get_or_create_commodity_id(self, commodity_name: str) -> int:
        """Get existing commodity ID or create a new one."""
        if commodity_name in self.existing_commodities:
            return self.existing_commodities[commodity_name]
        
        # Create new commodity ID
        new_id = self.commodity_id_counter
        self.commodity_id_counter += 1
        self.existing_commodities[commodity_name] = new_id
        
        # Add to SQLite database
        self.sqlite_cur.execute(
            'INSERT INTO commodity_types (commodity_id, commodity_name) VALUES (?, ?)',
            (new_id, commodity_name)
        )
        return new_id

    def process_spansh_stream(self, json_file: str) -> Generator[Dict[Any, Any], None, None]:
        """Stream the Spansh JSON file one system at a time."""
        with open(json_file, 'rb') as file:
            parser = ijson.items(file, 'item', use_float=True)
            for system in parser:
                yield system

    def process_edsm_stream(self, json_file: str) -> Generator[Dict[Any, Any], None, None]:
        """Stream the EDSM JSON file one station at a time."""
        with open(json_file, 'rb') as file:
            parser = ijson.items(file, 'item', use_float=True)
            for station in parser:
                yield station

    def extract_station_commodities(self, station: Dict) -> List[Dict]:
        """Extract relevant commodity data from a station."""
        commodities = []
        if 'market' in station and 'commodities' in station['market']:
            for commodity in station['market']['commodities']:
                commodity_name = commodity['name']
                if commodity_name in MINERAL_SIGNALS:
                    commodity_id = self.get_or_create_commodity_id(commodity_name)
                    commodities.append({
                        'station_name': station['name'],
                        'commodity_id': commodity_id,
                        'sell_price': commodity['sellPrice'],
                        'demand': commodity['demand']
                    })
        return commodities

    def process_spansh_file(self, json_file: str):
        """Process the Spansh JSON file and extract missing station data."""
        if self.args.nocount:
            total_entries_spansh = 22283
            total_entries = 20586
            self.console.print(f"[yellow]Using predefined count of {total_entries:,} systems[/]")
        else:
            self.console.print("[yellow]Counting systems in Spansh file...[/]")
            total_entries = 0
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[cyan]{task.completed:>9,}[/]"),
                console=self.console,
                expand=False
            ) as progress:
                task = progress.add_task("Counting systems", total=None)
                for _ in self.process_spansh_stream(json_file):
                    total_entries += 1
                    progress.update(task, completed=total_entries)
            self.console.print(f"[green]Total systems found: {total_entries:,}[/]")
        
        progress = Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(bar_width=40),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            "[cyan]{task.completed:>7,}[/]/[cyan]{task.total:,}[/]",
            "•",
            TimeRemainingColumn(),
            console=self.console,
            expand=False
        )
        
        task = progress.add_task(
            f"[cyan]Processing Spansh systems within {self.max_distance:,.0f} ly of Sol[/]",
            total=total_entries
        )
        
        def get_renderable():
            elapsed = time.time() - self.start_time
            speed = self.processed_systems / elapsed if elapsed > 0 else 0
            
            stats = Text()
            stats.append(f"Speed: {speed:.1f} systems/s\n\n", style="bold green")
            
            stats.append("Processing Statistics:\n", style="bold magenta")
            stats.append(f"Processed systems: {self.processed_systems:,}\n")
            stats.append(f"Skipped (distance): {self.skipped_distance:,}\n")
            stats.append(f"Processed stations: {self.spansh_processed_stations:,}\n")
            stats.append(f"Missing stations found: {self.missing_stations:,}\n")
            stats.append(f"Missing commodities: {self.missing_commodities:,}\n")
            stats.append(f"Skipped carriers: {self.skipped_carriers:,}\n")
            
            return Group(progress, stats)
        
        with Live(get_renderable(), console=self.console, refresh_per_second=4) as live:
            for system in self.process_spansh_stream(json_file):
                # Process system data here
                self.process_system(system)
                
                self.processed_systems += 1
                progress.update(task, advance=1)
                live.update(get_renderable())
                
                if self.processed_systems % 1000 == 0:
                    self.sqlite_conn.commit()

        # Add station count output at the end
        self.console.print(f"\n[bold magenta]After Spansh processing:[/]")
        self.console.print(f"[magenta]Total stations in memory: {len(self.existing_station_ids):,}[/]")
        self.sqlite_cur.execute('SELECT COUNT(*) FROM stations')
        sqlite_count = self.sqlite_cur.fetchone()[0]
        self.console.print(f"[magenta]Total stations in SQLite: {sqlite_count:,}[/]")

        # Add market statistics output
        self.console.print(f"\n[bold yellow]Spansh Market Statistics:[/]")
        self.console.print(f"[yellow]Missing market tag: {self.spansh_missing_market:,}[/]")
        self.console.print(f"[green]Filled market tag: {self.spansh_filled_market:,}[/]")
        self.console.print(f"[cyan]Updated economies: {self.spansh_updated_economies:,}[/]")
        
        # Verify no missing has_market values
        self.sqlite_cur.execute('SELECT COUNT(*) FROM stations WHERE has_market IS NULL')
        missing_market = self.sqlite_cur.fetchone()[0]
        if missing_market > 0:
            self.console.print(f"[red]Warning: {missing_market:,} stations still have NULL has_market values![/]")

    def process_edsm_file(self, json_file: str):
        """Process the EDSM JSON file and update missing economy data."""
        if self.args.nocount:
            total_entries = 353714
            self.console.print(f"[yellow]Using predefined count of {total_entries:,} stations[/]")
        else:
            self.console.print("[yellow]Counting stations in EDSM file...[/]")
            total_entries = 0
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[cyan]{task.completed:>9,}[/]"),
                console=self.console,
                expand=False
            ) as progress:
                task = progress.add_task("Counting stations", total=None)
                for _ in self.process_edsm_stream(json_file):
                    total_entries += 1
                    progress.update(task, completed=total_entries)
            self.console.print(f"[green]Total stations found: {total_entries:,}[/]")
        
        progress = Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(bar_width=40),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            "[cyan]{task.completed:>7,}[/]/[cyan]{task.total:,}[/]",
            "•",
            TimeRemainingColumn(),
            console=self.console,
            expand=False
        )
        
        task = progress.add_task("[cyan]Processing EDSM stations[/]", total=total_entries)
        
        # Add counter for has_market updates
        self.has_market_updates = 0
        self.last_commit_count = 0
        
        def get_renderable():
            elapsed = time.time() - self.start_time
            speed = self.edsm_processed_stations / elapsed if elapsed > 0 else 0
            
            stats = Text()
            stats.append(f"Speed: {speed:.1f} stations/s\n\n", style="bold green")
            
            stats.append("EDSM Processing Statistics:\n", style="bold magenta")
            stats.append(f"Processed EDSM stations: {self.edsm_processed_stations:,}\n")
            stats.append(f"Matched stations: {self.matched_stations:,}\n")
            stats.append(f"Has market updates: {self.has_market_updates:,}\n")
            if self.edsm_processed_stations > 0 and self.edsm_processed_stations % 1000 == 0:
                stats.append(f"Last commit at: {self.edsm_processed_stations:,} stations\n")
            stats.append("\n")
            
            stats.append("Economy Updates:\n", style="bold yellow")
            stats.append(f"Initially missing: {self.total_missing_economies:,}\n")
            stats.append(f"Updated: {self.updated_economies:,}\n")
            stats.append(f"Still missing: {self.total_missing_economies - self.updated_economies:,}\n\n")
            
            stats.append("Station Type Updates:\n", style="bold cyan")
            stats.append(f"Initially missing: {self.total_missing_types:,}\n")
            stats.append(f"Updated: {self.updated_types:,}\n")
            stats.append(f"Still missing: {self.total_missing_types - self.updated_types:,}\n")
            
            return Group(progress, stats)
        
        with Live(get_renderable(), console=self.console, refresh_per_second=4) as live:
            try:
                for station in self.process_edsm_stream(json_file):
                    # Process EDSM station data here
                    self.process_edsm_station(station)
                    
                    progress.update(task, advance=1)
                    live.update(get_renderable())
                    
                    if self.edsm_processed_stations % 1000 == 0:
                        # Commit current transaction
                        self.sqlite_conn.commit()
                        self.last_commit_count = self.edsm_processed_stations
                
                # Final commit
                self.sqlite_conn.commit()
            except Exception as e:
                self.sqlite_conn.rollback()
                raise e

        # Add station count output at the end
        self.console.print(f"\n[bold magenta]After EDSM processing:[/]")
        self.console.print(f"[magenta]Total stations in memory: {len(self.existing_station_ids):,}[/]")
        self.sqlite_cur.execute('SELECT COUNT(*) FROM stations')
        sqlite_count = self.sqlite_cur.fetchone()[0]
        self.console.print(f"[magenta]Total stations in SQLite: {sqlite_count:,}[/]")

        # Add market statistics output
        self.console.print(f"\n[bold yellow]EDSM Market Statistics:[/]")
        self.console.print(f"[yellow]Missing market tag: {self.edsm_missing_market:,}[/]")
        self.console.print(f"[green]Filled market tag: {self.edsm_filled_market:,}[/]")
        
        # Verify no missing has_market values
        self.sqlite_cur.execute('SELECT COUNT(*) FROM stations WHERE has_market IS NULL')
        missing_market = self.sqlite_cur.fetchone()[0]
        if missing_market > 0:
            self.console.print(f"[red]Warning: {missing_market:,} stations still have NULL has_market values![/]")

    def process_system(self, system: Dict):
        """Process a single system from Spansh data."""
        # Calculate distance from Sol
        coords = system.get('coords', {})
        x = float(coords.get('x', 0))
        y = float(coords.get('y', 0))
        z = float(coords.get('z', 0))
        distance = calculate_distance(x, y, z)
        
        # Skip if outside radius
        if distance > self.max_distance:
            self.skipped_distance += 1
            return
        
        # Convert powers list to JSON, excluding controlling power
        controlling_power = system.get('controllingPower')
        powers = system.get('powers', [])
        powers_acquiring = json.dumps([p for p in powers if p != controlling_power])
        
        # Add system to SQLite if needed
        self.sqlite_cur.execute('''
            INSERT OR REPLACE INTO systems 
            (id64, name, x, y, z, distance_from_sol, primary_economy, 
             secondary_economy, security, controlling_power, power_state, powers_acquiring)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            system['id64'],
            system['name'],
            x, y, z,
            distance,
            system.get('primaryEconomy'),
            system.get('secondaryEconomy'),
            system.get('security'),
            controlling_power,
            system.get('powerState'),
            powers_acquiring
        ))
        
        # Process stations
        self.process_system_stations(system)

    def process_system_stations(self, system: Dict):
        """Process stations in a system, both at system level and body level."""
        # Process system-level stations
        if 'stations' in system:
            for station in system['stations']:
                self.process_single_station(system['id64'], station, None)
        
        # Process body-level stations
        if 'bodies' in system:
            for body in system['bodies']:
                if 'stations' in body:
                    for station in body['stations']:
                        self.process_single_station(system['id64'], station, body['name'])

    def process_single_station(self, system_id64: int, station: Dict, body_name: Optional[str]):
        """Process a single station entry."""
        # Skip Fleet Carriers
        if station.get('type') in CARRIER_TYPES:
            self.skipped_carriers += 1
            return
        
        station_id = station.get('id')
        if not station_id:
            return
        
        # Track market status in memory
        has_market = 'market' in station
        self.station_market_status[(system_id64, station_id)] = has_market
        
        # Count all processed stations in Spansh
        self.spansh_processed_stations += 1
        
        # Check if this is a new station or needs updating
        is_new_station = (system_id64, station_id) not in self.existing_station_ids
        
        # Convert economies dict to JSON string if it exists
        economies_json = None
        if 'economies' in station:
            economies_json = json.dumps(station['economies'])
        
        if is_new_station:
            # Track has_market for new stations
            if 'market' not in station:
                self.spansh_missing_market += 1
            else:
                self.spansh_filled_market += 1
            
            # For new stations, do a full insert
            self.sqlite_cur.execute('''
                INSERT INTO stations 
                (system_id64, station_id, body, station_name, station_type, primary_economy,
                 economies, landing_pad_size, distance_to_arrival, update_time, has_market)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                system_id64,
                station_id,
                body_name,
                station.get('name'),
                station.get('type'),
                station.get('primaryEconomy'),
                economies_json,
                'L' if station.get('landingPads', {}).get('large', 0) > 0 
                else 'M' if station.get('landingPads', {}).get('medium', 0) > 0
                else 'S' if station.get('landingPads', {}).get('small', 0) > 0
                else 'Unknown',
                float(station.get('distanceToArrival', 0)),
                station.get('updateTime'),
                'market' in station
            ))
            self.missing_stations += 1
            
            # Only process commodities for new stations
            if 'market' in station:
                for commodity in self.extract_station_commodities(station):
                    self.sqlite_cur.execute('''
                        INSERT INTO station_commodities_mapped 
                        (system_id64, station_id, station_name, commodity_id, sell_price, demand)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        system_id64,
                        station_id,
                        commodity['station_name'],
                        commodity['commodity_id'],
                        commodity['sell_price'],
                        commodity['demand']
                    ))
                    self.missing_commodities += 1
        else:
            # Track has_market updates for existing stations
            if 'market' not in station:
                self.spansh_missing_market += 1
            else:
                self.spansh_filled_market += 1
            
            # For existing stations, update ALL fields
            self.sqlite_cur.execute('''
                UPDATE stations 
                SET body = ?,
                    station_name = ?,
                    station_type = ?,
                    primary_economy = ?,
                    economies = ?,
                    landing_pad_size = ?,
                    distance_to_arrival = ?,
                    update_time = ?,
                    has_market = ?
                WHERE system_id64 = ? AND station_id = ?
            ''', (
                body_name,
                station.get('name'),
                station.get('type'),
                station.get('primaryEconomy'),
                economies_json,
                'L' if station.get('landingPads', {}).get('large', 0) > 0 
                else 'M' if station.get('landingPads', {}).get('medium', 0) > 0
                else 'S' if station.get('landingPads', {}).get('small', 0) > 0
                else 'Unknown',
                float(station.get('distanceToArrival', 0)),
                station.get('updateTime'),
                'market' in station,
                system_id64,
                station_id
            ))
            
            if economies_json is not None:
                self.spansh_updated_economies += 1

    def process_edsm_station(self, station: Dict):
        """Process a single EDSM station entry."""
        # Increment the counter for all processed stations
        self.edsm_processed_stations += 1
        
        # Skip carriers
        if station.get('type') in CARRIER_TYPES:
            self.skipped_carriers += 1
            return
        
        station_id = station.get('marketId')  # This is our station_id
        system_id64 = station.get('systemId64')
        if not station_id or not system_id64:
            return
            
        # Track market status in memory
        has_market = station.get('haveMarket', False)
        self.station_market_status[(system_id64, station_id)] = has_market
        
        # Check distance from Sol
        coords = station.get('coords', {})
        distance = calculate_distance(
            float(coords.get('x', 0)),
            float(coords.get('y', 0)),
            float(coords.get('z', 0))
        )
        
        # Skip if outside radius
        if distance > self.max_distance:
            self.skipped_distance += 1
            return
        
        # Get the information update timestamp
        update_time = station.get('updateTime', {}).get('information')
        
        # Double check if station exists in database
        station_key = (system_id64, station_id)
        try:
            # First check our in-memory set
            exists_in_memory = station_key in self.existing_station_ids
            
            # Then verify against database
            self.sqlite_cur.execute(
                'SELECT 1 FROM stations WHERE system_id64 = ? AND station_id = ?',
                (system_id64, station_id)
            )
            exists_in_db = bool(self.sqlite_cur.fetchone())
            
            # Synchronize our in-memory state if needed
            if exists_in_db and not exists_in_memory:
                self.existing_station_ids.add(station_key)
                exists_in_memory = True
            
            if exists_in_memory or exists_in_db:
                # Track has_market updates
                if not station.get('haveMarket', False):
                    self.edsm_missing_market += 1
                else:
                    self.edsm_filled_market += 1
                
                self.matched_stations += 1
                
                # For existing stations, update has_market, economy, and type
                update_fields = ['has_market = ?']
                update_values = [station.get('haveMarket', False)]
                
                # Update economy if station needs it and has the data
                if station_id in self.stations_without_economy and station.get('economy'):
                    update_fields.append('primary_economy = ?')
                    update_values.append(station.get('economy'))
                    self.updated_economies += 1
                
                # Update type if station needs it and has the data
                if station_id in self.stations_without_type and station.get('type'):
                    update_fields.append('station_type = ?')
                    update_values.append(station.get('type'))
                    self.updated_types += 1
                
                # Add WHERE clause values
                update_values.extend([system_id64, station_id])
                
                # Execute the update
                self.sqlite_cur.execute(f'''
                    UPDATE stations 
                    SET {', '.join(update_fields)}
                    WHERE system_id64 = ? AND station_id = ?
                ''', update_values)
                
                self.has_market_updates += 1
                return
            
            # Track has_market for new stations
            if not station.get('haveMarket', False):
                self.edsm_missing_market += 1
            else:
                self.edsm_filled_market += 1
            
            # This is a new station from EDSM within our radius
            self.sqlite_cur.execute('''
                INSERT INTO stations 
                (system_id64, station_id, body, station_name, station_type, primary_economy,
                 landing_pad_size, distance_to_arrival, update_time, has_market)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                system_id64,
                station_id,
                None,  # body name not available in EDSM
                station.get('name'),
                station.get('type'),
                station.get('economy'),
                'Unknown',  # landing pad size not available in EDSM
                station.get('distanceToArrival', 0),
                update_time,
                station.get('haveMarket', False)
            ))
            
            # Add the new station to our tracking set using composite key
            self.existing_station_ids.add(station_key)
            
        except sqlite3.IntegrityError as e:
            # If we somehow still get a unique constraint error, log it and continue
            self.console.print(f"[yellow]Warning: Station {station_id} in system {system_id64} already exists (race condition), skipping...[/]")
            # Add to our tracking set to prevent future attempts
            self.existing_station_ids.add(station_key)
            return

    def save_sqlite_db(self, output_file: str):
        """Save the SQLite database - now just a no-op since we're writing directly to file."""
        # Database is already saved since we're writing to file directly
        file_size = Path(output_file).stat().st_size
        self.console.print(f"[green]SQLite database is ready! File size: {file_size/1024/1024:.1f} MB[/]")


    def close(self):
        """Close all database connections."""
        self.sqlite_conn.close()
        self.pg_conn.close()

    def cleanup_null_types(self):
        """Set remaining NULL station types to Surface Settlement."""
        self.console.print("\n[yellow]Cleaning up remaining NULL station types...[/]")
        
        # First count how many we need to update
        self.sqlite_cur.execute('SELECT COUNT(*) FROM stations WHERE station_type IS NULL')
        remaining_null = self.sqlite_cur.fetchone()[0]
        
        if remaining_null == 0:
            self.console.print("[green]No NULL station types found, skipping cleanup.[/]")
            return
        
        self.console.print(f"[yellow]Found {remaining_null:,} stations with NULL type, setting to 'Surface Settlement'[/]")
        
        # Update all remaining NULL types
        self.sqlite_cur.execute('''
            UPDATE stations 
            SET station_type = 'Surface Settlement'
            WHERE station_type IS NULL
        ''')
        self.sqlite_conn.commit()
        
        self.console.print(f"[green]Updated {remaining_null:,} stations to type 'Surface Settlement'[/]")

    def cleanup_landing_pads(self):
        """Convert 'Unknown' landing pad sizes to NULL."""
        self.console.print("\n[yellow]Converting 'Unknown' landing pad sizes to NULL...[/]")
        
        # First count how many we need to update
        self.sqlite_cur.execute("SELECT COUNT(*) FROM stations WHERE landing_pad_size = 'Unknown'")
        unknown_count = self.sqlite_cur.fetchone()[0]
        
        if unknown_count == 0:
            self.console.print("[green]No 'Unknown' landing pad sizes found.[/]")
            return
        
        self.console.print(f"[yellow]Found {unknown_count:,} stations with 'Unknown' landing pad size[/]")
        
        # Update all Unknown landing pad sizes to NULL
        self.sqlite_cur.execute('''
            UPDATE stations 
            SET landing_pad_size = NULL
            WHERE landing_pad_size = 'Unknown'
        ''')
        self.sqlite_conn.commit()
        
        self.console.print(f"[green]Updated {unknown_count:,} landing pad sizes to NULL[/]")

    def scan_spansh_markets(self, json_file: str):
        """Quickly scan Spansh data just to get has_market status when skipping full Spansh processing."""
        self.console.print("[yellow]Quick scan of Spansh data for market status...[/]")
        
        for system in self.process_spansh_stream(json_file):
            # Process system-level stations
            if 'stations' in system:
                for station in system['stations']:
                    if station.get('type') in CARRIER_TYPES:
                        continue
                    station_id = station.get('id')
                    if station_id:
                        has_market = 'market' in station
                        # Store market status in memory
                        self.station_market_status[(system['id64'], station_id)] = has_market
                        if has_market:
                            self.spansh_filled_market += 1
                        else:
                            self.spansh_missing_market += 1
            
            # Process body-level stations
            if 'bodies' in system:
                for body in system['bodies']:
                    if 'stations' in body:
                        for station in body['stations']:
                            if station.get('type') in CARRIER_TYPES:
                                continue
                            station_id = station.get('id')
                            if station_id:
                                has_market = 'market' in station
                                # Store market status in memory
                                self.station_market_status[(system['id64'], station_id)] = has_market
                                if has_market:
                                    self.spansh_filled_market += 1
                                else:
                                    self.spansh_missing_market += 1

    def verify_migration(self):
        """Verify that all data from SQLite was properly migrated to PostgreSQL."""
        self.console.print("\n[bold cyan]Verifying migration completeness...[/]")
        
        # Get counts from SQLite
        self.sqlite_cur.execute("""
            SELECT 
                (SELECT COUNT(*) FROM systems) as systems_count,
                (SELECT COUNT(*) FROM stations) as stations_count,
                (SELECT COUNT(*) FROM station_commodities_mapped) as commodities_count,
                (SELECT COUNT(*) FROM commodity_types) as commodity_types_count
        """)
        sqlite_counts = self.sqlite_cur.fetchone()
        
        # Get counts from PostgreSQL
        self.pg_cur.execute("""
            SELECT 
                (SELECT COUNT(*) FROM systems) as systems_count,
                (SELECT COUNT(*) FROM stations) as stations_count,
                (SELECT COUNT(*) FROM station_commodities_mapped) as commodities_count,
                (SELECT COUNT(*) FROM commodity_types) as commodity_types_count
        """)
        pg_counts = self.pg_cur.fetchone()
        
        # Create verification table
        table = Table(title="Migration Verification", show_header=True, header_style="bold magenta")
        table.add_column("Table", style="cyan")
        table.add_column("SQLite Count", justify="right", style="yellow")
        table.add_column("PostgreSQL Count", justify="right", style="green")
        table.add_column("Status", style="bold")
        
        # Add rows with verification status
        tables = ["Systems", "Stations", "Station Commodities", "Commodity Types"]
        for i, table_name in enumerate(tables):
            sqlite_count = sqlite_counts[i]
            pg_count = pg_counts[i]
            status = "[green]✓ OK[/]" if pg_count >= sqlite_count else "[red]✗ MISSING DATA[/]"
            table.add_row(
                table_name,
                f"{sqlite_count:,}",
                f"{pg_count:,}",
                status
            )
        
        self.console.print(table)
        
        # Additional verification for data integrity
        self.console.print("\n[yellow]Checking for orphaned records...[/]")
        
        # Check for stations without systems
        self.pg_cur.execute("""
            SELECT COUNT(*) FROM stations s
            WHERE NOT EXISTS (SELECT 1 FROM systems sys WHERE sys.id64 = s.system_id64)
        """)
        orphaned_stations = self.pg_cur.fetchone()[0]
        
        # Check for commodities without stations
        self.pg_cur.execute("""
            SELECT COUNT(*) FROM station_commodities_mapped scm
            WHERE NOT EXISTS (
                SELECT 1 FROM stations s 
                WHERE s.system_id64 = scm.system_id64 
                AND s.station_id = scm.station_id
            )
        """)
        orphaned_commodities = self.pg_cur.fetchone()[0]
        
        if orphaned_stations > 0 or orphaned_commodities > 0:
            self.console.print("[red]Found orphaned records:[/]")
            if orphaned_stations > 0:
                self.console.print(f"[red]- {orphaned_stations:,} stations without corresponding systems[/]")
            if orphaned_commodities > 0:
                self.console.print(f"[red]- {orphaned_commodities:,} commodities without corresponding stations[/]")
        else:
            self.console.print("[green]No orphaned records found. All foreign key constraints satisfied.[/]")

    def migrate_to_postgres(self):
        """Migrate data from SQLite to PostgreSQL without schema recreation."""
        try:
            # First ensure all required columns exist
            self.console.print("[yellow]Ensuring required columns exist...[/]")
            self.pg_cur.execute('''
                DO $$
                BEGIN
                    -- Check and add has_market column
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name = 'stations' 
                        AND column_name = 'has_market'
                    ) THEN
                        ALTER TABLE stations ADD COLUMN has_market BOOLEAN DEFAULT false;
                    END IF;
                    
                    -- Check and add economies column
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name = 'stations' 
                        AND column_name = 'economies'
                    ) THEN
                        ALTER TABLE stations ADD COLUMN economies JSONB;
                    END IF;
                    
                    -- Check and add primary_economy column
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name = 'systems' 
                        AND column_name = 'primary_economy'
                    ) THEN
                        ALTER TABLE systems ADD COLUMN primary_economy TEXT;
                    END IF;
                    
                    -- Check and add secondary_economy column
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name = 'systems' 
                        AND column_name = 'secondary_economy'
                    ) THEN
                        ALTER TABLE systems ADD COLUMN secondary_economy TEXT;
                    END IF;
                    
                    -- Check and add security column
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name = 'systems' 
                        AND column_name = 'security'
                    ) THEN
                        ALTER TABLE systems ADD COLUMN security TEXT;
                    END IF;
                END $$;
            ''')
            self.pg_conn.commit()
            
            # Get total counts first
            self.console.print("[yellow]Checking SQLite table counts...[/]")
            self.sqlite_cur.execute("SELECT COUNT(*) FROM stations")
            total_stations = self.sqlite_cur.fetchone()[0]
            self.console.print(f"SQLite stations count: {total_stations:,}")
            
            self.sqlite_cur.execute("SELECT COUNT(*) FROM station_commodities_mapped")
            total_commodities = self.sqlite_cur.fetchone()[0]
            self.console.print(f"SQLite commodities count: {total_commodities:,}")
            
            self.sqlite_cur.execute("SELECT COUNT(*) FROM systems")
            total_systems = self.sqlite_cur.fetchone()[0]
            self.console.print(f"SQLite systems count: {total_systems:,}")
            
            if total_stations == 0 and total_systems == 0:
                self.console.print("[red]ERROR: No data found in SQLite database![/]")
                return
            
            # First migrate systems
            self.console.print(f"\n[yellow]Migrating systems table ({total_systems:,} rows)...[/]")
            self.sqlite_cur.execute("""
                SELECT id64, name, x, y, z, distance_from_sol, primary_economy, 
                       secondary_economy, security, controlling_power, power_state, powers_acquiring
                FROM systems
            """)
            systems_data = self.sqlite_cur.fetchall()
            systems = [clean_row(row) for row in systems_data]
            
            self.console.print("[yellow]Inserting systems into PostgreSQL...[/]")
            for chunk in tqdm([systems[i:i + 1000] for i in range(0, len(systems), 1000)], desc="Processing"):
                execute_values(
                    self.pg_cur,
                    """
                    INSERT INTO systems 
                    (id64, name, x, y, z, distance_from_sol, primary_economy, 
                     secondary_economy, security, controlling_power, power_state, powers_acquiring) 
                    VALUES %s
                    ON CONFLICT (id64) DO UPDATE SET
                        name = EXCLUDED.name,
                        x = EXCLUDED.x,
                        y = EXCLUDED.y,
                        z = EXCLUDED.z,
                        distance_from_sol = EXCLUDED.distance_from_sol,
                        primary_economy = EXCLUDED.primary_economy,
                        secondary_economy = EXCLUDED.secondary_economy,
                        security = EXCLUDED.security,
                        controlling_power = EXCLUDED.controlling_power,
                        power_state = EXCLUDED.power_state,
                        powers_acquiring = EXCLUDED.powers_acquiring::jsonb
                    """,
                    chunk,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                )
                self.pg_conn.commit()
            
            # Load market status by checking for commodities in PostgreSQL
            self.console.print("\n[yellow]Checking actual market presence in PostgreSQL...[/]")
            self.pg_cur.execute("""
                SELECT DISTINCT system_id64, station_id 
                FROM station_commodities_mapped
            """)
            pg_market_status = {(row[0], row[1]): True for row in self.pg_cur.fetchall()}
            self.console.print(f"Found {len(pg_market_status):,} stations with actual markets in PostgreSQL")
            
            # Load market status from SQLite by checking commodities
            self.console.print("[yellow]Checking market presence in SQLite...[/]")
            self.sqlite_cur.execute("""
                SELECT DISTINCT system_id64, station_id 
                FROM station_commodities_mapped
            """)
            sqlite_market_status = {(row[0], row[1]): True for row in self.sqlite_cur.fetchall()}
            self.console.print(f"Found {len(sqlite_market_status):,} stations with markets in SQLite")
            
            # Combine market statuses, preferring SQLite values
            self.station_market_status = {**pg_market_status, **sqlite_market_status}
            self.console.print(f"Combined total of {len(self.station_market_status):,} stations with confirmed markets")
            
            # Get list of valid system IDs from PostgreSQL
            self.console.print("\n[yellow]Getting list of valid system IDs from PostgreSQL...[/]")
            self.pg_cur.execute("SELECT id64 FROM systems")
            valid_system_ids = {row[0] for row in self.pg_cur.fetchall()}
            self.console.print(f"Found {len(valid_system_ids):,} valid systems in PostgreSQL")

            # Then migrate stations, but only those with valid system IDs
            self.console.print(f"\n[yellow]Migrating stations table ({total_stations:,} rows)...[/]")
            self.sqlite_cur.execute("""
                SELECT system_id64, station_id, body, station_name, station_type, 
                       primary_economy, economies, landing_pad_size, distance_to_arrival, update_time 
                FROM stations
                WHERE system_id64 IN (SELECT id64 FROM systems)
            """)
            stations_data = self.sqlite_cur.fetchall()
            stations = [clean_row(row) for row in stations_data]
            skipped_stations = total_stations - len(stations)
            self.console.print(f"[yellow]Skipping {skipped_stations:,} stations with invalid system IDs[/]")
            
            self.console.print("[yellow]Inserting stations into PostgreSQL...[/]")
            for chunk in tqdm([stations[i:i + 1000] for i in range(0, len(stations), 1000)], desc="Processing"):
                execute_values(
                    self.pg_cur,
                    """
                    INSERT INTO stations 
                    (system_id64, station_id, body, station_name, station_type, 
                     primary_economy, economies, landing_pad_size, distance_to_arrival, update_time) 
                    VALUES %s
                    ON CONFLICT (system_id64, station_id) DO UPDATE SET
                        body = EXCLUDED.body,
                        station_name = EXCLUDED.station_name,
                        station_type = EXCLUDED.station_type,
                        primary_economy = EXCLUDED.primary_economy,
                        economies = EXCLUDED.economies::jsonb,
                        landing_pad_size = EXCLUDED.landing_pad_size,
                        distance_to_arrival = EXCLUDED.distance_to_arrival,
                        update_time = EXCLUDED.update_time
                    """,
                    chunk,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                )
                self.pg_conn.commit()
            
            # Finally, update has_market values
            if self.station_market_status:
                self.console.print("\n[yellow]Updating has_market status...[/]")
                updates = [(market_value, system_id64, station_id) 
                          for (system_id64, station_id), market_value in self.station_market_status.items()]
                
                for chunk in tqdm([updates[i:i + 1000] for i in range(0, len(updates), 1000)], desc="Processing"):
                    execute_values(
                        self.pg_cur,
                        """
                        UPDATE stations SET has_market = data.market_value::boolean
                        FROM (VALUES %s) AS data(market_value, system_id64, station_id)
                        WHERE stations.system_id64 = data.system_id64::bigint
                        AND stations.station_id = data.station_id::bigint
                        """,
                        chunk,
                        template="(%s, %s, %s)"
                    )
                    self.pg_conn.commit()
            
            # Verify counts after migration
            self.pg_cur.execute("SELECT COUNT(*) FROM stations")
            pg_stations = self.pg_cur.fetchone()[0]
            
            # Migrate commodities for valid stations
            self.console.print(f"\n[yellow]Migrating station commodities ({total_commodities:,} rows)...[/]")
            self.sqlite_cur.execute("""
                SELECT scm.system_id64, scm.station_id, scm.station_name, 
                       scm.commodity_id, scm.sell_price, scm.demand
                FROM station_commodities_mapped scm
                JOIN stations s ON s.system_id64 = scm.system_id64 
                    AND s.station_id = scm.station_id
                WHERE s.system_id64 IN (SELECT id64 FROM systems)
            """)
            commodities_data = self.sqlite_cur.fetchall()
            commodities = [clean_row(row) for row in commodities_data]
            skipped_commodities = total_commodities - len(commodities)
            self.console.print(f"[yellow]Skipping {skipped_commodities:,} commodities with invalid system/station IDs[/]")
            
            self.console.print("[yellow]Inserting commodities into PostgreSQL...[/]")
            for chunk in tqdm([commodities[i:i + 1000] for i in range(0, len(commodities), 1000)], desc="Processing"):
                execute_values(
                    self.pg_cur,
                    """
                    INSERT INTO station_commodities_mapped 
                    (system_id64, station_id, station_name, commodity_id, sell_price, demand)
                    VALUES %s
                    ON CONFLICT (system_id64, station_id, commodity_id) DO UPDATE SET
                        station_name = EXCLUDED.station_name,
                        sell_price = EXCLUDED.sell_price,
                        demand = EXCLUDED.demand
                    """,
                    chunk,
                    template="(%s, %s, %s, %s, %s, %s)"
                )
                self.pg_conn.commit()
            
            self.pg_cur.execute("SELECT COUNT(*) FROM station_commodities_mapped")
            pg_commodities = self.pg_cur.fetchone()[0]
            self.pg_cur.execute("SELECT COUNT(*) FROM systems")
            pg_systems = self.pg_cur.fetchone()[0]
            
            self.console.print("\n[bold green]Migration Summary:[/]")
            self.console.print(f"Stations: {total_stations:,} -> {pg_stations:,}")
            self.console.print(f"Station Commodities: {total_commodities:,} -> {pg_commodities:,}")
            self.console.print(f"Systems: {total_systems:,} -> {pg_systems:,}")
            
            # Add verification step at the end
            self.verify_migration()
            
        except Exception as e:
            self.console.print(f"[red]Error during migration: {str(e)}[/]")
            self.pg_conn.rollback()
            raise

def main():
    parser = argparse.ArgumentParser(description='Process missing stations from Spansh and EDSM data')
    parser.add_argument('--spansh', help='Path to the Spansh JSON data file')
    parser.add_argument('--edsm', help='Path to the EDSM JSON data file')
    parser.add_argument('--sqdb', help='Path to output SQLite database file')
    parser.add_argument('--migrate', action='store_true',
                      help='Migrate data directly from SQLite to PostgreSQL')
    parser.add_argument('--max-distance', type=float, required=True,
                      help='Maximum distance from Sol in light years')
    parser.add_argument('--db', default='postgresql://postgres:elephant9999!@localhost:5432/power_mining',
                      help='PostgreSQL connection string')
    parser.add_argument('--nocount', action='store_true',
                      help='Skip counting and use predefined values')
    parser.add_argument('--skip-spansh', action='store_true',
                      help='Skip full Spansh processing, only scan for market status')
    
    args = parser.parse_args()
    
    if not args.spansh and not args.edsm and not args.migrate:
        print("Either --spansh, --edsm, or --migrate must be provided!")
        sys.exit(1)
    
    if not args.sqdb and not args.migrate:
        print("--sqdb is required!")
        sys.exit(1)
    
    if args.migrate and not args.sqdb:
        print("--sqdb is required when using --migrate!")
        sys.exit(1)
    
    processor = StationProcessor(args.db, args.max_distance, args)
    
    try:
        console = Console()
        
        if args.migrate:
            console.print("\n[bold cyan]Migrating data from SQLite to PostgreSQL[/]")
            processor.migrate_to_postgres()
        else:
            if args.spansh:
                if args.skip_spansh:
                    console.print("\n[bold cyan]Step 1: Quick scan of Spansh data for market status[/]")
                    processor.scan_spansh_markets(args.spansh)
                else:
                    console.print("\n[bold cyan]Step 1: Processing Spansh data[/]")
                    processor.process_spansh_file(args.spansh)
            
            if args.edsm:
                console.print("\n[bold cyan]Step 2: Processing EDSM data[/]")
                processor.process_edsm_file(args.edsm)
            
            # Add cleanup step before saving/dumping
            console.print("\n[bold cyan]Step 3: Cleaning up remaining NULL station types[/]")
            processor.cleanup_null_types()
            
            # Add landing pad cleanup
            console.print("\n[bold cyan]Step 4: Converting Unknown landing pads to NULL[/]")
            processor.cleanup_landing_pads()
            
            if args.sqdb:
                console.print("\n[bold cyan]Step 5: Saving SQLite database[/]")
                processor.save_sqlite_db(args.sqdb)
        
        console.print("\n[bold green]Processing complete![/]")
        
    finally:
        processor.close()

if __name__ == '__main__':
    main()
