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

# Optional compression libraries
try:
    import zstandard
    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False

try:
    import lz4.frame
    LZ4_AVAILABLE = True
except ImportError:
    LZ4_AVAILABLE = False

def compress_data(data: str, method: str) -> str:
    """Compress string data using the specified method."""
    if method == 'none':
        return data
        
    # Convert string to bytes
    data_bytes = data.encode('utf-8')
    
    # Add compression method prefix to compressed data
    if method == 'zlib':
        compressed = zlib.compress(data_bytes)
        return f"__compressed__zlib__{compressed.hex()}"
    elif method == 'zstandard':
        if not ZSTD_AVAILABLE:
            raise ImportError("zstandard package not installed. Install with: pip install zstandard")
        cctx = zstandard.ZstdCompressor()
        compressed = cctx.compress(data_bytes)
        return f"__compressed__zstandard__{compressed.hex()}"
    elif method == 'lz4':
        if not LZ4_AVAILABLE:
            raise ImportError("lz4 package not installed. Install with: pip install lz4")
        compressed = lz4.frame.compress(data_bytes)
        return f"__compressed__lz4__{compressed.hex()}"
    else:
        raise ValueError(f"Unknown compression method: {method}")

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
    'LowTemperatureDiamond', 'Methane Clathrate',
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
    
    # Table for station commodity prices
    c.execute('''CREATE TABLE IF NOT EXISTS station_commodities (
        system_id64 INTEGER,
        station_id INTEGER,
        station_name TEXT,
        commodity_name TEXT,
        sell_price INTEGER,
        demand INTEGER,
        FOREIGN KEY(system_id64) REFERENCES systems(id64),
        FOREIGN KEY(system_id64, station_id) REFERENCES stations(system_id64, station_id)
    )''')
    
    # Create indices for common searches
    c.execute('CREATE INDEX IF NOT EXISTS idx_controlling_power ON systems(controlling_power)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_coordinates ON systems(x, y, z)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_distance ON systems(distance_from_sol)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_mineral_type ON mineral_signals(mineral_type)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_commodity_name ON station_commodities(commodity_name)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_commodity_search ON station_commodities(commodity_name, sell_price, demand)')
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
                        signals.append({
                            'body_name': body['name'],
                            'ring_name': ring['name'],
                            'mineral_type': mineral,
                            'signal_count': count,
                            'reserve_level': body.get('reserveLevel', 'Unknown'),
                            'ring_type': ring_type
                        })
    return signals

def extract_station_commodities(station: Dict) -> list:
    """Extract relevant commodity data from a station."""
    commodities = []
    if 'market' in station and 'commodities' in station['market']:
        for commodity in station['market']['commodities']:
            # Convert station commodity name to match our MINERAL_SIGNALS set
            commodity_name = commodity['name']
            if commodity_name == 'Low Temperature Diamonds':
                commodity_name = 'LowTemperatureDiamond'
            
            if commodity_name in MINERAL_SIGNALS:
                commodities.append({
                    'station_name': station['name'],
                    'commodity_name': commodity['name'],  # Keep original name for database
                    'sell_price': commodity['sellPrice'],
                    'demand': commodity['demand']
                })
    return commodities

def process_json_stream(json_file: str) -> Generator[Dict[Any, Any], None, None]:
    """Stream the JSON file one system at a time to avoid memory issues."""
    with open(json_file, 'rb') as file:
        parser = ijson.items(file, 'item')
        for system in parser:
            yield system

def convert_json_to_sqlite(json_file: str, db_file: str, max_distance: float, exclude_carriers: bool = False, compression: str = 'none', trim_entries: bool = False):
    """Convert the large JSON file to SQLite database."""
    conn = create_database(db_file)
    c = conn.cursor()
    
    try:
        processed = 0
        skipped_distance = 0
        skipped_carriers_stations = 0
        skipped_no_market = 0
        system_stations = 0
        body_stations = 0
        total_stations = 0
        trimmed_outfitting = 0
        trimmed_shipyard = 0
        start_time = time.time()
        last_update = start_time
        
        # Initialize progress bar
        pbar = tqdm(total=TOTAL_ENTRIES, desc="Converting systems", 
                   unit="systems", ncols=100, position=0, leave=True)
        
        # Create a second progress bar for stats
        stats_bar = tqdm(bar_format='{desc}', desc='', position=1, leave=True)
        
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
                pbar.update(1)
                continue
            
            # Create a copy of the system data for all operations
            system = json.loads(json.dumps(system, cls=DecimalEncoder))
            
            # Process stations from both system level and bodies
            all_stations = []
            system_level_stations = []
            body_level_stations = []
            
            # Add system-level stations with market
            if 'stations' in system:
                system_level_stations = [(station, None) for station in system['stations'] if 'market' in station]
                all_stations.extend(system_level_stations)
                system_stations += len(system_level_stations)
                skipped_no_market += len([s for s in system['stations'] if 'market' not in s])
            
            # Add stations from bodies with market
            if 'bodies' in system:
                for body in system['bodies']:
                    if 'stations' in body:
                        body_stations_list = [(station, body['name']) for station in body['stations'] if 'market' in station]
                        body_level_stations.extend(body_stations_list)
                        skipped_no_market += len([s for s in body['stations'] if 'market' not in s])
                all_stations.extend(body_level_stations)
                body_stations += len(body_level_stations)
            
            # Filter out carriers if requested
            if exclude_carriers:
                original_count = len(all_stations)
                all_stations = [station_info for station_info in all_stations 
                              if not (station_info[0].get('type') == 'Drake-Class Carrier' or 'carrierName' in station_info[0])]
                skipped_carriers_stations += original_count - len(all_stations)
            
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
                market_update_time = station['market'].get('updateTime')  # We know market exists due to filtering
                
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
            
            # Compress the full_data JSON if compression is enabled
            # try:
            #     full_data = compress_data(json.dumps(system, cls=DecimalEncoder), compression)
            # except ImportError as e:
            #     print(f"\nError: {str(e)}")
            #     sys.exit(1)
            # except Exception as e:
            #     print(f"\nError compressing data: {str(e)}")
            #     sys.exit(1)
            
            # Extract key fields for indexed columns
            system_data = {
                'id64': system.get('id64'),
                'name': system.get('name'),
                'x': x,
                'y': y,
                'z': z,
                'distance_from_sol': distance,
                'controlling_power': system.get('controllingPower'),
                'power_state': system.get('powerState'),
                'powers_acquiring': json.dumps([p for p in system.get('powers', []) if p != system.get('controllingPower')])
            }

            # Insert or update the system
            c.execute('''
                INSERT OR REPLACE INTO systems 
                (id64, name, x, y, z, distance_from_sol, controlling_power, power_state, powers_acquiring)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                system_data['id64'],
                system_data['name'],
                system_data['x'],
                system_data['y'],
                system_data['z'],
                system_data['distance_from_sol'],
                system_data['controlling_power'],
                system_data['power_state'],
                system_data['powers_acquiring']
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
                            system_data['id64'],
                            signal['body_name'],
                            signal['ring_name'],
                            signal['mineral_type'],
                            signal['signal_count'],
                            signal['reserve_level'],
                            signal['ring_type']
                        ))
            
            # Process station commodities - filter out carriers
            if 'stations' in system:
                for station in system['stations']:
                    # Skip carriers
                    if station.get('type') == 'Drake-Class Carrier' or 'carrierName' in station:
                        continue
                    for commodity in extract_station_commodities(station):
                        c.execute('''
                            INSERT INTO station_commodities 
                            (system_id64, station_id, station_name, commodity_name, sell_price, demand)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (
                            system_data['id64'],
                            station.get('id'),
                            commodity['station_name'],
                            commodity['commodity_name'],
                            commodity['sell_price'],
                            commodity['demand']
                        ))
            
            total_stations = system_stations + body_stations
            
            processed += 1
            pbar.update(1)
            
            # Update performance metrics every 5 seconds
            current_time = time.time()
            if current_time - last_update >= 5:
                elapsed = current_time - start_time
                entries_per_second = processed / elapsed
                stats = f"Stats: Processed: {processed:,} | Distance skipped: {skipped_distance:,}"
                if exclude_carriers:
                    stats += f" | Carriers skipped: {skipped_carriers_stations:,}"
                stats += f" | No market: {skipped_no_market:,}"
                stats += f" | Speed: {entries_per_second:.1f}/s | Stations in systems: {system_stations:,} | Stations on bodies: {body_stations:,} | Stations total: {total_stations:,}"
                if trim_entries:
                    stats += f" | Trimmed shipyard: {trimmed_shipyard:,} | Trimmed outfitting: {trimmed_outfitting:,}"
                stats_bar.set_description_str(stats)
                last_update = current_time
                
            if processed % 1000 == 0:
                conn.commit()
        
        conn.commit()
        pbar.close()
        stats_bar.close()
        
        # Final statistics
        total_time = time.time() - start_time
        print(f"\nConversion complete:")
        print(f"Total systems processed: {processed}")
        print(f"Systems skipped (outside radius): {skipped_distance}")
        print(f"Stations without market: {skipped_no_market}")
        if exclude_carriers:
            print(f"Fleet Carriers skipped: {skipped_carriers_stations}")
        if trim_entries:
            print(f"Shipyard entries trimmed: {trimmed_shipyard}")
            print(f"Outfitting entries trimmed: {trimmed_outfitting}")
        print(f"Total stations: {total_stations:,} (System: {system_stations:,}, Bodies: {body_stations:,})")
        print(f"Total time: {total_time:.1f} seconds")
        print(f"Average speed: {processed/total_time:.1f} entries/second")
    
    except Exception as e:
        print(f"\nError during conversion: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert Elite Dangerous JSON data to SQLite database')
    parser.add_argument('json_file', help='Path to the input JSON file')
    parser.add_argument('db_file', help='Path to the output SQLite database file')
    parser.add_argument('--max-distance', type=str, required=True,
                      help='Maximum distance from Sol in light years')
    parser.add_argument('--exclude-carriers', action='store_true',
                      help='Exclude Drake-Class Carriers from the database')
    parser.add_argument('-c', '--compression', 
                      choices=['none', 'zlib', 'zstandard', 'lz4'],
                      default='none',
                      help='Compression method for JSON data (default: none)')
    parser.add_argument('--trim-entries', action='store_true',
                      help='Remove shipyard and outfitting data from stations')
    
    args = parser.parse_args()
    
    if not Path(args.json_file).exists():
        print(f"Input file {args.json_file} does not exist!")
        sys.exit(1)
    
    try:
        max_distance = float(args.max_distance)
    except ValueError:
        print("Max distance must be a number in light years")
        sys.exit(1)
    
    convert_json_to_sqlite(args.json_file, args.db_file, max_distance, args.exclude_carriers, args.compression, args.trim_entries) 