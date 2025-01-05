from flask import Flask, render_template, request, jsonify, send_from_directory
import sqlite3
from typing import Dict, List, Optional
import math
import os
import json
import zlib  # Built-in compression
import mining_data
from mining_data import (
    get_material_ring_types, 
    get_non_hotspot_materials_list, 
    get_ring_type_case_statement,
    get_mining_type_conditions,
    get_price_comparison,
    normalize_commodity_name,
    get_potential_ring_types,
    PRICE_DATA,
    NON_HOTSPOT_MATERIALS
)
import res_data

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

# Get the absolute path of the directory containing server.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__, 
           template_folder=BASE_DIR,  # Set template folder to the root directory
           static_folder=None)  # Disable default static folder handling

# Routes for static files
@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path),
                             'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/<path:filename>')
def serve_static(filename):
    # Set correct MIME types for different file extensions
    mime_types = {
        '.js': 'application/javascript',
        '.css': 'text/css',
        '.html': 'text/html',
        '.ico': 'image/x-icon',
        '.svg': 'image/svg+xml',
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.gif': 'image/gif',
        '.woff': 'font/woff',
        '.woff2': 'font/woff2',
        '.ttf': 'font/ttf'
    }
    
    # Get the file extension
    _, ext = os.path.splitext(filename)
    # Get the corresponding MIME type, default to binary stream if not found
    mimetype = mime_types.get(ext.lower(), 'application/octet-stream')
    
    response = send_from_directory(BASE_DIR, filename, mimetype=mimetype)
    if ext.lower() == '.js':
        response.headers['Access-Control-Allow-Origin'] = '*'
    return response

@app.route('/css/<path:filename>')
def serve_css(filename):
    return send_from_directory(os.path.join(BASE_DIR, 'css'), filename)

@app.route('/js/<path:filename>')
def serve_js(filename):
    response = send_from_directory(os.path.join(BASE_DIR, 'js'), filename, mimetype='application/javascript')
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

@app.route('/fonts/<path:filename>')
def serve_fonts(filename):
    return send_from_directory(os.path.join(BASE_DIR, 'fonts'), filename)

@app.route('/img/<path:filename>')
def serve_images(filename):
    return send_from_directory(os.path.join(BASE_DIR, 'img'), filename)

@app.route('/img/loading/<path:filename>')
def serve_loading_js(filename):
    if filename.endswith('.js'):
        response = send_from_directory(os.path.join(BASE_DIR, 'img', 'loading'), filename, mimetype='application/javascript')
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response
    return send_from_directory(os.path.join(BASE_DIR, 'img', 'loading'), filename)

@app.route('/Config.ini')
def serve_config():
    """Serve the Config.ini file."""
    try:
        config_path = os.path.join(BASE_DIR, 'Config.ini')
        if not os.path.exists(config_path):
            # If Config.ini doesn't exist, create a default one
            default_config = """[Defaults]
system = Harma
controlling_power = Archon Delaine
max_distance = 200
search_results = 30
system_database = systems.db"""
            with open(config_path, 'w') as f:
                f.write(default_config)
        
        response = send_from_directory(BASE_DIR, 'Config.ini', mimetype='text/plain')
        # Add headers to prevent caching
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
    except Exception as e:
        app.logger.error(f"Error serving Config.ini: {str(e)}")
        # Return a default configuration as JSON if file serving fails
        return jsonify({
            'Defaults': {
                'system': 'Harma',
                'controlling_power': 'Archon Delaine',
                'max_distance': '200',
                'search_results': '30',
                'system_database': 'systems.db'
            }
        })

def decompress_data(data: str) -> str:
    """Decompress data if it was compressed during conversion."""
    if not data.startswith('__compressed__'):
        return data
        
    try:
        # Extract compression method and compressed data
        _, method, compressed_hex = data.split('__', 2)
        compressed = bytes.fromhex(compressed_hex)
        
        if method == 'zlib':
            decompressed = zlib.decompress(compressed)
        elif method == 'zstandard':
            if not ZSTD_AVAILABLE:
                raise ImportError("zstandard package not installed. Install with: pip install zstandard")
            dctx = zstandard.ZstdDecompressor()
            decompressed = dctx.decompress(compressed)
        elif method == 'lz4':
            if not LZ4_AVAILABLE:
                raise ImportError("lz4 package not installed. Install with: pip install lz4")
            decompressed = lz4.frame.decompress(compressed)
        else:
            raise ValueError(f"Unknown compression method: {method}")
            
        return decompressed.decode('utf-8')
    except Exception as e:
        app.logger.error(f"Error decompressing data: {str(e)}")
        return data  # Return original data if decompression fails

# Modify the row_factory to handle compressed data
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        value = row[idx]
        if col[0] == 'full_data' and isinstance(value, str):
            # Decompress if necessary
            value = decompress_data(value)
            # Parse JSON after decompression
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                app.logger.error(f"Error decoding JSON after decompression")
                value = None
        d[col[0]] = value
    return d

def get_db_connection():
    """Create a database connection with decompression support."""
    db_file = request.args.get('database', 'systems.db')
    # Ensure the database file exists
    if not os.path.exists(db_file):
        app.logger.error(f"Database file not found: {db_file}")
        return None
    conn = sqlite3.connect(db_file)
    conn.row_factory = dict_factory
    return conn

def calculate_distance(x1: float, y1: float, z1: float, x2: float, y2: float, z2: float) -> float:
    """Calculate distance between two points in 3D space."""
    return math.sqrt((x2-x1)**2 + (y2-y1)**2 + (z2-z1)**2)

def get_ring_materials():
    """Load ring materials and their associated ring types."""
    ring_materials = {}
    try:
        with open('data/ring_materials.csv', 'r') as f:
            next(f)  # Skip header
            for line in f:
                material, abbrev, ring_types, conditions, value = line.strip().split(',')
                ring_materials[material] = {
                    'ring_types': [t.strip() for t in ring_types.split('/')],
                    'abbreviation': abbrev,
                    'conditions': conditions,
                    'value': value
                }
    except Exception as e:
        app.logger.error(f"Error loading ring materials: {str(e)}")
    return ring_materials

@app.route('/')
def index():
    """Render the main page."""
    return render_template('index.html')

@app.route('/autocomplete')
def autocomplete():
    """Handle system name autocomplete."""
    try:
        search = request.args.get('q', '').strip()
        if len(search) < 2:
            return jsonify([])
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Search for system names that start with the input
        cursor.execute('''
            SELECT name, x, y, z 
            FROM systems 
            WHERE name LIKE ? || '%'
            LIMIT 10
        ''', (search,))
        
        results = [{'name': row['name'], 'coords': {'x': row['x'], 'y': row['y'], 'z': row['z']}} 
                  for row in cursor.fetchall()]
        
        conn.close()
        return jsonify(results)
    except Exception as e:
        app.logger.error(f"Autocomplete error: {str(e)}")
        return jsonify({'error': 'Error during autocomplete'}), 500

@app.route('/search')
def search():
    """Handle the main search functionality."""
    try:
        # Get search parameters
        ref_system = request.args.get('system', 'Sol')
        max_distance = float(request.args.get('distance', '10000'))
        controlling_power = request.args.get('controlling_power')
        power_states = request.args.getlist('power_state[]')
        signal_type = request.args.get('signal_type')
        ring_type_filter = request.args.get('ring_type_filter', 'All')
        limit = int(request.args.get('limit', '30'))
        mining_types = request.args.getlist('mining_types[]')

        # Early check for mining type filtering
        if mining_types and 'All' not in mining_types:
            # Load material mining data
            with open('data/mining_data.json', 'r') as f:
                material_data = json.load(f)
            
            # Check if material exists and has valid mining types
            commodity_data = next((item for item in material_data['materials'] if item['name'] == signal_type), None)
            if not commodity_data:
                return jsonify([])  # Return empty results if material isn't found
        
        # Check if this is a ring-type material
        ring_materials = get_ring_materials()
        is_ring_material = signal_type in ring_materials
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get reference system coordinates
        cursor.execute('SELECT x, y, z FROM systems WHERE name = ?', (ref_system,))
        ref_coords = cursor.fetchone()
        if not ref_coords:
            conn.close()
            return jsonify({'error': 'Reference system not found'}), 404
        
        ref_x, ref_y, ref_z = ref_coords['x'], ref_coords['y'], ref_coords['z']
        
        # Get mining type conditions if specified
        mining_type_condition = ''
        mining_type_params = []
        if mining_types and 'All' not in mining_types:
            mining_type_condition, mining_type_params = get_mining_type_conditions(signal_type, mining_types)

        # Add ring type filter conditions
        ring_type_condition = ''
        ring_type_params = []  # New list for ring type parameters
        if ring_type_filter != 'All':
            if ring_type_filter == 'Just Hotspots':
                ring_type_condition = ' AND ms.mineral_type IS NOT NULL'
            elif ring_type_filter == 'Without Hotspots':
                # For Without Hotspots, we want rings that either:
                # 1. Don't have a hotspot of this type, AND
                # 2. Are of a type where this material can be found
                ring_type_condition = ' AND (ms.mineral_type IS NULL OR ms.mineral_type != ?)'
                ring_type_params.append(signal_type)
                
                # Get potential ring types from mining_data.json
                try:
                    with open('data/mining_data.json', 'r') as f:
                        material_data = json.load(f)
                        commodity_data = next((item for item in material_data['materials'] if item['name'] == signal_type), None)
                        if commodity_data:
                            # Get ring types where this material can be mined
                            ring_types = []
                            for ring_type, ring_data in commodity_data['ring_types'].items():
                                if any([
                                    ring_data['surfaceLaserMining'],
                                    ring_data['surfaceDeposit'],
                                    ring_data['subSurfaceDeposit'],
                                    ring_data['core']
                                ]):
                                    ring_types.append(ring_type)
                            
                            if ring_types:
                                ring_type_condition += ' AND ms.ring_type IN (' + ','.join('?' * len(ring_types)) + ')'
                                ring_type_params.extend(ring_types)
                except Exception as e:
                    app.logger.error(f"Error checking mining_data.json: {str(e)}")
            else:
                # Specific ring type selected
                ring_type_condition = ' AND ms.ring_type = ?'
                ring_type_params.append(ring_type_filter)
                
                # Check if this material can be found in this ring type
                try:
                    with open('data/mining_data.json', 'r') as f:
                        material_data = json.load(f)
                        commodity_data = next((item for item in material_data['materials'] if item['name'] == signal_type), None)
                        if not commodity_data or ring_type_filter not in commodity_data['ring_types']:
                            return jsonify([])  # Return empty results if material can't be found in this ring type
                except Exception as e:
                    app.logger.error(f"Error checking mining_data.json: {str(e)}")
        
        # Define non-hotspot materials
        non_hotspot_minerals = get_non_hotspot_materials_list()
        is_non_hotspot = signal_type in non_hotspot_minerals
        
        if is_non_hotspot:
            # Get ring types from NON_HOTSPOT_MATERIALS dictionary
            ring_types = mining_data.NON_HOTSPOT_MATERIALS.get(signal_type, [])
            ring_types_str = ','.join('?' * len(ring_types))
            
            query = '''
            WITH relevant_systems AS (
                SELECT s.*, 
                    sqrt(((s.x - ?) * (s.x - ?)) + 
                            ((s.y - ?) * (s.y - ?)) + 
                            ((s.z - ?) * (s.z - ?))) as distance
                FROM systems s
                WHERE (((s.x - ?) * (s.x - ?)) + 
                    ((s.y - ?) * (s.y - ?)) + 
                    ((s.z - ?) * (s.z - ?))) <= ? * ?
            ),
            relevant_stations AS (
                SELECT sc.system_id64, sc.station_name, sc.sell_price, sc.demand
                FROM station_commodities sc
                WHERE (sc.commodity_name = ? OR 
                      (? = 'LowTemperatureDiamond' AND sc.commodity_name = 'Low Temperature Diamonds'))
                        AND sc.demand > 0
                        AND sc.sell_price > 0
            )
            SELECT DISTINCT
                s.name as system_name,
                s.id64 as system_id64,
                s.controlling_power,
                s.power_state,
                s.distance,
                ms.body_name,
                ms.ring_name,
                ms.ring_type,
                ms.mineral_type,
                ms.signal_count,
                ms.reserve_level,
                rs.station_name,
                st.landing_pad_size,
                st.distance_to_arrival as station_distance,
                st.station_type,
                rs.demand,
                rs.sell_price,
                st.update_time
            FROM relevant_systems s
            JOIN mineral_signals ms ON s.id64 = ms.system_id64
            LEFT JOIN relevant_stations rs ON s.id64 = rs.system_id64
            LEFT JOIN stations st ON s.id64 = st.system_id64 
                AND rs.station_name = st.station_name
            WHERE ms.ring_type IN (''' + ring_types_str + ''')''' + ring_type_condition + '''
            '''
            
            params = [
                ref_x, ref_x, ref_y, ref_y, ref_z, ref_z,  # for distance
                ref_x, ref_x, ref_y, ref_y, ref_z, ref_z, max_distance, max_distance,  # for WHERE clause
                signal_type, signal_type  # for commodity_name and LTD check
            ]
            params.extend(ring_types)  # for ring type IN (...)
            params.extend(ring_type_params)  # Add ring type parameters
            
            # Add mining type conditions if specified
            if mining_type_condition:
                query += f' AND {mining_type_condition}'
                params.extend(mining_type_params)
        
        elif is_ring_material:
            ring_types = ring_materials[signal_type]['ring_types']
            app.logger.info(f"Looking for rings of type: {ring_types}")
            ring_types_str = ','.join('?' * len(ring_types))
            
            query = '''
            WITH relevant_systems AS (
                SELECT s.*, 
                       (((s.x - ?) * (s.x - ?)) + 
                        ((s.y - ?) * (s.y - ?)) + 
                        ((s.z - ?) * (s.z - ?))) as distance_squared,
                       sqrt(((s.x - ?) * (s.x - ?)) + 
                            ((s.y - ?) * (s.y - ?)) + 
                            ((s.z - ?) * (s.z - ?))) as distance
                FROM systems s
                WHERE (((s.x - ?) * (s.x - ?)) + 
                      ((s.y - ?) * (s.y - ?)) + 
                      ((s.z - ?) * (s.z - ?))) <= ? * ?
            ),
            relevant_stations AS (
                SELECT DISTINCT 
                    s.id64,
                    s.name as system_name,
                    s.controlling_power,
                    s.power_state,
                    s.distance,
                    s.full_data,
                    ms.body_name,
                    ms.ring_name,
                    ms.ring_type,
                    ms.reserve_level,
                    rs.station_name,
                    rs.demand,
                    rs.sell_price,
                    st.landing_pad_size,
                    st.distance_to_arrival
                FROM relevant_systems s
                JOIN mineral_signals ms ON s.id64 = ms.system_id64
                LEFT JOIN station_commodities rs ON s.id64 = rs.system_id64 
                    AND rs.commodity_name = ?
                LEFT JOIN stations st ON s.id64 = st.system_id64 
                    AND rs.station_name = st.station_name
                WHERE ms.ring_type IN (''' + ring_types_str + ''')''' + ring_type_condition + '''
            )
            SELECT DISTINCT 
                rs.system_name,
                rs.controlling_power,
                rs.power_state,
                rs.distance,
                rs.body_name,
                rs.ring_name,
                rs.ring_type,
                rs.reserve_level,
                rs.station_name,
                st.landing_pad_size,
                st.distance_to_arrival as station_distance,
                rs.demand,
                rs.sell_price,
                st.update_time
            FROM relevant_stations rs
            JOIN mineral_signals ms ON rs.id64 = ms.system_id64
            LEFT JOIN stations st ON rs.id64 = st.system_id64 
                AND rs.station_name = st.station_name
            WHERE 1=1
            '''
            
            params = [
                ref_x, ref_x, ref_y, ref_y, ref_z, ref_z,  # for distance_squared
                ref_x, ref_x, ref_y, ref_y, ref_z, ref_z,  # for distance
                ref_x, ref_x, ref_y, ref_y, ref_z, ref_z, max_distance, max_distance,  # for WHERE clause
                signal_type,  # for relevant_stations
                signal_type   # for mineral_signals
            ]
            params.extend(ring_types)  # for ring type filter
            params.extend(ring_type_params)  # Add ring type parameters
            
            # Add mining type conditions if specified
            if mining_type_condition:
                query += f' AND {mining_type_condition}'
                params.extend(mining_type_params)
        
        else:
            # Original hotspot query
            query = '''
            WITH relevant_systems AS (
                SELECT s.*, 
                    sqrt(((s.x - ?) * (s.x - ?)) + 
                            ((s.y - ?) * (s.y - ?)) + 
                            ((s.z - ?) * (s.z - ?))) as distance
                FROM systems s
                WHERE (((s.x - ?) * (s.x - ?)) + 
                    ((s.y - ?) * (s.y - ?)) + 
                    ((s.z - ?) * (s.z - ?))) <= ? * ?
            ),
            relevant_stations AS (
                SELECT sc.system_id64, sc.station_name, sc.sell_price, sc.demand
                FROM station_commodities sc
                WHERE (sc.commodity_name = ? OR 
                      (? = 'LowTemperatureDiamond' AND sc.commodity_name = 'Low Temperature Diamonds'))
                        AND sc.demand > 0
                        AND sc.sell_price > 0
            )
            SELECT DISTINCT 
                s.name as system_name,
                s.id64 as system_id64,
                s.controlling_power,
                s.power_state,
                s.distance,
                ms.body_name,
                ms.ring_name,
                ms.ring_type,
                ms.mineral_type,
                ms.signal_count,
                ms.reserve_level,
                rs.station_name,
                st.landing_pad_size,
                st.distance_to_arrival as station_distance,
                st.station_type,
                rs.demand,
                rs.sell_price,
                st.update_time
            FROM relevant_systems s
            JOIN mineral_signals ms ON s.id64 = ms.system_id64''' + (
                ' AND ms.mineral_type = ?' if ring_type_filter != 'Without Hotspots' else '') + ring_type_condition + '''
            LEFT JOIN relevant_stations rs ON s.id64 = rs.system_id64
            LEFT JOIN stations st ON s.id64 = st.system_id64 
                AND rs.station_name = st.station_name
            WHERE 1=1
            '''
            
            params = [
                ref_x, ref_x, ref_y, ref_y, ref_z, ref_z,  # for distance
                ref_x, ref_x, ref_y, ref_y, ref_z, ref_z, max_distance, max_distance,  # for WHERE clause
                signal_type, signal_type  # for commodity_name and LTD check
            ]
            if ring_type_filter != 'Without Hotspots':
                params.append(signal_type)  # for mineral_type = ?
            params.extend(ring_type_params)  # Add ring type parameters
            
            # Add mining type conditions if specified
            if mining_type_condition:
                query += f' AND {mining_type_condition}'
                params.extend(mining_type_params)
        
        if controlling_power:
            query += ' AND s.controlling_power = ?'
            params.append(controlling_power)
        
        if power_states:
            query += ' AND s.power_state IN ({})'.format(','.join('?' * len(power_states)))
            params.extend(power_states)
        
        # Order by reserve level (pristine first) for ring materials, then by price and distance
        if is_ring_material:
            query += ''' ORDER BY 
                CASE 
                    WHEN ms.reserve_level = 'Pristine' THEN 1
                    WHEN ms.reserve_level = 'Major' THEN 2
                    WHEN ms.reserve_level = 'Common' THEN 3
                    WHEN ms.reserve_level = 'Low' THEN 4
                    WHEN ms.reserve_level = 'Depleted' THEN 5
                    ELSE 6
                END,
                rs.sell_price DESC NULLS LAST,
                s.distance ASC'''
        else:
            query += ' ORDER BY rs.sell_price DESC NULLS LAST, s.distance ASC'
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Process results
        processed_results = []
        current_system = None
        
        # First, collect all system_id64 and station_name pairs
        station_pairs = [(row['system_id64'], row['station_name']) 
                        for row in rows if row['station_name']]
        
        # Get all other commodities in a single query
        other_commodities = {}
        if station_pairs:
            other_cursor = conn.cursor()
            placeholders = ','.join(['(?,?)' for _ in station_pairs])
            params = [item for pair in station_pairs for item in pair]
            
            # Get the selected materials from the request
            selected_materials = request.args.getlist('selected_materials[]', type=str)
            
            if selected_materials and selected_materials != ['Default']:
                # Convert codes to full names using cached mapping
                full_names = [mining_data.MATERIAL_CODES.get(mat, mat) for mat in selected_materials]
                
                # Get all specified materials for each station
                other_cursor.execute(f'''
                    SELECT sc.system_id64, sc.station_name, sc.commodity_name, sc.sell_price, sc.demand,
                           COUNT(*) OVER (PARTITION BY sc.system_id64, sc.station_name) as total_commodities
                    FROM station_commodities sc
                    WHERE (sc.system_id64, sc.station_name) IN ({placeholders})
                    AND sc.commodity_name IN ({','.join('?' for _ in full_names)})
                    AND sc.sell_price > 0 AND sc.demand > 0
                    ORDER BY sc.system_id64, sc.station_name, sc.sell_price DESC
                ''', params + full_names)
                
                # Process results - store all materials for each station
                for row in other_cursor.fetchall():
                    key = (row['system_id64'], row['station_name'])
                    if key not in other_commodities:
                        other_commodities[key] = []
                    # Always append the material since it's one of our selected ones
                    other_commodities[key].append({
                        'name': row['commodity_name'],
                        'sell_price': row['sell_price'],
                        'demand': row['demand']
                    })
                    
                    # Debug log to verify we're getting all materials
                    if row['total_commodities'] > 1:
                        app.logger.info(f"Station {row['station_name']} has {row['total_commodities']} selected commodities")
            else:
                # Default behavior - just get top 6 by price
                other_cursor.execute(f'''
                    SELECT system_id64, station_name, commodity_name, sell_price, demand
                    FROM station_commodities
                    WHERE (system_id64, station_name) IN ({placeholders})
                    AND sell_price > 0 AND demand > 0
                    ORDER BY sell_price DESC
                ''', params)
                
                for row in other_cursor.fetchall():
                    key = (row['system_id64'], row['station_name'])
                    if key not in other_commodities:
                        other_commodities[key] = []
                    if len(other_commodities[key]) < 6:  # Limit to 6 commodities per station
                        other_commodities[key].append({
                            'name': row['commodity_name'],
                            'sell_price': row['sell_price'],
                            'demand': row['demand']
                        })
            
            other_cursor.close()
        
        for row in rows:
            if current_system is None or current_system['name'] != row['system_name']:
                if current_system is not None:
                    processed_results.append(current_system)
                
                current_system = {
                    'name': row['system_name'],
                    'controlling_power': row['controlling_power'],
                    'power_state': row['power_state'],
                    'distance': float(row['distance']),
                    'system_id64': row['system_id64'],
                    'rings': [],
                    'stations': [],
                    'all_signals': []
                }
            
            # Add ring if not already present
            if is_ring_material:
                ring_entry = {
                    'name': row['ring_name'],
                    'body_name': row['body_name'],
                    'signals': f"{signal_type} ({row['ring_type']}, {row['reserve_level']})"
                }
                if ring_entry not in current_system['rings']:
                    current_system['rings'].append(ring_entry)
            else:
                if ring_type_filter == 'Without Hotspots':
                    # For Without Hotspots, just show the ring type and reserve level
                    ring_entry = {
                        'name': row['ring_name'],
                        'body_name': row['body_name'],
                        'signals': f"{signal_type} ({row['ring_type']}, {row['reserve_level']})"
                    }
                    if ring_entry not in current_system['rings']:
                        current_system['rings'].append(ring_entry)
                else:
                    # For other filters, show hotspot signals
                    if row['mineral_type'] == signal_type:
                        ring_entry = {
                            'name': row['ring_name'],
                            'body_name': row['body_name'],
                            'signals': f"{signal_type}: {row['signal_count'] or ''} ({row['reserve_level']})"
                        }
                        if ring_entry not in current_system['rings']:
                            current_system['rings'].append(ring_entry)
                
            # Add to all_signals if not already present
            signal_entry = {
                'ring_name': row['ring_name'],
                'mineral_type': row['mineral_type'],
                'signal_count': row['signal_count'] or '',
                'reserve_level': row['reserve_level'],
                'ring_type': row['ring_type']
            }
            if signal_entry not in current_system['all_signals'] and signal_entry['mineral_type'] is not None:
                current_system['all_signals'].append(signal_entry)
            
            # Add station if present and not already added
            if row['station_name']:
                try:
                    # Get or create station entry
                    existing_station = next((s for s in current_system['stations'] if s['name'] == row['station_name']), None)
                    if existing_station:
                        # Update existing station's commodities
                        existing_station['other_commodities'] = other_commodities.get((row['system_id64'], row['station_name']), [])
                    else:
                        # Create new station entry
                        station_entry = {
                            'name': row['station_name'],
                            'pad_size': row['landing_pad_size'],
                            'distance': float(row['station_distance']) if row['station_distance'] else 0,
                            'demand': int(row['demand']) if row['demand'] else 0,
                            'sell_price': int(row['sell_price']) if row['sell_price'] else 0,
                            'station_type': row['station_type'],
                            'update_time': row.get('update_time'),
                            'system_id64': row['system_id64'],
                            'other_commodities': other_commodities.get((row['system_id64'], row['station_name']), [])
                        }
                        current_system['stations'].append(station_entry)
                except (TypeError, ValueError) as e:
                    app.logger.error(f"Error processing station data: {str(e)}")
                    continue
        
        if current_system is not None:
            processed_results.append(current_system)
        
        # Limit results before returning
        processed_results = processed_results[:limit]
        
        # After processing the main results, get all other signals for these systems
        if not is_non_hotspot and processed_results:
            system_ids = [system['system_id64'] for system in processed_results]
            placeholders = ','.join(['?' for _ in system_ids])
            
            # Get all signals for these systems
            cursor.execute(f'''
                SELECT system_id64, ring_name, mineral_type, signal_count, reserve_level, ring_type
                FROM mineral_signals
                WHERE system_id64 IN ({placeholders})
                AND mineral_type != ?
            ''', system_ids + [signal_type])
            
            # Group signals by system
            other_signals = {}
            for row in cursor.fetchall():
                if row['system_id64'] not in other_signals:
                    other_signals[row['system_id64']] = []
                other_signals[row['system_id64']].append({
                    'ring_name': row['ring_name'],
                    'mineral_type': row['mineral_type'],
                    'signal_count': row['signal_count'] or '',
                    'reserve_level': row['reserve_level'],
                    'ring_type': row['ring_type']
                })
            
            # Add other signals to the results
            for system in processed_results:
                system['all_signals'].extend(other_signals.get(system['system_id64'], []))
        
        conn.close()
        return jsonify(processed_results)
        
    except Exception as e:
        app.logger.error(f"Search error: {str(e)}")
        return jsonify({'error': f'Search error: {str(e)}'}), 500

@app.route('/search_highest')
def search_highest():
    """Handle the highest price search functionality."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get search parameters
        controlling_power = request.args.get('controlling_power')
        power_states = request.args.getlist('power_state[]')
        limit = int(request.args.get('limit', '30'))
        
        # Build power state filter
        power_state_filter = ''
        power_filter_params = []
        
        if controlling_power:
            power_state_filter += ' AND s.controlling_power = ?'
            power_filter_params.append(controlling_power)
        
        if power_states:
            placeholders = ','.join(['?' for _ in power_states])
            power_state_filter += f' AND s.power_state IN ({placeholders})'
            power_filter_params.extend(power_states)
        
        # Get the list of non-hotspot materials
        non_hotspot_materials = get_non_hotspot_materials_list()
        non_hotspot_str = ', '.join(f"'{material}'" for material in non_hotspot_materials)
        
        # Build the ring type case statement
        ring_type_cases = []
        for material, ring_types in NON_HOTSPOT_MATERIALS.items():
            ring_types_str = "', '".join(ring_types)
            ring_type_cases.append(f"WHEN hp.commodity_name = '{material}' AND ms.ring_type IN ('{ring_types_str}') THEN 1")
        
        ring_type_case = '\n'.join(ring_type_cases)
        
        query = '''
        WITH HighestPrices AS (
            -- First get all prices ordered by highest first
            SELECT DISTINCT 
                sc.commodity_name,
                sc.sell_price,
                sc.demand,
                s.id64 as system_id64,
                s.name as system_name,
                s.controlling_power,
                s.power_state,
                st.landing_pad_size,
                st.distance_to_arrival,
                st.station_type,
                sc.station_name,
                st.update_time
            FROM station_commodities sc
            JOIN systems s ON s.id64 = sc.system_id64
            JOIN stations st ON st.system_id64 = s.id64 AND st.station_name = sc.station_name
            WHERE sc.demand > 0
            AND sc.sell_price > 0''' + power_state_filter + '''
            ORDER BY sc.sell_price DESC
            LIMIT 1000
        ),
        MinableCheck AS (
            -- Then check each system if the material can be mined there
            SELECT DISTINCT
                hp.*,
                ms.mineral_type,
                ms.ring_type,
                ms.reserve_level,
                CASE
                    -- For hotspot materials
                    WHEN hp.commodity_name NOT IN (''' + non_hotspot_str + ''')
                        AND ms.mineral_type = hp.commodity_name THEN 1
                    -- For Low Temperature Diamonds
                    WHEN hp.commodity_name = 'Low Temperature Diamonds' 
                        AND ms.mineral_type = 'LowTemperatureDiamond' THEN 1
                    -- For non-hotspot materials
                    ''' + ring_type_case + '''
                    ELSE 0
                END as is_minable
            FROM HighestPrices hp
            JOIN mineral_signals ms ON hp.system_id64 = ms.system_id64
        )
        SELECT DISTINCT
            commodity_name,
            sell_price as max_price,
            system_name,
            controlling_power,
            power_state,
            landing_pad_size,
            distance_to_arrival,
            demand,
            reserve_level,
            station_name,
            station_type,
            update_time
        FROM MinableCheck
        WHERE is_minable = 1  -- Only include systems where the material can be mined
        ORDER BY max_price DESC
        LIMIT ?
        '''
        
        power_filter_params.append(limit)
        cursor.execute(query, power_filter_params)
        results = cursor.fetchall()
        
        conn.close()
        return jsonify(results)
    
    except Exception as e:
        app.logger.error(f"Search highest error: {str(e)}")
        return jsonify({'error': f'Search error: {str(e)}'}), 500

@app.route('/get_price_comparison', methods=['POST'])
def get_price_comparison_endpoint():
    """Handle price comparison requests."""
    try:
        data = request.json
        items = data.get('items', [])
        use_max = data.get('use_max', False)
        
        if not items:
            return jsonify([])
            
        results = []
        for item in items:
            price = int(item.get('price', 0))
            commodity = item.get('commodity')
            
            if not commodity:
                results.append({'color': None, 'indicator': ''})
                continue
                
            # Always normalize the commodity name first
            normalized_commodity = normalize_commodity_name(commodity)
            
            if normalized_commodity not in PRICE_DATA:
                # If normalization didn't work, try the original name
                if commodity in PRICE_DATA:
                    normalized_commodity = commodity
                else:
                    results.append({'color': None, 'indicator': ''})
                    continue
            
            reference_price = int(PRICE_DATA[normalized_commodity]['max_price' if use_max else 'avg_price'])
            color, indicator = get_price_comparison(price, reference_price)
            
            results.append({
                'color': color,
                'indicator': indicator
            })
        
        return jsonify(results)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/search_res_hotspots', methods=['POST'])
def search_res_hotspots():
    """Handle RES hotspot search functionality."""
    try:
        # Get reference system and database
        ref_system = request.args.get('system', 'Sol')
        database = request.json.get('database', 'systems.db')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        cursor.row_factory = res_data.dict_factory
        
        # Get reference system coordinates
        cursor.execute('SELECT x, y, z FROM systems WHERE name = ?', (ref_system,))
        ref_coords = cursor.fetchone()
        if not ref_coords:
            conn.close()
            return jsonify({'error': 'Reference system not found'}), 404
        
        ref_x, ref_y, ref_z = ref_coords['x'], ref_coords['y'], ref_coords['z']
        
        # Load RES hotspot data with database path
        hotspot_data = res_data.load_res_data(database)
        
        # Process each system
        results = []
        for entry in hotspot_data:
            # Get system info from database
            cursor.execute('''
                SELECT s.*, 
                    sqrt(((s.x - ?) * (s.x - ?)) + 
                         ((s.y - ?) * (s.y - ?)) + 
                         ((s.z - ?) * (s.z - ?))) as distance
                FROM systems s
                WHERE s.name = ?
            ''', (ref_x, ref_x, ref_y, ref_y, ref_z, ref_z, entry['system']))
            
            system = cursor.fetchone()
            if not system:
                continue
                
            # Get station data
            stations = res_data.get_station_commodities(conn, system['id64'])
            
            results.append({
                'system': entry['system'],
                'power': system['controlling_power'] or 'None',
                'distance': float(system['distance']),
                'ring': entry['ring'],
                'ls': entry['ls'],
                'res_zone': entry['res_zone'],
                'comment': entry['comment'],
                'stations': stations
            })
        
        conn.close()
        return jsonify(results)
    
    except Exception as e:
        app.logger.error(f"RES hotspot search error: {str(e)}")
        return jsonify({'error': f'Search error: {str(e)}'}), 500

@app.route('/search_high_yield_platinum', methods=['POST'])
def search_high_yield_platinum():
    try:
        # Get reference system and database
        ref_system = request.args.get('system', 'Sol')
        database = request.json.get('database', 'systems.db')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        cursor.row_factory = res_data.dict_factory
        
        # Get reference system coordinates
        cursor.execute('SELECT x, y, z FROM systems WHERE name = ?', (ref_system,))
        ref_coords = cursor.fetchone()
        if not ref_coords:
            conn.close()
            return jsonify({'error': 'Reference system not found'}), 404
        
        ref_x, ref_y, ref_z = ref_coords['x'], ref_coords['y'], ref_coords['z']
        
        # Load high yield platinum data
        data = res_data.load_high_yield_platinum()
        
        # Process each system
        results = []
        for entry in data:
            # Get system info from database
            cursor.execute('''
                SELECT s.*, 
                    sqrt(((s.x - ?) * (s.x - ?)) + 
                         ((s.y - ?) * (s.y - ?)) + 
                         ((s.z - ?) * (s.z - ?))) as distance
                FROM systems s
                WHERE s.name = ?
            ''', (ref_x, ref_x, ref_y, ref_y, ref_z, ref_z, entry['system']))
            
            system = cursor.fetchone()
            if not system:
                continue
                
            # Get station data
            stations = res_data.get_station_commodities(conn, system['id64'])
            
            results.append({
                'system': entry['system'],
                'power': system['controlling_power'] or 'None',
                'distance': float(system['distance']),
                'ring': entry['ring'],
                'percentage': entry['percentage'],  # Include the percentage from CSV
                'comment': entry['comment'],
                'stations': stations
            })
        
        conn.close()
        return jsonify(results)
    except Exception as e:
        app.logger.error(f"High yield platinum search error: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000) 