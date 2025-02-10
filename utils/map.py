from flask import Blueprint, jsonify, request, current_app
import psycopg2
from math import floor
import os
import json
import time
from utils.common import get_db_connection

map_bp = Blueprint('map', __name__)

class MapController:
    def __init__(self):
        self.chunk_size = 100  # Size of each spatial chunk
        self.cache_duration = 3600  # 1 hour in seconds
        self.systems_cache = None
        self.last_cache_update = 0
        self._cache_dir = None
        self._cache_file = None

    @property
    def cache_dir(self):
        if self._cache_dir is None:
            # Lazy initialization of cache directory
            try:
                self._cache_dir = os.path.join(os.getenv('CACHE_DIR', '/tmp'), 'power-mining-cache')
                os.makedirs(self._cache_dir, exist_ok=True)
                os.chmod(self._cache_dir, 0o777)
            except Exception as e:
                print(f"Failed to create cache directory: {str(e)}")
                self._cache_dir = '/tmp'
        return self._cache_dir

    @property
    def cache_file(self):
        if self._cache_file is None:
            self._cache_file = os.path.join(self.cache_dir, "systems_cache.json")
        return self._cache_file

    def update_cache_if_needed(self):
        """Update the cache file if it's expired or doesn't exist"""
        current_time = time.time()
        
        # Check if we need to update cache
        if (self.systems_cache is None or 
            current_time - self.last_cache_update > self.cache_duration or 
            not os.path.exists(self.cache_file)):
            
            try:
                conn = get_db_connection()
                if not conn:
                    raise ValueError("Could not connect to database")
                cur = conn.cursor()
                
                # Get all systems with minimal required data
                cur.execute("""
                    SELECT 
                        name,
                        x, y, z,
                        controlling_power,
                        power_state,
                        powers_acquiring
                    FROM systems
                    ORDER BY name
                """)
                
                systems = []
                for row in cur.fetchall():
                    systems.append({
                        'name': row[0],
                        'x': float(row[1]),
                        'y': float(row[2]),
                        'z': float(row[3]),
                        'controlling_power': row[4],
                        'power_state': row[5],
                        'powers_acquiring': row[6] if row[6] else []
                    })
                
                # Save to cache file
                with open(self.cache_file, 'w') as f:
                    json.dump(systems, f)
                
                self.systems_cache = systems
                self.last_cache_update = current_time
                
                cur.close()
                conn.close()
                
            except Exception as e:
                if 'conn' in locals() and conn:
                    conn.close()
                raise

    def get_chunk_key(self, x, y, z):
        """Convert coordinates to chunk identifier"""
        return (
            floor(x / self.chunk_size),
            floor(y / self.chunk_size),
            floor(z / self.chunk_size)
        )
        
    def get_systems_in_chunks(self, center_chunk, radius=1):
        """Get systems from chunks around the specified center chunk"""
        try:
            self.update_cache_if_needed()
            
            # Calculate chunk boundaries
            cx, cy, cz = center_chunk
            min_x = (cx - radius) * self.chunk_size
            max_x = (cx + radius + 1) * self.chunk_size
            min_y = (cy - radius) * self.chunk_size
            max_y = (cy + radius + 1) * self.chunk_size
            min_z = (cz - radius) * self.chunk_size
            max_z = (cz + radius + 1) * self.chunk_size
            
            # Filter systems from cache
            systems = [
                system for system in self.systems_cache
                if min_x <= system['x'] <= max_x and
                   min_y <= system['y'] <= max_y and
                   min_z <= system['z'] <= max_z
            ]
            
            return systems
            
        except Exception as e:
            raise

    def get_initial_systems(self, x=0, y=0, z=0):
        """Get systems around a center point for initial load"""
        chunk = self.get_chunk_key(x, y, z)
        return self.get_systems_in_chunks(chunk, radius=1)

    def get_all_controlled_systems(self):
        """Get all controlled systems with their coordinates and controlling power"""
        try:
            self.update_cache_if_needed()
            
            # Filter controlled systems from cache
            systems = [
                {
                    'name': system['name'],
                    'x': system['x'],
                    'y': system['y'],
                    'z': system['z'],
                    'power': system['controlling_power']
                }
                for system in self.systems_cache
                if system['controlling_power'] is not None
            ]
            
            return systems
            
        except Exception as e:
            return []

    def parse_chunk_param(self, chunk_param):
        """Parse chunk parameter from string 'x,y,z' to tuple (x,y,z)"""
        try:
            x, y, z = map(int, chunk_param.split(','))
            return (x, y, z)
        except:
            return (0, 0, 0)  # Default to Sol area if invalid

# Create blueprint routes
controller = None

@map_bp.record_once
def record_params(setup_state):
    global controller
    try:
        controller = MapController()
    except Exception as e:
        print(f"Failed to initialize MapController: {str(e)}")
        raise

@map_bp.route('/api/systems')
def get_systems():
    """Get systems for the current view"""
    try:
        if not controller:
            return jsonify({'error': 'Controller not initialized'}), 500

        # Get chunk from query parameters
        chunk_param = request.args.get('chunk', '0,0,0')
        chunk = controller.parse_chunk_param(chunk_param)
        
        try:
            # Get systems for the requested chunk
            systems = controller.get_systems_in_chunks(chunk, radius=1)
            if not isinstance(systems, list):
                systems = []
            return jsonify(systems)
        except Exception as e:
            return jsonify({'error': f'Database error: {str(e)}'}), 500
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@map_bp.route('/api/controlled_systems')
def get_controlled_systems():
    """Get all controlled systems endpoint"""
    try:
        if not controller:
            return jsonify({'error': 'Controller not initialized'}), 500
        systems = controller.get_all_controlled_systems()
        return jsonify(systems)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@map_bp.route('/api/system/<path:system_identifier>')
def get_system_by_identifier(system_identifier):
    """Get detailed system information by name or id64"""
    try:
        if not controller:
            return jsonify({'error': 'Controller not initialized'}), 500

        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cur = conn.cursor()
        
        # Try to parse as id64 first
        try:
            id64 = int(system_identifier)
            where_clause = "s.id64 = %s"
            print(f"Using id64 where clause: {where_clause} with value {id64}")
        except ValueError:
            # If not a number, treat as system name (case-insensitive)
            where_clause = "LOWER(s.name) = LOWER(%s)"
            print(f"Using name where clause: {where_clause} with value {system_identifier}")
        
        # Get system info with stations and commodities
        cur.execute(f"""
            WITH system_stations AS (
                SELECT 
                    st.*,
                    json_agg(
                        json_build_object(
                            'name', sc.commodity_name,
                            'demand', sc.demand,
                            'sellPrice', sc.sell_price
                        )
                    ) FILTER (WHERE sc.commodity_name IS NOT NULL) as commodities
                FROM systems s
                LEFT JOIN stations st ON s.id64 = st.system_id64
                LEFT JOIN station_commodities sc 
                    ON st.system_id64 = sc.system_id64 
                    AND st.station_id = sc.station_id
                WHERE {where_clause}
                GROUP BY st.system_id64, st.station_id, st.station_name, st.station_type, st.landing_pad_size, 
                         st.distance_to_arrival, st.update_time, st.primary_economy, st.body
            ), mineral_signals AS (
                SELECT 
                    ms.system_id64,
                    ms.body_name,
                    ms.ring_name,
                    ms.ring_type,
                    ms.mineral_type,
                    ms.signal_count,
                    ms.reserve_level
                FROM mineral_signals ms
                JOIN systems s ON s.id64 = ms.system_id64
                WHERE {where_clause}
                ORDER BY ms.ring_name, ms.mineral_type
            )
            SELECT 
                s.id64,
                s.name,
                s.x,
                s.y,
                s.z,
                s.controlling_power,
                s.power_state,
                s.powers_acquiring,
                s.distance_from_sol,
                COALESCE(NULLIF(s.system_state, 'NULL'), 'None') as system_state,
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object(
                        'name', ss.station_name,
                        'id', ss.station_id,
                        'updateTime', ss.update_time,
                        'distanceToArrival', ss.distance_to_arrival,
                        'primaryEconomy', ss.primary_economy,
                        'body', ss.body,
                        'type', ss.station_type,
                        'landingPads', ss.landing_pad_size,
                        'market', jsonb_build_object(
                            'commodities', ss.commodities
                        )
                    )) FILTER (WHERE ss.station_name IS NOT NULL),
                    '[]'::json
                ) as stations,
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object(
                        'body_name', ms.body_name,
                        'ring_name', ms.ring_name,
                        'ring_type', ms.ring_type,
                        'mineral_type', ms.mineral_type,
                        'signal_count', ms.signal_count,
                        'reserve_level', ms.reserve_level
                    )) FILTER (WHERE ms.ring_name IS NOT NULL AND ms.system_id64 = s.id64),
                    '[]'::json
                ) as mineral_signals
            FROM systems s
            LEFT JOIN system_stations ss ON s.id64 = ss.system_id64
            LEFT JOIN mineral_signals ms ON s.id64 = ms.system_id64
            WHERE {where_clause}
            GROUP BY s.id64, s.name, s.x, s.y, s.z, s.controlling_power, s.power_state, s.powers_acquiring, s.distance_from_sol, s.system_state
        """, (system_identifier, system_identifier, system_identifier))
        
        result = cur.fetchone()
        if not result:
            cur.close()
            conn.close()
            return jsonify({'error': 'System not found'}), 404
            
        # Format response according to API format
        response = {
            'id64': result[0],
            'name': result[1],
            'coords': {
                'x': float(result[2]),
                'y': float(result[3]),
                'z': float(result[4])
            },
            'controllingPower': result[5],
            'powerState': result[6],
            'powers': result[7] if result[7] else [],
            'distanceFromSol': float(result[8]) if result[8] else None,
            'systemState': result[9],
            'stations': result[10] if result[10] else [],
            'mineralSignals': result[11] if result[11] else []
        }
        
        cur.close()
        conn.close()
        return jsonify(response)
        
    except Exception as e:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
        print(f"Error in get_system_by_identifier: {str(e)}")
        print(f"System identifier: {system_identifier}")
        import traceback
        print("Full traceback:")
        traceback.print_exc()
        return jsonify({'error': f"Failed to fetch system data: {str(e)}"}), 500

@map_bp.route('/api/systems/search', methods=['GET'])
def search_systems():
    """Flexible system search endpoint supporting multiple search methods"""
    try:
        if not controller:
            return jsonify({'error': 'Controller not initialized'}), 500

        # Get search parameters
        names = request.args.getlist('name')
        id64s = request.args.getlist('id64')
        radius = request.args.get('radius', type=float)
        ref_x = request.args.get('x', type=float)
        ref_y = request.args.get('y', type=float)
        ref_z = request.args.get('z', type=float)
        cube_x1 = request.args.get('x1', type=float)
        cube_y1 = request.args.get('y1', type=float)
        cube_z1 = request.args.get('z1', type=float)
        cube_x2 = request.args.get('x2', type=float)
        cube_y2 = request.args.get('y2', type=float)
        cube_z2 = request.args.get('z2', type=float)

        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cur = conn.cursor()
        
        # Build the WHERE clause based on search parameters
        where_clauses = []
        params = []
        
        if names:
            where_clauses.append("LOWER(s.name) = ANY(ARRAY[" + ",".join(["%s"] * len(names)) + "])")
            params.extend([name.lower() for name in names])
            
        if id64s:
            where_clauses.append("s.id64 = ANY(ARRAY[" + ",".join(["%s"] * len(id64s)) + "])")
            params.extend([int(id64) for id64 in id64s])
            
        if radius and all(x is not None for x in [ref_x, ref_y, ref_z]):
            where_clauses.append("""
                POWER(s.x - %s, 2) + 
                POWER(s.y - %s, 2) + 
                POWER(s.z - %s, 2) <= POWER(%s, 2)
            """)
            params.extend([ref_x, ref_y, ref_z, radius])
            
        if all(x is not None for x in [cube_x1, cube_y1, cube_z1, cube_x2, cube_y2, cube_z2]):
            where_clauses.append("""
                s.x BETWEEN %s AND %s AND
                s.y BETWEEN %s AND %s AND
                s.z BETWEEN %s AND %s
            """)
            params.extend([cube_x1, cube_x2, cube_y1, cube_y2, cube_z1, cube_z2])
            
        if not where_clauses:
            return jsonify({'error': 'No valid search parameters provided'}), 400
            
        where_clause = " OR ".join(f"({clause})" for clause in where_clauses)
        
        # Execute the same query as get_system_by_identifier but with different WHERE clause
        query = f"""
            WITH system_stations AS (
                SELECT 
                    st.*,
                    json_agg(
                        json_build_object(
                            'name', sc.commodity_name,
                            'demand', sc.demand,
                            'sellPrice', sc.sell_price
                        )
                    ) FILTER (WHERE sc.commodity_name IS NOT NULL) as commodities
                FROM systems s
                LEFT JOIN stations st ON s.id64 = st.system_id64
                LEFT JOIN station_commodities sc 
                    ON st.system_id64 = sc.system_id64 
                    AND st.station_id = sc.station_id
                WHERE {where_clause}
                GROUP BY st.system_id64, st.station_id, st.station_name, st.station_type, st.landing_pad_size, 
                         st.distance_to_arrival, st.update_time, st.primary_economy, st.body
            ), mineral_signals AS (
                SELECT 
                    ms.system_id64,
                    ms.body_name,
                    ms.ring_name,
                    ms.ring_type,
                    ms.mineral_type,
                    ms.signal_count,
                    ms.reserve_level
                FROM mineral_signals ms
                JOIN systems s ON s.id64 = ms.system_id64
                WHERE {where_clause}
                ORDER BY ms.ring_name, ms.mineral_type
            )
            SELECT 
                s.id64,
                s.name,
                s.x,
                s.y,
                s.z,
                s.controlling_power,
                s.power_state,
                s.powers_acquiring,
                s.distance_from_sol,
                COALESCE(NULLIF(s.system_state, 'NULL'), 'None') as system_state,
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object(
                        'name', ss.station_name,
                        'id', ss.station_id,
                        'updateTime', ss.update_time,
                        'distanceToArrival', ss.distance_to_arrival,
                        'primaryEconomy', ss.primary_economy,
                        'body', ss.body,
                        'type', ss.station_type,
                        'landingPads', ss.landing_pad_size,
                        'market', jsonb_build_object(
                            'commodities', ss.commodities
                        )
                    )) FILTER (WHERE ss.station_name IS NOT NULL),
                    '[]'::json
                ) as stations,
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object(
                        'body_name', ms.body_name,
                        'ring_name', ms.ring_name,
                        'ring_type', ms.ring_type,
                        'mineral_type', ms.mineral_type,
                        'signal_count', ms.signal_count,
                        'reserve_level', ms.reserve_level
                    )) FILTER (WHERE ms.ring_name IS NOT NULL AND ms.system_id64 = s.id64),
                    '[]'::json
                ) as mineral_signals
            FROM systems s
            LEFT JOIN system_stations ss ON s.id64 = ss.system_id64
            LEFT JOIN mineral_signals ms ON s.id64 = ms.system_id64
            WHERE {where_clause}
            GROUP BY s.id64, s.name, s.x, s.y, s.z, s.controlling_power, s.power_state, s.powers_acquiring, s.distance_from_sol, s.system_state
        """
        
        cur.execute(query, params + [params[-1]])
        results = cur.fetchall()
        
        # Format response
        response = []
        for result in results:
            system = {
                'id64': result[0],
                'name': result[1],
                'coords': {
                    'x': float(result[2]),
                    'y': float(result[3]),
                    'z': float(result[4])
                },
                'controllingPower': result[5],
                'powerState': result[6],
                'powers': result[7] if result[7] else [],
                'distanceFromSol': float(result[8]) if result[8] else None,
                'systemState': result[9],
                'stations': result[10] if result[10] else [],
                'mineralSignals': result[11] if result[11] else []
            }
            response.append(system)
            
        cur.close()
        conn.close()
        return jsonify(response)
        
    except Exception as e:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
        return jsonify({'error': str(e)}), 500
