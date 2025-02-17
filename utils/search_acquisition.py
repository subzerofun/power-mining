import json
import os
from datetime import datetime
from flask import jsonify, request
"""from utils.mining_data import get_price_comparison, PRICE_DATA, normalize_commodity_name"""
from utils.common import log_message, get_db_connection, YELLOW, RED, BLUE
from utils import res_data
from utils.analytics import track_search  # Add analytics import

# Constants
SYSTEM_IN_RING_NAME = False

def search(display_format='full'):
    """Search for acquisition targets
    Args:
        display_format (str): 'full' for detailed view or 'highest' for highest prices view
    """
    try:
        # 1) Gather request parameters
        ref_system = request.args.get('system', 'Sol')
        max_dist = float(request.args.get('distance', '10000'))
        controlling_power = request.args.get('controlling_power')
        power_goal = request.args.get('power_goal', 'Acquire')
        signal_type = request.args.get('signal_type')
        landing_pad_size = request.args.get('landingPadSize', 'L')  # Get landing pad size
        
        # Get system states and handle 'Any' case properly
        system_states = request.args.getlist('system_state[]', type=str)
        if not system_states:
            system_states = ["Any"]
        
        # Debug log raw request args
        log_message(BLUE, "SEARCH", f"Raw request args: {dict(request.args)}")
        log_message(BLUE, "SEARCH", f"System states from request: {system_states}")
        
        # Handle default signal_type
        if not signal_type or signal_type == 'Any':
            signal_type = 'Monazite'  # Default to a common mining material
            
        ring_type_filter = request.args.get('ring_type_filter', 'Hotspots')
        limit = int(request.args.get('limit', '30'))
        mining_types = request.args.getlist('mining_types[]')
        min_demand = int(request.args.get('minDemand', '0'))
        max_demand = int(request.args.get('maxDemand', '0'))
        sel_mats = request.args.getlist('selected_materials[]', type=str)

        # Prepare search parameters for tracking
        search_params = {
            'ref_system': ref_system,
            'max_dist': max_dist,
            'controlling_power': controlling_power,
            'power_goal': power_goal,
            'signal_type': signal_type,
            'landing_pad_size': landing_pad_size,
            'system_states': system_states,
            'ring_type_filter': ring_type_filter,
            'limit': limit,
            'mining_types': mining_types,
            'min_demand': min_demand,
            'max_demand': max_demand,
            'sel_mats': sel_mats,
            'display_format': display_format
        }
        
        # Track search parameters in GA (non-blocking)
        track_search(search_params)

        # Logging
        log_message(BLUE, "SEARCH", "Search parameters:")
        log_message(BLUE, "SEARCH", f"- System: {ref_system}")
        log_message(BLUE, "SEARCH", f"- Distance: {max_dist}")
        log_message(BLUE, "SEARCH", f"- Power: {controlling_power}")
        log_message(BLUE, "SEARCH", f"- Signal type: {signal_type}")
        log_message(BLUE, "SEARCH", f"- Ring type filter: {ring_type_filter}")
        log_message(BLUE, "SEARCH", f"- Mining types: {mining_types}")
        log_message(BLUE, "SEARCH", f"- Min demand: {min_demand}")
        log_message(BLUE, "SEARCH", f"- Max demand: {max_demand}")
        log_message(BLUE, "SEARCH", f"- Selected materials: {sel_mats}")
        log_message(BLUE, "SEARCH", f"- System states: {system_states}")
        log_message(BLUE, "SEARCH", f"- Landing pad size: {landing_pad_size}")

        # 2) Database connection
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        c = conn.cursor()

        # 3) Reference system coordinates
        c.execute("SELECT x, y, z FROM systems WHERE name ILIKE %s", (ref_system,))
        ref_coords = c.fetchone()
        if not ref_coords:
            conn.close()
            return jsonify({'error': 'Reference system not found'}), 404

        rx, ry, rz = ref_coords['x'], ref_coords['y'], ref_coords['z']

        # 4) Load the material from mining_materials.json
        with open('data/mining_materials.json', 'r') as f:
            mat_data = json.load(f)['materials']
            # Convert LowTemperatureDiamond => Low Temperature Diamonds
            material_name = 'Low Temperature Diamonds' if signal_type == 'LowTemperatureDiamond' else signal_type
            material = mat_data.get(material_name)
            if not material:
                log_message(RED, "SEARCH", f"Material {material_name} not found in mining_materials.json")
                return jsonify([])
            log_message(BLUE, "SEARCH", f"Material data: {material}")

        # 5) Determine valid ring types from mining_types
        valid_ring_types = []
        if mining_types:
            log_message(BLUE, "MINING", f"Processing mining types: {mining_types}")
            log_message(BLUE, "MINING", f"Ring type filter: {ring_type_filter}")
            for ring_type, data in material['ring_types'].items():
                log_message(BLUE, "MINING", f"Checking ring type {ring_type} with data: {data}")
                if 'All' in mining_types:
                    if any([
                        data.get('surfaceLaserMining', False),
                        data.get('surfaceDeposit', False),
                        data.get('subSurfaceDeposit', False),
                        data.get('core', False)
                    ]):
                        valid_ring_types.append(ring_type)
                        log_message(BLUE, "MINING", f"Added {ring_type} to valid ring types (All mining types)")
                else:
                    # Check if this ring type supports ALL selected mining methods
                    supports_all = True
                    for mtype in mining_types:
                        if mtype.lower() == 'laser surface' and not data.get('surfaceLaserMining', False):
                            supports_all = False
                            log_message(YELLOW, "MINING", f"{ring_type} does not support laser surface mining")
                            break
                        elif mtype.lower() == 'surface' and not data.get('surfaceDeposit', False):
                            supports_all = False
                            log_message(YELLOW, "MINING", f"{ring_type} does not support surface mining")
                            break
                        elif mtype.lower() == 'subsurface' and not data.get('subSurfaceDeposit', False):
                            supports_all = False
                            log_message(YELLOW, "MINING", f"{ring_type} does not support subsurface mining")
                            break
                        elif mtype.lower() == 'core' and not data.get('core', False):
                            supports_all = False
                            log_message(YELLOW, "MINING", f"{ring_type} does not support core mining")
                            break
                    if supports_all:
                        valid_ring_types.append(ring_type)
                        log_message(BLUE, "MINING", f"Added {ring_type} to valid ring types (supports all selected mining types)")

            log_message(BLUE, "MINING", f"Final valid ring types: {valid_ring_types}")
            if not valid_ring_types:
                log_message(RED, "MINING", "No valid ring types found.")
                return jsonify([])

        # 6) Filter ring_type_filter if not Hotspots/Without Hotspots/All
        where_conditions = []
        where_params = []

        if ring_type_filter not in ['Hotspots', 'Without Hotspots', 'All']:
            if ring_type_filter not in valid_ring_types:
                log_message(RED, "MINING", f"Ring type {ring_type_filter} invalid.")
                return jsonify([])
            valid_ring_types = [ring_type_filter]
            # Add explicit ring type filter to where conditions
            where_conditions.append("ms.ring_type = %s")
            where_params.append(ring_type_filter)

        # 7) Possibly filter by reserve_level
        reserve_level = request.args.get('reserve_level', 'All')
        log_message(BLUE, "SEARCH", f"Reserve level filter: {reserve_level}")

        if reserve_level != 'All':
            where_conditions.append("ms.reserve_level = %s")
            where_params.append(reserve_level)

        # 8) Build the ring/hotspot join_condition + join_params
        join_condition = "s.id64 = ms.system_id64"
        join_params = []
        if ring_type_filter == 'Hotspots':
            # ms.mineral_type = %s AND ms.ring_type = ANY(%s::text[])
            join_condition += " AND ms.mineral_type = %s AND ms.ring_type = ANY(%s::text[])"
            join_params.extend([signal_type, valid_ring_types])
        elif ring_type_filter == 'Without Hotspots':
            # ms.mineral_type IS NULL AND ms.ring_type = ANY(%s::text[])
            join_condition += " AND ms.mineral_type IS NULL AND ms.ring_type = ANY(%s::text[])"
            join_params.append(valid_ring_types)
        else:
            # 'All': (ms.mineral_type = %s OR ms.mineral_type IS NULL) AND ms.ring_type = ANY(%s::text[])
            join_condition += " AND (ms.mineral_type = %s OR ms.mineral_type IS NULL) AND ms.ring_type = ANY(%s::text[])"
            join_params.extend([signal_type, valid_ring_types])

        # Debug logging for ring type filtering
        log_message(BLUE, "MINING", f"Join condition: {join_condition}")
        log_message(BLUE, "MINING", f"Join params: {join_params}")
        log_message(BLUE, "MINING", f"Valid ring types for mining: {valid_ring_types}")

        # 9) Build the main SQL query
        base_query = """
        WITH
        all_mining_signals AS (
          SELECT s.id64, s.name, ms.*
          FROM systems s
          JOIN mineral_signals ms ON s.id64 = ms.system_id64
          WHERE s.power_state IN ('Stronghold', 'Fortified')
          AND s.controlling_power = %s
        ),
        control_systems AS (
          SELECT 
            s.id64, s.name, s.x, s.y, s.z, 
            s.power_state, 
            s.controlling_power,
            s.powers_acquiring,
            ms.ring_type,
            ms.mineral_type,
            ms.reserve_level,
            ms.body_name,
            ms.ring_name,
            ms.signal_count
            FROM systems s
          JOIN mineral_signals ms ON {JOIN_CONDITION}
          WHERE 
            s.power_state IN ('Stronghold', 'Fortified')
            AND s.controlling_power = %s
            AND POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2) <= POWER(%s, 2)
            {EXTRA_WHERE}
        ),
        unoccupied_systems AS (
            SELECT DISTINCT ON (s.id64)
                s.id64, s.name, s.x, s.y, s.z,
                s.system_state,
                sc.station_name,
                sc.sell_price,
                sc.demand,
                st.landing_pad_size,
                st.distance_to_arrival,
                st.station_type,
                st.update_time,
                SQRT(POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2)) as ref_distance
            FROM systems s
            JOIN station_commodities sc ON s.id64 = sc.system_id64
            LEFT JOIN stations st ON s.id64 = st.system_id64 AND sc.station_name = st.station_name
            WHERE 
                s.controlling_power IS NULL
                AND POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2) <= POWER(%s, 2)
                AND sc.commodity_name = %s
                AND sc.sell_price > 0
                AND (
                    CASE 
                        WHEN %s = 0 AND %s = 0 THEN sc.demand = 0
                        WHEN %s = 0 THEN sc.demand <= %s
                        WHEN %s = 0 THEN sc.demand >= %s
                        ELSE sc.demand BETWEEN %s AND %s
                    END
                )
                {UNOCCUPIED_WHERE}
            ORDER BY s.id64, sc.sell_price DESC
        ),
        valid_pairs AS (
          SELECT 
            u.id64 as unoccupied_id64,
            u.name as unoccupied_name,
            u.ref_distance,
            u.station_name,
            u.sell_price,
            u.demand,
            u.landing_pad_size,
            u.distance_to_arrival as station_distance,
            u.station_type,
            u.update_time,
            u.system_state,
            c.id64 as mining_id64,
            c.name as mining_name,
            c.power_state,
            c.controlling_power,
            c.powers_acquiring,
            c.ring_type,
            c.mineral_type,
            c.reserve_level,
            c.body_name,
            c.ring_name,
            c.signal_count,
            SQRT(POWER(u.x - c.x, 2) + POWER(u.y - c.y, 2) + POWER(u.z - c.z, 2)) as control_distance
          FROM unoccupied_systems u
          CROSS JOIN control_systems c
          WHERE (
            (c.power_state = 'Stronghold' AND SQRT(POWER(u.x - c.x, 2) + POWER(u.y - c.y, 2) + POWER(u.z - c.z, 2)) <= 30)
            OR 
            (c.power_state = 'Fortified' AND SQRT(POWER(u.x - c.x, 2) + POWER(u.y - c.y, 2) + POWER(u.z - c.z, 2)) <= 20)
          )
        )
        SELECT 
            -- Unoccupied system
            unoccupied_name as system_name,
            'Unoccupied' as power_state,
            ref_distance as distance,
            station_name,
            landing_pad_size,
            station_distance as distance_to_arrival,
            station_type,
            demand,
            sell_price,
            update_time,
            unoccupied_id64 as system_id64,
            system_state,
            -- Mining system
            mining_name as mining_system_name,
            mining_id64,
            body_name,
            ring_name,
            ring_type,
            mineral_type,
            signal_count,
            reserve_level,
            power_state as mining_power_state,
            controlling_power,
            powers_acquiring,
            control_distance as mining_system_distance
        FROM valid_pairs
        WHERE landing_pad_size = %s OR %s = 'Any'
        ORDER BY 
            sell_price DESC,
            ref_distance ASC,
            CASE WHEN power_state = 'Stronghold' THEN 0 ELSE 1 END,
            control_distance ASC
        {LIMIT_CLAUSE}
        """

        # Add system state filter condition
        unoccupied_where = ""
        unoccupied_params = []  # Create new list for unoccupied system params
        if system_states and system_states != ["Any"]:
            log_message(BLUE, "SEARCH", f"Adding system state filter for states: {system_states}")
            unoccupied_where = "AND s.system_state = ANY(%s::text[])"
            unoccupied_params.append(system_states)
        else:
            log_message(BLUE, "SEARCH", "No system state filter applied (Any selected or no states provided)")

        # Replace placeholders for ring condition, extra where, limit, and unoccupied where
        query = base_query.replace(
            "{JOIN_CONDITION}", join_condition
        ).replace(
            "{EXTRA_WHERE}", ("AND " + " AND ".join(where_conditions)) if where_conditions else ""
        ).replace(
            "{UNOCCUPIED_WHERE}", unoccupied_where
        ).replace(
            "{LIMIT_CLAUSE}", "LIMIT %s" if limit else ""
        )

        # -- BUILD PARAMS IN THE CORRECT ORDER --
        # Step 1: all_mining_signals CTE params
        params = [
            controlling_power  # For all_mining_signals power filter
        ]

        # Step 2: Join params for ring/hotspots
        params.extend(join_params)

        # Step 3: Control systems CTE params
        params.extend([
            controlling_power,  # For control systems power filter
            rx, ry, rz,        # Control systems distance calc
            max_dist           # Control systems max distance
        ])

        # Step 3: Add any where_params for control systems
        params.extend(where_params)

        # Step 5: Unoccupied systems CTE params
        params.extend([
            rx, ry, rz,         # For ref_distance
            rx, ry, rz,         # For distance filter
            max_dist,           # For max_dist
            material['name'],   # For commodity_name
            min_demand, max_demand,  # For zero-zero check
            min_demand, max_demand,  # For min=0 check
            max_demand, min_demand,  # For max=0 check
            min_demand, max_demand   # For between check
        ])

        # Step 5.6: Add unoccupied system state filter params
        params.extend(unoccupied_params)

        # Step 5.9: Add landing pad size parameters for final SELECT
        params.extend([landing_pad_size, landing_pad_size])

        # Step 6: Add limit if specified
        if limit:
            params.append(limit)

        # Debug logging
        log_message(BLUE, "SEARCH", f"Final query params: {params}")
        log_message(BLUE, "SEARCH", f"System states filter: {system_states}")
        log_message(BLUE, "SEARCH", f"Unoccupied where clause: {unoccupied_where}")

        # Execute
        log_message(BLUE, "SEARCH", f"Executing query with params: {params}")
        c.execute(query, params)
        rows = c.fetchall()
        log_message(BLUE, "SEARCH", f"Query returned {len(rows)} rows")

        # Debug the first few rows to see what data we're getting
        for i, row in enumerate(rows[:3]):
            log_message(BLUE, "SEARCH", f"Sample row {i}:")
            log_message(BLUE, "SEARCH", f"- System: {row['system_name']}")
            log_message(BLUE, "SEARCH", f"- Station: {row['station_name']}")
            log_message(BLUE, "SEARCH", f"- Sell price: {row['sell_price']}")
            log_message(BLUE, "SEARCH", f"- Demand: {row['demand']}")
            log_message(BLUE, "SEARCH", f"- Mining system: {row['mining_system_name']}")

        # Process results based on display format
        if display_format == 'highest':
            # Format for highest prices display
            formatted_results = []
            seen_pairs = set()  # Track system+station pairs we've seen
            signal_counts = {}  # Track total signals per system
            
            # First pass: collect total signal counts per system
            for row in rows:
                mining_system = row['mining_system_name']
                if mining_system not in signal_counts:
                    signal_counts[mining_system] = {
                        'total_signals': 0,
                        'has_mining': False
                    }
                # Only count signals if they match our searched mineral type
                if row['mineral_type'] == signal_type:
                    signal_counts[mining_system]['total_signals'] += row['signal_count'] if row['signal_count'] else 0
                elif row['ring_type']:
                    signal_counts[mining_system]['has_mining'] = True
            
            # Second pass: format results
            for row in rows:
                system_station_key = (row['system_name'], row['station_name'])
                if system_station_key in seen_pairs:
                    continue
                    
                seen_pairs.add(system_station_key)
                
                # Get total signals for this mining system
                mining_system = row['mining_system_name']
                total_signals = signal_counts[mining_system]['total_signals']
                has_mining = signal_counts[mining_system]['has_mining']
                
                # For ring details, show total signals in system
                ring_details = ''
                if total_signals > 0:
                    ring_details = f"<img src='img/icons/hotspot-systemview.svg' width='13' height='13'> {total_signals} Hotspot{'s' if total_signals > 1 else ''}"
                elif has_mining:
                    ring_details = "Mining Available"

                formatted_results.append({
                    'commodity_name': material['name'],
                    'max_price': int(row['sell_price']) if row['sell_price'] is not None else 0,
                    'demand': int(row['demand']) if row['demand'] is not None else 0,
                    'system_name': row['system_name'],
                    'power_state': row['power_state'],
                    'station_name': row['station_name'],
                    'landing_pad_size': row['landing_pad_size'],
                    'distance_to_arrival': float(row['distance_to_arrival']) if row['distance_to_arrival'] is not None else 0,
                    'mining_system': row['mining_system_name'],
                    'mining_system_power_state': row['mining_power_state'],
                    'body_name': row['body_name'],
                    'ring_type': row['ring_type'],
                    'mineral_type': row['mineral_type'],
                    'signal_count': row['signal_count'],
                    'reserve_level': row['reserve_level'],
                    'controlling_power': row['controlling_power'],
                    'station_type': row['station_type'],
                    'update_time': row['update_time'].isoformat() if row['update_time'] is not None else None,
                    'ring_details': ring_details
                })
            return jsonify(formatted_results)
        else:
            # Original detailed format
            # Build final JSON
            pr = []
            cur_sys = None

            # Grab station info
            station_pairs = [(r['system_id64'], r['station_name']) for r in rows if r['station_name']]
            other_commodities = {}

            if station_pairs:
                oc = conn.cursor()
                ph = ','.join(['(%s,%s)'] * len(station_pairs))
                ps = [x for pair in station_pairs for x in pair]
                sel_mats = request.args.getlist('selected_materials[]', type=str)
                #log_message(BLUE, "SEARCH", f"Selected materials: {sel_mats}")

                if sel_mats and sel_mats != ['Default']:
                    # Map short->full from mining_materials.json
                    with open('data/mining_materials.json', 'r') as f:
                        mat_data_json = json.load(f)['materials']
                    short_to_full = {m['short']: m['name'] for m in mat_data_json.values()}
                    full_names = [short_to_full.get(s, s) for s in sel_mats]
                    #log_message(BLUE, "SEARCH", f"Using selected materials filter: {full_names}")

                    oc.execute(f"""
                        SELECT system_id64, station_name, commodity_name, sell_price, demand
                        FROM station_commodities
                        WHERE (system_id64, station_name) IN ({ph})
                          AND commodity_name = ANY(%s)
                          AND sell_price > 0 AND demand > 0
                        ORDER BY sell_price DESC
                    """, ps + [full_names])
                else:
                    oc.execute(f"""
                        SELECT system_id64, station_name, commodity_name, sell_price, demand
                        FROM station_commodities
                        WHERE (system_id64, station_name) IN ({ph})
                          AND sell_price > 0 AND demand > 0
                        ORDER BY sell_price DESC
                    """, ps)

                for row2 in oc.fetchall():
                    key = (row2['system_id64'], row2['station_name'])
                    if key not in other_commodities:
                        other_commodities[key] = []
                    if len(other_commodities[key]) < 6:
                        other_commodities[key].append({
                            'name': row2['commodity_name'],
                            'sell_price': row2['sell_price'],
                            'demand': row2['demand']
                        })

            # Process each row
            for row in rows:
                if cur_sys is None or cur_sys['name'] != row['system_name']:
                    if cur_sys:
                        # Convert mining_systems dict to list before appending
                        cur_sys['mining_systems'] = list(cur_sys['mining_systems'].values())
                        pr.append(cur_sys)  # Only append when switching to new system

                    power_info = []
                    if (row['controlling_power'] and 
                        row['power_state'] in ['Exploited', 'Fortified', 'Stronghold']):
                        power_info.append(f"<span style='color: yellow'>{row['controlling_power']} (Control)</span>")

                    if row['powers_acquiring']:
                        try:
                            for pw in row['powers_acquiring']:
                                if pw != row['controlling_power']:
                                    power_info.append(f"{pw} (Exploited)")
                        except Exception as e:
                            log_message(RED, "POWERS", f"Error w/ powers_acquiring: {e}")

                    cur_sys = {
                        'name': row['system_name'],
                        'controlling_power': '<br>'.join(power_info) if power_info else row['controlling_power'],
                        'power_state': row['power_state'],
                        'system_state': row['system_state'],
                        'distance': float(row['distance']),
                        'system_id64': row['system_id64'],
                        'mining_systems': {},
                        'stations': []
                    }

                    # Add station information
                    if row['station_name']:
                        try:
                            station_name = row['station_name']
                            if len(station_name) > 21:
                                station_name = station_name[:21] + '...'

                            stn = {
                                'name': station_name,
                                'pad_size': row['landing_pad_size'],
                                'distance': float(row['distance_to_arrival']) if row['distance_to_arrival'] else 0,
                                'demand': int(row['demand']) if row['demand'] else 0,
                                'sell_price': int(row['sell_price']) if row['sell_price'] else 0,
                                'station_type': row['station_type'],
                                'update_time': row['update_time'].strftime('%Y-%m-%d') if row['update_time'] else None,
                                'system_id64': row['system_id64'],
                                'commodity_name': material['name'],
                                'other_commodities': other_commodities.get((row['system_id64'], row['station_name']), [])
                            }
                            cur_sys['stations'].append(stn)
                        except Exception as e:
                            log_message(RED, "STATIONS", f"Error processing station {row['station_name']}: {e}")
                            pass

                # Get or create mining system entry
                mining_system_name = row['mining_system_name']
                if mining_system_name not in cur_sys['mining_systems']:
                    log_message(BLUE, "MINING", f"Creating mining system: {mining_system_name}")
                    cur_sys['mining_systems'][mining_system_name] = {
                        'name': mining_system_name,
                        'power_state': row['mining_power_state'],
                        'controlling_power': row['controlling_power'],
                        'powers_acquiring': json.loads(row['powers_acquiring']) if isinstance(row['powers_acquiring'], str) else row['powers_acquiring'],
                        'rings': [],
                        'id64': row['mining_id64'],
                        'all_signals': []  # Add this
                    }

                ring_data = material['ring_types'].get(row['ring_type'], {})
                display_ring_name = row['ring_name']
                if not SYSTEM_IN_RING_NAME and display_ring_name.startswith(row['system_name']):
                    display_ring_name = display_ring_name[len(row['system_name']):].lstrip()

                signal_text = f"{material['name']}"
                
                # Add signal to mining system's all_signals
                si = {
                    'ring_name': display_ring_name,
                    'mineral_type': row['mineral_type'],
                    'signal_count': row['signal_count'] or '',
                    'reserve_level': row['reserve_level'],
                    'ring_type': row['ring_type'],
                    'signal_text': signal_text
                }
                if si not in cur_sys['mining_systems'][mining_system_name]['all_signals']:
                    #log_message(BLUE, "MINING", f"Adding signal to {mining_system_name}:")
                    #log_message(BLUE, "MINING", f"- rings: {cur_sys['mining_systems'][mining_system_name]['rings']}")
                    #log_message(BLUE, "MINING", f"- all_signals: {cur_sys['mining_systems'][mining_system_name]['all_signals']}")
                    cur_sys['mining_systems'][mining_system_name]['all_signals'].append(si)

                # If it's the main hotspot
                if row['mineral_type'] == signal_type:
                    hotspot_text = ("Hotspot " if row['signal_count'] == 1 else
                                "Hotspots " if row['signal_count'] else "")
                    re = {
                        'name': display_ring_name + f" <img src='/img/icons/rings/{row['ring_type'].lower()}.png' width='16' height='16' class='ring-type-icon' alt='{row['ring_type']}' title='Ring Type: {row['ring_type']}' style='vertical-align: middle;'> <svg class='reserve-level-icon' width='14' height='13' style='margin-right: 2px; color: #f5730d'><title>Reserve Level: {row['reserve_level']}</title><use href='img/icons/reserve-level.svg#reserve-level-{row['reserve_level'].lower()}'></use></svg>",
                        'body_name': row['body_name'],
                        'signals': f"<img src='img/icons/hotspot-systemview.svg' width='13' height='13'> {material['name']}: {row['signal_count'] or ''} {hotspot_text}"
                    }
                    if re not in cur_sys['mining_systems'][mining_system_name]['rings']:
                        cur_sys['mining_systems'][mining_system_name]['rings'].append(re)

                    # Add signal to mining system's all_signals
                    if row['mineral_type']:
                        hotspot_text = ("Hotspot " if row['signal_count'] == 1 else
                                    "Hotspots " if row['signal_count'] else "")
                        signal_text = f"Test <img src='img/icons/hotspot-systemview.svg' width='13' height='13'> {row['mineral_type']}: {row['signal_count'] or ''} {hotspot_text}"
                    else:
                        signal_text = f"{material['name']}"

                elif any([
                    ring_data.get('surfaceLaserMining', False),
                    ring_data.get('surfaceDeposit', False),
                    ring_data.get('subSurfaceDeposit', False),
                    ring_data.get('core', False)
                ]):
                    re = {
                        'name': display_ring_name + f" <img src='/img/icons/rings/{row['ring_type'].lower()}.png' width='16' height='16' class='ring-type-icon' alt='{row['ring_type']}' title='Ring Type: {row['ring_type']}' style='vertical-align: middle;'> <svg class='reserve-level-icon' width='14' height='13' style='margin-right: 2px; color: #f5730d'><title>Reserve Level: {row['reserve_level']}</title><use href='img/icons/reserve-level.svg#reserve-level-{row['reserve_level'].lower()}'></use></svg>",
                        'body_name': row['body_name'],
                        'signals': f"{material['name']}"
                    }
                    if re not in cur_sys['mining_systems'][mining_system_name]['rings']:
                        cur_sys['mining_systems'][mining_system_name]['rings'].append(re)

                    # Create signal text
                    if row['mineral_type']:
                        hotspot_text = ("Hotspot " if row['signal_count'] == 1 else
                                    "Hotspots " if row['signal_count'] else "")
                        signal_text = f"<img src='img/icons/hotspot-systemview.svg' width='13' height='13'> {row['mineral_type']}: {row['signal_count'] or ''} {hotspot_text}"
                    else:
                        signal_text = f"{material['name']}"

            # Don't forget last system
            if cur_sys:
                # Convert mining_systems dict to list before appending
                #log_message(BLUE, "MINING", "Converting mining_systems to list:")
                #for name, system in cur_sys['mining_systems'].items():
                    #log_message(BLUE, "MINING", f"- {name}:")
                    #log_message(BLUE, "MINING", f"  rings: {system['rings']}")
                    #log_message(BLUE, "MINING", f"  all_signals: {system['all_signals']}")
                cur_sys['mining_systems'] = list(cur_sys['mining_systems'].values())
                pr.append(cur_sys)

            # Optionally load more signals
            if pr:
                for s in pr:
                    for mining_system in s['mining_systems']:  # Already a list, don't use values()
                        # Only get signals we don't already have
                        existing_minerals = [sig['mineral_type'] for sig in mining_system['all_signals']]
                        c.execute("""
                            SELECT system_id64, ring_name, mineral_type, signal_count, reserve_level, ring_type
                            FROM mineral_signals
                            WHERE system_id64 = %s AND mineral_type NOT IN %s
                        """, [mining_system['id64'], tuple(existing_minerals)])

                        for row3 in c.fetchall():
                            if row3['mineral_type']:
                                hotspot_text = ("Hotspot " if row3['signal_count'] == 1 else
                                            "Hotspots " if row3['signal_count'] else "")
                                signal_text = f"<img src='img/icons/hotspot-systemview.svg' width='13' height='13'> {row3['mineral_type']}: {row3['signal_count'] or ''} {hotspot_text}"
                            else:
                                signal_text = f"{material['name']}"

                            si = {
                                'ring_name': row3['ring_name'],
                                'mineral_type': row3['mineral_type'],
                                'signal_count': row3['signal_count'] or '',
                                'reserve_level': row3['reserve_level'],
                                'ring_type': row3['ring_type'],
                                'signal_text': signal_text
                            }
                            if si not in mining_system['all_signals']:
                                mining_system['all_signals'].append(si)

            return jsonify(pr)

    except Exception as e:
        log_message(RED, "ERROR", f"Search error: {str(e)}")
        return jsonify({'error': str(e)}), 500

    finally:
        if conn:
            conn.close()
