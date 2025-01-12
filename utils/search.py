import json
import os
from datetime import datetime
from flask import jsonify, request
from utils.mining_data import get_price_comparison, PRICE_DATA, normalize_commodity_name
from utils.common import log_message, get_db_connection, YELLOW, RED, BLUE
from utils import res_data

# Constants
SYSTEM_IN_RING_NAME = False

def search():
    try:
        # Get all input parameters from the request
        ref_system = request.args.get('system', 'Sol')
        max_dist = float(request.args.get('distance', '10000'))
        controlling_power = request.args.get('controlling_power')
        power_states = request.args.getlist('power_state[]')
        signal_type = request.args.get('signal_type')
        ring_type_filter = request.args.get('ring_type_filter', 'Hotspots')
        limit = int(request.args.get('limit', '30'))
        mining_types = request.args.getlist('mining_types[]')
        min_demand = int(request.args.get('minDemand', '0'))
        max_demand = int(request.args.get('maxDemand', '0'))

        # Log search parameters
        log_message(BLUE, "SEARCH", f"Search parameters:")
        log_message(BLUE, "SEARCH", f"- System: {ref_system}")
        log_message(BLUE, "SEARCH", f"- Distance: {max_dist}")
        log_message(BLUE, "SEARCH", f"- Power: {controlling_power}")
        log_message(BLUE, "SEARCH", f"- Power states: {power_states}")
        log_message(BLUE, "SEARCH", f"- Signal type: {signal_type}")
        log_message(BLUE, "SEARCH", f"- Ring type filter: {ring_type_filter}")
        log_message(BLUE, "SEARCH", f"- Mining types: {mining_types}")
        log_message(BLUE, "SEARCH", f"- Min demand: {min_demand}")
        log_message(BLUE, "SEARCH", f"- Max demand: {max_demand}")

        # Get database connection
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        c = conn.cursor()

        # Get reference system coordinates
        c.execute('SELECT x, y, z FROM systems WHERE name ILIKE %s', (ref_system,))
        ref_coords = c.fetchone()
        if not ref_coords:
            conn.close()
            return jsonify({'error': 'Reference system not found'}), 404

        rx, ry, rz = ref_coords['x'], ref_coords['y'], ref_coords['z']

        # Load material data from mining_materials.json
        with open('data/mining_materials.json', 'r') as f:
            mat_data = json.load(f)['materials']
            # Convert LowTemperatureDiamond to Low Temperature Diamonds for material lookup
            material_name = 'Low Temperature Diamonds' if signal_type == 'LowTemperatureDiamond' else signal_type
            material = mat_data.get(material_name)
            if not material:
                log_message(RED, "SEARCH", f"Material {material_name} not found in mining_materials.json")
                return jsonify([])
            log_message(BLUE, "SEARCH", f"Material data: {material}")

        # Build WHERE conditions for power filters
        where_conditions = []
        where_params = []

        if controlling_power:
            # For Exploited/Fortified/Stronghold states, check controlling_power
            if not power_states or all(state in ['Exploited', 'Fortified', 'Stronghold'] for state in power_states):
                where_conditions.append("s.controlling_power = %s")
                where_params.append(controlling_power)
            # For Prepared/In Prepare Radius states, check powers_acquiring
            elif all(state in ['Prepared', 'InPrepareRadius'] for state in power_states):
                where_conditions.append("%s::text = ANY(SELECT jsonb_array_elements_text(s.powers_acquiring::jsonb))")
                where_params.append(controlling_power)
            # For mixed states, check both
            else:
                where_conditions.append("(s.controlling_power = %s OR %s::text = ANY(SELECT jsonb_array_elements_text(s.powers_acquiring::jsonb)))")
                where_params.extend([controlling_power, controlling_power])

        if power_states:
            where_conditions.append("s.power_state = ANY(%s::text[])")
            where_params.append(power_states)

        # Handle mining types filter
        if mining_types and 'All' not in mining_types:
            valid_ring_types = []
            log_message(BLUE, "MINING", f"Processing mining types: {mining_types}")
            for ring_type, data in material['ring_types'].items():
                log_message(BLUE, "MINING", f"Checking ring type {ring_type} with data: {data}")
                for mining_type in mining_types:
                    if (mining_type.lower() == 'laser surface' and data.get('surfaceLaserMining', False)) or \
                       (mining_type.lower() == 'surface' and data.get('surfaceDeposit', False)) or \
                       (mining_type.lower() == 'subsurface' and data.get('subSurfaceDeposit', False)) or \
                       (mining_type.lower() == 'core' and data.get('core', False)):
                        valid_ring_types.append(ring_type)
                        log_message(BLUE, "MINING", f"Added valid ring type: {ring_type} for mining type: {mining_type}")
                        break
            
            log_message(BLUE, "MINING", f"Final valid ring types: {valid_ring_types}")
            if valid_ring_types:
                # For Core mining, we only want to filter by ring type if not looking for hotspots
                if not (mining_types == ['Core'] and ring_type_filter == 'Hotspots'):
                    where_conditions.append("ms.ring_type = ANY(%s::text[])")
                    where_params.append(valid_ring_types)
            else:
                log_message(RED, "MINING", "No valid ring types found for the selected mining types")
                return jsonify([])

        # Build the JOIN condition based on material type and ring type filter
        join_condition = "s.id64 = ms.system_id64"
        join_params = []

        if ring_type_filter == 'Hotspots':
            # For hotspots, we want rings with the specific mineral type
            join_condition += " AND ms.mineral_type = %s"
            join_params.append(signal_type)  # Use original signal_type for mineral_type comparison
        elif ring_type_filter == 'Without Hotspots':
            # For non-hotspots, we want rings with NULL mineral type but valid ring types
            join_condition += " AND ms.mineral_type IS NULL AND ms.ring_type = ANY(%s::text[])"
            valid_ring_types = [rt for rt, data in material['ring_types'].items() 
                              if any([data.get('surfaceLaserMining', False),
                                    data.get('surfaceDeposit', False),
                                    data.get('subSurfaceDeposit', False),
                                    data.get('core', False)])]
            join_params.append(valid_ring_types)
        else:
            # For 'All', include both hotspots and valid ring types
            join_condition += " AND (ms.mineral_type = %s OR (ms.mineral_type IS NULL AND ms.ring_type = ANY(%s::text[])))"
            valid_ring_types = [rt for rt, data in material['ring_types'].items() 
                              if any([data.get('surfaceLaserMining', False),
                                    data.get('surfaceDeposit', False),
                                    data.get('subSurfaceDeposit', False),
                                    data.get('core', False)])]
            join_params.extend([signal_type, valid_ring_types])  # Use original signal_type for mineral_type comparison

        # Build the complete query
        query = f"""
        WITH relevant_systems AS (
            SELECT s.*, SQRT(POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2)) as distance
            FROM systems s
            WHERE POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2) <= POWER(%s, 2)
        ),
        relevant_stations AS (
            SELECT sc.system_id64, sc.station_name, sc.sell_price, sc.demand
            FROM station_commodities sc
            WHERE sc.commodity_name = %s
            AND sc.sell_price > 0
            AND (
                CASE 
                    WHEN %s = 0 AND %s = 0 THEN sc.demand = 0
                    WHEN %s = 0 THEN sc.demand <= %s
                    WHEN %s = 0 THEN sc.demand >= %s
                    ELSE sc.demand BETWEEN %s AND %s
                END
            )
        )
        SELECT DISTINCT s.name as system_name, s.id64 as system_id64, s.controlling_power,
            s.power_state, s.distance, ms.body_name, ms.ring_name, ms.ring_type,
            ms.mineral_type, ms.signal_count, ms.reserve_level, rs.station_name,
            st.landing_pad_size, st.distance_to_arrival as station_distance,
            st.station_type, rs.demand, rs.sell_price, st.update_time,
            s.powers_acquiring,
            COALESCE(rs.sell_price, 0) as sort_price,
            CASE 
                WHEN ms.reserve_level = 'Pristine' THEN 1
                WHEN ms.reserve_level = 'Major' THEN 2
                WHEN ms.reserve_level = 'Common' THEN 3
                WHEN ms.reserve_level = 'Low' THEN 4
                WHEN ms.reserve_level = 'Depleted' THEN 5
                ELSE 6 
            END as reserve_level_order
        FROM relevant_systems s
        JOIN mineral_signals ms ON {join_condition}
        LEFT JOIN relevant_stations rs ON s.id64 = rs.system_id64
        LEFT JOIN stations st ON s.id64 = st.system_id64 AND rs.station_name = st.station_name
        """

        # Add WHERE conditions if present
        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)

        # Add ORDER BY
        query += """
        ORDER BY 
            sort_price DESC,
            reserve_level_order ASC,
            distance ASC
        """

        if limit:
            query += " LIMIT %s"

        # Initialize all parameters in the correct order
        params = [
            rx, ry, rz,  # Distance calculation in subquery
            rx, ry, rz,  # Distance filter in subquery
            max_dist,    # Maximum distance
            material['name'],  # For station_commodities
            min_demand, max_demand,  # For zero-zero check
            min_demand, max_demand,  # For min=0 check
            max_demand, min_demand,  # For max=0 check
            min_demand, max_demand   # For between check
        ]

        # Add JOIN parameters
        params.extend(join_params)

        # Add WHERE parameters
        params.extend(where_params)

        if limit:
            params.append(limit)

        # Execute query and process results
        log_message(BLUE, "SEARCH", f"Executing query with params: {params}")
        c.execute(query, params)
        rows = c.fetchall()
        log_message(BLUE, "SEARCH", f"Query returned {len(rows)} rows")

        # Process results
        pr = []
        cur_sys = None

        # Get other commodities for stations
        station_pairs = [(r['system_id64'], r['station_name']) for r in rows if r['station_name']]
        other_commodities = {}

        if station_pairs:
            oc = conn.cursor()
            ph = ','.join(['(%s,%s)'] * len(station_pairs))
            ps = [x for pair in station_pairs for x in pair]
            sel_mats = request.args.getlist('selected_materials[]', type=str)

            if sel_mats and sel_mats != ['Default']:
                oc.execute(f"""
                    SELECT sc.system_id64, sc.station_name, sc.commodity_name, sc.sell_price, sc.demand,
                    COUNT(*) OVER (PARTITION BY sc.system_id64, sc.station_name) total_commodities
                    FROM station_commodities sc
                    WHERE (sc.system_id64, sc.station_name) IN ({ph})
                    AND sc.commodity_name = ANY(%s::text[])
                    AND sc.sell_price > 0 AND sc.demand > 0
                    ORDER BY sc.system_id64, sc.station_name, sc.sell_price DESC
                """, ps + [sel_mats])
            else:
                oc.execute(f"""
                    SELECT system_id64, station_name, commodity_name, sell_price, demand
                    FROM station_commodities
                    WHERE (system_id64, station_name) IN ({ph})
                    AND sell_price > 0 AND demand > 0
                    ORDER BY sell_price DESC
                """, ps)

            for r2 in oc.fetchall():
                k = (r2['system_id64'], r2['station_name'])
                if k not in other_commodities:
                    other_commodities[k] = []
                if len(other_commodities[k]) < 6:
                    other_commodities[k].append({
                        'name': r2['commodity_name'],
                        'sell_price': r2['sell_price'],
                        'demand': r2['demand']
                    })

        # Process each row
        for row in rows:
            if cur_sys is None or cur_sys['name'] != row['system_name']:
                if cur_sys:
                    pr.append(cur_sys)
                
                # Format power information
                power_info = []
                if row['controlling_power'] and row['power_state'] in ['Exploited', 'Fortified', 'Stronghold']:
                    power_info.append(f'<span style="color: yellow">{row["controlling_power"]} (Control)</span>')
                
                # Add exploiting powers from powers_acquiring array
                if row['powers_acquiring']:
                    try:
                        log_message(BLUE, "POWERS", f"powers_acquiring for {row['system_name']}: {row['powers_acquiring']}")
                        for power in row['powers_acquiring']:
                            if power != row['controlling_power']:  # Don't show controlling power twice
                                power_info.append(f'{power} (Exploited)')
                    except Exception as e:
                        log_message(RED, "POWERS", f"Error processing powers_acquiring for {row['system_name']}: {e}")
                else:
                    log_message(BLUE, "POWERS", f"No powers_acquiring for {row['system_name']}")
                
                cur_sys = {
                    'name': row['system_name'],
                    'controlling_power': '<br>'.join(power_info) if power_info else row['controlling_power'],
                    'power_state': row['power_state'],
                    'distance': float(row['distance']),
                    'system_id64': row['system_id64'],
                    'rings': [],
                    'stations': [],
                    'all_signals': []
                }

            # Handle ring details based on mining_materials.json data
            ring_data = material['ring_types'].get(row['ring_type'], {})
            
            # Format ring name - remove system name if it appears at the start
            display_ring_name = row['ring_name']
            if not SYSTEM_IN_RING_NAME and display_ring_name.startswith(row['system_name']):
                display_ring_name = display_ring_name[len(row['system_name']):].lstrip()
            
            # Show ring details for both hotspots and valid rings
            if row['mineral_type'] == signal_type:
                # This is a hotspot
                hotspot_text = "Hotspot " if row['signal_count'] == 1 else "Hotspots " if row['signal_count'] else ""
                re = {
                    'name': display_ring_name,
                    'body_name': row['body_name'],
                    'signals': f"<img src='img/icons/hotspot-2.svg' width='11' height='11' class='hotspot-icon'> {material['name']}: {row['signal_count'] or ''} {hotspot_text}({row['reserve_level']})"
                }
                if re not in cur_sys['rings']:
                    cur_sys['rings'].append(re)
            elif any([ring_data.get('surfaceLaserMining', False),
                     ring_data.get('surfaceDeposit', False),
                     ring_data.get('subSurfaceDeposit', False),
                     ring_data.get('core', False)]):
                # This is a regular ring that can have this material
                re = {
                    'name': display_ring_name,
                    'body_name': row['body_name'],
                    'signals': f"{material['name']} ({row['ring_type']}, {row['reserve_level']})"
                }
                if re not in cur_sys['rings']:
                    cur_sys['rings'].append(re)

            # Add to all_signals if it's a hotspot
            if row['mineral_type']:
                si = {
                    'ring_name': row['ring_name'],
                    'mineral_type': row['mineral_type'],
                    'signal_count': row['signal_count'] or '',
                    'reserve_level': row['reserve_level'],
                    'ring_type': row['ring_type']
                }
                if si not in cur_sys['all_signals']:
                    cur_sys['all_signals'].append(si)

            # Add station information
            if row['station_name']:
                try:
                    ex = next((s for s in cur_sys['stations'] if s['name'] == row['station_name']), None)

                     # Process station name - truncate if longer than 21 chars
                    station_name = row['station_name']
                    if len(station_name) > 21:
                        station_name = station_name[:21] + '...'

                    if ex:
                        ex['other_commodities'] = other_commodities.get((row['system_id64'], row['station_name']), [])
                    else:
                        stn = {
                            'name': station_name,
                            'pad_size': row['landing_pad_size'],
                            'distance': float(row['station_distance']) if row['station_distance'] else 0,
                            'demand': int(row['demand']) if row['demand'] else 0,
                            'sell_price': int(row['sell_price']) if row['sell_price'] else 0,
                            'station_type': row['station_type'],
                            'update_time': row['update_time'].strftime('%Y-%m-%d') if row['update_time'] else None,
                            'system_id64': row['system_id64'],
                            'other_commodities': other_commodities.get((row['system_id64'], row['station_name']), [])
                        }
                        cur_sys['stations'].append(stn)
                except:
                    pass

        if cur_sys:
            pr.append(cur_sys)

        # Get other signals for each system
        if pr:
            sys_ids = [s['system_id64'] for s in pr]
            c.execute("""
                SELECT system_id64, ring_name, mineral_type, signal_count, reserve_level, ring_type
                FROM mineral_signals
                WHERE system_id64 = ANY(%s::bigint[]) AND mineral_type != %s
            """, [sys_ids, signal_type])

            other_sigs = {}
            for r in c.fetchall():
                if r['system_id64'] not in other_sigs:
                    other_sigs[r['system_id64']] = []
                other_sigs[r['system_id64']].append({
                    'ring_name': r['ring_name'],
                    'mineral_type': r['mineral_type'],
                    'signal_count': r['signal_count'] or '',
                    'reserve_level': r['reserve_level'],
                    'ring_type': r['ring_type']
                })

            for s in pr:
                s['all_signals'].extend(other_sigs.get(s['system_id64'], []))

        return jsonify(pr)

    except Exception as e:
        log_message(RED, "ERROR", f"Search error: {str(e)}")
        return jsonify({'error': str(e)}), 500

    finally:
        if conn:
            conn.close()

def search_highest():
    try:
        # Get power filters
        controlling_power = request.args.get('controlling_power')
        power_states = request.args.getlist('power_state[]')
        limit = int(request.args.get('limit', '30'))
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cur = conn.cursor()
        
        # Load material data from mining_materials.json
        with open('data/mining_materials.json', 'r') as f:
            mat_data = json.load(f)['materials']
        
        # Build all WHERE conditions first
        where_conditions = ["sc.demand > 0", "sc.sell_price > 0"]
        params = []
        
        if controlling_power:
            where_conditions.append("s.controlling_power = %s")
            params.append(controlling_power)
        
        if power_states:
            where_conditions.append("s.power_state = ANY(%s::text[])")
            params.append(power_states)
        
        where_clause = " AND ".join(where_conditions)
        
        # Build ring type case statement for each material
        ring_type_cases = []
        for material_name, material in mat_data.items():
            valid_ring_types = [rt for rt, data in material['ring_types'].items() 
                              if any([data.get('surfaceLaserMining', False),
                                    data.get('surfaceDeposit', False),
                                    data.get('subSurfaceDeposit', False),
                                    data.get('core', False)])]
            if valid_ring_types:
                ring_types_str = ','.join([f"'{rt}'" for rt in valid_ring_types])
                ring_type_cases.append(f"WHEN hp.commodity_name = '{material['name']}' AND ms.ring_type IN ({ring_types_str}) THEN 1")
        ring_type_case = '\n'.join(ring_type_cases)
        
        query = f"""
        WITH HighestPrices AS (
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
            WHERE {where_clause}
            ORDER BY sc.sell_price DESC
            LIMIT 1000
        ),
        MinableCheck AS (
            SELECT DISTINCT
                hp.*,
                ms.mineral_type,
                ms.ring_type,
                ms.reserve_level,
                CASE
                    WHEN ms.mineral_type = hp.commodity_name THEN 1
                    WHEN hp.commodity_name = 'Low Temperature Diamonds' 
                        AND ms.mineral_type = 'LowTemperatureDiamond' THEN 1
                    {ring_type_case}
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
        WHERE is_minable = 1
        ORDER BY max_price DESC
        LIMIT %s
        """
        
        params.append(limit)
        cur.execute(query, params)
        results = cur.fetchall()
        
        # Format results
        formatted_results = []
        for row in results:
            formatted_results.append({
                'commodity_name': row['commodity_name'],
                'max_price': int(row['max_price']) if row['max_price'] is not None else 0,
                'system_name': row['system_name'],
                'controlling_power': row['controlling_power'],
                'power_state': row['power_state'],
                'landing_pad_size': row['landing_pad_size'],
                'distance_to_arrival': float(row['distance_to_arrival']) if row['distance_to_arrival'] is not None else 0,
                'demand': int(row['demand']) if row['demand'] is not None else 0,
                'reserve_level': row['reserve_level'],
                'station_name': row['station_name'],
                'station_type': row['station_type'],
                'update_time': row['update_time'].isoformat() if row['update_time'] is not None else None
            })
        
        return jsonify(formatted_results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_price_comparison_endpoint():
    try:
        data=request.json; items=data.get('items',[]); use_max=data.get('use_max',False)
        if not items: return jsonify([])
        results=[]
        for item in items:
            price=int(item.get('price',0))
            commodity=item.get('commodity')
            if not commodity:
                results.append({'color':None,'indicator':''}); continue
            norm=normalize_commodity_name(commodity)
            if norm not in PRICE_DATA:
                if commodity in PRICE_DATA: norm=commodity
                else:
                    results.append({'color':None,'indicator':''}); continue
            ref=int(PRICE_DATA[norm]['max_price' if use_max else 'avg_price'])
            color,indicator=get_price_comparison(price,ref)
            results.append({'color':color,'indicator':indicator})
        return jsonify(results)
    except Exception as e:
        return jsonify({'error':str(e)}),500

def search_res_hotspots():
    try:
        # Get reference system from query parameters
        ref_system = request.args.get('system', 'Sol')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        c = conn.cursor()
        
        c.execute('SELECT x, y, z FROM systems WHERE name ILIKE %s', (ref_system,))
        ref_coords = c.fetchone()
        if not ref_coords:
            conn.close()
            return jsonify({'error': 'Reference system not found'}), 404
            
        rx, ry, rz = ref_coords['x'], ref_coords['y'], ref_coords['z']
        hotspot_data = res_data.load_res_data()
        if not hotspot_data:
            conn.close()
            return jsonify({'error': 'No RES hotspot data available'}), 404
            
        results = []
        for e in hotspot_data:
            c.execute('''SELECT s.*, sqrt(power(s.x - %s, 2) + power(s.y - %s, 2) + power(s.z - %s, 2)) as distance
                        FROM systems s WHERE s.name ILIKE %s''', 
                     (rx, ry, rz, e['system']))
            system = c.fetchone()
            if not system:
                continue
                
            st = res_data.get_station_commodities(conn, system['id64'])
            results.append({
                'system': e['system'],
                'power': system['controlling_power'] or 'None',
                'distance': float(system['distance']),
                'ring': e['ring'],
                'ls': e['ls'],
                'res_zone': e['res_zone'],
                'comment': e['comment'],
                'stations': st
            })
            
        conn.close()
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def search_high_yield_platinum():
    try:
        # Get reference system from query parameters
        ref_system = request.args.get('system', 'Sol')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        c = conn.cursor()
        
        c.execute('SELECT x, y, z FROM systems WHERE name = %s', (ref_system,))
        ref_coords = c.fetchone()
        if not ref_coords:
            conn.close()
            return jsonify({'error': 'Reference system not found'}), 404
            
        rx, ry, rz = ref_coords['x'], ref_coords['y'], ref_coords['z']
        data = res_data.load_high_yield_platinum()
        if not data:
            conn.close()
            return jsonify({'error': 'No high yield platinum data available'}), 404
            
        results = []
        for e in data:
            c.execute('''SELECT s.*, sqrt(power(s.x - %s, 2) + power(s.y - %s, 2) + power(s.z - %s, 2)) as distance
                        FROM systems s WHERE s.name = %s''', 
                     (rx, ry, rz, e['system']))
            system = c.fetchone()
            if not system:
                continue
                
            st = res_data.get_station_commodities(conn, system['id64'])
            results.append({
                'system': e['system'],
                'power': system['controlling_power'] or 'None',
                'distance': float(system['distance']),
                'ring': e['ring'],
                'percentage': e['percentage'],
                'comment': e['comment'],
                'stations': st
            })
            
        conn.close()
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
