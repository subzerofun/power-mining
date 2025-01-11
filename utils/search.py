import json
from datetime import datetime
from flask import jsonify, request
import mining_data
from mining_data import (
    get_non_hotspot_materials_list,
    normalize_commodity_name,
    get_price_comparison,
    PRICE_DATA,
    NON_HOTSPOT_MATERIALS
)
import res_data


# Import these from server.py
from server import get_db_connection, log_message, BLUE, RED, app

@app.route('/search')
def search():
    try:
        ref_system = request.args.get('system', 'Sol')
        max_dist = float(request.args.get('distance', '10000'))
        controlling_power = request.args.get('controlling_power')
        power_states = request.args.getlist('power_state[]')
        signal_type = request.args.get('signal_type')
        ring_type_filter = request.args.get('ring_type_filter', 'All')
        limit = int(request.args.get('limit', '30'))
        mining_types = request.args.getlist('mining_types[]')

        log_message(BLUE, "SEARCH", f"Search parameters:")
        log_message(BLUE, "SEARCH", f"- System: {ref_system}")
        log_message(BLUE, "SEARCH", f"- Distance: {max_dist}")
        log_message(BLUE, "SEARCH", f"- Power: {controlling_power}")
        log_message(BLUE, "SEARCH", f"- Power states: {power_states}")
        log_message(BLUE, "SEARCH", f"- Signal type: {signal_type}")
        log_message(BLUE, "SEARCH", f"- Ring type filter: {ring_type_filter}")
        log_message(BLUE, "SEARCH", f"- Mining types: {mining_types}")

        if mining_types and 'All' not in mining_types:
            with open('data/mining_data.json', 'r') as f:
                mat_data = json.load(f)
                log_message(BLUE, "SEARCH", f"Checking material {signal_type} in mining_data.json")
                cd = next((i for i in mat_data['materials'] if i['name'] == signal_type), None)
                if not cd:
                    log_message(RED, "SEARCH", f"Material {signal_type} not found in mining_data.json")
                    return jsonify([])
                log_message(BLUE, "SEARCH", f"Material data: {cd}")

        ring_materials = get_ring_materials()
        is_ring_material = False

        # Check mining_data.json for laser mining capability in selected ring type
        try:
            with open('data/mining_data.json', 'r') as f:
                mat_data = json.load(f)
                cd = next((i for i in mat_data['materials'] if i['name'] == signal_type), None)
                if cd and ring_type_filter != 'All' and ring_type_filter in cd['ring_types']:
                    ring_data = cd['ring_types'][ring_type_filter]
                    # If material can be laser mined in this ring type, treat it as a ring material
                    is_ring_material = ring_data.get('surfaceLaserMining', False)
                    log_message(BLUE, "SEARCH", f"Checking laser mining capability for {signal_type} in {ring_type_filter} rings: {is_ring_material}")
        except Exception as e:
            log_message(RED, "ERROR", f"Error checking mining data: {str(e)}")
            # Fallback to ring_materials.csv check
            is_ring_material = signal_type in ring_materials

        log_message(BLUE, "SEARCH", f"Is ring material: {is_ring_material}")

        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cur = conn.cursor()
        cur.execute('SELECT x, y, z FROM systems WHERE name = %s', (ref_system,))
        ref_coords = cur.fetchone()
        if not ref_coords:
            conn.close()
            return jsonify({'error': 'Reference system not found'}), 404

        rx, ry, rz = ref_coords['x'], ref_coords['y'], ref_coords['z']
        mining_cond = ''
        mining_params = []
        if mining_types and 'All' not in mining_types:
            mining_cond, mining_params = get_mining_type_conditions(signal_type, mining_types)

        ring_cond = ''
        ring_params = []
        if ring_type_filter != 'All':
            if ring_type_filter == 'Just Hotspots':
                ring_cond = ' AND ms.mineral_type IS NOT NULL'
            elif ring_type_filter == 'Without Hotspots':
                ring_cond = ' AND (ms.mineral_type IS NULL OR ms.mineral_type != %s)'
                ring_params.append(signal_type)
                try:
                    with open('data/mining_data.json', 'r') as f:
                        mat_data = json.load(f)
                        cd = next((item for item in mat_data['materials'] if item['name'] == signal_type), None)
                        if cd:
                            rt = []
                            for r_type, rd in cd['ring_types'].items():
                                if any([rd['surfaceLaserMining'], rd['surfaceDeposit'], rd['subSurfaceDeposit'], rd['core']]):
                                    rt.append(r_type)
                            if rt:
                                ring_cond += ' AND ms.ring_type = ANY(%s::text[])'
                                ring_params.append(rt)
                except:
                    pass
            else:
                ring_cond = ' AND ms.ring_type = %s'
                ring_params.append(ring_type_filter)
                log_message(BLUE, "SEARCH", f"Adding ring type filter: {ring_type_filter}")
                try:
                    with open('data/mining_data.json', 'r') as f:
                        mat_data = json.load(f)
                        cd = next((i for i in mat_data['materials'] if i['name'] == signal_type), None)
                        log_message(BLUE, "SEARCH", f"Material data for ring type check: {cd}")
                        if not cd or ring_type_filter not in cd['ring_types']:
                            log_message(RED, "SEARCH", f"Material {signal_type} not found in ring type {ring_type_filter}")
                            return jsonify([])
                        ring_data = cd['ring_types'][ring_type_filter]
                        # Check if ANY mining method is valid for this ring type
                        if not any([
                            ring_data.get('surfaceLaserMining', False),
                            ring_data.get('surfaceDeposit', False),
                            ring_data.get('subSurfaceDeposit', False),
                            ring_data.get('core', False)
                        ]):
                            log_message(RED, "SEARCH", f"No valid mining methods for {signal_type} in {ring_type_filter} rings")
                            return jsonify([])
                        log_message(BLUE, "SEARCH", f"Ring type data: {cd['ring_types'][ring_type_filter]}")
                except Exception as e:
                    log_message(RED, "ERROR", f"Error checking ring type: {str(e)}")
                    pass

        # Define non-hotspot materials
        non_hotspot = get_non_hotspot_materials_list()
        is_non_hotspot = signal_type in non_hotspot
        non_hotspot_str = ','.join(f"'{material}'" for material in non_hotspot)
        
        # Build the ring type case statement
        ring_type_cases = []
        for material, ring_types in mining_data.NON_HOTSPOT_MATERIALS.items():
            ring_types_str = ','.join(f"'{rt}'" for rt in ring_types)
            ring_type_cases.append(f"WHEN hp.commodity_name = '{material}' AND ms.ring_type IN ({ring_types_str}) THEN 1")
        ring_type_case = '\n'.join(ring_type_cases)
        
        if is_non_hotspot:
            # Get ring types from NON_HOTSPOT_MATERIALS dictionary
            ring_types = mining_data.NON_HOTSPOT_MATERIALS.get(signal_type, [])
            
            # Build all WHERE conditions first
            where_conditions = ["ms.ring_type = ANY(%s::text[])"]
            params = []  # Start with empty params and build in order
            
            # Add distance and signal params first
            params.extend([rx, rx, ry, ry, rz, rz, max_dist])
            params.extend([signal_type, signal_type])
            params.append(ring_types)  # For the ring_type ANY condition
            params.append(ring_types)  # For the ANY condition in JOIN            
            
            if controlling_power:
                where_conditions.append("s.controlling_power = %s")
                params.append(controlling_power)

            if power_states:
                where_conditions.append("s.power_state = ANY(%s)")
                params.append(power_states)

            if mining_cond:
                where_conditions.append(mining_cond)
                params.extend(mining_params)

            query = f"""
            WITH relevant_systems AS (
                SELECT s.*, SQRT(POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2)) as distance
                FROM systems s
                WHERE POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2) <= POWER(%s, 2)
            ),
            relevant_stations AS (
                SELECT sc.system_id64, sc.station_name, sc.sell_price, sc.demand
                FROM station_commodities sc
                WHERE (sc.commodity_name = %s OR (%s = 'LowTemperatureDiamond' AND sc.commodity_name = 'Low Temperature Diamonds'))
                AND sc.demand > 0 AND sc.sell_price > 0
            )
            SELECT DISTINCT s.name as system_name, s.id64 as system_id64, s.controlling_power,
                s.power_state, s.distance, ms.body_name, ms.ring_name, ms.ring_type,
                ms.mineral_type, ms.signal_count, ms.reserve_level, rs.station_name,
                st.landing_pad_size, st.distance_to_arrival as station_distance,
                st.station_type, rs.demand, rs.sell_price, st.update_time,
                rs.sell_price as sort_price,
                CASE 
                    WHEN ms.reserve_level = 'Pristine' THEN 1
                    WHEN ms.reserve_level = 'Major' THEN 2
                    WHEN ms.reserve_level = 'Common' THEN 3
                    WHEN ms.reserve_level = 'Low' THEN 4
                    WHEN ms.reserve_level = 'Depleted' THEN 5
                    ELSE 6 
                END as reserve_level_order
            FROM relevant_systems s
            JOIN mineral_signals ms ON s.id64 = ms.system_id64 
            AND ms.mineral_type IS NULL  -- For regular rings
            AND ms.ring_type = ANY(%s::text[])  -- Match any of the valid ring typess
            LEFT JOIN relevant_stations rs ON s.id64 = rs.system_id64
            LEFT JOIN stations st ON s.id64 = st.system_id64 AND rs.station_name = st.station_name
            WHERE """ + " AND ".join(where_conditions) + """
            ORDER BY sort_price DESC NULLS LAST, s.distance ASC"""

            if limit:
                query += " LIMIT %s"
                params.append(limit)

        else:
            # Build all WHERE conditions first
            where_conditions = ["1=1"]  # Start with a dummy condition
            params = []  # We'll build this in order of appearance in the query
            
            # Build the query with parameters in exact order of placeholders
            query = f"""
            WITH relevant_systems AS (
                SELECT s.*, SQRT(POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2)) as distance
                FROM systems s
                WHERE POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2) <= POWER(%s, 2)
            ),
            relevant_stations AS (
                SELECT sc.system_id64, sc.station_name, sc.sell_price, sc.demand
                FROM station_commodities sc
                WHERE (sc.commodity_name = %s OR (%s = 'LowTemperatureDiamond' AND sc.commodity_name = 'Low Temperature Diamonds'))
                AND sc.demand > 0 AND sc.sell_price > 0
            )"""
            
            # Add parameters in order of appearance
            params.extend([rx, rx, ry, ry, rz, rz, max_dist])  # Distance calculation
            params.extend([signal_type, signal_type])  # CTE parameters
            params.append(signal_type)  # For hotspot check in JOIN
            params.append(ring_type_filter)  # For ring type check in JOIN  
            
            # Build the rest of the query
            query += """
            SELECT DISTINCT s.name as system_name, s.id64 as system_id64, s.controlling_power,
                s.power_state, s.distance, ms.body_name, ms.ring_name, ms.ring_type,
                ms.mineral_type, ms.signal_count, ms.reserve_level, rs.station_name,
                st.landing_pad_size, st.distance_to_arrival as station_distance,
                st.station_type, rs.demand, rs.sell_price, st.update_time,
                rs.sell_price as sort_price,
                CASE 
                    WHEN ms.reserve_level = 'Pristine' THEN 1
                    WHEN ms.reserve_level = 'Major' THEN 2
                    WHEN ms.reserve_level = 'Common' THEN 3
                    WHEN ms.reserve_level = 'Low' THEN 4
                    WHEN ms.reserve_level = 'Depleted' THEN 5
                    ELSE 6 
                END as reserve_level_order
            FROM relevant_systems s"""

            query += """ JOIN mineral_signals ms ON s.id64 = ms.system_id64 
            AND (
                ms.mineral_type = %s  -- For hotspots
                OR (
                    ms.mineral_type IS NULL  -- For regular rings
                    AND ms.ring_type = %s    -- With matching ring type
                )
            )"""

            query += """
            LEFT JOIN relevant_stations rs ON s.id64 = rs.system_id64
            LEFT JOIN stations st ON s.id64 = st.system_id64 AND rs.station_name = st.station_name
            WHERE """

            # Add WHERE conditions in order
            if controlling_power:
                where_conditions.append("s.controlling_power = %s")
                params.append(controlling_power)

            if power_states:
                where_conditions.append("s.power_state = ANY(%s)")
                params.append(power_states)

            if mining_cond:
                where_conditions.append(mining_cond)
                params.extend(mining_params)

            if ring_cond:
                where_conditions.append(ring_cond.lstrip(" AND "))
                params.extend(ring_params)

            query += " AND ".join(where_conditions)

            # Add ORDER BY
            if is_ring_material:
                query += """
                ORDER BY 
                    reserve_level_order,
                    rs.sell_price DESC NULLS LAST,
                    s.distance ASC"""
            else:
                query += " ORDER BY sort_price DESC NULLS LAST, s.distance ASC"

            if limit:
                query += " LIMIT %s"
                params.append(limit)

        log_message(BLUE, "SEARCH", f"Final SQL query: {query}")
        log_message(BLUE, "SEARCH", f"Query parameters: {params}")
        
        try:
            cur.execute(query, params)
        except Exception as e:
            log_message(RED, "ERROR", f"Error executing query: {e}")
            return jsonify({'error': f'Error executing query: {e}'}), 500

        rows = cur.fetchall()
        app.logger.info(f"Query returned {len(rows)} rows")

        pr = []
        cur_sys = None

        station_pairs = [(r['system_id64'], r['station_name']) for r in rows if r['station_name']]
        other_commodities = {}

        if station_pairs:
            oc = conn.cursor()
            ph = ','.join(['(%s,%s)'] * len(station_pairs))
            ps = [x for pair in station_pairs for x in pair]
            sel_mats = request.args.getlist('selected_materials[]', type=str)

            if sel_mats and sel_mats != ['Default']:
                full_names = [mining_data.MATERIAL_CODES.get(m,m) for m in sel_mats]
                oc.execute(f"""
                    SELECT sc.system_id64, sc.station_name, sc.commodity_name, sc.sell_price, sc.demand,
                    COUNT(*) OVER (PARTITION BY sc.system_id64, sc.station_name) total_commodities
                    FROM station_commodities sc
                    WHERE (sc.system_id64, sc.station_name) IN ({ph})
                    AND sc.commodity_name = ANY(%s::text[])
                    AND sc.sell_price > 0 AND sc.demand > 0
                    ORDER BY sc.system_id64, sc.station_name, sc.sell_price DESC
                """, ps + [full_names])
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

        for row in rows:
            if cur_sys is None or cur_sys['name'] != row['system_name']:
                if cur_sys:
                    pr.append(cur_sys)
                cur_sys = {
                    'name': row['system_name'],
                    'controlling_power': row['controlling_power'],
                    'power_state': row['power_state'],
                    'distance': float(row['distance']),
                    'system_id64': row['system_id64'],
                    'rings': [],
                    'stations': [],
                    'all_signals': []
                }

            if is_ring_material:
                re = {
                    'name': row['ring_name'],
                    'body_name': row['body_name'],
                    'signals': f"{signal_type} ({row['ring_type']}, {row['reserve_level']})"
                }
                if re not in cur_sys['rings']:
                    cur_sys['rings'].append(re)
            else:
                if ring_type_filter == 'Without Hotspots':
                    re = {
                        'name': row['ring_name'],
                        'body_name': row['body_name'],
                        'signals': f"{signal_type} ({row['ring_type']}, {row['reserve_level']})"
                    }
                    if re not in cur_sys['rings']:
                        cur_sys['rings'].append(re)
                else:
                    if row['mineral_type'] == signal_type:
                        re = {
                            'name': row['ring_name'],
                            'body_name': row['body_name'],
                            'signals': f"{signal_type}: {row['signal_count'] or ''} ({row['reserve_level']})"
                        }
                        if re not in cur_sys['rings']:
                            cur_sys['rings'].append(re)

            si = {
                'ring_name': row['ring_name'],
                'mineral_type': row['mineral_type'],
                'signal_count': row['signal_count'] or '',
                'reserve_level': row['reserve_level'],
                'ring_type': row['ring_type']
            }
            if si not in cur_sys['all_signals'] and si['mineral_type']:
                cur_sys['all_signals'].append(si)

            if row['station_name']:
                try:
                    ex = next((s for s in cur_sys['stations'] if s['name'] == row['station_name']), None)
                    if ex:
                        ex['other_commodities'] = other_commodities.get((row['system_id64'], row['station_name']), [])
                    else:
                        stn = {
                            'name': row['station_name'],
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

        # Apply the limit here, after processing all results
        pr = pr[:limit]

        if not is_non_hotspot and pr:
            sys_ids = [s['system_id64'] for s in pr]
            cur.execute("""
                SELECT system_id64, ring_name, mineral_type, signal_count, reserve_level, ring_type
                FROM mineral_signals
                WHERE system_id64 = ANY(%s::bigint[]) AND mineral_type != %s
            """, [sys_ids, signal_type])

            other_sigs = {}
            for r in cur.fetchall():
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

        conn.close()
        return jsonify(pr)

    except Exception as e:
        app.logger.error(f"Search error: {str(e)}")
        return jsonify({'error': f'Search error: {str(e)}'}), 500

@app.route('/search_highest')
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
        
        # Build all WHERE conditions first
        where_conditions = ["sc.demand > 0", "sc.sell_price > 0"]
        params = []
        
        if controlling_power:
            where_conditions.append("s.controlling_power = %s")
            params.append(controlling_power)
        
        if power_states:
            where_conditions.append("s.power_state = ANY(%s)")
            params.append(power_states)
        
        where_clause = " AND ".join(where_conditions)
        
        # Get the list of non-hotspot materials
        non_hotspot = get_non_hotspot_materials_list()
        non_hotspot_str = ','.join([f"'{material}'" for material in non_hotspot])
        
        # Build ring type case statement
        ring_type_cases = []
        for material, ring_types in mining_data.NON_HOTSPOT_MATERIALS.items():
            ring_types_str = ','.join([f"'{rt}'" for rt in ring_types])
            ring_type_cases.append(f"WHEN hp.commodity_name = '{material}' AND ms.ring_type IN ({ring_types_str}) THEN 1")
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
                    WHEN hp.commodity_name NOT IN ({non_hotspot_str})
                        AND ms.mineral_type = hp.commodity_name THEN 1
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
        
        conn.close()
        return jsonify(formatted_results)
        
    except Exception as e:
        app.logger.error(f"Search highest error: {str(e)}")
        return jsonify({'error': f'Search highest error: {str(e)}'}), 500

@app.route('/get_price_comparison', methods=['POST'])
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

@app.route('/search_res_hotspots', methods=['POST'])
def search_res_hotspots():
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
        hotspot_data = res_data.load_res_data()
        if not hotspot_data:
            conn.close()
            return jsonify({'error': 'No RES hotspot data available'}), 404
            
        results = []
        for e in hotspot_data:
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
                'ls': e['ls'],
                'res_zone': e['res_zone'],
                'comment': e['comment'],
                'stations': st
            })
            
        conn.close()
        return jsonify(results)
    except Exception as e:
        app.logger.error(f"RES hotspot search error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/search_high_yield_platinum', methods=['POST'])
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
        app.logger.error(f"High yield platinum search error: {str(e)}")
        return jsonify({'error': str(e)}), 500
