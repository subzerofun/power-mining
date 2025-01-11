import json
from datetime import datetime
from flask import jsonify, request
import mining_data
from mining_data import (
    get_non_hotspot_materials_list,
    normalize_commodity_name,
    get_price_comparison,
    get_mining_type_conditions,
    PRICE_DATA,
    NON_HOTSPOT_MATERIALS
)
import res_data
from utils.common import (
    get_db_connection, log_message, 
    BLUE, RED, get_ring_materials
)

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

        # Get reference system coordinates
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

        # Load material mining data
        with open('data/mining_data.json', 'r') as f:
            mat_data = json.load(f)
            log_message(BLUE, "SEARCH", f"Checking material {signal_type} in mining_data.json")
            cd = next((i for i in mat_data['materials'] if i['name'] == signal_type), None)
            if not cd:
                log_message(RED, "SEARCH", f"Material {signal_type} not found in mining_data.json")
                return jsonify([])
            log_message(BLUE, "SEARCH", f"Material data: {cd}")

        # Build WHERE conditions
        where_conditions = []
        where_params = []

        if controlling_power:
            where_conditions.append("s.controlling_power = %s")
            where_params.append(controlling_power)

        if power_states:
            where_conditions.append("s.power_state = ANY(%s::text[])")
            where_params.append(power_states)

        # Get mining type conditions if specified
        mining_cond = ''
        mining_params = []
        if mining_types and 'All' not in mining_types:
            mining_cond, mining_params = get_mining_type_conditions(signal_type, mining_types)
            if mining_cond:
                where_conditions.append(mining_cond)
                where_params.extend(mining_params)

        # Handle ring type filter
        ring_cond = ''
        ring_params = []
        if ring_type_filter != 'All':
            if ring_type_filter == 'Just Hotspots':
                ring_cond = ' AND ms.mineral_type = %s'
                ring_params.append(signal_type)
            elif ring_type_filter == 'Without Hotspots':
                # For "Without Hotspots", include rings where the material can be mined without hotspots
                valid_ring_types = []
                for ring_type, ring_data in cd['ring_types'].items():
                    if any([
                        ring_data.get('surfaceLaserMining', False),
                        ring_data.get('surfaceDeposit', False),
                        ring_data.get('subSurfaceDeposit', False)
                    ]):
                        valid_ring_types.append(ring_type)
                if valid_ring_types:
                    ring_cond = ' AND ms.mineral_type IS NULL AND ms.ring_type = ANY(%s::text[])'
                    ring_params.append(valid_ring_types)
            else:
                ring_cond = ' AND ms.ring_type = %s'
                ring_params.append(ring_type_filter)

        # Add ring conditions to WHERE clause if present
        if ring_cond:
            where_conditions.append(ring_cond.lstrip(' AND'))
            where_params.extend(ring_params)

        # Add parameters for the query
        params = [
            rx, ry, rz,  # Distance calculation in subquery
            rx, ry, rz,  # Distance filter in subquery
            max_dist,    # Maximum distance
            signal_type, # For station_commodities
            signal_type, # For LTD check
            signal_type, # For hotspots
            signal_type  # For regular rings
        ]

        # Add parameters for WHERE conditions
        params.extend(where_params)

        # Now build the complete query with all conditions
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
        JOIN mineral_signals ms ON s.id64 = ms.system_id64 
        AND (
            ms.mineral_type = %s  -- For hotspots
            OR (
                ms.mineral_type IS NULL  -- For regular rings
                AND ms.ring_type = %s    -- With matching ring type
            )
        )
        LEFT JOIN relevant_stations rs ON s.id64 = rs.system_id64
        LEFT JOIN stations st ON s.id64 = st.system_id64 AND rs.station_name = st.station_name
        """

        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)

        query += """
        ORDER BY 
            sort_price DESC,
            reserve_level_order ASC,
            distance ASC
        """

        if limit:
            query += " LIMIT %s"
            params.append(limit)

        # Execute query and process results
        log_message(BLUE, "SEARCH", f"Executing query with params: {params}")
        c.execute(query, params)
        rows = c.fetchall()
        log_message(BLUE, "SEARCH", f"Query returned {len(rows)} rows")

        # Process results
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
