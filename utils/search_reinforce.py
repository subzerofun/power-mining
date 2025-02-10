import json
from datetime import datetime
from flask import jsonify, request
from utils.mining_data import get_price_comparison, PRICE_DATA, normalize_commodity_name
from utils.common import log_message, get_db_connection, YELLOW, RED, BLUE
from utils import res_data

# Import our new modular components
from utils.search_common import (
    get_search_params, log_search_params, get_reference_coords,
    load_material_data, get_valid_ring_types, get_other_commodities,
    format_power_info, format_station_info, get_other_signals,
    format_ring_info, SYSTEM_IN_RING_NAME
)
from utils.search_queries import (
    get_base_cte, get_station_cte, get_ring_join_conditions,
    get_main_select, get_main_joins, get_order_by,
    build_complete_query
)
from utils.search_power import build_reinforce_conditions
from utils.search_any import build_any_material_query

def search(display_format='full'):
    """Main search function for Reinforce power goal
    Args:
        display_format (str): 'full' for detailed view or 'highest' for highest prices view
    """
    try:
        # Get search parameters
        params = get_search_params()
        params['display_format'] = display_format  # Add display format to params
        log_search_params(params)

        # Get database connection
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        # Get reference coordinates
        coords = get_reference_coords(conn, params['ref_system'])
        if not coords:
            conn.close()
            return jsonify({'error': 'Reference system not found'}), 404

        # Load material data
        material = load_material_data(params['signal_type'])
        if not material:
            conn.close()
            return jsonify([])

        # Get valid ring types based on mining types
        valid_ring_types = get_valid_ring_types(material, params['mining_types'])
        if not valid_ring_types:
            conn.close()
            return jsonify([])

        # Build power conditions
        where_conditions = []
        where_params = []
        
        # Handle power filtering
        if not params['controlling_power'] or params['controlling_power'] == "Any":
            # Must have an actual power (not NULL)
            where_conditions.append("s.controlling_power IS NOT NULL")
            print("Any selected")
        elif params['controlling_power'] == "None":
            # Must be NULL in database and not contested - combine all conditions
            where_conditions.append("(s.controlling_power IS NULL AND (s.powers_acquiring IS NULL OR s.powers_acquiring = '[]'::jsonb) AND s.power_state IS NULL)")
            print("None selected")
        elif params['controlling_power']:
            # Specific power selected - use reinforcement conditions
            opposing_power = request.args.get('opposing_power', 'Any')
            power_conditions = build_reinforce_conditions(params['controlling_power'], opposing_power)
            if power_conditions:
                where_conditions.extend(power_conditions[0])
                where_params.extend(power_conditions[1])

        # Build and execute query based on signal type
        if params['signal_type'] == 'Any':
            query, query_params = build_any_material_query(
                params, coords, valid_ring_types,
                where_conditions, where_params
            )
        else:
            query, query_params = build_complete_query(
                params, coords, material, valid_ring_types,
                where_conditions, where_params
            )
        
        log_message(BLUE, "SEARCH", f"Executing query with params: {query_params}")
        c = conn.cursor()
        c.execute(query, query_params)
        rows = c.fetchall()
        
        # Debug log the first row's fields
        if rows:
            row_dict = dict(rows[0])
            log_message(BLUE, "DEBUG", f"Available fields in row: {list(row_dict.keys())}")
            log_message(BLUE, "DEBUG", f"First row values: {row_dict}")
        
        log_message(BLUE, "SEARCH", f"Query returned {len(rows)} rows")
        
        # Process results based on display format
        if display_format == 'highest':
            # Format for highest prices display
            formatted_results = []
            seen_keys = set()  # Track unique combinations
            
            # First pass: collect all results
            for row in rows:
                # Create a unique key for this combination
                key = (row['system_id64'], row['station_name'])
                
                # Skip if we've seen this exact combination before
                if key in seen_keys:
                    continue
                    
                seen_keys.add(key)
                
                formatted_results.append({
                    'mineral_type': row['mineral_type'],  # Include both fields
                    'commodity_name': row['commodity_name'],  # Include both fields
                    'max_price': int(row['sell_price']) if row['sell_price'] is not None else 0,
                    'system_name': row['system_name'],
                    'controlling_power': row['controlling_power'],
                    'power_state': row['power_state'],
                    'powers_acquiring': row['powers_acquiring'] if row['powers_acquiring'] else [],
                    'landing_pad_size': row['landing_pad_size'],
                    'distance_to_arrival': float(row['distance_to_arrival']) if row['distance_to_arrival'] is not None else 0,
                    'demand': int(row['demand']) if row['demand'] is not None else 0,
                    'reserve_level': row['reserve_level'],
                    'station_name': row['station_name'],
                    'station_type': row['station_type'],
                    'update_time': row['update_time'].isoformat() if row['update_time'] is not None else None
                })
            
            # Sort by price descending
            formatted_results.sort(key=lambda x: (-x['max_price'], x['system_name'], x['station_name']))
            
            return jsonify(formatted_results)
        
        # Process results for full display format
        result = []
        current_system = None
        
        # Get other commodities for stations
        station_pairs = [(r['system_id64'], r['station_name']) for r in rows if r['station_name']]
        other_commodities = {}

        if station_pairs:
            oc = conn.cursor()
            ph = ','.join(['(%s,%s)'] * len(station_pairs))
            ps = [x for pair in station_pairs for x in pair]
            sel_mats = params['sel_mats']
            log_message(BLUE, "SEARCH", f"Selected materials: {sel_mats}")

            if sel_mats and sel_mats != ['Default']:
                # Map short->full from mining_materials.json
                with open('data/mining_materials.json', 'r') as f:
                    mat_data = json.load(f)['materials']
                # Create mapping of short names to full names
                short_to_full = {mat['short']: mat['name'] for mat in mat_data.values()}
                # Convert short names to full names
                full_names = [short_to_full.get(short, short) for short in sel_mats]
                log_message(BLUE, "SEARCH", f"Using selected materials filter with full names: {full_names}")
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
            if current_system is None or current_system['name'] != row['system_name']:
                if current_system:
                    result.append(current_system)
                
                # Format power information for display
                power_info = format_power_info(row)
                
                current_system = {
                    'name': row['system_name'],
                    'controlling_power': row['controlling_power'],  # Raw power name for icon coloring
                    'power_info': power_info,  # Formatted HTML for display
                    'power_state': row['power_state'],
                    'system_state': row['system_state'],
                    'powers_acquiring': row['powers_acquiring'] if row['powers_acquiring'] else [],
                    'distance': float(row['distance']),
                    'system_id64': row['system_id64'],
                    'rings': [],
                    'stations': [],
                    'all_signals': []
                }

            # Add ring information
            ring_info = format_ring_info(row, material, params['signal_type'])
            if ring_info and ring_info not in current_system['rings']:
                current_system['rings'].append(ring_info)

            # Add station information
            if row['station_name']:
                station_info = format_station_info(row, other_commodities)
                if station_info:
                    existing = next((s for s in current_system['stations'] if s['name'] == station_info['name']), None)
                    if not existing:
                        current_system['stations'].append(station_info)

        # Add the last system
        if current_system:
            result.append(current_system)

        # Get other signals for each system
        system_ids = [r['system_id64'] for r in rows]
        other_signals = get_other_signals(conn, system_ids, params['signal_type'])
        
        # Add other signals to systems
        for system in result:
            system['all_signals'] = other_signals.get(system['system_id64'], [])

        conn.close()
        return jsonify(result)

    except Exception as e:
        log_message(RED, "ERROR", f"Search error: {str(e)}")
        return jsonify({'error': str(e)}), 500

