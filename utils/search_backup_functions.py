def search_highest_2():
    """Search for highest prices based on power goal"""
    try:
        # Get power goal and basic filters
        power_goal = request.args.get('power_goal', 'Reinforce')  # Default to Reinforce if not specified
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
        
        # Handle power goal filtering
        if power_goal == 'Reinforce':
            if controlling_power and controlling_power != 'Any':
                where_conditions.append("s.controlling_power = %s")
                params.append(controlling_power)
        elif power_goal == 'Undermine':
            if controlling_power and controlling_power != 'Any':
                where_conditions.append("s.controlling_power != %s")
                params.append(controlling_power)
        elif power_goal == 'Acquisition':
            if controlling_power and controlling_power != 'Any':
                # First find systems controlled by your power that are Fortified or Stronghold
                base_systems_query = """
                    WITH PowerSystems AS (
                        SELECT id64, x, y, z 
                        FROM systems 
                        WHERE controlling_power = %s 
                        AND power_state IN ('Fortified', 'Stronghold')
                    )
                """
                params.append(controlling_power)
                
                # Then find unoccupied systems within range of those systems
                where_conditions.append("""
                    EXISTS (
                        SELECT 1 FROM PowerSystems ps
                        WHERE sqrt(power(s.x - ps.x, 2) + power(s.y - ps.y, 2) + power(s.z - ps.z, 2)) <= 
                            CASE 
                                WHEN s.power_state = 'Fortified' THEN 20
                                WHEN s.power_state = 'Stronghold' THEN 30
                                ELSE 0
                            END
                    )
                    AND s.controlling_power IS NULL 
                    AND (s.powers_acquiring IS NULL OR s.powers_acquiring = '[]'::jsonb)
                """)
        
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
                s.powers_acquiring::jsonb as powers_acquiring,
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
            powers_acquiring,
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
                'powers_acquiring': row['powers_acquiring'] if row['powers_acquiring'] else [],
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

def search_res_hotspots():
    """RES hotspots search - keeping original implementation"""
    try:
        # Get reference system from query parameters
        ref_system = request.args.get('system', 'Sol')
        
        print("Testing");

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
    """High yield platinum search - keeping original implementation"""
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