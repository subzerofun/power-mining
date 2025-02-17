"""SQL query templates for different search modes"""

def get_base_cte():
    """Get the base Common Table Expression for system distance calculation"""
    return """
    WITH relevant_systems AS (
        SELECT s.*, SQRT(POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2)) as distance
        FROM systems s
        WHERE POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2) <= POWER(%s, 2)
    )
    """

def get_station_cte():
    """Get the Common Table Expression for filtering stations"""
    return """
    , relevant_stations AS (
        SELECT sc.system_id64, sc.station_name, sc.commodity_name, sc.sell_price, sc.demand
        FROM station_commodities sc
        JOIN stations st ON sc.system_id64 = st.system_id64 AND sc.station_name = st.station_name
        WHERE (%s = 'Any' OR sc.commodity_name = %s)  -- Handle 'Any' case
        AND sc.sell_price > 0
        AND (
            (%s = 0 AND %s = 0) OR  -- No demand limits specified
            (%s = 0 AND sc.demand <= %s) OR  -- Only max specified
            (%s = 0 AND sc.demand >= %s) OR  -- Only min specified
            (sc.demand >= %s AND sc.demand <= %s)  -- Both min and max specified
        )
        AND (
            %s = 'Any' OR  -- Any pad size
            st.landing_pad_size = 'Unknown' OR  -- Always include Unknown
            st.landing_pad_size = %s  -- Match exact pad size
        )
    )
    """

def get_main_select():
    """Get the main SELECT statement"""
    return """
    SELECT DISTINCT ON (s.name, st.station_name, ms.ring_name, ms.mineral_type, s.distance, CASE WHEN rs.sell_price IS NULL THEN 1 ELSE 0 END, COALESCE(rs.sell_price, 0))
        s.name as system_name,
        s.id64 as system_id64,
        s.controlling_power,
        s.power_state,
        s.powers_acquiring,
        s.system_state as system_state,
        s.distance,
        ms.ring_name,
        ms.body_name,
        ms.mineral_type,
        ms.signal_count,
        ms.reserve_level,
        ms.ring_type,
        st.station_name,
        st.landing_pad_size,
        st.distance_to_arrival,
        st.station_type,
        st.update_time,
        rs.sell_price,
        rs.demand,
        CASE 
            WHEN ms.mineral_type IS NOT NULL THEN ms.mineral_type
            ELSE rs.commodity_name
        END as commodity_name
    """

def get_main_joins():
    """Get the main JOIN statements with placeholder for join condition"""
    return """
    FROM relevant_systems s
    JOIN mineral_signals ms ON {join_condition}
    LEFT JOIN relevant_stations rs ON s.id64 = rs.system_id64
    LEFT JOIN stations st ON s.id64 = st.system_id64 AND rs.station_name = st.station_name
    """

def get_order_by():
    """Get the ORDER BY clause"""
    return """
    ORDER BY COALESCE(rs.sell_price, 0) DESC,
             s.distance
    """

def get_ring_join_conditions(ring_type_filter, signal_type, valid_ring_types):
    """Get the JOIN conditions for ring/hotspot filtering"""
    join_condition = "s.id64 = ms.system_id64"
    join_params = []

    # Always apply valid ring types filter first
    mineral_condition = "ms.ring_type = ANY(%s::text[])"
    join_params.append(valid_ring_types)

    # Then add mineral type conditions
    if ring_type_filter == 'Hotspots':
        if signal_type == 'Any':
            mineral_condition += " AND ms.mineral_type IS NOT NULL"
        else:
            mineral_condition += " AND ms.mineral_type = %s"
            join_params.append(signal_type)
    elif ring_type_filter == 'Without Hotspots':
        mineral_condition += " AND ms.mineral_type IS NULL"
    else:  # 'All' or specific ring type
        if signal_type == 'Any':
            mineral_condition += " AND (ms.mineral_type IS NOT NULL OR ms.mineral_type IS NULL)"
        else:
            mineral_condition += " AND (ms.mineral_type = %s OR ms.mineral_type IS NULL)"
            join_params.append(signal_type)

    # Return just the mineral conditions
    return mineral_condition, join_params

def build_complete_query(params, coords, material, valid_ring_types, where_conditions, where_params):
    """Build complete query with all parameters"""
    rx, ry, rz = coords
    
    # Get mineral conditions
    mineral_condition, join_params = get_ring_join_conditions(params['ring_type_filter'], params['signal_type'], valid_ring_types)
    
    # Build query
    query = get_base_cte()
    
    # Add station CTE if needed
    if params['min_demand'] > 0 or params['max_demand'] > 0 or (material and material['name']):
        query += get_station_cte()
    
    query += get_main_select()
    query += get_main_joins().format(join_condition="s.id64 = ms.system_id64")
    
    # Build WHERE conditions
    all_conditions = []
    
    # Add ring type filter if specific ring type selected
    if params['ring_type_filter'] not in ['Hotspots', 'Without Hotspots', 'All']:
        if params['ring_type_filter'] in valid_ring_types:
            all_conditions.append("ms.ring_type = %s")
            where_params.append(params['ring_type_filter'])
        else:
            all_conditions.append("FALSE")
    
    # Add reserve level filter if not 'All'
    if params['reserve_level'] != 'All':
        all_conditions.append("ms.reserve_level = %s")
        where_params.append(params['reserve_level'])
    
    # Add existing conditions
    all_conditions.extend(where_conditions)
    
    # Add system state filter
    if params['system_states'] != ["Any"]:
        all_conditions.append("s.system_state = ANY(%s::text[])")
        where_params.append(params['system_states'])
    
    # Add mineral conditions
    if mineral_condition:
        all_conditions.append(mineral_condition)
    
    # Add WHERE clause if we have conditions
    if all_conditions:
        query += " WHERE " + " AND ".join(all_conditions)
    
    query += get_order_by()
    
    # Only apply limit for non-highest price searches
    if params['limit'] and params.get('display_format') != 'highest':
        query += " LIMIT %s"
    
    # Build parameters
    query_params = [
        rx, ry, rz,  # Distance calculation
        rx, ry, rz,  # Distance filter
        params['max_dist']
    ]
    
    # Add station CTE params if needed
    if params['min_demand'] > 0 or params['max_demand'] > 0 or (material and material['name']):
        query_params.extend([
            params['signal_type'],  # For commodity filter
            material['name'],  # For commodity filter
            params['min_demand'], params['max_demand'],  # Zero-zero check
            params['min_demand'], params['max_demand'],  # Min=0 check
            params['max_demand'], params['min_demand'],  # Max=0 check
            params['min_demand'], params['max_demand'],  # Between check
            params['landing_pad_size'],  # For Any case
            params['landing_pad_size']   # For exact pad size match
        ])
    
    query_params.extend(where_params)
    query_params.extend(join_params)
    
    # Only add limit param for non-highest price searches
    if params['limit'] and params.get('display_format') != 'highest':
        query_params.append(params['limit'])
    
    return query, query_params


def build_optimized_query(params, coords, material, valid_ring_types, where_conditions, where_params):
    """Build optimized query following specific filtering order"""
    rx, ry, rz = coords
    
    # Get mineral conditions
    mineral_condition, join_params = get_ring_join_conditions(params['ring_type_filter'], params['signal_type'], valid_ring_types)
    
    # Debug logging for initial conditions
    print("\nInitial conditions:")
    print("Where conditions:", where_conditions)
    print("Where params:", where_params)
    print("Mineral condition:", mineral_condition)
    print("Join params:", join_params)
    print("Total initial params:", len(where_params) + len(join_params))
    
    # Build query following the step-by-step filtering approach
    # Each CTE applies filters in a specific order for optimization
    query = """
    WITH 
    -- Step 1: Filter systems by distance, power, mining type
    mineable_systems AS (
        -- First get all valid mineable systems
        SELECT s.*, ms.*, SQRT(POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2)) as distance
        FROM systems s
        JOIN mineral_signals ms ON s.id64 = ms.system_id64
        WHERE POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2) <= POWER(%s, 2)  -- Distance filter
        """ + (" AND " + " AND ".join(where_conditions) if where_conditions else "") + """  -- Power filters
        """ + (" AND " + mineral_condition if mineral_condition else "") + """  -- Mining type validation
        """ + (""" AND s.system_state = ANY(%s::text[])""" if params['system_states'] != ["Any"] else "") + """  -- System state filter
        """ + ("""AND ms.ring_type = %s""" if params['ring_type_filter'] not in ['Hotspots', 'Without Hotspots', 'All'] else "") + ("""
        AND ms.reserve_level = %s""" if params['reserve_level'] != 'All' else "") + """  -- Reserve level filter
    ),
    -- Debug mineable systems
    debug_mineable AS (
        SELECT COUNT(*) as count FROM mineable_systems
    ),
    -- Step 2: Join with stations and apply station filters
    sellable_systems AS (
        -- Then find valid stations in those systems
        SELECT ms.*, st.station_name, st.landing_pad_size, st.distance_to_arrival, st.station_type, st.update_time,
               sc.sell_price, sc.demand, sc.commodity_name
        FROM mineable_systems ms
        LEFT JOIN stations st ON ms.id64 = st.system_id64
        LEFT JOIN station_commodities sc ON ms.id64 = sc.system_id64 AND st.station_name = sc.station_name
        WHERE TRUE
        """ + ("""
        AND (
            %s = 'Any' OR  -- Any pad size
            st.landing_pad_size = 'Unknown' OR  -- Always include Unknown
            st.landing_pad_size = %s  -- Match exact pad size
        )""") + """
        """ + ("""
        AND (%s = 'Any' OR sc.commodity_name = %s)  -- Material sellable check
        AND sc.sell_price > 0  -- Only positive prices
        AND (  -- Demand filter
            (%s = 0 AND %s = 0) OR
            (%s = 0 AND sc.demand <= %s) OR
            (%s = 0 AND sc.demand >= %s) OR
            (sc.demand >= %s AND sc.demand <= %s)
        )""" if params['min_demand'] > 0 or params['max_demand'] > 0 or (material and material['name']) else "") + """
    ),
    -- Debug sellable systems
    debug_sellable AS (
        SELECT COUNT(*) as count FROM sellable_systems
    ),
    -- Step 3: Rank systems by price and distance
    system_ranks AS (
        SELECT name,
               MAX(COALESCE(sell_price, 0)) as max_price,
               ROW_NUMBER() OVER (ORDER BY MAX(COALESCE(sell_price, 0)) DESC, MIN(distance)) as system_rank
        FROM sellable_systems
        GROUP BY name
    ),
    -- Debug system ranks
    debug_ranks AS (
        SELECT COUNT(*) as count FROM system_ranks
    ),
    -- Step 4: Get all stations for ranked systems
    ranked_stations AS (
        SELECT s.*, sr.system_rank, sr.max_price
        FROM sellable_systems s
        JOIN system_ranks sr ON s.name = sr.name
        WHERE s.sell_price >= sr.max_price  -- Only keep stations with max price
        """ + ("""AND sr.system_rank <= %s""" if params['limit'] and params.get('display_format') != 'highest' else "") + """
    ),
    -- Debug ranked stations
    debug_ranked AS (
        SELECT COUNT(*) as count FROM ranked_stations
    )
SELECT 
    s.name as system_name,
    s.id64 as system_id64,
    s.controlling_power,
    s.power_state,
    s.powers_acquiring,
    s.system_state,
    s.distance,
    s.ring_name,
    s.body_name,
    s.mineral_type,
    s.signal_count,
    s.reserve_level,
    s.ring_type,
    s.station_name,
    s.landing_pad_size,
    s.distance_to_arrival,
    s.station_type,
    s.update_time,
    """ + ("s.sell_price, s.demand, " if (params['min_demand'] > 0 or params['max_demand'] > 0 or (material and material['name'])) else "NULL as sell_price, NULL as demand, ") + """
    CASE 
        WHEN s.mineral_type IS NOT NULL THEN s.mineral_type
        ELSE """ + ("s.commodity_name" if (params['min_demand'] > 0 or params['max_demand'] > 0 or (material and material['name'])) else "NULL") + """
    END as commodity_name
FROM ranked_stations s
ORDER BY s.system_rank,
         s.sell_price DESC NULLS LAST,
         s.distance"""
    
    # Build parameters in exact order of usage in SQL
    query_params = [
        rx, ry, rz,  # Distance calculation
        rx, ry, rz,  # Distance filter
        params['max_dist']
    ]
    
    # Add power params - only if we have them
    if where_params:
        query_params.extend(where_params)
    
    # Add mineral params
    query_params.extend(join_params)
    
    # Add system state params
    if params['system_states'] != ["Any"]:
        query_params.append(params['system_states'])
    
    # Add ring type params
    if params['ring_type_filter'] not in ['Hotspots', 'Without Hotspots', 'All']:
        query_params.append(params['ring_type_filter'])
    
    # Add reserve level params
    if params['reserve_level'] != 'All':
        query_params.append(params['reserve_level'])
    
    
    # Always add landing pad params
    query_params.extend([
        params['landing_pad_size'],  # For Any case
        params['landing_pad_size']   # For exact pad size match
    ])

    # Add demand params
    if params['min_demand'] > 0 or params['max_demand'] > 0 or (material and material['name']):
        query_params.extend([
            params['signal_type'],  # For Any check
            material['name'],       # For commodity match
            params['min_demand'], params['max_demand'],  # Zero-zero check
            params['min_demand'], params['max_demand'],  # Min=0 check
            params['max_demand'], params['min_demand'],  # Max=0 check
            params['min_demand'], params['max_demand']   # Between check
        ])

    
    # Add limit param
    if params['limit'] and params.get('display_format') != 'highest':
        query_params.append(params['limit'])
    
    # Debug logging for query execution
    print("\nDebug counts will be shown after execution")
    print("\nFinal query params:", query_params)
    print("Total params:", len(query_params))
    
    # Count placeholders in query
    placeholder_count = query.count('%s')
    print("\nPlaceholder count:", placeholder_count)
    print("Parameter count:", len(query_params))
    if placeholder_count != len(query_params):
        print("WARNING: Mismatch between placeholders and parameters!")
        print("Query needs", placeholder_count, "params but got", len(query_params))
    
    return query, query_params
