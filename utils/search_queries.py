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
        AND CASE 
            WHEN %s = 'Any' THEN true  -- Any pad size
            WHEN st.landing_pad_size = 'Unknown' THEN true  -- Always include Unknown
            WHEN %s = 'S' THEN st.landing_pad_size = 'S'
            WHEN %s = 'M' THEN st.landing_pad_size = 'M'
            WHEN %s = 'L' THEN st.landing_pad_size = 'L'
            ELSE true  -- Fallback case
        END
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

    if ring_type_filter == 'Hotspots':
        if signal_type == 'Any':
            # For Any, just check that there is a hotspot
            join_condition += " AND ms.mineral_type IS NOT NULL"
        else:
            # For specific mineral, check for that hotspot
            join_condition += " AND ms.mineral_type = %s"
            join_params.append(signal_type)
    elif ring_type_filter == 'Without Hotspots':
        # No hotspots but specific ring type
        join_condition += " AND ms.mineral_type IS NULL AND ms.ring_type = ANY(%s::text[])"
        join_params.append(valid_ring_types)
    else:
        # All: Either has hotspot (of any type for Any, or specific type) or matches ring type
        if signal_type == 'Any':
            join_condition += " AND (ms.mineral_type IS NOT NULL OR (ms.mineral_type IS NULL AND ms.ring_type = ANY(%s::text[])))"
            join_params.append(valid_ring_types)
        else:
            join_condition += " AND (ms.mineral_type = %s OR (ms.mineral_type IS NULL AND ms.ring_type = ANY(%s::text[])))"
            join_params.extend([signal_type, valid_ring_types])

    return join_condition, join_params

def build_complete_query(params, coords, material, valid_ring_types, where_conditions, where_params):
    """Build complete query with all parameters"""
    rx, ry, rz = coords
    
    # Build ring join condition
    join_condition, join_params = get_ring_join_conditions(params['ring_type_filter'], params['signal_type'], valid_ring_types)
    
    # Add ring type filter to WHERE conditions if specific ring type selected
    if params['ring_type_filter'] not in ['Hotspots', 'Without Hotspots', 'All']:
        # When a specific ring type is selected, check if it's valid for this material
        if params['ring_type_filter'] in valid_ring_types:
            where_conditions.append("ms.ring_type = %s")
            where_params.append(params['ring_type_filter'])
        else:
            # If the selected ring type isn't valid for this material, force no results
            where_conditions.append("FALSE")
    
    # Add reserve level filter if not 'All'
    if params['reserve_level'] != 'All':
        where_conditions.append("ms.reserve_level = %s")
        where_params.append(params['reserve_level'])
    
    # Add system state filter - match any of the selected states
    if params['system_states'] != ["Any"]:
        where_conditions.append("s.system_state = ANY(%s::text[])")
        where_params.append(params['system_states'])
    
    # Build query
    query = get_base_cte()
    
    # Add station CTE if needed
    if params['min_demand'] > 0 or params['max_demand'] > 0 or material['name']:
        query += get_station_cte()
    
    query += get_main_select()
    query += get_main_joins().format(join_condition=join_condition)
    
    if where_conditions:
        query += " WHERE " + " AND ".join(where_conditions)
    
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
    if params['min_demand'] > 0 or params['max_demand'] > 0 or material['name']:
        query_params.extend([
            params['signal_type'],  # For commodity filter
            material['name'],  # For commodity filter
            params['min_demand'], params['max_demand'],  # Zero-zero check
            params['min_demand'], params['max_demand'],  # Min=0 check
            params['max_demand'], params['min_demand'],  # Max=0 check
            params['min_demand'], params['max_demand'],  # Between check
            params['landing_pad_size'],  # For Any/Unknown case
            params['landing_pad_size'],  # For S case
            params['landing_pad_size'],  # For M case
            params['landing_pad_size']   # For L case
        ])
    
    query_params.extend(join_params)
    query_params.extend(where_params)
    
    # Only add limit param for non-highest price searches
    if params['limit'] and params.get('display_format') != 'highest':
        query_params.append(params['limit'])
    
    return query, query_params