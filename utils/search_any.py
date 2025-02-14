"""Query builder for Any material search"""
from utils.search_queries import (
    get_base_cte, get_station_cte, get_ring_join_conditions,
    get_main_select, get_main_joins, get_order_by
)
from utils.common import log_message, BLUE

def build_any_material_query(params, coords, valid_ring_types, where_conditions, where_params):
    """Build query specifically for 'Any' material search that respects all filters"""
    rx, ry, rz = coords
    
    # Use same base CTE as normal search
    query = get_base_cte()
    
    # Add CTE for finding minable materials in each system
    query += """
    , minable_materials AS (
        SELECT DISTINCT ON (s.id64, ms.mineral_type, ms.ring_type)
            s.id64 as system_id64,
            COALESCE(ms.mineral_type, sc.commodity_name) as mineral_type,
            ms.ring_type,
            ms.reserve_level,
            ms.body_name,
            ms.ring_name,
            ms.signal_count
        FROM relevant_systems s
        JOIN mineral_signals ms ON s.id64 = ms.system_id64
        LEFT JOIN station_commodities sc ON s.id64 = sc.system_id64
        WHERE ms.ring_type = ANY(%s::text[])  -- Always apply valid ring types first
        AND CASE 
            WHEN %s = 'Hotspots' THEN ms.mineral_type IS NOT NULL
            WHEN %s = 'Without Hotspots' THEN ms.mineral_type IS NULL
            WHEN %s NOT IN ('Hotspots', 'Without Hotspots', 'All') THEN ms.ring_type = %s
            ELSE true
        END
        AND CASE
            WHEN %s != 'All' THEN ms.reserve_level = %s
            ELSE true
        END
    )
    , station_materials AS (
        SELECT 
            s.id64 as system_id64,
            sc.station_name,
            sc.commodity_name,
            sc.sell_price,
            sc.demand,
            st.landing_pad_size,
            st.distance_to_arrival,
            st.station_type,
            st.update_time,
            mm.mineral_type,
            mm.ring_type,
            mm.reserve_level,
            mm.body_name,
            mm.ring_name,
            mm.signal_count,
            ROW_NUMBER() OVER (PARTITION BY s.id64 ORDER BY sc.sell_price DESC) as price_rank
        FROM relevant_systems s
        JOIN minable_materials mm ON s.id64 = mm.system_id64
        JOIN station_commodities sc ON s.id64 = sc.system_id64 
            AND (mm.mineral_type = sc.commodity_name OR mm.ring_type = ANY(%s::text[]))
        JOIN stations st ON sc.system_id64 = st.system_id64 AND sc.station_name = st.station_name
        WHERE sc.sell_price > 0
        AND (
            (%s = 0 AND %s = 0) OR  -- No demand limits
            (%s = 0 AND sc.demand <= %s) OR  -- Only max
            (%s = 0 AND sc.demand >= %s) OR  -- Only min
            (sc.demand >= %s AND sc.demand <= %s)  -- Both
        )
        AND CASE 
            WHEN %s = 'Any' THEN true
            WHEN st.landing_pad_size = 'Unknown' THEN true
            WHEN %s = 'S' THEN st.landing_pad_size = 'S'
            WHEN %s = 'M' THEN st.landing_pad_size = 'M'
            WHEN %s = 'L' THEN st.landing_pad_size = 'L'
            ELSE true
        END
    )
    , best_prices AS (
        SELECT 
            s.id64 as system_id64,
            s.name as system_name,
            s.controlling_power,
            s.power_state,
            s.powers_acquiring,
            s.system_state,
            s.distance,
            sm.commodity_name as mineral_type,  -- Use commodity_name as mineral_type
            sm.ring_type,
            sm.reserve_level,
            sm.body_name,
            sm.ring_name,
            sm.signal_count,
            sm.station_name,
            sm.landing_pad_size,
            sm.distance_to_arrival,
            sm.station_type,
            sm.update_time,
            sm.sell_price,
            sm.demand
        FROM relevant_systems s
        JOIN station_materials sm ON s.id64 = sm.system_id64
        WHERE sm.price_rank = 1
    )"""
    
    # Modify where conditions to use bp instead of s
    modified_where_conditions = []
    for condition in where_conditions:
        modified_condition = condition.replace('s.', 'bp.')
        modified_where_conditions.append(modified_condition)
    
    # Use same query structure for both formats - order by price
    query += """
    SELECT DISTINCT ON (bp.sell_price)
        bp.system_name,
        bp.system_id64,
        bp.controlling_power,
        bp.power_state,
        bp.powers_acquiring,
        bp.system_state,
        bp.distance,
        bp.mineral_type,  -- This will be the commodity_name from the station
        bp.mineral_type as commodity_name,  -- Add this for consistency with other search types
        bp.ring_type,
        bp.reserve_level,
        bp.body_name,
        bp.ring_name,
        bp.signal_count,
        bp.station_name,
        bp.landing_pad_size,
        bp.distance_to_arrival,
        bp.station_type,
        bp.update_time,
        bp.sell_price,
        bp.demand
    FROM best_prices bp
    """
    if modified_where_conditions:
        query += " WHERE " + " AND ".join(modified_where_conditions)
    query += " ORDER BY bp.sell_price DESC"
    
    if params.get('limit'):
        query += " LIMIT %s"
    
    # Build parameters in same order as normal search
    query_params = [
        rx, ry, rz,  # Distance calculation
        rx, ry, rz,  # Distance filter
        params['max_dist'],
        # Minable materials params
        valid_ring_types,  # For ring type check
        params['ring_type_filter'],  # For hotspots check
        params['ring_type_filter'],  # For without hotspots check
        valid_ring_types,  # For without hotspots ring types
        params['ring_type_filter'],  # For specific ring type check
        params['ring_type_filter'],  # For specific ring type value
        params['reserve_level'],  # For reserve level check
        params['reserve_level'],  # For reserve level value
        # Station prices params
        valid_ring_types,  # For ring type matching
        params['min_demand'], params['max_demand'],  # Zero-zero check
        params['min_demand'], params['max_demand'],  # Min=0 check
        params['max_demand'], params['min_demand'],  # Max=0 check
        params['min_demand'], params['max_demand'],  # Between check
        params['landing_pad_size'],  # For Any/Unknown case
        params['landing_pad_size'],  # For S case
        params['landing_pad_size'],  # For M case
        params['landing_pad_size']   # For L case
    ]
    
    # Add power condition params
    query_params.extend(where_params)
    
    # Add limit if specified
    if params.get('limit'):
        query_params.append(params['limit'])
    
    return query, query_params 