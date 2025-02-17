"""Query builder for Any material search"""
from utils.search_queries import (
    get_base_cte, get_station_cte, get_ring_join_conditions,
    get_main_select, get_main_joins, get_order_by
)
from utils.common import log_message, BLUE

def build_any_material_query(params, coords, valid_ring_types, where_conditions, where_params):
    """Build query specifically for 'Any' material search that respects all filters"""
    rx, ry, rz = coords

    # Debug logging of ALL input parameters
    log_message(BLUE, "SEARCH", "Input parameters:")
    log_message(BLUE, "SEARCH", f"- Reference system coordinates: ({rx}, {ry}, {rz})")
    log_message(BLUE, "SEARCH", f"- Max distance: {params['max_dist']}")
    log_message(BLUE, "SEARCH", f"- Power conditions: {where_conditions}")
    log_message(BLUE, "SEARCH", f"- Power params: {where_params}")
    log_message(BLUE, "SEARCH", f"- System state: {params.get('system_state', 'Any')}")
    log_message(BLUE, "SEARCH", f"- Ring type filter: {params['ring_type_filter']}")
    log_message(BLUE, "SEARCH", f"- Valid ring types: {valid_ring_types}")
    log_message(BLUE, "SEARCH", f"- Landing pad size: {params['landing_pad_size']}")
    log_message(BLUE, "SEARCH", f"- Demand range: {params['min_demand']} - {params['max_demand']}")
    
    # Use same base CTE as normal search but force materialization
    query = """
    WITH RECURSIVE
    -- Step 1: Filter by power conditions first (simple index lookup)
    filtered_by_power AS MATERIALIZED (
        SELECT s.*
        FROM systems s
        WHERE """ + ((" AND ".join(where_conditions)) if where_conditions else "TRUE") + """
    )
    -- Step 2: Filter by system state (simple index lookup)
    , filtered_by_state AS MATERIALIZED (
        SELECT s.*
        FROM filtered_by_power s
        WHERE CASE 
            WHEN %s != 'Any' THEN s.system_state = %s
            ELSE true
        END
    )
    -- Step 3: Calculate distance only on remaining systems
    , relevant_systems AS MATERIALIZED (
        SELECT s.*, SQRT(POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2)) as distance
        FROM filtered_by_state s
        WHERE POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2) <= POWER(%s, 2)
    )
    -- Step 4: Find valid mining systems (no commodity data yet!)
    , minable_materials AS MATERIALIZED (
        SELECT DISTINCT ON (s.id64, ms.mineral_type, ms.ring_type)
            s.id64 as system_id64,
            s.name,
            s.controlling_power,
            s.power_state,
            s.powers_acquiring,
            s.system_state,
            s.distance,
            ms.mineral_type,
            ms.ring_type,
            ms.reserve_level,
            ms.body_name,
            ms.ring_name,
            ms.signal_count
        FROM relevant_systems s
        JOIN mineral_signals ms ON s.id64 = ms.system_id64
        WHERE ms.ring_type = ANY(%s::text[])  -- Apply valid ring types first
        AND CASE 
            WHEN %s = 'Hotspots' THEN ms.mineral_type IS NOT NULL
            WHEN %s = 'Without Hotspots' THEN ms.mineral_type IS NULL
            ELSE true  -- For 'All' or specific ring type, don't filter on mineral_type
        END
    )
    -- Step 5: Only NOW look at stations for valid mining systems
    , filtered_stations AS MATERIALIZED (
        SELECT 
            st.system_id64,
            st.station_id,
            st.station_name,
            st.landing_pad_size,
            st.distance_to_arrival,
            st.station_type,
            st.update_time,
            sc.commodity_name,
            sc.sell_price,
            sc.demand
        FROM stations st
        JOIN station_commodities sc ON st.system_id64 = sc.system_id64 
            AND st.station_id = sc.station_id
        WHERE sc.sell_price > 0
        AND EXISTS (SELECT 1 FROM minable_materials mm WHERE mm.system_id64 = st.system_id64)  -- Only look at remaining systems
        AND (
            (%s = 0 AND %s = 0) OR  -- No demand limits
            (%s = 0 AND sc.demand <= %s) OR  -- Only max
            (%s = 0 AND sc.demand >= %s) OR  -- Only min
            (sc.demand >= %s AND sc.demand <= %s)  -- Both
        )
        AND (
            %s = 'Any' OR  -- Any pad size
            st.landing_pad_size = 'Unknown' OR  -- Always include Unknown
            st.landing_pad_size = %s  -- Match exact pad size
        )
    )
    -- Step 6: Match stations with minerals
    , station_materials AS MATERIALIZED (
        SELECT 
            mm.*,
            fs.station_id,
            fs.station_name,
            fs.landing_pad_size,
            fs.distance_to_arrival,
            fs.station_type,
            fs.update_time,
            fs.commodity_name,
            fs.sell_price,
            fs.demand,
            -- Prioritize matches between hotspot mineral and commodity
            ROW_NUMBER() OVER (
                PARTITION BY mm.system_id64, fs.station_id 
                ORDER BY 
                    CASE 
                        WHEN mm.mineral_type IS NOT NULL AND mm.mineral_type = fs.commodity_name THEN 1
                        ELSE 0
                    END DESC,
                    fs.sell_price DESC NULLS LAST
            ) as price_rank
        FROM minable_materials mm
        LEFT JOIN filtered_stations fs ON mm.system_id64 = fs.system_id64
    )
    -- Step 7: Get final results with best prices
    , best_prices AS MATERIALIZED (
        SELECT 
            name as system_name,
            system_id64,
            controlling_power,
            power_state,
            powers_acquiring,
            system_state,
            distance,
            CASE
                WHEN mineral_type IS NOT NULL AND mineral_type = commodity_name THEN mineral_type
                WHEN mineral_type IS NOT NULL THEN mineral_type || ' (Hotspot)'
                ELSE commodity_name
            END as display_name,
            commodity_name,  -- Keep original commodity_name for price coloring
            mineral_type,    -- Keep mineral_type for display
            ring_type,
            reserve_level,
            body_name,
            ring_name,
            signal_count,
            station_name,
            landing_pad_size,
            distance_to_arrival,
            station_type,
            update_time,
            sell_price,
            demand,
            COALESCE(sell_price, 0) as sort_price  -- Add sort price for consistent ordering
        FROM station_materials
        WHERE price_rank = 1  -- Only get highest price per station
        AND (
            %s != 'Hotspots'  -- Not in Hotspots mode
            OR (mineral_type IS NOT NULL AND mineral_type = commodity_name)  -- Match hotspot mineral
        )
    )"""
    
    # Build parameters in same order as normal search
    query_params = [
        # Power condition params first (for filtered_by_power)
        *where_params,
        # System state params (for filtered_by_state)
        params.get('system_state', 'Any'),  # For state comparison
        params.get('system_state', 'Any'),  # For state value
        # Distance calculation params (for relevant_systems)
        rx, ry, rz,  # Distance calculation
        rx, ry, rz,  # Distance filter
        params['max_dist'],
        # Minable materials params
        valid_ring_types,  # For ring type check
        params['ring_type_filter'],  # For hotspots check
        params['ring_type_filter'],  # For without hotspots check
        # Station materials params
        params['min_demand'], params['max_demand'],  # Zero-zero check
        params['min_demand'], params['max_demand'],  # Min=0 check
        params['max_demand'], params['min_demand'],  # Max=0 check
        params['min_demand'], params['max_demand'],  # Between check
        params['landing_pad_size'],  # For Any/Unknown case
        params['landing_pad_size'],  # For S case
        # Best prices params
        params['ring_type_filter']  # For hotspot mode check
    ]
    
    # Debug logging for parameters
    #log_message(BLUE, "SEARCH", f"Query parameters:")
    #for i, param in enumerate(query_params):
    #    log_message(BLUE, "SEARCH", f"Param {i}: {param}")
    
    # Final SELECT with price-based ordering
    query += """
    SELECT 
        bp.system_name,
        bp.system_id64,
        bp.controlling_power,
        bp.power_state,
        bp.powers_acquiring,
        bp.system_state,
        bp.distance,
        bp.display_name,
        bp.commodity_name,
        bp.mineral_type,
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
    ORDER BY bp.sort_price DESC,
             bp.distance ASC"""
    
    if params.get('limit'):
        query += " LIMIT %s"
        query_params.append(params['limit'])
    
    # Final debug logging
    #log_message(BLUE, "SEARCH", f"Final parameter count: {len(query_params)}")
    #log_message(BLUE, "SEARCH", f"Query placeholder count: {query.count('%s')}")
    
    return query, query_params 