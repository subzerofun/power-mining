"""Power-specific search logic"""

from utils.search_common import (
    get_search_params, log_search_params, get_reference_coords,
    load_material_data, get_valid_ring_types, get_other_commodities,
    format_power_info, format_station_info, get_other_signals
)
from utils.search_queries import (
    get_base_cte, get_station_cte, get_ring_join_conditions,
    get_main_select, get_main_joins, get_order_by
)
from utils.common import log_message, get_db_connection, RED, BLUE
from flask import jsonify, request

def get_power_states():
    """Get power states from request parameters"""
    return request.args.getlist('power_state[]')

def get_opposing_power_filter(opposing_power):
    """Build WHERE conditions for opposing power filtering
    
    Args:
        opposing_power: The opposing power filter value
        
    Returns:
        Tuple of (where_conditions, where_params)
    """
    conditions = []
    params = []
    
    if opposing_power and opposing_power != "Any":
        if opposing_power == "None":
            conditions.append("s.controlling_power IS NULL")
        elif opposing_power in ["One", "Two", "Multiple"]:
            array_length = "(SELECT COUNT(*) FROM jsonb_array_elements_text(s.powers_acquiring::jsonb))"
            if opposing_power == "One":
                conditions.append(f"{array_length} = 1")
            elif opposing_power == "Two":
                conditions.append(f"{array_length} = 2")
            else:  # Multiple
                conditions.append(f"{array_length} > 1")
        else:
            # Specific power filter
            conditions.append("s.controlling_power = %s")
            params.append(opposing_power)
            
    return conditions, params

def build_power_conditions(power_goal, controlling_power, power_states=None):
    """Build WHERE conditions and params based on power goal"""
    conditions = []
    params = []
    
    # When controlling_power is "Any", only apply power states filter
    #if not controlling_power or controlling_power == "Any":
    #    if power_states:
    #        conditions.append("s.power_state = ANY(%s::text[])")
    #        params.append(power_states)
    #    return conditions, params
        
    # When power_goal is None, we only filter by controlling_power
    #if not power_goal or power_goal == "None":
    #    if controlling_power == "None":
    #        conditions.append("s.controlling_power IS NULL")
    #    if power_states:
    #        conditions.append("s.power_state = ANY(%s::text[])")
    #        params.append(power_states)
    #    return conditions, params
        
    # Handle specific power goals
    if power_goal == 'Reinforce':
        conditions, params = build_reinforce_conditions(controlling_power, None)
    elif power_goal == 'Undermine':
        conditions, params = build_undermine_conditions(controlling_power, None)
    elif power_goal == 'Acquire':
        conditions.append("s.controlling_power IS NULL")
        conditions.append("NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(s.powers_acquiring::jsonb))")
    elif power_goal.startswith('Opposing:'):
        opposing_type = power_goal.split(':')[1]
        opp_conditions, opp_params = get_opposing_power_filter(opposing_type)
        conditions.extend(opp_conditions)
        params.extend(opp_params)
    else:
        # Default power state logic - show systems we control or are acquiring
        conditions.append("(s.controlling_power = %s OR %s::text = ANY(SELECT jsonb_array_elements_text(s.powers_acquiring::jsonb)))")
        params.extend([controlling_power, controlling_power])
            
    # Add power states filter if provided and not in Reinforce mode
    if power_states and power_goal != 'Reinforce':
        conditions.append("s.power_state = ANY(%s::text[])")
        params.append(power_states)
        
    return conditions, params

def build_query(params, join_condition, where_conditions):
    """Build the complete SQL query"""
    query = get_base_cte()
    query += get_station_cte()
    query += get_main_select()
    query += get_main_joins().format(join_condition=join_condition)
    
    if where_conditions:
        query += " WHERE " + " AND ".join(where_conditions)
    
    query += get_order_by()
    
    if params['limit']:
        query += " LIMIT %s"
    
    return query

def get_query_params(params, rx, ry, rz, material, join_params, where_params):
    """Get all query parameters in correct order"""
    base_params = [
        rx, ry, rz,  # Distance calculation in subquery
        rx, ry, rz,  # Distance filter in subquery
        params['max_dist'],    # Maximum distance
        material['name'],  # For station_commodities
        params['min_demand'], params['max_demand'],  # For zero-zero check
        params['min_demand'], params['max_demand'],  # For min=0 check
        params['max_demand'], params['min_demand'],  # For max=0 check
        params['min_demand'], params['max_demand']   # For between check
    ]
    
    # Add JOIN parameters
    base_params.extend(join_params)
    
    # Add WHERE parameters
    base_params.extend(where_params)
    
    if params['limit']:
        base_params.append(params['limit'])
    
    return base_params

def build_undermine_conditions(controlling_power, opposing_power):
    """Build WHERE conditions for Undermine power goal
    
    Args:
        controlling_power: The power doing the undermining
        opposing_power: The power being undermined (or filter type)
    
    Returns:
        Tuple of (where_conditions, where_params)
    """
    where_conditions = []
    where_params = []
    
    # Base conditions:
    # 1. System must NOT be controlled by your power
    # 2. Your power must be in powers_acquiring
    where_conditions.append("s.controlling_power != %s")
    where_conditions.append("%s::text = ANY(SELECT jsonb_array_elements_text(s.powers_acquiring::jsonb))")
    where_params.extend([controlling_power, controlling_power])

    # Handle opposing power filters
    if opposing_power and opposing_power != "Any":
        if opposing_power in ["One", "Two", "Multiple"]:
            # Count number of powers in powers_acquiring
            array_length = "(SELECT COUNT(*) FROM jsonb_array_elements_text(s.powers_acquiring::jsonb))"
            if opposing_power == "One":
                where_conditions.append(f"{array_length} = 1")
            elif opposing_power == "Two":
                where_conditions.append(f"{array_length} = 2")
            else:  # Multiple
                where_conditions.append(f"{array_length} > 1")
        else:
            # Specific power filter
            where_conditions.append("s.controlling_power = %s")
            where_params.append(opposing_power)
    
    return where_conditions, where_params

def build_reinforce_conditions(controlling_power, opposing_power):
    """Build WHERE conditions for Reinforce power goal
    
    Args:
        controlling_power: The power doing the reinforcing
        opposing_power: The power being opposed (or filter type)
    
    Returns:
        Tuple of (where_conditions, where_params)
    """
    where_conditions = []
    where_params = []
    
    # Base conditions:
    # 1. System must be controlled by your power
    # 2. System must be in a reinforceable state
    where_conditions.append("s.controlling_power = %s")
    where_conditions.append("s.power_state IN ('Exploited', 'Fortified', 'Stronghold')")
    where_params.append(controlling_power)

    # Handle opposing power filters
    if opposing_power and opposing_power != "Any":
        if opposing_power == "None":
            # No powers in powers_acquiring array
            where_conditions.append("(s.powers_acquiring IS NULL OR s.powers_acquiring = '[]'::jsonb)")
        elif opposing_power in ["One", "Two", "Multiple"]:
            # Count number of powers in powers_acquiring
            array_length = "(SELECT COUNT(*) FROM jsonb_array_elements_text(s.powers_acquiring::jsonb))"
            if opposing_power == "One":
                where_conditions.append(f"{array_length} = 1")
            elif opposing_power == "Two":
                where_conditions.append(f"{array_length} = 2")
            else:  # Multiple
                where_conditions.append(f"{array_length} > 1")
        else:
            # Specific power filter - check if they're in powers_acquiring
            where_conditions.append("%s::text = ANY(SELECT jsonb_array_elements_text(s.powers_acquiring::jsonb))")
            where_params.append(opposing_power)
    
    return where_conditions, where_params 