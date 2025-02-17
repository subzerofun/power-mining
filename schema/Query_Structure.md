# Search Query Structure

Base numbers:

## Tables:
systems: 10385 rows
id64, name, x, y, z, controlling_power, power_state, powers_acquiring, system_state, distance_from_sol

mineral_signals: 138925 rows
system_id64, body_name, ring_name, ring_type, signal_count, reserve_level

stations: 268421 rows
system_id64, station_id, station_name, station_type, primary_economy, distance_to_arrival, landing_pad_size, update_time

station_commodities_mapped: 8005731 rows
system_id64, station_id, station_name, commodity_id, sell_price, demand

station_commodities: 8005731 rows
system_id64, station_id, station_name, commodity_name, sell_price, demand

commodity_types 
* ID MAPPING from station_commodities_mapped
to station_commodities *
commodity_id, commodity_name



# Input parameters:
ref_system: system_name
max_dist: XXX
controlling_power: controlling_power
signal_type: Main Mineral/Metal
ring_type_filter: All/Hotspots/Without Hotspots/Metal Rich/Metallic/Icy/Rocky
limit: XX
mining_types: ['All'] ## Different mining types
min_demand: 0
max_demand: 90000
sel_mats: ['Default'] ## Selected additonal materials for station display
reserve_level: All
system_states: ['Any']
landing_pad_size: Any
power_goal: ## Reinforce/Acquire/Undermine

hidden parameter, decided by search type: 
display_format: full/highest ## Output format for website

## Search flow:
server.py -> @app.route('/search')
    if power_goal == 'Reinforce':
        return search_reinforce() -> search_reinforce.py
    elif power_goal == 'Undermine':
        return search_undermine() -> search_undermine.py
    elif power_goal == 'Acquire':
        return search_acquisition() -> search_acquisition.py

## Reinforce:
search_reinforce.py:
params = get_search_params()
conn = get_db_connection()
coords =get_reference_coords()
material = load_material_data()
valid_ring_types =get_valid_ring_types()

If mineral/metal input: 'Any':
build_any_material_query(params, coords, valid_ring_types,where_conditions, where_params)
If mineral/metal input: 'Specific':
build_optimized_query(params, coords, valid_ring_types,where_conditions, where_params)

query:
1. Filter by power conditions (systems)
2. Filter by system state (systems)
3. Filter systems by distance (systems)
4. Find valid mining systems (systems, mineral_signals)
5. Only NOW look at stations for valid mining systems (systems, mineral_signals, stations, station_commodities)
6. Match stations with minerals and get highest price per system/station (systems, mineral_signals, stations, station_commodities)
7. Get final results with best prices (systems, mineral_signals, stations, station_commodities)





