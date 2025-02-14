"""Common utilities for search functionality"""

import json
from datetime import datetime
from flask import jsonify, request
from utils.mining_data import normalize_commodity_name
from utils.common import log_message, get_db_connection, YELLOW, RED, BLUE

# Constants
SYSTEM_IN_RING_NAME = False

# Power colors mapping
POWER_COLORS = {
    'Aisling Duval': '#0099ff',
    'Edmund Mahon': '#019c00',
    'A. Lavigny-Duval': '#7f00ff',
    'Nakato Kaine': '#a3f127',
    'Felicia Winters': '#ffc400',
    'Denton Patreus': '#00ffff',
    'Jerome Archer': '#df1de4',
    'Zemina Torval': '#0040ff',
    'Pranav Antal': '#ffff00',
    'Li Yong-Rui': '#33d688',
    'Archon Delaine': '#ff0000',
    'Yuri Grom': '#ff8000'
}

def get_search_params():
    """Extract and validate common search parameters from request"""
    # Get controlling power first to determine if we should include other power params
    controlling_power = request.args.get('controlling_power', '')
    
    # Get system states, default to ["Any"] if none selected
    system_states = request.args.getlist('system_state[]', type=str)
    if not system_states:
        system_states = ["Any"]
    
    params = {
        'ref_system': request.args.get('system', 'Sol'),
        'max_dist': float(request.args.get('distance', '10000')),
        'controlling_power': controlling_power if controlling_power and controlling_power != "Any" else None,
        'signal_type': request.args.get('signal_type'),
        'ring_type_filter': request.args.get('ring_type_filter', 'Hotspots'),
        'limit': int(request.args.get('limit', '30')),
        'mining_types': request.args.getlist('mining_types[]'),
        'min_demand': int(request.args.get('minDemand', '0')),
        'max_demand': int(request.args.get('maxDemand', '0')),
        'sel_mats': request.args.getlist('selected_materials[]', type=str),
        'reserve_level': request.args.get('reserve_level', 'All'),
        'system_states': system_states,
        'landing_pad_size': request.args.get('landingPadSize', 'L')  # Default to L to show all
    }
    
    # Only include power-related params if controlling_power is set and not "Any"
    if controlling_power and controlling_power != "Any":
        params['power_goal'] = request.args.get('power_goal', 'Reinforce')
    else:
        params['power_goal'] = None
        
    return params

def log_search_params(params):
    """Log search parameters for debugging"""
    log_message(BLUE, "SEARCH", "Search parameters:")
    for key, value in params.items():
        log_message(BLUE, "SEARCH", f"- {key}: {value}")

def get_reference_coords(conn, ref_system):
    """Get coordinates for reference system"""
    c = conn.cursor()
    c.execute('SELECT x, y, z FROM systems WHERE name ILIKE %s', (ref_system,))
    coords = c.fetchone()
    if coords:
        return coords['x'], coords['y'], coords['z']
    return None

def load_material_data(signal_type):
    """Load and validate material data from mining_materials.json"""
    try:
        with open('data/mining_materials.json', 'r') as f:
            mat_data = json.load(f)['materials']
            
        # Handle "Any" case
        if signal_type == 'Any':
            return {
                'name': 'Any',
                'ring_types': {
                    'Rocky': {
                        'surfaceLaserMining': True,
                        'surfaceDeposit': True,
                        'subSurfaceDeposit': True,
                        'core': True
                    },
                    'Metal Rich': {
                        'surfaceLaserMining': True,
                        'surfaceDeposit': True,
                        'subSurfaceDeposit': True,
                        'core': True
                    },
                    'Metallic': {
                        'surfaceLaserMining': True,
                        'surfaceDeposit': True,
                        'subSurfaceDeposit': True,
                        'core': True
                    },
                    'Icy': {
                        'surfaceLaserMining': True,
                        'surfaceDeposit': True,
                        'subSurfaceDeposit': True,
                        'core': True
                    }
                }
            }
            
        # Special case for LowTemperatureDiamond
        if signal_type == 'LowTemperatureDiamond':
            signal_type = 'Low Temperature Diamonds'
            
        for material_name, material in mat_data.items():
            if material['name'].lower() == signal_type.lower():
                return material
                
        log_message(RED, "MINING", f"Material {signal_type} not found in mining_materials.json")
        return None
        
    except Exception as e:
        log_message(RED, "MINING", f"Error loading mining_materials.json: {e}")
        return None

def get_valid_ring_types(material, mining_types):
    """Get valid ring types based on mining types and material data"""
    valid_ring_types = []
    
    # If no mining types specified, return all ring types that support any mining method
    if not mining_types:
        for ring_type, data in material['ring_types'].items():
            if any([
                data.get('surfaceLaserMining', False),
                data.get('surfaceDeposit', False),
                data.get('subSurfaceDeposit', False),
                data.get('core', False)
            ]):
                valid_ring_types.append(ring_type)
        return valid_ring_types
        
    # Check each ring type against the selected mining methods
    for ring_type, data in material['ring_types'].items():
        # Check if ring type supports ALL selected mining methods
        supports_all = True
        for mining_type in mining_types:
            mining_type = mining_type.lower()
            if mining_type == 'laser surface' and not data.get('surfaceLaserMining', False):
                supports_all = False
                break
            elif mining_type == 'surface' and not data.get('surfaceDeposit', False):
                supports_all = False
                break
            elif mining_type == 'subsurface' and not data.get('subSurfaceDeposit', False):
                supports_all = False
                break
            elif mining_type == 'core' and not data.get('core', False):
                supports_all = False
                break
            elif mining_type == 'all':
                # For 'All', check if the ring supports any mining method
                if not any([
                    data.get('surfaceLaserMining', False),
                    data.get('surfaceDeposit', False),
                    data.get('subSurfaceDeposit', False),
                    data.get('core', False)
                ]):
                    supports_all = False
                    break
        
        if supports_all:
            valid_ring_types.append(ring_type)
                    
    return valid_ring_types

def get_other_commodities(conn, station_pairs, sel_mats):
    """Get other commodities for specified stations"""
    other_commodities = {}
    if not station_pairs or not sel_mats:
        return other_commodities
        
    try:
        c = conn.cursor()
        station_list = [(str(sid), sname) for sid, sname in station_pairs]
        
        # Build query for other commodities
        query = """
            SELECT system_id64, station_name, commodity_name, sell_price, demand
            FROM station_commodities
            WHERE (system_id64, station_name) = ANY(%s)
            AND commodity_name = ANY(%s)
            AND sell_price > 0 AND demand > 0
            ORDER BY sell_price DESC
        """
        
        c.execute(query, [station_list, sel_mats])
        
        for row in c.fetchall():
            key = (row['system_id64'], row['station_name'])
            if key not in other_commodities:
                other_commodities[key] = []
            if len(other_commodities[key]) < 6:
                other_commodities[key].append({
                    'name': row['commodity_name'],
                    'sell_price': row['sell_price'],
                    'demand': row['demand']
                })
                
    except Exception as e:
        log_message(RED, "ERROR", f"Error getting other commodities: {e}")
        
    return other_commodities

def format_power_info(row):
    """Format power information for display"""
    power_info = []
    if (row['controlling_power'] and 
        row['power_state'] in ['Exploited', 'Fortified', 'Stronghold']):
        color = POWER_COLORS.get(row['controlling_power'], '#DDD')
        power_info.append(f"<span style='display: inline-block; width: 8px; height: 8px; border-radius: 50%; background-color: {color}; margin-right: 4px;'></span><span style='color: yellow'>{row['controlling_power']}</span>")

    if row['powers_acquiring']:
        try:
            for pw in row['powers_acquiring']:
                if pw != row['controlling_power']:
                    color = POWER_COLORS.get(pw, '#DDD')
                    power_info.append(f"<span style='display: inline-block; width: 8px; height: 8px; border-radius: 50%; background-color: {color}; margin-right: 4px;'></span>{pw}")
        except Exception as e:
            log_message(RED, "POWERS", f"Error w/ powers_acquiring: {e}")

    return '<br>'.join(power_info) if power_info else row['controlling_power']

def format_ring_info(row, material, signal_type):
    """Format ring information with proper hotspot icons and mining type checks"""
    ring_data = material['ring_types'].get(row['ring_type'], {})
    
    # Format ring name - remove system name if it appears at the start
    display_ring_name = row['ring_name']
    if not SYSTEM_IN_RING_NAME and display_ring_name.startswith(row['system_name']):
        display_ring_name = display_ring_name[len(row['system_name']):].lstrip()
    
    # Show ring details for both hotspots and valid rings
    if row['mineral_type']:
        # This is a hotspot - use the actual mineral type found
        if row['mineral_type'] == 'Low Temperature Diamonds': mineral_name = 'Low T. Diamonds'
        else: mineral_name = row['mineral_type']
        hotspot_text = "Hotspot " if row['signal_count'] == 1 else "Hotspots " if row['signal_count'] else ""
        return {
            'name': display_ring_name + f" <img src='/img/icons/rings/{row['ring_type'].lower()}.png' width='16' height='16' class='ring-type-icon' alt='{row['ring_type']}' title='Ring Type: {row['ring_type']}' style='vertical-align: middle;'> <svg class='reserve-level-icon' width='14' height='13' style='margin-right: 2px; color: #f5730d'><title>Reserve Level: {row['reserve_level']}</title><use href='img/icons/reserve-level.svg#reserve-level-{row['reserve_level'].lower()}'></use></svg>",
            'body_name': row['body_name'],
            'signals': f"<img src='img/icons/hotspot-systemview.svg' width='13' height='13' class='hotspot-icon'> {mineral_name}: {row['signal_count'] or ''} {hotspot_text}"
        }
    elif any([
        ring_data.get('surfaceLaserMining', False),
        ring_data.get('surfaceDeposit', False),
        ring_data.get('subSurfaceDeposit', False),
        ring_data.get('core', False)
    ]):
        # This is a regular ring - use the commodity_name from the row if available, otherwise use material name
        mineral_name = row.get('commodity_name') or material['name']
        if mineral_name == 'Low Temperature Diamonds': mineral_name = 'Low T. Diamonds'
        return {
            'name': display_ring_name + f" <img src='/img/icons/rings/{row['ring_type'].lower()}.png' width='16' height='16' class='ring-type-icon' alt='{row['ring_type']}' title='Ring Type: {row['ring_type']}' style='vertical-align: middle;'> <svg class='reserve-level-icon' width='14' height='13' style='margin-right: 2px; color: #f5730d'><title>Reserve Level: {row['reserve_level']}</title><use href='img/icons/reserve-level.svg#reserve-level-{row['reserve_level'].lower()}'></use></svg>",
            'body_name': row['body_name'],
            'signals': f"{mineral_name}"
        }

def format_station_info(row, other_commodities):
    """Format station information for display"""
    if not row['station_name']:
        return None
        
    station_info = {
        'name': row['station_name'],
        'station_type': row['station_type'],
        'pad_size': row['landing_pad_size'],
        'distance': float(row['distance_to_arrival']) if row['distance_to_arrival'] is not None else 0,
        'sell_price': int(row['sell_price']) if row['sell_price'] is not None else 0,
        'demand': int(row['demand']) if row['demand'] is not None else 0,
        'update_time': row['update_time'].isoformat() if row['update_time'] is not None else None,
        'mineral_type': row['mineral_type'],  # Add mineral_type from the query
        'commodity_name': row['mineral_type'] or row['commodity_name'],  # Use mineral_type as primary, fallback to commodity_name
        'other_commodities': []
    }
    
    # Add other commodities if available
    key = (row['system_id64'], row['station_name'])
    if key in other_commodities:
        station_info['other_commodities'] = other_commodities[key]
        
    return station_info

def get_other_signals(conn, system_ids, signal_type):
    """Get other mineral signals for systems"""
    other_signals = {}
    try:
        c = conn.cursor()
        c.execute("""
            SELECT ms.system_id64, ms.ring_name, ms.mineral_type, ms.signal_count, ms.reserve_level, ms.ring_type,
                   s.name as system_name
            FROM mineral_signals ms
            JOIN systems s ON s.id64 = ms.system_id64
            WHERE ms.system_id64 = ANY(%s::bigint[]) 
            AND (LOWER(ms.mineral_type) != LOWER(%s) OR ms.mineral_type IS NULL)
        """, [system_ids, signal_type])
        
        for row in c.fetchall():
            if row['system_id64'] not in other_signals:
                other_signals[row['system_id64']] = []
                
            # Format ring name
            display_ring_name = row['ring_name']
            if not SYSTEM_IN_RING_NAME and display_ring_name.startswith(row['system_name']):
                display_ring_name = display_ring_name[len(row['system_name']):].lstrip()
                
            if row['mineral_type']:
                hotspot_text = "Hotspot " if row['signal_count'] == 1 else "Hotspots " if row['signal_count'] else ""
                signal_text = f"<img src='img/icons/hotspot-2.svg' width='11' height='11' class='hotspot-icon'> {row['mineral_type']}: {row['signal_count'] or ''} {hotspot_text}({row['reserve_level']})"
            else:
                signal_text = f"{signal_type} [{row['ring_type']}, {row['reserve_level']}]"
            
            other_signals[row['system_id64']].append({
                'ring_name': display_ring_name,
                'mineral_type': row['mineral_type'],
                'signal_count': row['signal_count'] or '',
                'reserve_level': row['reserve_level'],
                'ring_type': row['ring_type'],
                'signal_text': signal_text
            })
            
    except Exception as e:
        log_message(RED, "ERROR", f"Error getting other signals: {e}")
        
    return other_signals

def format_ring_name(ring_name, system_name):
    """Format ring name by optionally removing system name prefix"""
    if not SYSTEM_IN_RING_NAME and ring_name.startswith(system_name):
        return ring_name[len(system_name):].lstrip()
    return ring_name
