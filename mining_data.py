"""Constants and helper functions for mining materials."""

import csv
import json
import psycopg2
from psycopg2.extras import DictCursor

# Materials that can be mined without hotspots
NON_HOTSPOT_MATERIALS = {
    # Metallic only
    'Gold': ['Metallic'],
    'Osmium': ['Metallic'],
    'Gallite': ['Metallic', 'Rocky'],
    'Palladium': ['Metallic'],
    'Painite': ['Metallic'],

    # Metallic or Metal Rich
    'Cobalt': ['Metallic', 'Metal Rich'],
    'Copper': ['Metallic', 'Metal Rich'],
    'Gallium': ['Metallic', 'Metal Rich'],
    'Silver': ['Metallic', 'Metal Rich'],
    'Aluminium': ['Metallic', 'Metal Rich'],
    'Beryllium': ['Metallic', 'Metal Rich'],
    'Bismuth': ['Metallic', 'Metal Rich'],
    'Hafnium': ['Metallic', 'Metal Rich'],
    'Indium': ['Metallic', 'Metal Rich'],
    'Lanthanum': ['Metallic', 'Metal Rich'],
    'Praseodymium': ['Metallic', 'Metal Rich'],
    'Samarium': ['Metallic', 'Metal Rich'],
    'Tantalum': ['Metallic', 'Metal Rich'],
    'Thallium': ['Metallic', 'Metal Rich'],
    'Thorium': ['Metallic', 'Metal Rich'],
    'Titanium': ['Metallic', 'Metal Rich'],
    'Uranium': ['Metallic', 'Metal Rich'],
    
    # Rocky only
    'Bauxite': ['Rocky'],
    'Lepidolite': ['Rocky'],
    'Moissanite': ['Rocky'],
    'Jadeite': ['Rocky'],
    'Pyrophyllite': ['Rocky'],
    'Taaffeite': ['Rocky'],
    
    # Rocky and Icy
    'Bertrandite': ['Rocky', 'Icy'],
    
    # Rocky and Metal Rich
    'Coltan': ['Rocky', 'Metal Rich'],
    'Indite': ['Rocky', 'Metal Rich'],
    'Uraninite': ['Rocky', 'Metal Rich'],
    
    # Rocky and Metallic
    'Gallite': ['Rocky', 'Metallic'],
    
    # Icy only
    'Methane Clathrate': ['Icy'],
    'Methanol Monohydrate Crystals': ['Icy'],
    'Goslarite': ['Icy'],
    'Cryolite': ['Icy'],
    'Lithium Hydroxide': ['Icy'],
    'Void Opal': ['Icy'],
    
    # Metal Rich and Rocky
    'Rutile': ['Metal Rich', 'Rocky']
}

# Load material mappings from JSON
with open('data/materials.json', 'r') as f:
    MATERIAL_MAPPINGS = json.load(f)

def get_material_ring_types(material_name: str) -> list:
    """Get the required ring types for a given material."""
    # Special case for Low Temperature Diamonds
    if material_name == 'Low Temperature Diamonds':
        return ['hotspot', 'LowTemperatureDiamond']
    
    # Non-hotspot materials
    if material_name in NON_HOTSPOT_MATERIALS:
        return NON_HOTSPOT_MATERIALS[material_name]
    
    # All other materials are hotspot-based
    return ['hotspot']

def is_non_hotspot_material(material_name: str) -> bool:
    """Check if a material can be mined without hotspots."""
    return material_name in NON_HOTSPOT_MATERIALS

def get_material_sql_conditions(material_name: str) -> tuple[str, list]:
    """Get SQL conditions and parameters for finding systems where a material can be mined."""
    ring_types = get_material_ring_types(material_name)
    
    if material_name == 'Low Temperature Diamonds':
        return 'ms.mineral_type = %s', ['LowTemperatureDiamond']
    elif 'hotspot' in ring_types:
        return 'ms.mineral_type = %s', [material_name]
    else:
        placeholders = ','.join(['%s' for _ in ring_types])
        return f'ms.ring_type IN ({placeholders})', ring_types 

def get_ring_type_case_statement(commodity_column: str = 'commodity_name') -> str:
    """Generate SQL CASE statement for checking ring types."""
    cases = []
    for material, ring_types in NON_HOTSPOT_MATERIALS.items():
        if len(ring_types) == 1:
            cases.append(f"WHEN '{material}' THEN '{ring_types[0]}'")
        else:
            types_str = "', '".join(ring_types)
            cases.append(f"WHEN '{material}' THEN ms.ring_type IN ('{types_str}')")
    
    return f'CASE {commodity_column}\n' + '\n'.join(f'{case}' for case in cases) + '\nEND'

def get_non_hotspot_materials_list():
    """Get list of non-hotspot materials."""
    non_hotspot_minerals = {'Bauxite', 'Bertrandite', 
                        'Coltan', 'Gallite', 'Goslarite', 'Indite', 'Lepidolite', 'Methane Clathrate', 
                        'Methanol Monohydrate Crystals', 'Moissanite', 'Rutile', 
                        'Uraninite', 'Jadeite', 'Pyrophyllite', 'Taaffeite', 'Cryolite', 'Lithium Hydroxide', 'Void Opal'}
    non_hotspot_metals = {'Aluminium', 'Beryllium', 'Cobalt', 'Copper', 'Gallium', 'Gold', 'Hafnium 178', 'Indium',
                        'Lanthanum', 'Lithium', 'Osmium', 'Palladium','Praseodymium', 'Samarium', 'Silver', 'Tantalum', 
                        'Thallium', 'Thorium', 'Titanium', 'Uranium'}
    return list(non_hotspot_minerals | non_hotspot_metals)

def load_price_data():
    """Load price data from CSV file."""
    price_data = {}
    with open('data/current_prices.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            price_data[row['Material']] = {
                'avg_price': int(row['Average Price']),
                'max_price': int(row['Max Price'])
            }
    return price_data

def get_price_comparison(current_price, reference_price):
    """Calculate price comparison and return color and indicator."""
    if current_price == 0 or reference_price == 0:
        return None, ''
    
    percentage = (current_price / reference_price - 1) * 100  # Calculate percentage difference
    
    # Handle positive percentages first
    if percentage >= 125:
        return '#f0ff00', '     +++++'
    elif percentage >= 100:
        return '#fff000', '     ++++'
    elif percentage >= 75:
        return '#ffcc00', '     +++'
    elif percentage >= 50:
        return '#ff9600', '     ++'
    elif percentage >= 25:
        return '#ff7e00', '     +'
    # Handle near-average range
    elif percentage >= -5:
        return None, ''
    # Handle negative percentages
    elif percentage >= -25:
        return '#ff2a00', '     -'
    elif percentage >= -50:
        return '#af0019', '     --'
    else:
        return '#af0019', '     ---'

def normalize_commodity_name(name):
    """Normalize commodity names for price lookup."""
    # Special case for LowTemperatureDiamond
    if name == 'LowTemperatureDiamond':
        return 'Low Temperature Diamonds'
    
    # Create reverse mapping (full name to full name)
    full_names = {v: v for v in MATERIAL_MAPPINGS.values()}
    
    # Combine both mappings
    all_mappings = {**MATERIAL_MAPPINGS, **full_names}
    
    # Return the full name if found in mappings, otherwise return the original name
    return all_mappings.get(name, name)

def get_material_codes():
    """Load and return mapping of material codes to full names."""
    return MATERIAL_MAPPINGS.copy()

# Cache the material codes
MATERIAL_CODES = get_material_codes()

# Load price data when module is imported
PRICE_DATA = load_price_data() 

def get_mining_type_conditions(commodity: str, mining_types: list) -> tuple[str, list]:
    """Get SQL conditions for filtering by mining type."""
    if not mining_types or 'All' in mining_types:
        return '', []
        
    # Load material mining data
    try:
        with open('data/mining_data.json', 'r') as f:
            material_data = json.load(f)
            
        # Find the commodity data
        commodity_data = next((item for item in material_data['materials'] if item['name'] == commodity), None)
        if not commodity_data:
            # Check if it's a ring material from NON_HOTSPOT_MATERIALS
            if commodity in NON_HOTSPOT_MATERIALS:
                ring_types = NON_HOTSPOT_MATERIALS[commodity]
                conditions = []
                params = []
                for ring_type in ring_types:
                    conditions.append('(ms.ring_type = %s AND ms.mineral_type IS NULL)')
                    params.append(ring_type)
                return '(' + ' OR '.join(conditions) + ')', params
            return '', []
        
        # Build conditions for each ring type
        conditions = []
        params = []
        
        # Split mining types into core and non-core
        has_core = 'Core' in mining_types
        non_core_types = [mt for mt in mining_types if mt != 'Core']
        
        for ring_type, ring_data in commodity_data['ring_types'].items():
            # Handle core mining - requires hotspots if the material has hotspots for core mining
            if has_core and ring_data['core']:
                if ring_data['hotspot']:
                    conditions.append('(ms.ring_type = %s AND ms.mineral_type = %s)')
                    params.extend([ring_type, commodity])
                else:
                    conditions.append('(ms.ring_type = %s AND ms.mineral_type IS NULL)')
                    params.append(ring_type)
            
            # Handle non-core mining methods
            if non_core_types:
                non_core_matches = []
                for mining_type in non_core_types:
                    if mining_type == 'Laser Surface' and ring_data['surfaceLaserMining']:
                        non_core_matches.append(True)
                    elif mining_type == 'Surface Deposit' and ring_data['surfaceDeposit']:
                        non_core_matches.append(True)
                    elif mining_type == 'Sub Surface Deposit' and ring_data['subSurfaceDeposit']:
                        non_core_matches.append(True)
                
                if non_core_matches:
                    # For rings where the material has hotspots, include both hotspot and non-hotspot rings
                    if ring_data['hotspot']:
                        conditions.append('(ms.ring_type = %s AND (ms.mineral_type IS NULL OR ms.mineral_type = %s))')
                        params.extend([ring_type, commodity])
                    else:
                        conditions.append('(ms.ring_type = %s AND ms.mineral_type IS NULL)')
                        params.append(ring_type)
    
    except Exception as e:
        log_message(RED, "ERROR", f"Error loading mining_data.json: {str(e)}")
        return '', []
        
    if not conditions:
        return '1=0', []  # No matches possible
        
    return '(' + ' OR '.join(conditions) + ')', params

def get_ring_materials():
    """Load ring materials and their associated ring types from mining_data.json."""
    materials = {}
    
    try:
        with open('data/mining_data.json', 'r') as f:
            material_data = json.load(f)
            
            for item in material_data['materials']:
                # Get list of ring types where this material can be found
                valid_ring_types = []
                for ring_type, ring_data in item['ring_types'].items():
                    if any([
                        ring_data['surfaceLaserMining'],
                        ring_data['surfaceDeposit'],
                        ring_data['subSurfaceDeposit'],
                        ring_data['core']
                    ]):
                        valid_ring_types.append(ring_type)
                
                if valid_ring_types:  # Only add if material can be found in at least one ring type
                    materials[item['name']] = {
                        'ring_types': valid_ring_types,
                        'abbreviation': '',  # These fields are kept for backward compatibility
                        'conditions': item['conditions'],
                        'value': ''
                    }
    
    except Exception as e:
        app.logger.error(f"Error loading mining_data.json: {str(e)}")
        
    return materials

def get_potential_ring_types(material_name: str) -> list:
    """Get list of ring types where a material can potentially be found."""
    if material_name in NON_HOTSPOT_MATERIALS:
        return NON_HOTSPOT_MATERIALS[material_name]
        
    try:
        with open('data/mining_data.json', 'r') as f:
            material_data = json.load(f)
            
        # Find the material data
        material_info = next((item for item in material_data['materials'] if item['name'] == material_name), None)
        if not material_info:
            return []
            
        # Get list of ring types where this material can be found
        valid_ring_types = []
        for ring_type, ring_data in material_info['ring_types'].items():
            if any([
                ring_data['surfaceLaserMining'],
                ring_data['surfaceDeposit'],
                ring_data['subSurfaceDeposit'],
                ring_data['core']
            ]):
                valid_ring_types.append(ring_type)
                
        return valid_ring_types
        
    except Exception as e:
        app.logger.error(f"Error loading mining_data.json: {str(e)}")
        return [] 