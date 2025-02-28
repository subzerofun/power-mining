# bot_data.py
# Author: subzerofun
# Version: 1.0.0
# Date: 2025-02-21
# Description: This file contains the data processing functions for the bot.

from typing import Dict, List
from bot_common import format_mineral_name
import aiohttp
import urllib.parse

# Price thresholds for mining types
CREDIT_THRESHOLD = 30000  # Minimum price threshold for showing minerals

LASER_THRESHOLDS = {
    'Platinum': 90000,
    'Painite': 70000,
    'Osmium': 50000,
    'Low Temperature Diamonds': 70000,
    'Bromellite': 50000
}

CORE_THRESHOLDS = {
    'Alexandrite': 200000,
    'Benitoite': 200000,
    'Grandidierite': 200000,
    'Monazite': 200000,
    'Musgravite': 200000,
    'Rhodplumsite': 200000,
    'Serendibite': 200000,
    'Void Opal': 200000
}

# List of preferred minerals (high-value targets)
PREFERRED_LASER_MINERALS = {'Platinum', 'Painite', 'Osmium', 'Low Temperature Diamonds', 'Bromellite', 'Palladium', 'Gold'}
ALLOWED_CORE_MINERALS = {'Alexandrite', 'Benitoite', 'Grandidierite', 'Monazite', 'Musgravite',
                        'Rhodplumsite', 'Serendibite', 'Void Opal'}

# Ring type mappings for different mining methods
RING_MAPPINGS = {
    'laser': {
        'Icy': ['Bertrandite', 'Bromellite', 'Cryolite', 'Goslarite', 'Hydrogen Peroxide',
                'Liquid Oxygen', 'Lithium Hydroxide', 'Low Temperature Diamonds', 'Methane Clathrate',
                'Methanol Monohydrate Crystals', 'Tritium', 'Water'],
        'Rocky': ['Bauxite', 'Bertrandite', 'Gallite', 'Indite', 'Jadeite', 'Lepidolite',
                 'Moissanite', 'Pyrophyllite', 'Rutile', 'Taaffeite', 'Uraninite'],
        'Metal Rich': ['Aluminium', 'Beryllium', 'Bismuth', 'Cobalt', 'Coltan', 'Copper',
                      'Gallium', 'Hafnium 178', 'Indite', 'Indium', 'Lanthanum', 'Lithium',
                      'Praseodymium', 'Rutile', 'Samarium', 'Silver', 'Tantalum', 'Thallium', 
                      'Thorium', 'Titanium', 'Uranium', 'Uraninite'],
        'Metallic': ['Aluminium', 'Beryllium', 'Bismuth', 'Cobalt', 'Copper', 'Gallite',
                    'Gallium', 'Gold', 'Hafnium 178', 'Indium', 'Lanthanum', 'Lithium',
                    'Osmium', 'Painite', 'Palladium', 'Platinum', 'Praseodymium', 'Samarium',
                    'Silver', 'Tantalum', 'Thallium', 'Thorium', 'Titanium', 'Uranium']
    },
    'core': {
        'Icy': ['Alexandrite', 'Bromellite', 'Low Temperature Diamonds', 'Void Opal'],
        'Rocky': ['Alexandrite', 'Benitoite', 'Grandidierite', 'Monazite', 'Musgravite',
                 'Rhodplumsite', 'Serendibite'],
        'Metal Rich': ['Alexandrite', 'Benitoite', 'Grandidierite', 'Monazite', 'Musgravite',
                      'Painite', 'Platinum', 'Rhodplumsite', 'Serendibite'],
        'Metallic': ['Monazite', 'Painite', 'Platinum']
    }
}

def process_hotspots(data: Dict) -> List[Dict]:
    """Process hotspot data from mineral signals and station markets"""
    hotspots = []
    
    # Get all mineral signals with hotspots
    for signal in data.get('mineralSignals', []):
        if signal.get('mineral_type'):  # Skip null mineral types
            # Find matching station prices
            best_price = 0
            best_station = None
            best_station_data = None
            
            for station in data.get('stations', []):
                if station.get('market', {}).get('commodities'):
                    for commodity in station['market']['commodities']:
                        if commodity['name'] == signal['mineral_type'] and commodity['demand']:
                            if commodity['sellPrice'] > best_price:
                                best_price = commodity['sellPrice']
                                best_station = station['name']
                                best_station_data = station
            
            # Add hotspot regardless of station availability
            hotspots.append({
                'ring_name': signal['ring_name'],
                'mineral_type': format_mineral_name(signal['mineral_type']),
                'ring_type': signal['ring_type'],
                'reserve_level': signal['reserve_level'],
                'signal_count': signal['signal_count'],
                'station': best_station if best_station else "No station buying",
                'price': best_price,
                'stations': data.get('stations', []),  # Include full station data
                'system_name': data['name']  # Add system name
            })
    
    # Sort by price descending but show all hotspots
    return sorted(hotspots, key=lambda x: x['price'], reverse=True)

def process_mineable_rings(data: Dict) -> List[Dict]:
    """Process mineable rings without hotspots"""
    mineable = {}  # Use dictionary to track unique minerals
    
    # Get rings without specific hotspots
    for signal in data.get('mineralSignals', []):
        if not signal.get('mineral_type'):  # Only process null mineral types
            ring_type = signal['ring_type']
            
            # Get all possible minerals for this ring type
            possible_minerals = set()
            for mining_type in ['laser', 'core']:
                if ring_type in RING_MAPPINGS[mining_type]:
                    possible_minerals.update(RING_MAPPINGS[mining_type][ring_type])
            
            # Find best prices for possible minerals
            for mineral in possible_minerals:
                best_price = 0
                best_station = None
                best_station_data = None
                
                for station in data.get('stations', []):
                    if station.get('market', {}).get('commodities'):
                        for commodity in station['market']['commodities']:
                            if commodity['name'] == mineral and commodity['demand']:
                                if commodity['sellPrice'] > best_price:
                                    best_price = commodity['sellPrice']
                                    best_station = station['name']
                                    best_station_data = station
                
                # Only add minerals that meet the price threshold
                if best_price >= CREDIT_THRESHOLD:
                    # Only update if price is better than existing or mineral not seen yet
                    mineral_key = format_mineral_name(mineral)
                    if mineral_key not in mineable or best_price > mineable[mineral_key]['price']:
                        mineable[mineral_key] = {
                            'ring_name': signal['ring_name'],
                            'ring_type': ring_type,
                            'mineral': mineral_key,
                            'reserve_level': signal['reserve_level'],
                            'station': best_station,
                            'price': best_price,
                            'stations': data.get('stations', []),
                            'system_name': data['name']  # Add system name
                        }
    
    # Convert dictionary to list and sort by price
    result = list(mineable.values())
    return sorted(result, key=lambda x: x['price'], reverse=True)

def process_laser_mining(data: Dict) -> List[Dict]:
    """Process laser mining opportunities"""
    laser = {}  # Use dictionary to track unique minerals
    
    for signal in data.get('mineralSignals', []):
        ring_type = signal['ring_type']
        if ring_type not in RING_MAPPINGS['laser']:
            continue
            
        # Process both hotspots and potential minerals
        minerals_to_check = set()
        if signal.get('mineral_type'):
            minerals_to_check.add(signal['mineral_type'])
        minerals_to_check.update(RING_MAPPINGS['laser'][ring_type])
        
        for mineral in minerals_to_check:
            # Skip if not in RING_MAPPINGS['laser'][ring_type]
            if mineral not in RING_MAPPINGS['laser'][ring_type]:
                continue
                
            best_price = 0
            best_station = None
            is_hotspot = bool(signal.get('mineral_type') == mineral)
            
            for station in data.get('stations', []):
                if station.get('market', {}).get('commodities'):
                    for commodity in station['market']['commodities']:
                        if commodity['name'] == mineral and commodity['demand']:
                            threshold = LASER_THRESHOLDS.get(mineral, 70000)
                            if commodity['sellPrice'] >= threshold or mineral in PREFERRED_LASER_MINERALS:
                                if commodity['sellPrice'] > best_price:
                                    best_price = commodity['sellPrice']
                                    best_station = station
            
            if best_price > 0:
                mineral_key = format_mineral_name(mineral)
                # Update if price is better or if it's a hotspot
                if (mineral_key not in laser or 
                    best_price > laser[mineral_key]['price'] or 
                    (is_hotspot and not laser[mineral_key]['is_hotspot'])):
                    laser[mineral_key] = {
                        'ring_name': signal['ring_name'],
                        'mineral': mineral_key,
                        'ring_type': ring_type,
                        'reserve_level': signal['reserve_level'],
                        'station': best_station['name'],
                        'price': best_price,
                        'is_hotspot': is_hotspot,
                        'stations': data.get('stations', []),
                        'system_name': data['name']  # Add system name
                    }
    
    # Convert dictionary to list and sort by price
    result = list(laser.values())
    return sorted(result, key=lambda x: (x['is_hotspot'], x['price']), reverse=True)

def process_core_mining(data: Dict) -> List[Dict]:
    """Process core mining opportunities"""
    core = {}  # Use dictionary to track unique minerals
    
    for signal in data.get('mineralSignals', []):
        ring_type = signal['ring_type']
        if ring_type not in RING_MAPPINGS['core']:
            continue
            
        # Process both hotspots and potential minerals
        minerals_to_check = set()
        if signal.get('mineral_type'):
            minerals_to_check.add(signal['mineral_type'])
        minerals_to_check.update(RING_MAPPINGS['core'][ring_type])
        
        for mineral in minerals_to_check:
            # Skip if not in RING_MAPPINGS['core'][ring_type]
            if mineral not in RING_MAPPINGS['core'][ring_type]:
                continue
                
            best_price = 0
            best_station = None
            is_hotspot = bool(signal.get('mineral_type') == mineral)
            
            for station in data.get('stations', []):
                if station.get('market', {}).get('commodities'):
                    for commodity in station['market']['commodities']:
                        if commodity['name'] == mineral and commodity['demand']:
                            threshold = CORE_THRESHOLDS.get(mineral, 300000)
                            if commodity['sellPrice'] >= threshold or mineral in ALLOWED_CORE_MINERALS:
                                if commodity['sellPrice'] > best_price:
                                    best_price = commodity['sellPrice']
                                    best_station = station
            
            if best_price > 0:
                mineral_key = format_mineral_name(mineral)
                # Update if price is better or if it's a hotspot
                if (mineral_key not in core or 
                    best_price > core[mineral_key]['price'] or 
                    (is_hotspot and not core[mineral_key]['is_hotspot'])):
                    core[mineral_key] = {
                        'ring_name': signal['ring_name'],
                        'mineral': mineral_key,
                        'ring_type': ring_type,
                        'reserve_level': signal['reserve_level'],
                        'station': best_station['name'],
                        'price': best_price,
                        'is_hotspot': is_hotspot,
                        'stations': data.get('stations', []),
                        'system_name': data['name']  # Add system name
                    }
    
    # Convert dictionary to list and sort by price
    result = list(core.values())
    return sorted(result, key=lambda x: (x['is_hotspot'], x['price']), reverse=True)

async def check_system_status(system_name: str, api_base: str) -> dict:
    """Check system status and return basic system info"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{api_base}{system_name}") as response:
                if response.status == 200:
                    return await response.json()
                return None
    except Exception as e:
        print(f"Error checking system status: {e}")
        return None

async def get_acquisition_data(system_name: str, api_base: str, search_type: str = "from_acquisition") -> dict:
    """Get acquisition data for a system"""
    try:
        encoded_power = urllib.parse.quote("Archon Delaine")
        url = f"{api_base}{system_name}?power={encoded_power}"
        if search_type == "for_acquisition":
            url += "&search=for_acquisition"
            
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                return None
    except Exception as e:
        print(f"Error getting acquisition data: {e}")
        return None

def process_acquisition_header(data: Dict) -> Dict:
    """Process acquisition data for header display"""
    # Get the source system info
    source_system = next((s for s in data['systems'] if s['systemType'] == 'Acquisition'), None)
    if not source_system:
        return None
        
    # Count stations by size and type
    large_stations = 0
    medium_stations = 0
    small_stations = 0
    settlements = 0
    surface_settlements = 0
    
    for station in source_system.get('stations', []):
        pad_size = station.get('landingPads', 'Unknown')
        station_type = station.get('type', '')
        
        if station_type == 'Settlement':
            settlements += 1
        elif station_type == 'Surface Settlement':
            surface_settlements += 1
        else:
            if pad_size == 'L':
                large_stations += 1
            elif pad_size == 'M':
                medium_stations += 1
            elif pad_size == 'S':
                small_stations += 1
        
    # Count controlled systems
    controlled_systems = [s for s in data['systems'] if s['systemType'] in ['Fortified', 'Stronghold']]
    
    return {
        'name': source_system['name'],
        'population': source_system['population'],
        'systemState': source_system['systemState'],
        'controlled_count': len(controlled_systems),
        'controlled_systems': [{
            'name': sys['name'],
            'type': sys['systemType'],
            'distance': sys['distanceToSource'],
            'population': sys['population'],
            'systemState': sys['systemState']
        } for sys in controlled_systems],
        'stations': {
            'large': large_stations,
            'medium': medium_stations,
            'small': small_stations,
            'settlements': settlements,
            'surface_settlements': surface_settlements
        }
    }

def process_acquisition_stations(data: Dict) -> List[Dict]:
    """Process station data for acquisition mining opportunities"""
    # Get source system (unoccupied system)
    source_system = next((s for s in data['systems'] if s['systemType'] == 'Acquisition'), None)
    if not source_system:
        return []
        
    # Get all controlled systems
    controlled_systems = [s for s in data['systems'] if s['systemType'] in ['Fortified', 'Stronghold']]
    
    # Process stations and their commodities
    station_commodities = []
    
    for station in source_system.get('stations', []):
        if not station.get('market', {}).get('commodities'):
            continue
            
        for commodity in station['market']['commodities']:
            mineral_name = commodity['name']
            price = commodity['sellPrice']
            demand = commodity['demand']
            
            # Skip if no demand or below threshold
            threshold = LASER_THRESHOLDS.get(mineral_name, CORE_THRESHOLDS.get(mineral_name, CREDIT_THRESHOLD))
            if not demand or (price < threshold and 
                            mineral_name not in PREFERRED_LASER_MINERALS and 
                            mineral_name not in ALLOWED_CORE_MINERALS):
                continue
                
            # Find mining opportunities in controlled systems
            mining_systems = []
            for sys in controlled_systems:
                hotspot_count = 0
                available_rings = 0
                
                for signal in sys.get('mineralSignals', []):
                    if signal.get('mineral_type') == mineral_name:
                        hotspot_count += 1
                    else:
                        # Check if ring type supports this mineral
                        ring_type = signal['ring_type']
                        for mining_type in ['laser', 'core']:
                            if (ring_type in RING_MAPPINGS[mining_type] and 
                                mineral_name in RING_MAPPINGS[mining_type][ring_type]):
                                available_rings += 1
                                break
                
                if hotspot_count > 0 or available_rings > 0:
                    mining_systems.append({
                        'name': sys['name'],
                        'hotspots': hotspot_count,
                        'rings': available_rings,
                        'distance': sys['distanceToSource']
                    })
            
            if mining_systems:
                station_commodities.append({
                    'station': station,
                    'mineral': format_mineral_name(mineral_name),
                    'price': price,
                    'demand': demand,
                    'mining_systems': sorted(mining_systems, key=lambda x: (-x['hotspots'], -x['rings']))
                })
    
    # Sort by price descending
    return sorted(station_commodities, key=lambda x: x['price'], reverse=True)
