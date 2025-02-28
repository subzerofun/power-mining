# bot_display.py
# Author: subzerofun
# Version: 1.0.0
# Date: 2025-02-21
# Description: This file contains the display functions for the bot.
# It is used to display the data in a readable format.

from prettytable import PrettyTable, ALL, HEADER, FRAME, NONE
from typing import Dict, List
from datetime import datetime, timedelta
from bot_common import (color_text, calculate_control_points, 
                       format_price, format_timestamp, configure_table_style,
                       format_ring_name, format_reserve_level, format_station_name,
                       COLORS, format_compact_ring_entry, format_mineral_name)
from bot_data import (RING_MAPPINGS, LASER_THRESHOLDS, ALLOWED_CORE_MINERALS,
                      CORE_THRESHOLDS, CREDIT_THRESHOLD, PREFERRED_LASER_MINERALS)

def create_system_header(data: Dict) -> str:
    """Create the system information header using PrettyTable"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    system_name = color_text(data['name'], 'red')
    
    # Format population with point as thousand separator
    population_str = 'None'
    if data.get('population') is not None:
        population_str = "{:,.0f}".format(data['population']).replace(',', '.')
    
    table = PrettyTable()
    table = configure_table_style(table)
    table.field_names = [f"{system_name} MINING REPORT {current_time}"]
    table.header = True  # Override default to hide header for this specific table
    table.hrules = FRAME
    table.preserve_internal_border = True

    table.add_row([f"Controlling Power: {data.get('controllingPower', 'None')}"])
    table.add_row([f"Undermining: {', '.join(data.get('powers', []) or ['None'])}"])
    table.add_row([f"Power State: {data.get('powerState', 'None')}"])
    table.add_row([f"Population: {population_str}"])
    table.add_row([f"System State: {data.get('systemState', 'None')}"])
    
    return table.get_string()

def create_hotspots_table(data):
    """Create table for hotspots using PrettyTable"""
    table = PrettyTable()
    table = configure_table_style(table)
    table.field_names = ['Hotspots', 'Type', 'RSRV', 'Mineral/Metal', 'Station', 'UPD ', 'Sell for', 'Demand', 'CTRL PTS']
    
    # Additional specific settings for this table
    table.align['Sell for'] = 'r'
    table.align['Demand'] = 'r'
    table.align['CTRL PTS'] = 'r'
    table.align['UPD'] = 'r'
    
    for spot in data:
        # Only process station data if there's a real station
        if spot['station'] != "No station buying":
            control_points = calculate_control_points(spot['price'])
            station_data = next((s for s in spot.get('stations', []) if s['name'] == spot['station']), None)
            update_time = station_data['updateTime'] if station_data else None
            commodity_data = next((c for c in station_data['market']['commodities'] if c['name'] == spot['mineral_type']), None) if station_data and station_data.get('market') else None
            demand = commodity_data['demand'] if commodity_data else 'N/A'
            
            table.add_row([
                f"{format_ring_name(spot['ring_name'], spot['system_name'])}" + color_text(f" ● {spot['signal_count']}", 'gold'),
                spot['ring_type'],
                format_reserve_level(spot['reserve_level']),
                color_text(spot['mineral_type'], 'white', width=20),
                format_station_name(spot['station'], station_data),
                format_timestamp(update_time) if update_time else 'unknown',
                format_price(spot['price']),
                str(demand) if isinstance(demand, int) else demand,
                f"60x = {control_points}"
            ])
        else:
            # For hotspots without stations
            table.add_row([
                f"{format_ring_name(spot['ring_name'], spot['system_name'])}" + color_text(f" ● {spot['signal_count']}", 'gold'),
                spot['ring_type'],
                format_reserve_level(spot['reserve_level']),
                color_text(spot['mineral_type'], 'white', width=20),
                spot['station'],  # "No station buying"
                "---",           # No update time
                "---",           # No price
                "---",           # No demand
                "---"            # No control points
            ])
    
    return table.get_string()

def create_mineable_table(data):
    """Create table for mineable rings"""
    table = PrettyTable()
    table = configure_table_style(table)
    table.field_names = ['Ring', 'Type', 'RSRV', 'Mineral/Metal', 'Station', 'UPD ', 'Sell for', 'Demand', 'CTRL PTS']
    
    # Additional specific settings for this table
    table.align['Sell for'] = 'r'
    table.align['Demand'] = 'r'
    table.align['CTRL PTS'] = 'r'
    table.align['UPD'] = 'r'
    
    for ring in data:
        control_points = calculate_control_points(ring['price'])
        station_data = next((s for s in ring.get('stations', []) if s['name'] == ring['station']), None)
        update_time = station_data['updateTime'] if station_data else None
        commodity_data = next((c for c in station_data['market']['commodities'] if c['name'] == ring['mineral']), None) if station_data and station_data.get('market') else None
        demand = commodity_data['demand'] if commodity_data else 'N/A'
        
        table.add_row([
            format_ring_name(ring['ring_name'], ring['system_name']),
            ring['ring_type'],
            format_reserve_level(ring['reserve_level']),
            color_text(ring['mineral'], 'white', width=20),
            format_station_name(ring['station'], station_data),
            format_timestamp(update_time) if update_time else 'unknown',
            format_price(ring['price']),
            str(demand) if isinstance(demand, int) else demand,
            f"60x = {control_points}"
        ])
    
    return table.get_string()

def create_laser_table(data):
    """Create table for laser mining"""
    table = PrettyTable()
    table = configure_table_style(table)
    table.field_names = ['Laser Mining', '●', 'Mining', 'Ring', 'Type', 'RSRV', 'Station', 'UPD ', 'Sell for', 'Demand']
    
    # Additional specific settings for this table
    table.align['Sell for'] = 'r'
    table.align['Demand'] = 'r'
    table.align['●'] = 'c'
    table.align['RSRV'] = 'c'  # Center align the reserve bars
    table.align['UPD'] = 'r'
    
    for item in data:
        station_data = next((s for s in item.get('stations', []) if s['name'] == item['station']), None)
        update_time = station_data['updateTime'] if station_data else None
        commodity_data = next((c for c in station_data['market']['commodities'] if c['name'] == item['mineral']), None) if station_data and station_data.get('market') else None
        demand = commodity_data['demand'] if commodity_data else 'N/A'
        
        table.add_row([
            color_text(item['mineral'], 'white', width=20),
            '✓' if item['is_hotspot'] else '0',
            'Laser',
            format_ring_name(item['ring_name'], item['system_name']),
            item['ring_type'],
            format_reserve_level(item['reserve_level']),
            format_station_name(item['station'], station_data),
            format_timestamp(update_time) if update_time else 'unknown',
            format_price(item['price']),
            str(demand) if isinstance(demand, int) else demand
        ])
    
    return table.get_string()

def create_core_table(data):
    """Create table for core mining"""
    table = PrettyTable()
    table = configure_table_style(table)
    table.field_names = ['Core Mining', '●', 'Mining', 'Ring', 'Type', 'RSRV', 'Station', 'UPD ', 'Sell for', 'Demand']
    
    # Additional specific settings for this table
    table.align['Sell for'] = 'r'
    table.align['Demand'] = 'r'
    table.align['●'] = 'c'
    table.align['RSRV'] = 'c'  # Center align the reserve bar
    table.align['UPD'] = 'r'
    
    for item in data:
        station_data = next((s for s in item.get('stations', []) if s['name'] == item['station']), None)
        update_time = station_data['updateTime'] if station_data else None
        commodity_data = next((c for c in station_data['market']['commodities'] if c['name'] == item['mineral']), None) if station_data and station_data.get('market') else None
        demand = commodity_data['demand'] if commodity_data else 'N/A'
        
        table.add_row([
            color_text(item['mineral'], 'white', width=20),
            '✓' if item['is_hotspot'] else '0',
            'Core',
            format_ring_name(item['ring_name'], item['system_name']),
            item['ring_type'],
            format_reserve_level(item['reserve_level']),
            format_station_name(item['station'], station_data),
            format_timestamp(update_time) if update_time else 'unknown',
            format_price(item['price']),
            str(demand) if isinstance(demand, int) else demand
        ])
    
    return table.get_string()

def create_stations_table(data: Dict) -> str:
    """Creates a table showing stations and their mining opportunities"""
    table = PrettyTable()
    table = configure_table_style(table)
    table.field_names = ['Station', 'DST', 'UPD', 'Mineral/Metal', 'Sell', 'Demand', 'Rings', 'CP@60']
    
    # Additional specific settings for this table
    table.align['DST'] = 'r'
    table.align['Sell'] = 'r'
    table.align['Demand'] = 'r'
    table.align['CP@60'] = 'r'
    
    # Process all mineral signals to map minerals to rings
    mineral_rings = {}  # mineral -> list of (ring_name, is_hotspot, reserve_level)
    for signal in data.get('mineralSignals', []):
        mineral_type = signal.get('mineral_type')
        # Format ring name without system name
        ring_name = format_ring_name(signal['ring_name'], data['name'])
        if mineral_type:  # Hotspot
            if mineral_type not in mineral_rings:
                mineral_rings[mineral_type] = []
            mineral_rings[mineral_type].append((ring_name, True, signal['reserve_level']))
        else:  # Regular ring
            ring_type = signal['ring_type']
            # Check both laser and core minerals for this ring type
            for mining_type in ['laser', 'core']:
                if ring_type in RING_MAPPINGS[mining_type]:
                    for mineral in RING_MAPPINGS[mining_type][ring_type]:
                        if mineral not in mineral_rings:
                            mineral_rings[mineral] = []
                        mineral_rings[mineral].append((ring_name, False, signal['reserve_level']))
    
    # Process each station
    for station in data.get('stations', []):
        if not station.get('market', {}).get('commodities'):
            continue
            
        station_minerals = []  # List of (mineral, price, demand, rings) tuples
        
        # Check each commodity at the station
        for commodity in station['market']['commodities']:
            mineral_name = commodity['name']
            price = commodity['sellPrice']
            demand = commodity['demand']
            
            # Skip if no demand or rings for this mineral
            if not demand or mineral_name not in mineral_rings:
                continue
                
            # Check thresholds
            threshold = LASER_THRESHOLDS.get(mineral_name, CORE_THRESHOLDS.get(mineral_name, CREDIT_THRESHOLD))
            if price < threshold and mineral_name not in PREFERRED_LASER_MINERALS and mineral_name not in ALLOWED_CORE_MINERALS:
                continue
                
            # Format rings list (hotspots first, then regular, max 3)
            rings = mineral_rings[mineral_name]
            rings_hotspots = [r for r in rings if r[1]]  # rings with hotspots
            rings_regular = [r for r in rings if not r[1]]  # rings without hotspots
            
            # Sort and limit to 3 total rings
            formatted_rings = []
            for ring_name, is_hotspot, reserve_level in (rings_hotspots + rings_regular)[:3]:
                formatted_rings.append(format_compact_ring_entry(ring_name, is_hotspot, reserve_level))
            
            rings_text = ','.join(formatted_rings)
            if len(rings) > 3:
                rings_text += ',...'
                
            station_minerals.append((mineral_name, price, demand, rings_text))
        
        # Sort by price and take top 10
        station_minerals.sort(key=lambda x: x[1], reverse=True)
        for mineral, price, demand, rings_text in station_minerals[:10]:
            control_points = calculate_control_points(price)
            table.add_row([
                format_station_name(station['name'], station, 30),
                f"{int(station.get('distanceToArrival', 0))} ls",
                format_timestamp(station['updateTime']) if station.get('updateTime') else 'unknown',
                color_text(format_mineral_name(mineral), 'white', width=20),
                format_price(price),
                str(demand),
                rings_text,
                str(control_points)
            ])
    
    return table.get_string()

def create_acquisition_header(data: Dict) -> str:
    """Create header for acquisition system display"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    system_name = color_text(data['name'], 'red')
    
    # Format population with point as thousand separator
    population_str = 'None'
    if data.get('population') is not None:
        population_str = "{:,.0f}".format(data['population']).replace(',', '.')
    
    table = PrettyTable()
    table = configure_table_style(table)
    table.field_names = [f"{system_name} ACQUISITION REPORT {current_time}"]
    table.header = True
    table.hrules = FRAME
    table.preserve_internal_border = True
    
    table.add_row([f"Power State: Unoccupied"])
    table.add_row([f"Population: {population_str}"])
    table.add_row([f"System State: {data.get('systemState', 'None')}"])
    table.add_row([f"Stations: {data['stations']['large']} [L], {data['stations']['medium']} [M], {data['stations']['small']} [S]"])
    table.add_row([f"Settlements: {data['stations']['settlements']}, Surface Settlements (OD): {data['stations']['surface_settlements']}"])
    table.add_row([f"────────────────────────────────────────────────────"])
    table.add_row([f"Controlled systems in reach: {data['controlled_count']}"])
    
    # Add controlled systems
    for sys in data['controlled_systems']:
        table.add_row([f"────────────────────────────────────────────────────"])
        if sys['type'] == "Fortified": systemIcon = "▼"
        else: systemIcon = "◆"
        table.add_row([
            f"{color_text(systemIcon, 'red')} {color_text(sys['name'], 'red')}, ({sys['type']}), Distance: {sys['distance']:.1f} ly\n"
            f"  Population: {sys['population']:,}, System State: {sys['systemState']}"
        ])
    
    return table.get_string()

def create_acquisition_stations_table(data: List[Dict]) -> str:
    """Create table showing station mining opportunities in controlled systems"""
    table = PrettyTable()
    table = configure_table_style(table)
    table.field_names = ['Mineral/Metal', 'Sell', 'DMD','Station', 'DST', 'UPD', 'Mining systems', 'CP@60']
    
    # Additional specific settings for this table
    table.align['DST'] = 'r'
    table.align['Sell'] = 'r'
    table.align['DMD'] = 'r'
    table.align['CP@60'] = 'r'
    
    last_station = None
    last_mineral = None
    
    for entry in data:
        station = entry['station']
        mineral = entry['mineral']
        price = entry['price']
        demand = entry['demand']
        control_points = calculate_control_points(price)
        
        # First row shows station info and first mining system
        first_system = entry['mining_systems'][0]
        
        if first_system['hotspots'] > 1 or first_system['hotspots'] == 0: hotspotstring = "Hotspots"
        else: hotspotstring = "Hotspot"
        hsColor = 'gold' if first_system['hotspots'] > 0 else 'white'
        hotspotstring = color_text(f"{first_system['hotspots']} {hotspotstring}", hsColor)

        table.add_row([
            color_text(mineral, 'white', width=15),
            format_price(price),
            str(demand),
            format_station_name(station['name'], station, 18),
            f"{int(station.get('distanceToArrival', 0))}",
            format_timestamp(station['updateTime']) if station.get('updateTime') else 'unknown',
            f"{color_text(first_system['name'], 'red')}: {color_text('●', 'gold') if first_system['hotspots'] > 0 else '-'} {hotspotstring}, {first_system['rings']} rings available",
            str(control_points)
        ])
        
        # Additional rows for other mining systems
        for sys in entry['mining_systems'][1:]:
            if sys['hotspots'] > 1 or sys['hotspots'] == 0: hotspotstring = "Hotspots"
            else: hotspotstring = "Hotspot"
            hsColor = 'gold' if sys['hotspots'] > 0 else 'white'
            hotspotstring = color_text(f"{sys['hotspots']} {hotspotstring}", hsColor)
            table.add_row([
                '', '', '', '', '', '',
                f"{color_text(sys['name'], 'red')}: {color_text('●', 'gold') if sys['hotspots'] > 0 else '-'} {hotspotstring}, {sys['rings']} rings available",
                ''
            ])
        #table.add_row(["──","──","──","──","──","──","──","──"])

    return table.get_string()

async def send_chunked_message(channel, content, chunk_size=None):
    """Send a message in chunks if it's too long"""
    lines = content.split('\n')
    current_chunk = []
    current_length = 0
    
    for line in lines:
        # Add 1 for the newline character
        line_length = len(line) + 1
        
        # If adding this line would exceed limit, send current chunk and start new one
        if current_length + line_length > 1900:
            chunk_content = '\n'.join(current_chunk)
            message = f"```ansi\n{chunk_content}\n```"
            sent_message = await channel.send(message)
            print(f"\nBot message sent:")
            print(message)
            current_chunk = [line]
            current_length = line_length
        else:
            current_chunk.append(line)
            current_length += line_length
    
    # Send any remaining lines
    if current_chunk:
        chunk_content = '\n'.join(current_chunk)
        message = f"```ansi\n{chunk_content}\n```"
        sent_message = await channel.send(message)
        print(f"\nBot message sent:")
        print(message)

async def send_chunked_interaction(interaction, content, chunk_size=None):
    """Send a message in chunks for interactions"""
    lines = content.split('\n')
    current_chunk = []
    current_length = 0
    
    for line in lines:
        # Add 1 for the newline character
        line_length = len(line) + 1
        
        # If adding this line would exceed limit, send current chunk and start new one
        if current_length + line_length > 1900:
            chunk_content = '\n'.join(current_chunk)
            await interaction.followup.send(f"```ansi\n{chunk_content}\n```")
            current_chunk = [line]
            current_length = line_length
        else:
            current_chunk.append(line)
            current_length += line_length
    
    # Send any remaining lines
    if current_chunk:
        chunk_content = '\n'.join(current_chunk)
        await interaction.followup.send(f"```ansi\n{chunk_content}\n```")