import csv
import sqlite3
import json
import zlib
import base64
from pathlib import Path
from typing import Dict, List, Optional

def dict_factory(cursor, row):
    """Simple dict factory without decompression."""
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

def load_res_data(database_path) -> List[Dict]:
    """Load RES hotspot data from CSV file."""
    res_data = []
    with open('data/plat-hs-and-res-maps.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            res_data.append({
                'system': row['System'],
                'ring': row['Ring'],
                'ls': row['ls'],
                'res_zone': row['RES/Pt HS?'],
                'comment': row['For edtools list']
            })
    return res_data

def get_system_info(conn: sqlite3.Connection, system_name: str) -> Optional[Dict]:
    """Get system information from database."""
    cursor = conn.cursor()
    cursor.row_factory = dict_factory
    cursor.execute('''
        SELECT name, controlling_power, x, y, z
        FROM systems
        WHERE name = ?
    ''', (system_name,))
    return cursor.fetchone()

def get_station_commodities(conn: sqlite3.Connection, system_id64: int) -> List[Dict]:
    """Get station commodity information for a system."""
    cursor = conn.cursor()
    cursor.row_factory = dict_factory
    
    # Get all commodities in a single query
    cursor.execute('''
        SELECT DISTINCT 
            s.station_name,
            s.landing_pad_size,
            s.distance_to_arrival,
            s.station_type,
            s.update_time,
            sc.commodity_name,
            sc.sell_price,
            sc.demand,
            CASE 
                WHEN sc.commodity_name IN ('Platinum', 'Painite', 'Osmium') THEN 1 
                ELSE 2 
            END as priority
        FROM stations s
        JOIN station_commodities sc ON s.system_id64 = sc.system_id64 
            AND s.station_name = sc.station_name
        WHERE s.system_id64 = ?
        AND sc.sell_price > 0 AND sc.demand > 0
        ORDER BY s.station_name, priority, sc.sell_price DESC
    ''', (system_id64,))
    
    stations = {}
    current_station = None
    other_count = 0
    
    for row in cursor.fetchall():
        station_name = row['station_name']
        
        if station_name not in stations:
            stations[station_name] = {
                'name': station_name,
                'pad_size': row['landing_pad_size'],
                'distance': row['distance_to_arrival'],
                'station_type': row['station_type'],
                'update_time': row['update_time'],
                'other_commodities': []
            }
            current_station = station_name
            other_count = 0
            
        # Add commodity if it's a priority commodity or if we haven't hit the limit for other commodities
        if row['priority'] == 1 or other_count < 3:
            stations[station_name]['other_commodities'].append({
                'name': row['commodity_name'],
                'sell_price': row['sell_price'],
                'demand': row['demand']
            })
            if row['priority'] == 2:
                other_count += 1
    
    return list(stations.values())

def calculate_distance(x1: float, y1: float, z1: float, x2: float, y2: float, z2: float) -> float:
    """Calculate distance between two points in 3D space."""
    return ((x2-x1)**2 + (y2-y1)**2 + (z2-z1)**2) ** 0.5 

def load_high_yield_platinum():
    """Load high yield platinum hotspot data from CSV file."""
    data = []
    csv_path = Path(__file__).parent / 'data' / 'plat-high-yield-hotspots.csv'
    
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append({
                'system': row['Name'],
                'dst': row['Dist'],
                'ring': row['Ring'],
                'percentage': row['Percentage'],
                'comment': row['Comment']
            })
    
    # Sort by distance
    data.sort(key=lambda x: float(x['dst']))
    return data 