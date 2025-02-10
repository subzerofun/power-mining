import csv
import json
import zlib
import base64
from pathlib import Path
from typing import Dict, List, Optional
import psycopg2
from psycopg2.extras import DictCursor
import os
from flask import jsonify, request, current_app
from utils.common import BASE_DIR, get_db_connection
from utils.search_common import format_station_info, get_other_commodities
from utils.search_power import get_opposing_power_filter, build_power_conditions

def dict_factory(cursor, row):
    """Simple dict factory without decompression."""
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

def load_res_data() -> List[Dict]:
    """Load RES hotspot data from CSV file."""
    try:
        res_data = []
        csv_path = os.path.join(BASE_DIR, 'data/plat-hs-and-res-maps.csv')
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row['System'] or not row['Ring']:  # Skip empty rows
                    continue
                res_data.append({
                    'system': row['System'],
                    'ring': row['Ring'],
                    'ls': row['ls'],
                    'res_zone': row['RES/Pt HS?'],
                    'comment': row['For edtools list']
                })
        return res_data
    except Exception as e:
        print(f"Error loading RES data: {str(e)}")
        return []

def get_system_info(conn, system_name: str) -> Optional[Dict]:
    """Get system information from database."""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT name, controlling_power, powers_acquiring, x, y, z
        FROM systems
        WHERE name = %s
    ''', (system_name,))
    return cursor.fetchone()

def get_station_commodities(conn, system_id64: int) -> List[Dict]:
    """Get station commodity information for a system."""
    cursor = conn.cursor()
    
    # Get all commodities in a single query
    cursor.execute('''
        SELECT 
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
        WHERE s.system_id64 = %s
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
                'update_time': row['update_time'].strftime('%Y-%m-%d') if row['update_time'] else None,
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
    try:
        data = []
        csv_path = os.path.join(BASE_DIR, 'data/plat-high-yield-hotspots.csv')
        
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row['Name'] or not row['Ring']:  # Skip empty rows
                    continue
                data.append({
                    'system': row['Name'],
                    'ring': row['Ring'],
                    'percentage': row['Percentage'],
                    'comment': row['Comment']
                })
        
        return data
    except Exception as e:
        print(f"Error loading high yield platinum data: {str(e)}")
        return [] 

def search_res_hotspots():
    """RES hotspots search with distance, limit and power filters"""
    try:
        # Get all query parameters from URL
        ref_system = request.args.get('system', 'Sol')
        max_distance = float(request.args.get('distance', '100'))  # Default 100 Ly
        limit = int(request.args.get('limit', '10'))  # Default 10 results
        controlling_power = request.args.get('controlling_power', 'Any')
        opposing_power = request.args.get('opposing_power', 'Any')
        
        print(f"Search parameters - ref_system: {ref_system}, distance: {max_distance}, limit: {limit}, "
              f"controlling_power: {controlling_power}, opposing_power: {opposing_power}")
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        c = conn.cursor(cursor_factory=DictCursor)
        
        c.execute('SELECT x, y, z FROM systems WHERE name ILIKE %s', (ref_system,))
        ref_coords = c.fetchone()
        if not ref_coords:
            conn.close()
            return jsonify({'error': 'Reference system not found'}), 404
            
        rx, ry, rz = ref_coords['x'], ref_coords['y'], ref_coords['z']
        hotspot_data = load_res_data()
        if not hotspot_data:
            conn.close()
            return jsonify({'error': 'No RES hotspot data available'}), 404
            
        results = []
        for e in hotspot_data:
            # Build SQL with power conditions
            sql = '''SELECT s.*, sqrt(power(s.x - %s, 2) + power(s.y - %s, 2) + power(s.z - %s, 2)) as distance
                    FROM systems s WHERE s.name ILIKE %s'''
            params = [rx, ry, rz, e['system']]
            
            # Add power conditions
            if controlling_power != 'Any' or opposing_power != 'Any':
                opp_conditions, opp_params = get_opposing_power_filter(opposing_power)
                # For simple controlling power filtering, use empty string as power_goal
                power_conditions, power_params = build_power_conditions('', controlling_power)
                
                if opp_conditions:
                    sql += " AND " + " AND ".join(opp_conditions)
                    params.extend(opp_params)
                if power_conditions:
                    sql += " AND " + " AND ".join(power_conditions)
                    params.extend(power_params)
            
            c.execute(sql, params)
            system = c.fetchone()
            if not system:
                continue
                
            # Skip if beyond max distance
            if float(system['distance']) > max_distance:
                continue
                
            st = get_station_commodities(conn, system['id64'])
            results.append({
                'system': e['system'],
                'controlling_power': system['controlling_power'] or 'None',
                'powers_acquiring': system['powers_acquiring'],
                'power_state': 'Control',
                'distance': float(system['distance']),
                'ring': e['ring'],
                'ls': e['ls'],
                'res_zone': e['res_zone'],
                'comment': e['comment'],
                'stations': st
            })
        
        print(f"Found {len(results)} results before limit")
        # Sort by distance and limit results
        results.sort(key=lambda x: x['distance'])
        results = results[:limit]
        print(f"Returning {len(results)} results after limit")
        
        conn.close()
        return jsonify(results)
    except Exception as e:
        current_app.logger.error(f"Error in search_res_hotspots: {str(e)}")
        return jsonify({'error': str(e)}), 500

def search_high_yield_platinum():
    """High yield platinum search with distance, limit and power filters"""
    try:
        # Get all query parameters from URL
        ref_system = request.args.get('system', 'Sol')
        max_distance = float(request.args.get('distance', '100'))  # Default 100 Ly
        limit = int(request.args.get('limit', '10'))  # Default 10 results
        controlling_power = request.args.get('controlling_power', 'Any')
        opposing_power = request.args.get('opposing_power', 'Any')
        
        print(f"Search parameters - ref_system: {ref_system}, distance: {max_distance}, limit: {limit}, "
              f"controlling_power: {controlling_power}, opposing_power: {opposing_power}")
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        c = conn.cursor(cursor_factory=DictCursor)
        
        c.execute('SELECT x, y, z FROM systems WHERE name = %s', (ref_system,))
        ref_coords = c.fetchone()
        if not ref_coords:
            conn.close()
            return jsonify({'error': 'Reference system not found'}), 404
            
        rx, ry, rz = ref_coords['x'], ref_coords['y'], ref_coords['z']
        data = load_high_yield_platinum()
        if not data:
            conn.close()
            return jsonify({'error': 'No high yield platinum data available'}), 404
            
        results = []
        for e in data:
            # Build SQL with power conditions
            sql = '''SELECT s.*, sqrt(power(s.x - %s, 2) + power(s.y - %s, 2) + power(s.z - %s, 2)) as distance
                    FROM systems s WHERE s.name = %s'''
            params = [rx, ry, rz, e['system']]
            
            # Add power conditions
            if controlling_power != 'Any' or opposing_power != 'Any':
                opp_conditions, opp_params = get_opposing_power_filter(opposing_power)
                # For simple controlling power filtering, use empty string as power_goal
                power_conditions, power_params = build_power_conditions('', controlling_power)
                
                if opp_conditions:
                    sql += " AND " + " AND ".join(opp_conditions)
                    params.extend(opp_params)
                if power_conditions:
                    sql += " AND " + " AND ".join(power_conditions)
                    params.extend(power_params)
            
            c.execute(sql, params)
            system = c.fetchone()
            if not system:
                continue
                
            # Skip if beyond max distance
            if float(system['distance']) > max_distance:
                continue
                
            st = get_station_commodities(conn, system['id64'])
            results.append({
                'system': e['system'],
                'controlling_power': system['controlling_power'] or 'None',
                'powers_acquiring': system['powers_acquiring'],
                'power_state': 'Control',
                'distance': float(system['distance']),
                'ring': e['ring'],
                'percentage': e['percentage'],
                'comment': e['comment'],
                'stations': st
            })
        
        print(f"Found {len(results)} results before limit")
        # Sort by distance and limit results
        results.sort(key=lambda x: x['distance'])
        results = results[:limit]
        print(f"Returning {len(results)} results after limit")
        
        conn.close()
        return jsonify(results)
    except Exception as e:
        print(f"Error in search_high_yield_platinum: {str(e)}")
        return jsonify({'error': str(e)}), 500 