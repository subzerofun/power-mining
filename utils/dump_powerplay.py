#!/usr/bin/env python3
import json
import psycopg2
import os
from urllib.parse import urlparse
from colorama import init, Fore, Back, Style

# Initialize colorama
init()

# Power color mappings
POWER_COLORS = {
    'Aisling Duval': (0x00, 0x99, 0xff),
    'Edmund Mahon': (0x01, 0x9c, 0x00),
    'A. Lavigny-Duval': (0x7f, 0x00, 0xff),
    'Nakato Kaine': (0xa3, 0xf1, 0x27),
    'Felicia Winters': (0xff, 0xc4, 0x00),
    'Zachary Hudson': (0xff, 0xc4, 0x00),  # Same as Winters
    'Denton Patreus': (0x00, 0xff, 0xff),
    'Jerome Archer': (0xdf, 0x1d, 0xe4),
    'Zemina Torval': (0x00, 0x40, 0xff),
    'Pranav Antal': (0xff, 0xff, 0x00),
    'Li Yong-Rui': (0x33, 0xd6, 0x88),
    'Archon Delaine': (0xff, 0x00, 0x00),
    'Yuri Grom': (0xff, 0x80, 0x00)
}

def get_closest_ansi_color(r, g, b):
    # Direct mapping from debug output
    power_name = next((name for name, color in POWER_COLORS.items() if color == (r, g, b)), None)
    if power_name:
        direct_colors = {
            'A. Lavigny-Duval': Fore.MAGENTA,
            'Aisling Duval': Fore.LIGHTBLUE_EX,
            'Archon Delaine': Fore.RED,
            'Denton Patreus': Fore.CYAN,
            'Edmund Mahon': Fore.GREEN,
            'Felicia Winters': Fore.YELLOW,
            'Jerome Archer': Fore.LIGHTMAGENTA_EX,
            'Li Yong-Rui': Fore.LIGHTCYAN_EX,
            'Nakato Kaine': Fore.LIGHTGREEN_EX,
            'Pranav Antal': Fore.LIGHTYELLOW_EX,
            'Yuri Grom': Fore.LIGHTRED_EX,
            'Zachary Hudson': Fore.YELLOW,
            'Zemina Torval': Fore.BLUE
        }
        return direct_colors.get(power_name, Fore.WHITE)
    return Fore.WHITE

def dump_powerplay_data(db_url, output_file, no_hudson=False):
    """Dump powerplay data from database into a JSON file."""
    
    # Parse database URL
    url = urlparse(db_url)
    dbname = url.path[1:]  # Remove leading slash
    user = url.username
    password = url.password
    host = url.hostname
    port = url.port

    # Connect to the database
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

    try:
        with conn.cursor() as cur:
            # First count unpopulated systems
            cur.execute("""
                SELECT COUNT(*) 
                FROM systems 
                WHERE population IS NULL OR population = 0;
            """)
            unpopulated_count = cur.fetchone()[0]

            # Get all systems with their power control status, excluding unpopulated systems
            cur.execute("""
                WITH stronghold_carriers AS (
                    SELECT DISTINCT system_id64
                    FROM stations
                    WHERE station_name LIKE '%Stronghold Carrier%'
                )
                SELECT 
                    s.id64,
                    s.name,
                    s.x,
                    s.y,
                    s.z,
                    s.controlling_power,
                    s.power_state,
                    s.powers_acquiring,
                    s.distance_from_sol,
                    CASE WHEN sc.system_id64 IS NOT NULL THEN true END as has_stronghold_carrier
                FROM systems s
                LEFT JOIN stronghold_carriers sc ON s.id64 = sc.system_id64
                WHERE (s.population IS NOT NULL AND s.population > 0)
                ORDER BY s.name;
            """)
            
            systems = []
            hudson_controlling_count = 0
            hudson_acquiring_count = 0

            for row in cur.fetchall():
                # Handle Hudson filtering if enabled
                controlling_power = row[5]
                powers_acquiring = row[7] if row[7] else []

                if no_hudson:
                    if controlling_power == 'Zachary Hudson':
                        controlling_power = None
                        hudson_controlling_count += 1
                    if powers_acquiring and 'Zachary Hudson' in powers_acquiring:
                        powers_acquiring.remove('Zachary Hudson')
                        hudson_acquiring_count += 1

                system = {
                    'id64': row[0],
                    'name': row[1],
                    'x': float(row[2]),
                    'y': float(row[3]),
                    'z': float(row[4]),
                    'controlling_power': controlling_power,
                    'power_state': row[6],
                    'powers_acquiring': powers_acquiring,
                    'distance_from_sol': float(row[8])
                }
                if row[9]:  # has_stronghold_carrier is true
                    system['hasStrongholdCarrier'] = True
                systems.append(system)

            # Write to JSON file
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump({'systems': systems}, f, indent=2)
            
            # Generate statistics by power
            power_stats = {}
            unoccupied_count = 0  # Counter for systems without controlling power
            
            for system in systems:
                power = system.get('controlling_power')
                state = system.get('power_state')
                powers_acquiring = system.get('powers_acquiring', [])

                # Handle systems with a controlling power
                if power:
                    if power not in power_stats:
                        power_stats[power] = {
                            'Stronghold Carrier': 0,
                            'Stronghold': 0,
                            'Fortified': 0,
                            'Exploited': 0,
                            'Contested': 0,
                            'Prepared': 0,
                            'InPrepareRadius': 0,
                            'Expansion': 0,  # Added Expansion state
                            'Controlled': 0,  # Added Controlled state
                            'HomeSystem': 0   # Added HomeSystem state
                        }
                    
                    if state and state != 'Unoccupied':  # Skip Unoccupied state
                        power_stats[power][state] += 1
                    
                    if system.get('hasStrongholdCarrier'):
                        power_stats[power]['Stronghold Carrier'] += 1
                
                # Handle systems in powers_acquiring list
                elif state in ['Contested', 'Prepared', 'InPrepareRadius', 'Expansion'] and powers_acquiring:  # Added Expansion
                    for acquiring_power in powers_acquiring:
                        if acquiring_power not in power_stats:
                            power_stats[acquiring_power] = {
                                'Stronghold Carrier': 0,
                                'Stronghold': 0,
                                'Fortified': 0,
                                'Exploited': 0,
                                'Contested': 0,
                                'Prepared': 0,
                                'InPrepareRadius': 0,
                                'Expansion': 0,  # Added Expansion state
                                'Controlled': 0,  # Added Controlled state
                                'HomeSystem': 0   # Added HomeSystem state
                            }
                        power_stats[acquiring_power][state] += 1
                
                # Count unoccupied systems
                elif not power and not powers_acquiring:
                    unoccupied_count += 1
            
            # Print statistics
            print(f"\nPower Statistics:")
            print("=" * 50)
            # Sort powers alphabetically
            for power in sorted(power_stats.keys()):
                stats = power_stats[power]
                # Get power color and print which one we're using
                if power in POWER_COLORS:
                    r, g, b = POWER_COLORS[power]
                    color = get_closest_ansi_color(r, g, b)
                    print(f"\nPower: {color}{power}{Style.RESET_ALL}")
                else:
                    print(f"\nPower: {power}")
                print("-" * 20)
                for state, count in stats.items():
                    if count > 0:  # Only show non-zero counts
                        print(f"{state}: {count} systems")
            
            # Print unoccupied systems count
            print(f"\nUnoccupied: {unoccupied_count} systems")
            print(f"Unpopulated systems excluded: {unpopulated_count}")
            
            if no_hudson:
                print(f"\nZachary Hudson filtered out:")
                print(f"- Controlling systems: {hudson_controlling_count}")
                print(f"- Acquiring systems: {hudson_acquiring_count}")
            
            print(f"\nSuccessfully dumped {len(systems)} systems to {output_file}")

    finally:
        conn.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Dump powerplay data to JSON file')
    parser.add_argument('--db', required=True, help='Database URL')
    parser.add_argument('--output', default='json/powerplay.json', help='Output JSON file path')
    parser.add_argument('--no-hudson', action='store_true', help='Remove Zachary Hudson from output')
    
    args = parser.parse_args()
    dump_powerplay_data(args.db, args.output, args.no_hudson) 