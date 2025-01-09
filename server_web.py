import os, sys, math, json, zlib, time, signal, atexit, argparse, asyncio, subprocess, threading, psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_sock import Sock
import zmq
import psutil
from typing import Dict, List, Optional
import mining_data_web as mining_data, res_data_web as res_data
from mining_data_web import (
    get_material_ring_types, get_non_hotspot_materials_list,
    get_ring_type_case_statement, get_mining_type_conditions,
    get_price_comparison, normalize_commodity_name,
    get_potential_ring_types, PRICE_DATA, NON_HOTSPOT_MATERIALS
)
import tempfile
import io

YELLOW, BLUE, RESET = '\033[93m', '\033[94m', '\033[0m'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Environment variables for configuration
DATABASE_URL = None  # Will be set from args or env in main()

# Process ID file for update_live_web.py
PID_FILE = os.path.join(tempfile.gettempdir(), 'update_live_web.pid')

# ZMQ setup for status updates
STATUS_PORT = int(os.getenv('STATUS_PORT', '5557'))
zmq_context = zmq.Context()

app = Flask(__name__, template_folder=BASE_DIR, static_folder=None)
sock = Sock(app)
updater_process = None
live_update_requested = False
eddn_status = {"state": "offline", "last_db_update": None}

def kill_updater_process():
    global updater_process
    if updater_process:
        try:
            p = psutil.Process(updater_process.pid)
            if "update_live_web.py" in p.cmdline():  # Verify it's our process
                for c in p.children(recursive=True):
                    try: c.kill()
                    except: pass
                if os.name == 'nt': updater_process.send_signal(signal.CTRL_BREAK_EVENT)
                else: updater_process.terminate()
                try: updater_process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    if os.name == 'nt': os.kill(updater_process.pid, signal.SIGTERM)
                    else: updater_process.kill()
            updater_process = None
        except: pass

def stop_updater():
    global eddn_status
    eddn_status["state"] = "offline"
    kill_updater_process()
    if os.path.exists(PID_FILE):
        try:
            os.remove(PID_FILE)
        except:
            pass

def cleanup_handler(signum, frame):
    print("\nReceived signal to shutdown...")
    print("Stopping EDDN Update Service...")
    stop_updater()
    if os.path.exists(PID_FILE):
        try:
            os.remove(PID_FILE)
        except:
            pass
    print("Stopping Web Server...")
    os._exit(0)

atexit.register(kill_updater_process)
signal.signal(signal.SIGINT, cleanup_handler)
signal.signal(signal.SIGTERM, cleanup_handler)
if os.name == 'nt':
    signal.signal(signal.SIGBREAK, cleanup_handler)
    signal.signal(signal.SIGABRT, cleanup_handler)

def handle_output(line):
    """Handle output from the EDDN updater process"""
    global eddn_status
    line = line.strip()
    print(f"{YELLOW if '[INIT]' in line or '[STOPPING]' in line or '[TERMINATED]' in line else BLUE}{line}{RESET}", flush=True)
    
    if "[INIT]" in line:
        eddn_status["state"] = "starting"
    elif "Loaded" in line and "commodities from CSV" in line:
        eddn_status["state"] = "starting"
    elif "Listening to EDDN" in line:
        eddn_status["state"] = "running"
    elif "[DATABASE] Writing to Database starting..." in line:
        eddn_status["state"] = "updating"
        eddn_status["last_db_update"] = datetime.now().isoformat()
        eddn_status["update_start_time"] = time.time()
    elif "[DATABASE] Writing to Database finished." in line:
        if "update_start_time" in eddn_status:
            elapsed = time.time() - eddn_status["update_start_time"]
            if elapsed < 1:
                time.sleep(1 - elapsed)
            del eddn_status["update_start_time"]
        eddn_status["state"] = "running"
    elif "[STOPPING]" in line or "[TERMINATED]" in line:
        eddn_status["state"] = "offline"
        print(f"{YELLOW}[STATUS] EDDN updater stopped{RESET}", flush=True)
    elif "Error:" in line or "[ERROR]" in line:
        eddn_status["state"] = "error"
        print(f"{YELLOW}[STATUS] EDDN updater encountered an error{RESET}", flush=True)

@sock.route('/ws')
def handle_websocket(ws):
    try:
        # Create ZMQ subscriber for status updates
        subscriber = zmq_context.socket(zmq.SUB)
        subscriber.connect(f"tcp://localhost:{STATUS_PORT}")
        subscriber.setsockopt_string(zmq.SUBSCRIBE, "")
        
        # Send initial status
        ws.send(json.dumps({"state": "offline", "last_db_update": None}))
        
        while True:
            try:
                # Use poll to avoid blocking forever
                if subscriber.poll(100):  # 100ms timeout
                    message = subscriber.recv_string()
                    ws.send(message)
            except zmq.ZMQError:
                continue
            
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        subscriber.close()

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path),'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/<path:filename>')
def serve_static(filename):
    mime_types = {'.js':'application/javascript','.css':'text/css','.html':'text/html','.ico':'image/x-icon',
                  '.svg':'image/svg+xml','.png':'image/png','.jpg':'image/jpeg','.jpeg':'image/jpeg',
                  '.gif':'image/gif','.woff':'font/woff','.woff2':'font/woff2','.ttf':'font/ttf'}
    _, ext = os.path.splitext(filename)
    mt = mime_types.get(ext.lower(),'application/octet-stream')
    r = send_from_directory(BASE_DIR, filename, mimetype=mt)
    if ext.lower() == '.js': r.headers['Access-Control-Allow-Origin'] = '*'
    return r

@app.route('/css/<path:filename>')
def serve_css(filename): return send_from_directory(os.path.join(BASE_DIR, 'css'), filename)

@app.route('/js/<path:filename>')
def serve_js(filename):
    r = send_from_directory(os.path.join(BASE_DIR, 'js'), filename, mimetype='application/javascript')
    r.headers['Access-Control-Allow-Origin'] = '*'
    return r

@app.route('/fonts/<path:filename>')
def serve_fonts(filename): return send_from_directory(os.path.join(BASE_DIR, 'fonts'), filename)

@app.route('/img/<path:filename>')
def serve_images(filename): return send_from_directory(os.path.join(BASE_DIR, 'img'), filename)

@app.route('/img/loading/<path:filename>')
def serve_loading_js(filename):
    if filename.endswith('.js'):
        r = send_from_directory(os.path.join(BASE_DIR, 'img','loading'), filename, mimetype='application/javascript')
        r.headers['Access-Control-Allow-Origin']='*'; return r
    return send_from_directory(os.path.join(BASE_DIR,'img','loading'),filename)

@app.route('/Config.ini')
def serve_config():
    try:
        path = os.path.join(BASE_DIR,'Config.ini')
        if not os.path.exists(path):
            with open(path, 'w') as f: f.write("[Defaults]\nsystem = Harma\ncontrolling_power = Archon Delaine\nmax_distance = 200\nsearch_results = 30\n")
        r = send_from_directory(BASE_DIR, 'Config.ini', mimetype='text/plain')
        r.headers['Cache-Control']='no-cache, no-store, must-revalidate'; r.headers['Pragma']='no-cache'; r.headers['Expires']='0'
        return r
    except Exception as e:
        app.logger.error(f"Error serving Config.ini: {str(e)}")
        return jsonify({'Defaults':{'system':'Harma','controlling_power':'Archon Delaine','max_distance':'200','search_results':'30'}})


def dict_factory(cursor,row):
    d = {}
    for i,col in enumerate(cursor.description):
        d[col[0]]=row[i]
    return d

def get_db_connection():
    """Get a database connection"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.cursor_factory = DictCursor  # This provides dict-like access similar to sqlite3's dict_factory
        return conn
    except Exception as e:
        app.logger.error(f"Database connection error: {str(e)}")
        return None

def calculate_distance(x1,y1,z1,x2,y2,z2): return math.sqrt((x2-x1)**2+(y2-y1)**2+(z2-z1)**2)

def get_ring_materials():
    rm={}
    try:
        with open('data/ring_materials.csv','r') as f:
            next(f)
            for line in f:
                mat,ab,rt,cond,val=line.strip().split(',')
                rm[mat]={'ring_types':[x.strip() for x in rt.split('/')],'abbreviation':ab,'conditions':cond,'value':val}
    except Exception as e:
        app.logger.error(f"Error loading ring materials: {str(e)}")
    return rm

@app.route('/')
def index(): return render_template('index_web.html')

@app.route('/autocomplete')
def autocomplete():
    try:
        s=request.args.get('q','').strip()
        if len(s)<2: return jsonify([])
        conn=get_db_connection()
        if not conn: return jsonify({'error':'Database not found'}),500
        c=conn.cursor()
        c.execute("SELECT name,x,y,z FROM systems WHERE name LIKE ? || '%' LIMIT 10",(s,))
        res=[{'name':r['name'],'coords':{'x':r['x'],'y':r['y'],'z':r['z']}} for r in c.fetchall()]
        conn.close(); return jsonify(res)
    except Exception as e:
        app.logger.error(f"Autocomplete error: {str(e)}")
        return jsonify({'error':'Error during autocomplete'}),500

@app.route('/search')
def search():
    try:
        ref_system = request.args.get('system', 'Sol')
        max_dist = float(request.args.get('distance', '10000'))
        controlling_power = request.args.get('controlling_power')
        power_states = request.args.getlist('power_state[]')
        signal_type = request.args.get('signal_type')
        ring_type_filter = request.args.get('ring_type_filter', 'All')
        limit = int(request.args.get('limit', '30'))
        mining_types = request.args.getlist('mining_types[]')

        if mining_types and 'All' not in mining_types:
            with open('data/mining_data.json', 'r') as f:
                mat_data = json.load(f)
            if not next((i for i in mat_data['materials'] if i['name'] == signal_type), None):
                return jsonify([])

        ring_materials = get_ring_materials()
        is_ring_material = signal_type in ring_materials
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cur = conn.cursor()
        cur.execute('SELECT x, y, z FROM systems WHERE name = %s', (ref_system,))
        ref_coords = cur.fetchone()
        if not ref_coords:
            conn.close()
            return jsonify({'error': 'Reference system not found'}), 404

        rx, ry, rz = ref_coords['x'], ref_coords['y'], ref_coords['z']
        mining_cond = ''
        mining_params = []
        if mining_types and 'All' not in mining_types:
            mining_cond, mining_params = get_mining_type_conditions(signal_type, mining_types)

        ring_cond = ''
        ring_params = []
        if ring_type_filter != 'All':
            if ring_type_filter == 'Just Hotspots':
                ring_cond = ' AND ms.mineral_type IS NOT NULL'
            elif ring_type_filter == 'Without Hotspots':
                ring_cond = ' AND (ms.mineral_type IS NULL OR ms.mineral_type != %s)'
                ring_params.append(signal_type)
                try:
                    with open('data/mining_data.json', 'r') as f:
                        mat_data = json.load(f)
                        cd = next((item for item in mat_data['materials'] if item['name'] == signal_type), None)
                        if cd:
                            rt = []
                            for r_type, rd in cd['ring_types'].items():
                                if any([rd['surfaceLaserMining'], rd['surfaceDeposit'], rd['subSurfaceDeposit'], rd['core']]):
                                    rt.append(r_type)
                            if rt:
                                ring_cond += ' AND ms.ring_type = ANY(%s::text[])'
                                ring_params.append(rt)
                except:
                    pass
            else:
                ring_cond = ' AND ms.ring_type = %s'
                ring_params.append(ring_type_filter)
                try:
                    with open('data/mining_data.json', 'r') as f:
                        mat_data = json.load(f)
                        cd = next((i for i in mat_data['materials'] if i['name'] == signal_type), None)
                        if not cd or ring_type_filter not in cd['ring_types']:
                            return jsonify([])
                except:
                    pass

        non_hotspot = get_non_hotspot_materials_list()
        is_non_hotspot = signal_type in non_hotspot
        non_hotspot_str = ','.join(f"'{material}'" for material in non_hotspot)
        
        # Build the ring type case statement
        ring_type_cases = []
        for material, ring_types in mining_data.NON_HOTSPOT_MATERIALS.items():
            ring_types_str = ','.join(f"'{rt}'" for rt in ring_types)
            ring_type_cases.append(f"WHEN hp.commodity_name = '{material}' AND ms.ring_type IN ({ring_types_str}) THEN 1")
        ring_type_case = '\n'.join(ring_type_cases)
        
        if is_non_hotspot:
            # Get ring types from NON_HOTSPOT_MATERIALS dictionary
            ring_types = mining_data.NON_HOTSPOT_MATERIALS.get(signal_type, [])
            
            # Build all WHERE conditions first
            where_conditions = ["ms.ring_type = ANY(%s::text[])"]
            params = [rx, rx, ry, ry, rz, rz, max_dist, signal_type, signal_type, ring_types]
            
            if controlling_power:
                where_conditions.append("s.controlling_power = %s")
                params.append(controlling_power)

            if power_states:
                where_conditions.append("s.power_state = ANY(%s::text[])")
                params.append(power_states)

            if mining_cond:
                where_conditions.append(mining_cond)
                params.extend(mining_params)

            query = f"""
            WITH relevant_systems AS (
                SELECT s.*, SQRT(POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2)) as distance
                FROM systems s
                WHERE POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2) <= POWER(%s, 2)
            ),
            relevant_stations AS (
                SELECT sc.system_id64, sc.station_name, sc.sell_price, sc.demand
                FROM station_commodities sc
                WHERE (sc.commodity_name = %s OR (%s = 'LowTemperatureDiamond' AND sc.commodity_name = 'Low Temperature Diamonds'))
                AND sc.demand > 0 AND sc.sell_price > 0
            )
            SELECT DISTINCT s.name as system_name, s.id64 as system_id64, s.controlling_power,
                s.power_state, s.distance, ms.body_name, ms.ring_name, ms.ring_type,
                ms.mineral_type, ms.signal_count, ms.reserve_level, rs.station_name,
                st.landing_pad_size, st.distance_to_arrival as station_distance,
                st.station_type, rs.demand, rs.sell_price, st.update_time,
                rs.sell_price as sort_price
            FROM relevant_systems s
            JOIN mineral_signals ms ON s.id64 = ms.system_id64
            LEFT JOIN relevant_stations rs ON s.id64 = rs.system_id64
            LEFT JOIN stations st ON s.id64 = st.system_id64 AND rs.station_name = st.station_name
            WHERE """ + " AND ".join(where_conditions) + """
            ORDER BY sort_price DESC NULLS LAST, s.distance ASC"""

        else:
            # Build all WHERE conditions first
            where_conditions = ["1=1"]  # Start with a dummy condition
            params = [rx, rx, ry, ry, rz, rz, max_dist, signal_type, signal_type]

            if controlling_power:
                where_conditions.append("s.controlling_power = %s")
                params.append(controlling_power)

            if power_states:
                where_conditions.append("s.power_state = ANY(%s::text[])")
                params.append(power_states)

            if mining_cond:
                where_conditions.append(mining_cond)
                params.extend(mining_params)

            if ring_cond:
                where_conditions.append(ring_cond.lstrip(" AND "))
                params.extend(ring_params)

            query = f"""
            WITH relevant_systems AS (
                SELECT s.*, SQRT(POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2)) as distance
                FROM systems s
                WHERE POWER(s.x - %s, 2) + POWER(s.y - %s, 2) + POWER(s.z - %s, 2) <= POWER(%s, 2)
            ),
            relevant_stations AS (
                SELECT sc.system_id64, sc.station_name, sc.sell_price, sc.demand
                FROM station_commodities sc
                WHERE (sc.commodity_name = %s OR (%s = 'LowTemperatureDiamond' AND sc.commodity_name = 'Low Temperature Diamonds'))
                AND sc.demand > 0 AND sc.sell_price > 0
            )
            SELECT DISTINCT s.name as system_name, s.id64 as system_id64, s.controlling_power,
                s.power_state, s.distance, ms.body_name, ms.ring_name, ms.ring_type,
                ms.mineral_type, ms.signal_count, ms.reserve_level, rs.station_name,
                st.landing_pad_size, st.distance_to_arrival as station_distance,
                st.station_type, rs.demand, rs.sell_price, st.update_time,
                rs.sell_price as sort_price
            FROM relevant_systems s
            """

            if ring_type_filter != 'Without Hotspots':
                query += " JOIN mineral_signals ms ON s.id64 = ms.system_id64 AND ms.mineral_type = %s"
                params.append(signal_type)
            else:
                query += " JOIN mineral_signals ms ON s.id64 = ms.system_id64"

            query += """
            LEFT JOIN relevant_stations rs ON s.id64 = rs.system_id64
            LEFT JOIN stations st ON s.id64 = st.system_id64 AND rs.station_name = st.station_name
            WHERE """ + " AND ".join(where_conditions)

            if is_ring_material:
                query += """
                ORDER BY 
                    CASE 
                        WHEN ms.reserve_level = 'Pristine' THEN 1
                        WHEN ms.reserve_level = 'Major' THEN 2
                        WHEN ms.reserve_level = 'Common' THEN 3
                        WHEN ms.reserve_level = 'Low' THEN 4
                        WHEN ms.reserve_level = 'Depleted' THEN 5
                        ELSE 6 
                    END,
                    rs.sell_price DESC NULLS LAST,
                    s.distance ASC
                """
            else:
                query += " ORDER BY sort_price DESC NULLS LAST, s.distance ASC"

            if limit:
                query += " LIMIT %s"
                params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()
        pr = []
        cur_sys = None

        station_pairs = [(r['system_id64'], r['station_name']) for r in rows if r['station_name']]
        other_commodities = {}

        if station_pairs:
            oc = conn.cursor()
            ph = ','.join(['(%s,%s)'] * len(station_pairs))
            ps = [x for pair in station_pairs for x in pair]
            sel_mats = request.args.getlist('selected_materials[]', type=str)

            if sel_mats and sel_mats != ['Default']:
                full_names = [mining_data.MATERIAL_CODES.get(m,m) for m in sel_mats]
                oc.execute(f"""
                    SELECT sc.system_id64, sc.station_name, sc.commodity_name, sc.sell_price, sc.demand,
                    COUNT(*) OVER (PARTITION BY sc.system_id64, sc.station_name) total_commodities
                    FROM station_commodities sc
                    WHERE (sc.system_id64, sc.station_name) IN ({ph})
                    AND sc.commodity_name = ANY(%s::text[])
                    AND sc.sell_price > 0 AND sc.demand > 0
                    ORDER BY sc.system_id64, sc.station_name, sc.sell_price DESC
                """, ps + [full_names])
            else:
                oc.execute(f"""
                    SELECT system_id64, station_name, commodity_name, sell_price, demand
                    FROM station_commodities
                    WHERE (system_id64, station_name) IN ({ph})
                    AND sell_price > 0 AND demand > 0
                    ORDER BY sell_price DESC
                """, ps)

            for r2 in oc.fetchall():
                k = (r2['system_id64'], r2['station_name'])
                if k not in other_commodities:
                    other_commodities[k] = []
                if len(other_commodities[k]) < 6:
                    other_commodities[k].append({
                        'name': r2['commodity_name'],
                        'sell_price': r2['sell_price'],
                        'demand': r2['demand']
                    })

        for row in rows:
            if cur_sys is None or cur_sys['name'] != row['system_name']:
                if cur_sys:
                    pr.append(cur_sys)
                cur_sys = {
                    'name': row['system_name'],
                    'controlling_power': row['controlling_power'],
                    'power_state': row['power_state'],
                    'distance': float(row['distance']),
                    'system_id64': row['system_id64'],
                    'rings': [],
                    'stations': [],
                    'all_signals': []
                }

            if is_ring_material:
                re = {
                    'name': row['ring_name'],
                    'body_name': row['body_name'],
                    'signals': f"{signal_type} ({row['ring_type']}, {row['reserve_level']})"
                }
                if re not in cur_sys['rings']:
                    cur_sys['rings'].append(re)
            else:
                if ring_type_filter == 'Without Hotspots':
                    re = {
                        'name': row['ring_name'],
                        'body_name': row['body_name'],
                        'signals': f"{signal_type} ({row['ring_type']}, {row['reserve_level']})"
                    }
                    if re not in cur_sys['rings']:
                        cur_sys['rings'].append(re)
                else:
                    if row['mineral_type'] == signal_type:
                        re = {
                            'name': row['ring_name'],
                            'body_name': row['body_name'],
                            'signals': f"{signal_type}: {row['signal_count'] or ''} ({row['reserve_level']})"
                        }
                        if re not in cur_sys['rings']:
                            cur_sys['rings'].append(re)

            si = {
                'ring_name': row['ring_name'],
                'mineral_type': row['mineral_type'],
                'signal_count': row['signal_count'] or '',
                'reserve_level': row['reserve_level'],
                'ring_type': row['ring_type']
            }
            if si not in cur_sys['all_signals'] and si['mineral_type']:
                cur_sys['all_signals'].append(si)

            if row['station_name']:
                try:
                    ex = next((s for s in cur_sys['stations'] if s['name'] == row['station_name']), None)
                    if ex:
                        ex['other_commodities'] = other_commodities.get((row['system_id64'], row['station_name']), [])
                    else:
                        stn = {
                            'name': row['station_name'],
                            'pad_size': row['landing_pad_size'],
                            'distance': float(row['station_distance']) if row['station_distance'] else 0,
                            'demand': int(row['demand']) if row['demand'] else 0,
                            'sell_price': int(row['sell_price']) if row['sell_price'] else 0,
                            'station_type': row['station_type'],
                            'update_time': row.get('update_time'),
                            'system_id64': row['system_id64'],
                            'other_commodities': other_commodities.get((row['system_id64'], row['station_name']), [])
                        }
                        cur_sys['stations'].append(stn)
                except:
                    pass

        if cur_sys:
            pr.append(cur_sys)

        if not is_non_hotspot and pr:
            sys_ids = [s['system_id64'] for s in pr]
            cur.execute("""
                SELECT system_id64, ring_name, mineral_type, signal_count, reserve_level, ring_type
                FROM mineral_signals
                WHERE system_id64 = ANY(%s::bigint[]) AND mineral_type != %s
            """, [sys_ids, signal_type])

            other_sigs = {}
            for r in cur.fetchall():
                if r['system_id64'] not in other_sigs:
                    other_sigs[r['system_id64']] = []
                other_sigs[r['system_id64']].append({
                    'ring_name': r['ring_name'],
                    'mineral_type': r['mineral_type'],
                    'signal_count': r['signal_count'] or '',
                    'reserve_level': r['reserve_level'],
                    'ring_type': r['ring_type']
                })

            for s in pr:
                s['all_signals'].extend(other_sigs.get(s['system_id64'], []))

        conn.close()
        return jsonify(pr)

    except Exception as e:
        app.logger.error(f"Search error: {str(e)}")
        return jsonify({'error': f'Search error: {str(e)}'}), 500

@app.route('/search_highest')
def search_highest():
    try:
        # Get power filters
        controlling_power = request.args.get('controlling_power')
        power_states = request.args.getlist('power_state[]')
        limit = int(request.args.get('limit', '30'))
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cur = conn.cursor()
        
        # Build all WHERE conditions first
        where_conditions = ["sc.demand > 0", "sc.sell_price > 0"]
        params = []
        
        if controlling_power:
            where_conditions.append("s.controlling_power = %s")
            params.append(controlling_power)
        
        if power_states:
            placeholders = ','.join(['%s' for _ in power_states])
            where_conditions.append(f"s.power_state IN ({placeholders})")
            params.extend(power_states)
        
        where_clause = " AND ".join(where_conditions)
        
        # Get the list of non-hotspot materials
        non_hotspot = get_non_hotspot_materials_list()
        non_hotspot_str = "'" + "','".join(non_hotspot) + "'"
        
        # Build ring type case statement
        ring_type_cases = []
        for material, ring_types in mining_data.NON_HOTSPOT_MATERIALS.items():
            ring_types_str = "'" + "','".join(ring_types) + "'"
            ring_type_cases.append(f"WHEN hp.commodity_name = '{material}' AND ms.ring_type IN ({ring_types_str}) THEN 1")
        ring_type_case = '\n'.join(ring_type_cases)
        
        query = f"""
        WITH HighestPrices AS (
            SELECT DISTINCT 
                sc.commodity_name,
                sc.sell_price,
                sc.demand,
                s.id64 as system_id64,
                s.name as system_name,
                s.controlling_power,
                s.power_state,
                st.landing_pad_size,
                st.distance_to_arrival,
                st.station_type,
                sc.station_name,
                st.update_time
            FROM station_commodities sc
            JOIN systems s ON s.id64 = sc.system_id64
            JOIN stations st ON st.system_id64 = s.id64 AND st.station_name = sc.station_name
            WHERE {where_clause}
            ORDER BY sc.sell_price DESC
            LIMIT 1000
        ),
        MinableCheck AS (
            SELECT DISTINCT
                hp.*,
                ms.mineral_type,
                ms.ring_type,
                ms.reserve_level,
                CASE
                    WHEN hp.commodity_name NOT IN ({non_hotspot_str})
                        AND ms.mineral_type = hp.commodity_name THEN 1
                    WHEN hp.commodity_name = 'Low Temperature Diamonds' 
                        AND ms.mineral_type = 'LowTemperatureDiamond' THEN 1
                    {ring_type_case}
                    ELSE 0
                END as is_minable
            FROM HighestPrices hp
            JOIN mineral_signals ms ON hp.system_id64 = ms.system_id64
        )
        SELECT DISTINCT
            commodity_name,
            sell_price as max_price,
            system_name,
            controlling_power,
            power_state,
            landing_pad_size,
            distance_to_arrival,
            demand,
            reserve_level,
            station_name,
            station_type,
            update_time
        FROM MinableCheck
        WHERE is_minable = 1
        ORDER BY max_price DESC
        LIMIT %s
        """
        
        params.append(limit)
        cur.execute(query, params)
        results = cur.fetchall()
        
        # Format results
        formatted_results = []
        for row in results:
            formatted_results.append({
                'commodity_name': row['commodity_name'],
                'max_price': int(row['max_price']) if row['max_price'] is not None else 0,
                'system_name': row['system_name'],
                'controlling_power': row['controlling_power'],
                'power_state': row['power_state'],
                'landing_pad_size': row['landing_pad_size'],
                'distance_to_arrival': float(row['distance_to_arrival']) if row['distance_to_arrival'] is not None else 0,
                'demand': int(row['demand']) if row['demand'] is not None else 0,
                'reserve_level': row['reserve_level'],
                'station_name': row['station_name'],
                'station_type': row['station_type'],
                'update_time': row['update_time'].isoformat() if row['update_time'] is not None else None
            })
        
        conn.close()
        return jsonify(formatted_results)
        
    except Exception as e:
        app.logger.error(f"Search highest error: {str(e)}")
        return jsonify({'error': f'Search highest error: {str(e)}'}), 500

@app.route('/get_price_comparison', methods=['POST'])
def get_price_comparison_endpoint():
    try:
        data=request.json; items=data.get('items',[]); use_max=data.get('use_max',False)
        if not items: return jsonify([])
        results=[]
        for item in items:
            price=int(item.get('price',0))
            commodity=item.get('commodity')
            if not commodity:
                results.append({'color':None,'indicator':''}); continue
            norm=normalize_commodity_name(commodity)
            if norm not in PRICE_DATA:
                if commodity in PRICE_DATA: norm=commodity
                else:
                    results.append({'color':None,'indicator':''}); continue
            ref=int(PRICE_DATA[norm]['max_price' if use_max else 'avg_price'])
            color,indicator=get_price_comparison(price,ref)
            results.append({'color':color,'indicator':indicator})
        return jsonify(results)
    except Exception as e:
        return jsonify({'error':str(e)}),500

@app.route('/search_res_hotspots', methods=['POST'])
def search_res_hotspots():
    try:
        # Get reference system from query parameters
        ref_system = request.args.get('system', 'Sol')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        c = conn.cursor()
        
        c.execute('SELECT x, y, z FROM systems WHERE name = %s', (ref_system,))
        ref_coords = c.fetchone()
        if not ref_coords:
            conn.close()
            return jsonify({'error': 'Reference system not found'}), 404
            
        rx, ry, rz = ref_coords['x'], ref_coords['y'], ref_coords['z']
        hotspot_data = res_data.load_res_data()
        if not hotspot_data:
            conn.close()
            return jsonify({'error': 'No RES hotspot data available'}), 404
            
        results = []
        for e in hotspot_data:
            c.execute('''SELECT s.*, sqrt(power(s.x - %s, 2) + power(s.y - %s, 2) + power(s.z - %s, 2)) as distance
                        FROM systems s WHERE s.name = %s''', 
                     (rx, ry, rz, e['system']))
            system = c.fetchone()
            if not system:
                continue
                
            st = res_data.get_station_commodities(conn, system['id64'])
            results.append({
                'system': e['system'],
                'power': system['controlling_power'] or 'None',
                'distance': float(system['distance']),
                'ring': e['ring'],
                'ls': e['ls'],
                'res_zone': e['res_zone'],
                'comment': e['comment'],
                'stations': st
            })
            
        conn.close()
        return jsonify(results)
    except Exception as e:
        app.logger.error(f"RES hotspot search error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/search_high_yield_platinum', methods=['POST'])
def search_high_yield_platinum():
    try:
        # Get reference system from query parameters
        ref_system = request.args.get('system', 'Sol')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        c = conn.cursor()
        
        c.execute('SELECT x, y, z FROM systems WHERE name = %s', (ref_system,))
        ref_coords = c.fetchone()
        if not ref_coords:
            conn.close()
            return jsonify({'error': 'Reference system not found'}), 404
            
        rx, ry, rz = ref_coords['x'], ref_coords['y'], ref_coords['z']
        data = res_data.load_high_yield_platinum()
        if not data:
            conn.close()
            return jsonify({'error': 'No high yield platinum data available'}), 404
            
        results = []
        for e in data:
            c.execute('''SELECT s.*, sqrt(power(s.x - %s, 2) + power(s.y - %s, 2) + power(s.z - %s, 2)) as distance
                        FROM systems s WHERE s.name = %s''', 
                     (rx, ry, rz, e['system']))
            system = c.fetchone()
            if not system:
                continue
                
            st = res_data.get_station_commodities(conn, system['id64'])
            results.append({
                'system': e['system'],
                'power': system['controlling_power'] or 'None',
                'distance': float(system['distance']),
                'ring': e['ring'],
                'percentage': e['percentage'],
                'comment': e['comment'],
                'stations': st
            })
            
        conn.close()
        return jsonify(results)
    except Exception as e:
        app.logger.error(f"High yield platinum search error: {str(e)}")
        return jsonify({'error': str(e)}), 500

def run_server(host,port,args):
    global live_update_requested, eddn_status
    app.config['SEND_FILE_MAX_AGE_DEFAULT']=0
    print(f"Running on http://{host}:{port}")
    if args.live_update:
        live_update_requested=True
        eddn_status["state"]="starting"
        start_updater()
        time.sleep(0.5)
    else:
        eddn_status["state"]="offline"
    return app

async def main():
    parser = argparse.ArgumentParser(description='Power Mining Web Server')
    parser.add_argument('--host', default='127.0.0.1', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to')
    parser.add_argument('--live-update', action='store_true', help='Enable live EDDN updates')
    parser.add_argument('--db', help='Database URL (e.g. postgresql://user:pass@host:port/dbname)')
    args = parser.parse_args()

    # Set DATABASE_URL from argument or environment variable
    global DATABASE_URL
    DATABASE_URL = args.db or os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        print("ERROR: Database URL must be provided via --db argument or DATABASE_URL environment variable")
        return 1

    # Bind websocket to all interfaces but web server to specified host
    ws_server = await websockets.serve(
        handle_websocket, 
        '0.0.0.0',  # Always bind to all interfaces
        WEBSOCKET_PORT
    )
    app_obj = run_server(args.host, args.port, args)
    async def check_quit():
        while True:
            try:
                if await asyncio.get_event_loop().run_in_executor(None,lambda:sys.stdin.readline().strip())=='q':
                    print("\nQuitting..."); print("Stopping EDDN Update Service...")
                    kill_updater_process(); print("Stopping Web Server...")
                    ws_server.close(); os._exit(0)
            except: break
            await asyncio.sleep(0.1)
    try:
        await asyncio.gather(
            ws_server.wait_closed(),
            asyncio.to_thread(lambda: app_obj.run(host=args.host,port=args.port,use_reloader=False,debug=False,processes=1)),
            check_quit()
        )
    except (KeyboardInterrupt,SystemExit):
        print("\nShutting down..."); print("Stopping EDDN Update Service...")
        kill_updater_process(); print("Stopping Web Server...")
        ws_server.close(); os._exit(0)

def check_existing_process():
    """Check if another instance is running"""
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, 'r') as f:
                old_pid = int(f.read().strip())
            try:
                # Check if process exists and is actually update_live_web.py
                p = psutil.Process(old_pid)
                if "update_live_web.py" in p.cmdline():
                    print(f"{YELLOW}[STATUS] Update service already running with PID: {old_pid}{RESET}")
                    return True
                else:
                    os.remove(PID_FILE)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                os.remove(PID_FILE)
        except:
            if os.path.exists(PID_FILE):
                os.remove(PID_FILE)
    return False

def start_updater():
    global updater_process, eddn_status, DATABASE_URL
    
    # Only start if explicitly requested
    if not live_update_requested and not os.getenv('ENABLE_LIVE_UPDATE', 'false').lower() == 'true':
        return
    
    # Check for existing process
    if check_existing_process():
        return
    
    eddn_status["state"] = "starting"
    
    def monitor_process():
        """Monitor the updater process and restart it if it dies"""
        while True:
            if updater_process and updater_process.poll() is not None:
                print(f"{YELLOW}[STATUS] EDDN updater terminated unexpectedly, restarting...{RESET}")
                if not check_existing_process():  # Only restart if no other process is running
                    start_updater_process()
            time.sleep(5)  # Check every 5 seconds
    
    def handle_output_stream(pipe):
        """Handle output from the EDDN updater process"""
        try:
            with io.TextIOWrapper(pipe, encoding='utf-8', errors='replace') as tp:
                while True:
                    line = tp.readline()
                    if not line:
                        break
                    if line.strip():
                        handle_output(line.strip())
        except Exception as e:
            print(f"Error in output stream: {e}", file=sys.stderr)
    
    def start_updater_process():
        global updater_process
        try:
            cmd = [sys.executable, "update_live_web.py", "--auto"]
            if DATABASE_URL:
                cmd.extend(["--db", DATABASE_URL])
                
            updater_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=False,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
            )
            
            # Write PID to file
            try:
                with open(PID_FILE, 'w') as f:
                    f.write(str(updater_process.pid))
            except Exception as e:
                print(f"Warning: Could not write PID file: {e}", file=sys.stderr)
            
            print(f"{YELLOW}[STATUS] Starting EDDN Live Update (PID: {updater_process.pid}){RESET}", flush=True)
            
            # Start output handling threads
            threading.Thread(target=handle_output_stream, args=(updater_process.stdout,), daemon=True).start()
            threading.Thread(target=handle_output_stream, args=(updater_process.stderr,), daemon=True).start()
            
            time.sleep(0.5)
            if updater_process.poll() is None:
                eddn_status["state"] = "starting"
            else:
                eddn_status["state"] = "error"
                print(f"{YELLOW}[ERROR] EDDN updater failed to start{RESET}", file=sys.stderr)
                if os.path.exists(PID_FILE):
                    try:
                        os.remove(PID_FILE)
                    except:
                        pass
                        
        except Exception as e:
            print(f"Error starting updater: {e}", file=sys.stderr)
            eddn_status["state"] = "error"
            if os.path.exists(PID_FILE):
                try:
                    os.remove(PID_FILE)
                except:
                    pass
    
    # Start the initial process
    start_updater_process()
    
    # Start the monitor thread
    threading.Thread(target=monitor_process, daemon=True).start()

# Gunicorn entry point
app_wsgi = None

def create_app(*args, **kwargs):
    global app_wsgi, DATABASE_URL
    if app_wsgi is None:
        app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
        app_wsgi = app
        
        # Set DATABASE_URL for all workers
        DATABASE_URL = os.getenv('DATABASE_URL')
        if not DATABASE_URL:
            print("ERROR: DATABASE_URL environment variable must be set")
            sys.exit(1)
        
        # Start updater if environment variable is set
        if os.getenv('ENABLE_LIVE_UPDATE', 'false').lower() == 'true':
            global live_update_requested
            live_update_requested = True
            eddn_status["state"] = "starting"
            start_updater()
    
    return app_wsgi

if __name__ == '__main__':
    asyncio.run(main())
