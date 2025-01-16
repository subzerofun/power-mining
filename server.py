import os, sys, math, json, zlib, time, signal, atexit, argparse, asyncio, subprocess, threading, psycopg2
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_sock import Sock
from json import JSONEncoder
import zmq
import psutil
from typing import Dict, List, Optional
import tempfile
import io
from utils import mining_data, res_data
from utils.common import (
    get_db_connection,
    BLUE, RED, YELLOW, GREEN, CYAN, ORANGE, RESET,
    BASE_DIR
)
from utils.search import (
    search,
    search_highest,
    get_price_comparison_endpoint,
    search_res_hotspots,
    search_high_yield_platinum
)
from utils.map import map_bp

# Custom JSON encoder to handle datetime objects
class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d')
        return super().default(obj)

# Create Flask app instance
app = Flask(__name__, template_folder=BASE_DIR, static_folder=None)
app.json_encoder = CustomJSONEncoder
sock = Sock(app)
live_update_requested = False
eddn_status = {"state": "offline", "last_db_update": None}

# Gunicorn entry point
app_wsgi = None

def calculate_distance(x1, y1, z1, x2, y2, z2): 
    return math.sqrt((x2-x1)**2 + (y2-y1)**2 + (z2-z1)**2)

def create_app(*args, **kwargs):
    global app_wsgi, DATABASE_URL
    if app_wsgi is None:
        app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
        
        # Set DATABASE_URL for all workers
        DATABASE_URL = os.getenv('DATABASE_URL')
        if not DATABASE_URL:
            print("ERROR: DATABASE_URL environment variable must be set")
            sys.exit(1)
            
        # Set DATABASE_URL in app config
        app.config['DATABASE_URL'] = DATABASE_URL
        
        app_wsgi = app
        
        # Setup ZMQ in each worker to receive status
        setup_zmq()
    
    return app_wsgi

# Global flag for development mode
DEV_MODE = False  # Will be set from args in main()

if not DEV_MODE:
    try:
        import websockets
        from websockets.exceptions import ConnectionClosed
    except ImportError:
        print("WebSocket dependencies not found. Run 'pip install websockets' to enable live updates.")
        print("Or run with --dev flag to disable WebSocket functionality: python server.py --dev")
        sys.exit(1)

# Environment variables for configuration
DATABASE_URL = None  # Will be set from args or env in main()

# ZMQ setup for status updates
STATUS_PORT = 5558  # Port to receive status from update daemon
zmq_context = None
status_socket = None
last_status_time = 0
shared_status = {"state": "offline", "last_db_update": None}  # Shared across workers

# Debug levels
DEBUG_LEVEL = 0  # 0 = silent, 1 = critical/important, 2 = normal, 3 = verbose/detailed

def log_message(color, tag, message, level=1):
    """Log a message with timestamp and PID"""
    # Skip messages if debug level is 0 or message level is higher than DEBUG_LEVEL
    if DEBUG_LEVEL == 0 or level > DEBUG_LEVEL:
        return
        
    timestamp = datetime.now().strftime("%Y:%m:%d-%H:%M:%S")
    worker_id = os.environ.get('GUNICORN_WORKER_ID', 'MAIN')
    print(f"{color}[{timestamp}] [{tag}-{os.getpid()}] {message}{RESET}", flush=True)

def monitor_status():
    """Monitor status updates from the daemon"""
    global shared_status, last_status_time, status_socket
    log_message(RED, "ZMQ-DEBUG", f"Starting monitor_status thread in worker {os.getpid()}", level=2)
    
    consecutive_errors = 0
    while True:
        try:
            if status_socket and status_socket.poll(100):
                try:
                    message = status_socket.recv_string()
                    last_status_time = time.time()
                    status_data = json.loads(message)
                    shared_status.update(status_data)
                    log_message(RED, "ZMQ-DEBUG", 
                        f"Worker {os.getpid()} received status: {status_data.get('state')} "
                        f"from daemon PID: {status_data.get('daemon_pid')}", level=3)
                    consecutive_errors = 0  # Reset error counter on successful receive
                except zmq.ZMQError as e:
                    consecutive_errors += 1
                    log_message(RED, "ZMQ-DEBUG", f"ZMQ error in monitor thread: {e}", level=1)
                    if consecutive_errors > 3:
                        log_message(RED, "ZMQ-DEBUG", "Too many consecutive errors, recreating socket...", level=1)
                        setup_zmq()  # Recreate the socket
                        consecutive_errors = 0
                    time.sleep(1)
                except json.JSONDecodeError as e:
                    log_message(RED, "ERROR", f"Failed to decode status message: {e}", level=1)
                except Exception as e:
                    log_message(RED, "ERROR", f"Error in monitor thread: {e}", level=1)

            # Check if we haven't received status updates for a while
            if time.time() - last_status_time > 60:
                if shared_status["state"] == "connecting":
                    log_message(RED, "ZMQ-DEBUG", f"Worker {os.getpid()} - Still trying to connect to daemon...", level=2)
                else:
                    shared_status["state"] = "offline"
                    log_message(RED, "ZMQ-DEBUG", f"Worker {os.getpid()} - No status updates for 60s, marking offline", level=2)
                # Try to reconnect if we haven't received updates for a while
                if consecutive_errors == 0:  # Only try once to avoid spam
                    log_message(RED, "ZMQ-DEBUG", "Attempting to reconnect to daemon...", level=2)
                    setup_zmq()
                    consecutive_errors += 1
            
            time.sleep(0.1)
            
        except Exception as e:
            log_message(RED, "ERROR", f"Error in monitor thread: {e}", level=1)
            time.sleep(1)

def setup_zmq():
    """Setup ZMQ connection to update daemon"""
    global zmq_context, status_socket, shared_status, last_status_time
    
    # Initialize shared status and time with "connecting" state
    shared_status = {"state": "connecting", "last_db_update": None}  # Changed from "offline" to "connecting"
    last_status_time = time.time()
    
    try:
        # Create new context and socket
        if zmq_context:
            try:
                status_socket.close()
                zmq_context.term()
            except:
                pass
                
        zmq_context = zmq.Context()
        status_socket = zmq_context.socket(zmq.SUB)
        
        # Set socket options
        status_socket.setsockopt(zmq.RCVTIMEO, 1000)  # 1 second timeout
        status_socket.setsockopt(zmq.LINGER, 0)       # Don't wait on close
        status_socket.setsockopt_string(zmq.SUBSCRIBE, "")  # Subscribe to all messages
        status_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)     # Enable keepalive
        status_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 60)  # Probe after 60s
        status_socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)  # Probe every 1s
        
        # Connect to daemon's publish port using service name
        endpoint = f"tcp://daemon:5558"
        status_socket.connect(endpoint)
        log_message(RED, "ZMQ-DEBUG", f"Worker {os.getpid()} connecting to daemon at {endpoint}", level=2)
        
        # Try to get initial status
        if status_socket.poll(1000):  # Wait up to 1 second for initial status
            try:
                message = status_socket.recv_string()
                status_data = json.loads(message)
                shared_status.update(status_data)
                last_status_time = time.time()
                log_message(RED, "ZMQ-DEBUG", f"Worker {os.getpid()} received initial status: {shared_status['state']}", level=2)
            except Exception as e:
                log_message(RED, "ZMQ-DEBUG", f"Worker {os.getpid()} - Failed to get initial status: {e}", level=2)
        else:
            log_message(RED, "ZMQ-DEBUG", f"Worker {os.getpid()} - No initial status received after 1s", level=2)
        
        # Start status monitoring thread
        status_thread = threading.Thread(target=monitor_status, daemon=True)
        status_thread.start()
        
    except zmq.error.ZMQError as e:
        log_message(RED, f"ZMQ-DEBUG", f"Worker {os.getpid()} could not connect to daemon: {e}", level=1)
        shared_status = {"state": "connecting", "last_db_update": None}  # Keep as "connecting" on error
    except Exception as e:
        log_message(RED, f"ZMQ-DEBUG", f"Worker {os.getpid()} setup error: {e}", level=1)
        shared_status = {"state": "connecting", "last_db_update": None}  # Keep as "connecting" on error

@sock.route('/ws')
def handle_websocket(ws):
    """Handle WebSocket connections and send status updates to clients"""
    try:
        # Send initial status
        log_message(RED, "WS-DEBUG", f"New WebSocket client connected to worker {os.getpid()}", level=2)
        log_message(RED, "WS-DEBUG", f"Sending initial status: {shared_status}", level=3)
        ws.send(json.dumps(shared_status))
        
        while True:
            try:
                # Use poll to avoid blocking forever
                if status_socket and status_socket.poll(100):  # 100ms timeout
                    message = status_socket.recv_string()
                    try:
                        status_data = json.loads(message)
                        log_message(RED, "WS-DEBUG", f"Worker {os.getpid()} forwarding status to client: {status_data}", level=3)
                        ws.send(message)
                    except json.JSONDecodeError:
                        log_message(RED, "WS-DEBUG", "Failed to decode status message", level=1)
                        continue
                else:
                    if DEV_MODE==False:
                        log_message(RED, "WS-DEBUG", f"Worker {os.getpid()} - No ZMQ message to forward to WebSocket", level=3)
            except zmq.ZMQError as e:
                log_message(RED, "WS-DEBUG", f"ZMQ error in WebSocket handler: {e}", level=1)
                continue
            except Exception as e:
                log_message(RED, "WS-DEBUG", f"Error in WebSocket handler: {e}", level=1)
                break
            
            # Send periodic status updates even if no new messages
            try:
                ws.send(json.dumps(shared_status))
                if DEV_MODE==False:
                    log_message(RED, "WS-DEBUG", f"Worker {os.getpid()} sent periodic status: {shared_status}", level=3)
            except Exception as e:
                log_message(RED, "WS-DEBUG", f"Failed to send periodic update: {e}", level=1)
                break
                
            time.sleep(1)  # Send update every second

    except Exception as e:
        log_message(RED, "WS-DEBUG", f"WebSocket connection error: {e}", level=1)
    finally:
        log_message(RED, "WS-DEBUG", f"WebSocket client disconnected from worker {os.getpid()}", level=2)

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
            with open(path, 'w') as f: f.write("[Defaults]\nsystem = Cubeo\ncontrolling_power = Aisling Duval\nmax_distance = 200\nsearch_results = 30\n")
        r = send_from_directory(BASE_DIR, 'Config.ini', mimetype='text/plain')
        r.headers['Cache-Control']='no-cache, no-store, must-revalidate'; r.headers['Pragma']='no-cache'; r.headers['Expires']='0'
        return r
    except Exception as e:
        app.logger.error(f"Error serving Config.ini: {str(e)}")
        return jsonify({'Defaults':{'system':'Harma','controlling_power':'Archon Delaine','max_distance':'200','search_results':'30'}})

@app.route('/')
def index(): return render_template('index.html')

@app.route('/autocomplete')
def autocomplete():
    """Handle system name autocomplete."""
    try:
        search = request.args.get('q', '').strip()
        if len(search) < 2:
            return jsonify([])
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Search for system names that start with the input
        cursor.execute('''
            SELECT name, x, y, z 
            FROM systems 
            WHERE name ILIKE %s || '%%'
            LIMIT 10
        ''', (search,))
        
        results = [{'name': row['name'], 'coords': {'x': row['x'], 'y': row['y'], 'z': row['z']}} 
                  for row in cursor.fetchall()]
        
        conn.close()
        return jsonify(results)
    except Exception as e:
        app.logger.error(f"Autocomplete error: {str(e)}")
        return jsonify({'error': 'Error during autocomplete'}), 500

@app.route('/search')
def search_route():
    return search()

@app.route('/search_highest')
def search_highest_route():
    return search_highest()

@app.route('/get_price_comparison', methods=['POST'])
def get_price_comparison_route():
    return get_price_comparison_endpoint()

@app.route('/search_res_hotspots', methods=['POST'])
def search_res_hotspots_route():
    return search_res_hotspots()

@app.route('/search_high_yield_platinum', methods=['POST'])
def search_high_yield_platinum_route():
    return search_high_yield_platinum()

def run_server(host,port,args):
    global live_update_requested, eddn_status
    app.config['SEND_FILE_MAX_AGE_DEFAULT']=0
    print(f"Running on http://{host}:{port}")
    if args.live_update:
        live_update_requested=True
        eddn_status["state"]="starting"
        #start_updater()
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
    parser.add_argument('--dev', action='store_true', help='Run in development mode without WebSocket functionality')
    parser.add_argument('--debug-level', type=int, choices=[0, 1, 2, 3], default=1,
                       help='Debug level (0=silent, 1=critical, 2=normal, 3=verbose)')
    args = parser.parse_args()

    # Set DEBUG_LEVEL from argument
    global DEBUG_LEVEL
    DEBUG_LEVEL = args.debug_level

    # Set DATABASE_URL from argument or environment variable
    global DATABASE_URL, DEV_MODE
    DATABASE_URL = args.db or os.getenv('DATABASE_URL')
    DEV_MODE = args.dev  # Update the global DEV_MODE based on args
    if not DATABASE_URL:
        print("ERROR: Database URL must be provided via --db argument or DATABASE_URL environment variable")
        return 1

    # Set DATABASE_URL in app config
    app.config['DATABASE_URL'] = DATABASE_URL

    # Bind websocket to all interfaces but web server to specified host
    ws_server = None
    if not DEV_MODE:
        ws_server = await websockets.serve(
            handle_websocket, 
            '0.0.0.0',  # Always bind to all interfaces
            WEBSOCKET_PORT
        )
    
    app_obj = run_server(args.host, args.port, args)
    
    def signal_handler(signum, frame):
        print("\nShutting down...")
        print("Stopping EDDN Update Service...")
        if ws_server:
            ws_server.close()
        cleanup_zmq()
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        if DEV_MODE:
            app_obj.run(host=args.host, port=args.port, use_reloader=False, debug=False)
        else:
            await asyncio.gather(
                ws_server.wait_closed(),
                asyncio.to_thread(lambda: app_obj.run(host=args.host, port=args.port, use_reloader=False, debug=False))
            )
    except (KeyboardInterrupt, SystemExit):
        print("\nShutting down...")
        print("Stopping EDDN Update Service...")
        if ws_server:
            ws_server.close()
        cleanup_zmq()
        sys.exit(0)

# Add cleanup for ZMQ context
def cleanup_zmq():
    try:
        status_socket.close()
        zmq_context.term()
    except:
        pass

atexit.register(cleanup_zmq)

# Register routes from search.py
app.add_url_rule('/search', 'search', search)
app.add_url_rule('/search_highest', 'search_highest', search_highest)
app.add_url_rule('/get_price_comparison', 'get_price_comparison', get_price_comparison_endpoint, methods=['POST'])
app.add_url_rule('/search_res_hotspots', 'search_res_hotspots', search_res_hotspots, methods=['POST'])
app.add_url_rule('/search_high_yield_platinum', 'search_high_yield_platinum', search_high_yield_platinum, methods=['POST'])

app.register_blueprint(map_bp)

if __name__ == '__main__':
    if DEV_MODE:
        print("Starting in development mode (WebSocket disabled)")
    else:
        print("Starting in production mode")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down...")
        print("Stopping EDDN Update Service...")
        cleanup_zmq()
        sys.exit(0)