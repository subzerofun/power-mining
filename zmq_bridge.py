import asyncio
import websockets
import zmq.asyncio
import json
from datetime import datetime, timezone
import socket as socket_lib  # Rename the socket module import
import argparse  # Add argparse for command-line arguments

async def zmq_to_websocket(websocket, path):
    print(f"\n=== New WebSocket Connection ===")
    print(f"[{datetime.now()}] Client connected from: {websocket.remote_address}")
    
    # Print network information
    hostname = socket_lib.gethostname()
    try:
        local_ip = socket_lib.gethostbyname(hostname)
        print(f"Bridge running on: {hostname} ({local_ip})")
    except Exception as e:
        print(f"Could not get local IP: {e}")

    # Setup ZMQ subscriber
    print(f"\n=== Setting up ZMQ Subscriber ===")
    context = zmq.asyncio.Context()
    zmq_socket = context.socket(zmq.SUB)  # Renamed from socket to zmq_socket
    zmq_socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    # Try multiple connection addresses
    addresses = [
        "tcp://powermining-update-1:5559",  # Connect to the update container by name
        "tcp://localhost:5559",             # Fallback to localhost
        "tcp://127.0.0.1:5559",             # Another localhost fallback
        "tcp://0.0.0.0:5559"                # Final fallback
    ]
    
    connected = False
    for addr in addresses:
        try:
            print(f"[{datetime.now()}] Attempting to connect to {addr}")
            zmq_socket.connect(addr)
            print(f"[{datetime.now()}] ✓ Successfully connected to {addr}")
            connected = True
            break
        except Exception as e:
            print(f"[{datetime.now()}] ✗ Failed to connect to {addr}: {e}")
    
    if not connected:
        print(f"[{datetime.now()}] ✗ Could not connect to any ZMQ endpoint!")
        return

    print(f"\n=== Starting Message Loop ===")
    try:
        while True:
            try:
                # Set a timeout for receiving messages
                message = await asyncio.wait_for(zmq_socket.recv_string(), timeout=5.0)
                print(f"[{datetime.now()}] Received ZMQ message")
                
                try:
                    data = json.loads(message)
                    if 'timestamp' not in data:
                        data['timestamp'] = datetime.now(timezone.utc).isoformat()
                    
                    # Send to WebSocket
                    await websocket.send(json.dumps(data))
                    print(f"[{datetime.now()}] ✓ Forwarded message to WebSocket client")
                    
                except json.JSONDecodeError:
                    print(f"[{datetime.now()}] ✗ Invalid JSON message: {message[:100]}...")
                    continue
                
            except asyncio.TimeoutError:
                print(f"[{datetime.now()}] No messages received in last 5 seconds...")
                continue
            except Exception as e:
                print(f"[{datetime.now()}] ✗ Error processing message: {e}")
                continue
                
    except websockets.exceptions.ConnectionClosed:
        print(f"[{datetime.now()}] WebSocket connection closed by client")
    finally:
        zmq_socket.close()
        context.term()
        print(f"[{datetime.now()}] Cleaned up ZMQ connection")

async def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='ZMQ to WebSocket Bridge')
    parser.add_argument('--server', help='Server hostname:ip for the bridge')
    args = parser.parse_args()
    
    print(f"\n=== Starting WebSocket Bridge ===")
    print(f"[{datetime.now()}] Initializing...")
    
    # Get server information
    hostname = socket_lib.gethostname()
    local_ip = None
    
    try:
        # If --server argument is provided, use that information
        if args.server:
            server_parts = args.server.split(':')
            if len(server_parts) == 2:
                hostname = server_parts[0]
                local_ip = server_parts[1]
                print(f"Using provided server information: hostname={hostname}, IP={local_ip}")
            else:
                print(f"Invalid server format. Expected format: hostname:ip")
                print(f"Falling back to automatic detection")
        
        # If IP wasn't provided or was invalid, try to detect it
        if not local_ip:
            local_ip = socket_lib.gethostbyname(hostname)
    except Exception as e:
        print(f"Could not get local IP: {e}")
        local_ip = "0.0.0.0"  # Fallback
    
    try:
        server = await websockets.serve(
            zmq_to_websocket,
            "0.0.0.0",  # Changed from localhost to allow external connections
            5560,
            ping_interval=None
        )
        print(f"[{datetime.now()}] ✓ WebSocket server running on ws://0.0.0.0:5560")
        
        # Print network information
        print(f"Server hostname: {hostname}")
        print(f"Server IP: {local_ip}")
            
        await server.wait_closed()
        
    except Exception as e:
        print(f"[{datetime.now()}] ✗ Failed to start server: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"[{datetime.now()}] Shutting down by user request...")
    except Exception as e:
        print(f"[{datetime.now()}] Fatal error: {e}")