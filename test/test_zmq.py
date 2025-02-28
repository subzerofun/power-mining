#!/usr/bin/env python3
import zmq
import time
import json
import argparse
import socket
from datetime import datetime

def get_network_info():
    """Get all network interfaces and their IPs"""
    interfaces = {}
    try:
        hostname = socket.gethostname()
        interfaces['hostname'] = hostname
        interfaces['hostname_ip'] = socket.gethostbyname(hostname)
        
        # Get all network interfaces
        for interface in socket.if_nameindex():
            try:
                ip = socket.gethostbyname(interface[1])
                interfaces[interface[1]] = ip
            except:
                continue
    except Exception as e:
        print(f"Error getting network info: {e}")
    return interfaces

def test_publisher(port):
    """Test ZMQ publisher on various interfaces"""
    context = zmq.Context()
    publisher = context.socket(zmq.PUB)
    
    # Try binding to different interfaces
    interfaces = [
        "0.0.0.0",
        "127.0.0.1",
        "localhost",
        "*"
    ]
    
    network_info = get_network_info()
    print("\nNetwork Information:")
    for name, ip in network_info.items():
        print(f"  {name}: {ip}")
        if ip not in interfaces:
            interfaces.append(ip)
    
    print("\nTesting Publisher Bindings:")
    successful_binds = []
    
    for interface in interfaces:
        try:
            address = f"tcp://{interface}:{port}"
            publisher.bind(address)
            print(f"✓ Successfully bound to {address}")
            
            # Get actual endpoint
            endpoint = publisher.getsockopt(zmq.LAST_ENDPOINT).decode()
            print(f"  Actual endpoint: {endpoint}")
            
            successful_binds.append(address)
            
            # Send a test message
            msg = {
                "timestamp": datetime.now().isoformat(),
                "type": "test",
                "data": "Hello from publisher!"
            }
            publisher.send_string(json.dumps(msg))
            print(f"  Sent test message on {address}")
            
            # Unbind for next test
            publisher.unbind(address)
        except Exception as e:
            print(f"✗ Failed to bind to {address}: {e}")
    
    return successful_binds

def test_subscriber(addresses, port):
    """Test ZMQ subscriber connecting to various addresses"""
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt_string(zmq.SUBSCRIBE, "")
    subscriber.setsockopt(zmq.RCVTIMEO, 1000)  # 1 second timeout
    
    print("\nTesting Subscriber Connections:")
    
    # Add some common addresses to try
    all_addresses = set(addresses + [
        f"tcp://localhost:{port}",
        f"tcp://127.0.0.1:{port}",
        f"tcp://0.0.0.0:{port}"
    ])
    
    for address in all_addresses:
        try:
            subscriber.connect(address)
            print(f"✓ Connected to {address}")
            
            # Try to receive a message
            print(f"  Waiting for message on {address}...")
            try:
                message = subscriber.recv_string()
                data = json.loads(message)
                print(f"  ✓ Received message: {data}")
            except zmq.error.Again:
                print(f"  ✗ No message received after 1 second")
            except Exception as e:
                print(f"  ✗ Error receiving message: {e}")
            
            # Disconnect for next test
            subscriber.disconnect(address)
        except Exception as e:
            print(f"✗ Failed to connect to {address}: {e}")

def main():
    parser = argparse.ArgumentParser(description='Test ZMQ connections')
    parser.add_argument('--port', type=int, default=5559, help='Port to test (default: 5559)')
    parser.add_argument('--mode', choices=['pub', 'sub', 'both'], default='both', 
                      help='Test mode: publisher, subscriber, or both (default: both)')
    args = parser.parse_args()
    
    print(f"\nTesting ZMQ connections on port {args.port}")
    
    successful_binds = []
    if args.mode in ['pub', 'both']:
        successful_binds = test_publisher(args.port)
    
    if args.mode in ['sub', 'both']:
        test_subscriber(successful_binds, args.port)

if __name__ == "__main__":
    main()