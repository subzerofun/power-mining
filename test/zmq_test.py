#!/usr/bin/env python3
import zmq
import time
import json
from datetime import datetime
import socket

def print_network_info():
    print("\n=== Network Information ===")
    try:
        hostname = socket.gethostname()
        print(f"Hostname: {hostname}")
        print(f"Hostname IP: {socket.gethostbyname(hostname)}")
        
        # Try to get all addresses
        try:
            addresses = socket.getaddrinfo(hostname, None)
            print("\nAll addresses:")
            for addr in addresses:
                print(f"  {addr[4][0]}")
        except Exception as e:
            print(f"Could not get addresses: {e}")
            
    except Exception as e:
        print(f"Error getting network info: {e}")

def test_publisher():
    print("\n=== Testing Publisher ===")
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    
    # Test different bind addresses
    addresses = [
        "tcp://127.0.0.1:5559",
        "tcp://localhost:5559",
        "tcp://0.0.0.0:5559"
    ]
    
    for addr in addresses:
        try:
            print(f"\nTrying to bind to {addr}")
            socket.bind(addr)
            print("✓ Bind successful")
            
            # Send a test message
            msg = {"time": datetime.now().isoformat(), "test": "Hello"}
            socket.send_string(json.dumps(msg))
            print("✓ Sent test message")
            
            # Unbind and try next address
            socket.unbind(addr)
        except Exception as e:
            print(f"✗ Bind failed: {e}")
    
    socket.close()
    context.term()

def test_subscriber():
    print("\n=== Testing Subscriber ===")
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    socket.setsockopt(zmq.RCVTIMEO, 2000)  # 2 second timeout
    
    # Test different connect addresses
    addresses = [
        "tcp://127.0.0.1:5559",
        "tcp://localhost:5559",
        "tcp://0.0.0.0:5559"
    ]
    
    for addr in addresses:
        try:
            print(f"\nTrying to connect to {addr}")
            socket.connect(addr)
            print("✓ Connect successful")
            
            # Try to receive
            print("Waiting for message (2 sec timeout)...")
            try:
                msg = socket.recv_string()
                print(f"✓ Received: {msg}")
            except zmq.error.Again:
                print("✗ No message received (timeout)")
            
            # Disconnect and try next
            socket.disconnect(addr)
        except Exception as e:
            print(f"✗ Connect failed: {e}")
    
    socket.close()
    context.term()

def main():
    print("\n=== ZMQ Connection Test ===")
    print_network_info()
    
    # First run publisher test
    test_publisher()
    
    # Then run subscriber test
    test_subscriber()

if __name__ == "__main__":
    main()