"""Analytics tracking utilities"""
import os
import json
import time
import uuid
import hashlib
import requests
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

# GA4 endpoints
GA_ENDPOINT = "https://www.google-analytics.com/mp/collect"
GA_DEBUG_ENDPOINT = "https://www.google-analytics.com/debug/mp/collect"

# Get GA credentials from environment variables
GA_MEASUREMENT_ID = os.getenv('GA_MEASUREMENT_ID', 'G-CWJ4BDYEV0')  # Your Measurement ID
GA_API_SECRET = os.getenv('GA_API_SECRET', '_GZEyNNUQhSYKUp46qQKEQ')  # Your API secret

# Debug mode for development/testing
DEBUG_MODE = os.getenv('GA_DEBUG_MODE', 'false').lower() == 'true'

def validate_event(event_data: Dict[str, Any]) -> Tuple[bool, Optional[List[Dict[str, str]]]]:
    """
    Validate event data using GA4 debug endpoint before sending to production.
    
    Args:
        event_data: The event data to validate
        
    Returns:
        Tuple of (is_valid, validation_messages)
    """
    try:
        # Build debug URL
        debug_url = f"{GA_DEBUG_ENDPOINT}?measurement_id={GA_MEASUREMENT_ID}&api_secret={GA_API_SECRET}"
        
        # Send to validation server
        response = requests.post(
            debug_url,
            json=event_data,
            timeout=2,  # Longer timeout for debug endpoint
            headers={'Content-Type': 'application/json'}
        )
        
        # Parse validation response
        validation_result = response.json()
        
        # Check for validation messages
        if 'validationMessages' in validation_result:
            return False, validation_result['validationMessages']
        
        return True, None
        
    except Exception as e:
        # Log validation error but don't prevent event from being sent
        print(f"GA4 validation error: {str(e)}")
        return True, None

def normalize_and_hash(value: str, data_type: str = 'text') -> str:
    """
    Normalize and hash values according to GA4 requirements.
    Only used for actual personal data, not game data.
    
    Args:
        value: Value to normalize and hash
        data_type: Type of data ('email', 'phone', 'name', 'street', or 'text')
    
    Returns:
        Normalized and hashed value in hex format
    """
    if not value:
        return ''
        
    # Convert to string and lowercase
    value = str(value).lower().strip()
    
    if data_type == 'email':
        # Remove dots before @ for gmail/googlemail
        if '@gmail.com' in value or '@googlemail.com' in value:
            username, domain = value.split('@')
            username = username.replace('.', '')
            value = f"{username}@{domain}"
    elif data_type == 'phone':
        # Format to E164: remove non-digits and add + prefix
        value = ''.join(c for c in value if c.isdigit())
        if value and not value.startswith('+'):
            value = f"+{value}"
    elif data_type in ('name', 'street'):
        # Remove digits and symbols for names
        if data_type == 'name':
            value = ''.join(c for c in value if c.isalpha() or c.isspace())
        # Remove only symbols for street
        elif data_type == 'street':
            value = ''.join(c for c in value if c.isalnum() or c.isspace())
            
    # Hash using SHA-256 and return hex string
    return hashlib.sha256(value.encode()).hexdigest()

def generate_session_id() -> str:
    """Generate a unique session ID using timestamp and random uuid"""
    return f"{int(time.time())}-{str(uuid.uuid4())[:8]}"

def get_user_properties(search_params: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Extract user properties from search parameters.
    These help segment users by their search preferences.
    """
    return {
        "preferred_mining_type": {
            "value": search_params.get('signal_type', 'Any')
        },
        "preferred_ring_type": {
            "value": search_params.get('ring_type_filter', 'All')
        },
        "search_radius": {
            "value": str(search_params.get('max_dist', 0))
        },
        "pad_size_requirement": {
            "value": search_params.get('landing_pad_size', 'Any')
        },
        "power_preference": {
            "value": search_params.get('controlling_power', 'Any')
        },
        "power_goal": {
            "value": search_params.get('power_goal', '')
        }
    }

def track_search(search_params: Dict[str, Any], debug: bool = DEBUG_MODE) -> None:
    """
    Track search parameters in Google Analytics.
    This is a fire-and-forget function that won't affect the main search flow.
    
    Args:
        search_params: Dictionary of search parameters
        debug: Whether to enable debug mode for GA4 DebugView
    """
    try:
        if not GA_MEASUREMENT_ID or not GA_API_SECRET:
            return
            
        # Calculate engagement time (time spent constructing the search)
        engagement_time = 1200  # Default 1.2 seconds for search construction
            
        # Create a clean version of search params for tracking
        tracked_params = {
            # Basic search parameters
            'ref_system': search_params.get('ref_system', ''),  # No need to hash game system names
            'max_dist': search_params.get('max_dist', 0),
            'signal_type': search_params.get('signal_type', ''),
            'ring_type_filter': search_params.get('ring_type_filter', ''),
            'reserve_level': search_params.get('reserve_level', ''),
            'system_states': ','.join(search_params.get('system_states', [])),
            'landing_pad_size': search_params.get('landing_pad_size', ''),
            'min_demand': search_params.get('min_demand', 0),
            'max_demand': search_params.get('max_demand', 0),
            
            # Power-related parameters
            'controlling_power': search_params.get('controlling_power', ''),
            'power_goal': search_params.get('power_goal', ''),
            'opposing_power': search_params.get('opposing_power', ''),
            
            # Mining and display parameters
            'mining_types': ','.join(search_params.get('mining_types', [])),
            'selected_materials': ','.join(search_params.get('sel_mats', [])),
            'display_format': search_params.get('display_format', 'full'),
            'limit': search_params.get('limit', 20),
            
            # Required parameters for real-time reporting and DebugView
            'session_id': generate_session_id(),
            'engagement_time_msec': engagement_time
        }
        
        # Add debug mode parameters if enabled
        if debug:
            tracked_params['debug_mode'] = True
        
        # Get current timestamp in microseconds
        current_time_micros = int(time.time() * 1000000)
        
        # Generate a persistent client ID based on session
        client_id = str(uuid.uuid4())
        
        # Prepare the event data with standard and custom events
        event_data = {
            "client_id": client_id,
            "user_id": client_id,  # Use same ID for user tracking
            "timestamp_micros": current_time_micros,
            "non_personalized_ads": True,
            "user_properties": get_user_properties(search_params),
            "events": [
                # Standard GA4 events for immediate visibility
                {
                    "name": "search",  # Standard GA4 search event
                    "params": {
                        "search_term": search_params.get('ref_system', ''),
                        "engagement_time_msec": engagement_time
                    }
                },
                {
                    "name": "select_item",  # Standard GA4 item selection event
                    "params": {
                        "item_list_id": "mining_types",
                        "items": [{"item_name": t} for t in search_params.get('mining_types', [])],
                        "engagement_time_msec": engagement_time
                    }
                },
                # Our custom event with full details
                {
                    "name": "mining_search",
                    "params": tracked_params
                }
            ]
        }
        
        # Validate event data first
        is_valid, validation_messages = validate_event(event_data)
        
        if not is_valid and debug:
            # Show detailed validation errors in debug mode
            print("\nGA4 validation failed. Issues found:")
            for msg in validation_messages:
                print(f"- Field: {msg.get('fieldPath', 'unknown')}")
                print(f"  Error: {msg.get('description', 'no description')}")
                print(f"  Code: {msg.get('validationCode', 'unknown')}")
                print()  # Empty line between messages
        
        # Build the URL with authentication
        url = f"{GA_ENDPOINT}?measurement_id={GA_MEASUREMENT_ID}&api_secret={GA_API_SECRET}"
        
        # Send the event asynchronously
        response = requests.post(
            url,
            json=event_data,
            timeout=1,  # Short timeout to prevent blocking
            headers={'Content-Type': 'application/json'}
        )
        
        if debug:
            print(f"GA4 event sent with status {response.status_code}. Check DebugView in GA4 for events: search, select_item, mining_search")
            
    except Exception as e:
        # Log error but don't affect main functionality
        if debug:
            print(f"GA4 tracking error: {str(e)}")
        pass 