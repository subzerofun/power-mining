"""
Verification functions for the EDDN Update Service.

This module contains functions for tracking and verifying database updates.
"""

import time
from datetime import datetime, timezone
import json

# ANSI color codes
YELLOW = '\033[93m'
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
CYAN = '\033[96m'
ORANGE = '\033[38;5;208m'
MAGENTA = '\033[95m'
RESET = '\033[0m'


class VerificationTracker:
    """
    Tracks database updates for verification purposes.
    """
    
    def __init__(self, logger_func=None):
        """
        Initialize the verification tracker.
        
        Args:
            logger_func: Function to use for logging
        """
        self.pending_updates = {}
        self.logger_func = logger_func
    
    def track_update(self, update_type, status, system_id64=None, system_name=None, station_name=None, details=None):
        """
        Track an update for verification purposes.
        
        Args:
            update_type (str): Type of update (power, system_state, commodity)
            status (str): Status of the update (success, failed)
            system_id64 (int, optional): System ID64
            system_name (str, optional): System name
            station_name (str, optional): Station name
            details (dict, optional): Additional details about the update
            
        Returns:
            str: The generated update_id
        """
        # Generate a unique update ID
        identifier = system_id64 or "unknown"
        if station_name:
            identifier = f"{identifier}_{station_name.replace(' ', '_')}"
        update_id = f"{update_type}_{identifier}_{int(time.time())}"
        
        # Create update record
        update_record = {
            'type': update_type,
            'status': status,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        # Add optional fields if provided
        if system_id64:
            update_record['system_id64'] = system_id64
        if system_name:
            update_record['system_name'] = system_name
        if station_name:
            update_record['station_name'] = station_name
        if details:
            update_record.update(details)
        
        # Store the update
        self.pending_updates[update_id] = update_record
        
        # Log the update with appropriate color
        if self.logger_func:
            color = GREEN if status == 'success' else RED
            message = f"{update_type.upper()} update {status}: {update_id}"
            if status == 'success':
                message = f"✓ {message}"
            else:
                message = f"✗ {message}"
                
            if details and 'reason' in details:
                message += f" - Reason: {details['reason']}"
                
            self.logger_func("VERIFY", message, level=1, color=color)
        
        return update_id
    
    def get_verification_stats(self):
        """
        Get statistics about tracked updates.
        
        Returns:
            dict: Statistics about tracked updates
        """
        stats = {
            'total': len(self.pending_updates),
            'by_type': {},
            'by_status': {},
            'success_rate': 0.0,
            'last_update': None
        }
        
        if not self.pending_updates:
            return stats
            
        # Find the most recent update timestamp
        latest_time = None
        for update_id, update_data in self.pending_updates.items():
            timestamp = update_data.get('timestamp')
            if timestamp:
                try:
                    update_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    if latest_time is None or update_time > latest_time:
                        latest_time = update_time
                except (ValueError, TypeError):
                    pass
        
        if latest_time:
            stats['last_update'] = latest_time.isoformat()
        
        # Count by type and status
        for update_id, update_data in self.pending_updates.items():
            update_type = update_data.get('type', 'unknown')
            update_status = update_data.get('status', 'unknown')
            
            # Count by type
            if update_type not in stats['by_type']:
                stats['by_type'][update_type] = 0
            stats['by_type'][update_type] += 1
            
            # Count by status
            if update_status not in stats['by_status']:
                stats['by_status'][update_status] = 0
            stats['by_status'][update_status] += 1
            
            # Count by type and status
            type_status_key = f"{update_type}_{update_status}"
            if type_status_key not in stats:
                stats[type_status_key] = 0
            stats[type_status_key] += 1
        
        # Calculate success rate
        success_count = stats['by_status'].get('success', 0)
        failed_count = stats['by_status'].get('failed', 0)
        total_completed = success_count + failed_count
        
        if total_completed > 0:
            stats['success_rate'] = (success_count / total_completed) * 100.0
        
        # Format the stats for display
        stats_display = f"Verification Stats:\n"
        stats_display += f"Total updates tracked: {stats['total']}\n"
        stats_display += f"Success rate: {stats['success_rate']:.1f}%\n"
        stats_display += f"Last update: {stats['last_update']}\n\n"
        
        stats_display += "By Type:\n"
        for update_type, count in sorted(stats['by_type'].items()):
            stats_display += f"  {update_type}: {count}\n"
        
        stats_display += "\nBy Status:\n"
        for status, count in sorted(stats['by_status'].items()):
            color_code = GREEN if status == 'success' else RED if status == 'failed' else ''
            stats_display += f"  {status}: {count}\n"
        
        # Log the stats
        if self.logger_func:
            self.logger_func("VERIFY", stats_display, level=1, color=ORANGE)
        
        return stats


def format_commodity_list(commodities, group_size=3):
    """
    Format a list of commodities for better readability.
    
    Args:
        commodities (list): List of commodity names
        group_size (int): Number of commodities per line
        
    Returns:
        str: Formatted commodity list
    """
    commodity_list = list(commodities)
    commodity_display = ""
    for i in range(0, len(commodity_list), group_size):
        group = commodity_list[i:i+group_size]
        commodity_display += ", ".join(group) + "\n"
    return commodity_display.strip()
