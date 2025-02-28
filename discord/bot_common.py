# bot_common.py
# Author: subzerofun
# Version: 1.0.0
# Date: 2025-02-21
# Description: Common utilities and constants shared across bot modules

from datetime import datetime, timedelta
from typing import Dict, List

# ANSI color codes for Discord
COLORS = {
    'green': '[0;32m',
    'white': '[0;37m',
    'blue': '[0m[0;34m',
    'teal': '[0m[0;36m',
    'red': '[0;31m',
    'dark-gray': '[0;30m',
    'pink': '[0m[0;35m',
    'cyan': '[0m[0;34m',
    'gold': '[0m[0;33m',
    'magenta': '[0m[0;35m',
    'reset': '[0m',
    'bold': '[0m[0m[1;2m[0;2m'
}

# Display settings
SHORTEN_RING_NAMES = True  # Set to False to show full ring names

# Reserve level display
RESERVE_BARS = {
    'Unknown': '░░░░',
    'Depleted': '░░░░',
    'Low': '█░░░',
    'Common': '██░░',
    'Major': '███░',
    'Pristine': '████'
}


def color_text(text: str, color: str, width: int = None) -> str:
    """Add color to text while maintaining table compatibility"""
    colored_text = f"{COLORS.get(color, '')}{text}[0m"
    if width is not None:
        # Add padding to match the desired visible width
        padding = ' ' * (width - len(text))
        colored_text = f"{COLORS.get(color, '')}{text}{padding}[0m"
    return colored_text

def calculate_control_points(price: int) -> int:
    """Calculate control points based on price"""
    return int(price * 60 / 1340 / 4)

def format_price(price: int) -> str:
    """Format price with commas and color based on value"""
    if price >= 300000:
        color = 'magenta'
    elif price >= 100000:
        color = 'green'
    elif price >= 50000:
        color = 'gold'
    else:
        color = 'dark-gray'
    return color_text(f"{price:,}", color)

def format_timestamp(timestamp_str: str) -> str:
    """Format timestamp into human-readable time difference"""
    try:
        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        now = datetime.now(timestamp.tzinfo)
        diff = now - timestamp
        
        years = diff.days // 365
        months = diff.days // 30
        days = diff.days
        hours = diff.seconds // 3600
        minutes = (diff.seconds % 3600) // 60
        
        if years > 0:
            return f"{years}y"
        elif months > 0:
            return f"{months}m"
        elif days > 2:
            return f"{days}d"
        elif days > 0 or hours > 0:
            return f"{hours + days*24}h"
        else:
            return f"{minutes}m"
    except:
        return "unknown"

def configure_table_style(table) -> object:
    """Configure default style for all PrettyTables"""
    table.align = 'l'  # Default left alignment
    table.border = True
    table.header = True
    table.preserve_internal_border = False
    table.vertical_char = "│"
    table.horizontal_char = "─"
    table.junction_char = "┼"
    table.top_junction_char = "┬"
    table.bottom_junction_char = "┴"
    table.left_junction_char = "├"
    table.right_junction_char = "┤"
    table.bottom_right_junction_char = "╯"
    table.bottom_left_junction_char = "╰"
    table.top_right_junction_char = "╮"
    table.top_left_junction_char = "╭"
    return table 

def format_mineral_name(mineral_name: str) -> str:
    """Format mineral names for display"""
    if mineral_name == 'Low Temperature Diamonds':
        return 'Low T. Diamonds'
    return mineral_name 

def format_ring_name(ring_name: str, system_name: str) -> str:
    """Format ring name based on settings"""
    if not SHORTEN_RING_NAMES:
        return ring_name
    
    # Remove system name from ring name
    return ring_name.replace(system_name, '').strip() 

def format_reserve_level(level: str) -> str:
    """Format reserve level as bars"""
    return RESERVE_BARS.get(level, '░░░░░')  # Default to Unknown if level not found

def format_station_name(station_name: str, station_data: Dict = None, max_length: int = 18) -> str:
    """Format station name with landing pad size and truncate if too long"""
    if station_name == "No station buying":
        return station_name
        
    # Get landing pad info
    pad_size = station_data.get('landingPads', '?') if station_data else '?'
    if pad_size == 'Unknown':
        pad_size = '?'
    pad_info = f" [{pad_size}]"
    pad_length = len(pad_info)
    
    # Calculate available space for station name
    name_space = max_length - pad_length
    
    # Truncate station name if needed
    if len(station_name) > name_space:
        station_name = station_name[:name_space-3] + "..."
        
    return f"{station_name}{pad_info}" 

def format_compact_ring_entry(ring_name: str, has_hotspot: bool, reserve_level: str) -> str:
    """Format ring entry for stations table
    Example: '4A●[P]' for a pristine ring with hotspot
    """
    # Remove 'Ring' and spaces from name
    compact_name = ring_name.replace('Ring', '').replace(' ', '')
    # Add hotspot symbol if present
    hotspot_symbol = '●' if has_hotspot else ''
    # Get first letter of reserve level
    reserve_char = reserve_level[0] if reserve_level else '?'
    
    return f"{compact_name}{hotspot_symbol}[{reserve_char}]" 