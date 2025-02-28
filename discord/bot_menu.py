# bot menu.py
# Author: subzerofun
# Version: 1.0.0
# Date: 2025-02-21
# Description: This file contains the menu functions for the bot.
# It is used to display the menu to the user.

from discord import ButtonStyle
from typing import Dict, List
from prettytable import PrettyTable

# Button configurations for consistent reuse
MENU_BUTTONS = [
    {
        'label': 'Hotspots',
        'custom_id': 'view_hotspots',
        'style': ButtonStyle.primary,
    },
    {
        'label': 'Laser mining',
        'custom_id': 'view_laser',
        'style': ButtonStyle.success,
    },
    {
        'label': 'Core mining',
        'custom_id': 'view_core',
        'style': ButtonStyle.danger,
    },
    {
        'label': 'All mineable',
        'custom_id': 'view_mineable',
        'style': ButtonStyle.secondary,
    },
    {
        'label': 'Stations',
        'custom_id': 'view_stations',
        'style': ButtonStyle.primary,
    }
]

def create_menu_components() -> List[Dict]:
    """Creates the menu components for the reinforcement view"""
    return MENU_BUTTONS

# Command descriptions for help
COMMAND_DESCRIPTIONS = {
    'hotspots': 'Shows hotspot locations and their mineral types',
    'laser': 'Shows laser mining opportunities',
    'core': 'Shows core mining opportunities',
    'mineable': 'Shows all mineable resources',
    'stations': 'Shows all stations in the system',
    'help': 'Shows this help message'
}

def create_help_message() -> str:
    """Creates the help message with available commands"""
    help_text = "**Available Commands**\n\n"
    help_text += "/reinforce <system_name> - Shows menu with mining options\n\n"
    help_text += "**View Types**\n"
    
    for command, description in COMMAND_DESCRIPTIONS.items():
        help_text += f"`{command}` - {description}\n"
    
    help_text += "\n**Examples**\n"
    help_text += "/reinforce Sol - Shows menu for Sol system\n"
    help_text += "/reinforce Sol laser - Shows laser mining in Sol\n"
    help_text += "/reinforce help - Shows this help message"
    
    return help_text

