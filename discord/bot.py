# bot.py
# Discord bot for MeritMiner
# Author: subzerofun
# Version: 1.0.0
# Date: 2025-02-21
# Description: This bot is used to display the mining data for a given system.
# It is designed to be used in a Discord server.


import os
import random
import discord
import json
import urllib.parse
import requests
import time
import signal
import sys
import re
from datetime import datetime, timedelta
from table2ascii import table2ascii as t2a, PresetStyle, Alignment
from prettytable import PrettyTable, ALL
from dotenv import load_dotenv
import argparse  # Add argparse for command-line arguments
from bot_data import (process_hotspots, process_mineable_rings,
                     process_laser_mining, process_core_mining,
                     check_system_status, get_acquisition_data,
                     process_acquisition_header, process_acquisition_stations)
from bot_display import (create_system_header, create_hotspots_table,
                        create_mineable_table, create_laser_table,
                        create_core_table, create_stations_table,
                        create_acquisition_header, create_acquisition_stations_table,
                        send_chunked_message, send_chunked_interaction)
from bot_menu import (create_menu_components, create_help_message)
from bot_common import (color_text, format_price, calculate_control_points)
import asyncio
import threading
import platform  # Add platform module to detect OS

# Conditionally import msvcrt only on Windows
if platform.system() == 'Windows':
    import msvcrt  # for Windows keyboard detection
from discord.ui import View, Button

# Constants for API endpoints
API_BASE_DEV = "http://127.0.0.1:5000/api/system/"
API_BASE_PROD = "https://meritminer.cc/api/system/"
API_ACQUIRE_DEV = "http://127.0.0.1:5000/api/systems/acquire/"
API_ACQUIRE_PROD = "https://meritminer.cc/api/systems/acquire/"

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Discord bot for MeritMiner')
parser.add_argument('--token', help='Discord bot token')
parser.add_argument('--guild', help='Discord guild ID')
parser.add_argument('--dev', action='store_true', help='Enable development mode (use local API endpoints)')
args = parser.parse_args()

# Load environment variables
load_dotenv()

# Use command-line arguments if provided, otherwise fall back to environment variables
DISCORD_TOKEN = args.token or os.getenv('DISCORD_TOKEN')
DISCORD_GUILD = args.guild or os.getenv('DISCORD_GUILD')

# Set DEV_MODE based on command-line argument (default to production mode)
DEV_MODE = args.dev
if DEV_MODE:
    print("Running in DEVELOPMENT mode - using local API endpoints")
else:
    print("Running in PRODUCTION mode - using remote API endpoints")

if not DISCORD_TOKEN:
    print("Error: Discord token not provided. Use --token argument or set DISCORD_TOKEN environment variable.")
    sys.exit(1)

# For backward compatibility
TOKEN = DISCORD_TOKEN
GUILD = DISCORD_GUILD

# Set up intents
intents = discord.Intents.default()
intents.message_content = True  # Enable message content intent
client = discord.Client(intents=intents)


# Signal handler for graceful shutdown
async def shutdown(signal, loop):
    """Cleanup tasks tied to the service's shutdown."""
    print('\nReceived signal to terminate bot')
    
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    
    print(f'Cancelling {len(tasks)} outstanding tasks')
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()
    print('Shutdown complete.')

# After client initialization
should_exit = threading.Event()

async def clean_shutdown():
    """Perform a clean shutdown of the bot"""
    print('\nInitiating clean shutdown...')
    await client.close()
    should_exit.set()

def handle_exception(loop, context):
    """Handle exceptions in the event loop."""
    msg = context.get("exception", context["message"])
    print(f"Error: {msg}")


def get_api_url(system_name):
    """Format system name for API URL"""
    # Replace special characters and encode for URL
    formatted_name = system_name.replace("+", "%2B").replace("'", "%27").replace("-", "%2D")
    encoded_name = urllib.parse.quote(formatted_name)
    base_url = API_BASE_DEV if DEV_MODE else API_BASE_PROD
    return f"{base_url}{encoded_name}"

def fetch_system_data(system_name):
    """Fetch system data from API"""
    try:
        response = requests.get(get_api_url(system_name))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def process_mining_data(data):
    """Process mining data into four categories"""
    return {
        'system_name': data['name'],  # Add system name to the data
        'hotspots': process_hotspots(data),
        'mineable': process_mineable_rings(data),
        'laser': process_laser_mining(data),
        'core': process_core_mining(data)
    }

@client.event
async def on_ready():
    print(f'{client.user.name} has connected to Discord!')
    
    # Set up signal handlers (only on non-Windows platforms)
    if sys.platform != 'win32':
        loop = asyncio.get_event_loop()
        signals = (signal.SIGTERM, signal.SIGINT)
        for s in signals:
            loop.add_signal_handler(
                s, lambda s=s: asyncio.create_task(shutdown(s, loop))
            )
        
        # Handle exceptions in the event loop
        loop.set_exception_handler(handle_exception)

@client.event
async def on_member_join(member):
    await member.create_dm()
    await member.dm_channel.send(
        f'Hi {member.name}, welcome to my Discord server!'
    )

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content.lower() == 'q':
        await message.channel.send("Shutting down bot...")
        await clean_shutdown()
        return

    if message.content.startswith('/acquire'):
        print(f"\nReceived command: {message.content}")
        
        # Split message into parts
        parts = message.content.split()
        if len(parts) < 2:
            response = "Please provide a system name. Usage: /acquire <system_name>"
            await message.channel.send(response)
            print(f"\nBot response: {response}")
            return
            
        # Handle system names with spaces
        system_name = ' '.join(parts[1:])
        print(f"\nProcessing acquisition request for system: {system_name}")
        
        # Get API endpoints based on mode
        api_base = API_BASE_DEV if DEV_MODE else API_BASE_PROD
        acquire_base = API_ACQUIRE_DEV if DEV_MODE else API_ACQUIRE_PROD
        
        # First check system status
        system_data = await check_system_status(system_name, api_base)
        if not system_data:
            await message.channel.send(f"System '{system_name}' not found.")
            return
            
        controlling_power = system_data.get('controllingPower')
        
        #╭────────────────────────────────────────────────────────╮
        #│  Search for mining systems from the acquisition system │
        #╰────────────────────────────────────────────────────────╯
        if not controlling_power:
            await message.channel.send("Acquisition system detected, searching for mining systems ...")
            acquisition_data = await get_acquisition_data(system_name, acquire_base, "from_acquisition")
            if acquisition_data:
                # Process header data
                header_data = process_acquisition_header(acquisition_data)
                if header_data:
                    # Send header
                    header = create_acquisition_header(header_data)
                    await message.channel.send(f"```ansi\n{header}\n```")
                    
                    # Process and send stations table
                    stations_data = process_acquisition_stations(acquisition_data)
                    if stations_data:
                        stations_table = create_acquisition_stations_table(stations_data)
                        await send_chunked_message(message.channel, stations_table, chunk_size=8)
                    else:
                        await message.channel.send("No mining opportunities found in controlled systems.")
                else:
                    await message.channel.send("Error processing acquisition data.")

        #╭────────────────────────────────────────────────────────────╮
        #│  Search for acquisition systems from the controlled system │
        #╰────────────────────────────────────────────────────────────╯
        elif controlling_power == "Archon Delaine":
            await message.channel.send("Controlled system detected, searching for Acquisition systems in reach ...")
            acquisition_data = await get_acquisition_data(system_name, acquire_base, "for_acquisition")
            if acquisition_data:
                # TODO: Process and display acquisition data
                # For now, just store it in a variable
                message.acquisition_data = acquisition_data
                
        else:
            await message.channel.send("Neither Unoccupied nor controlled system detected. Try again with a valid target.")

    elif message.content.startswith('/reinforce'):
        print(f"\nReceived command: {message.content}")
        
        # Split message into parts
        parts = message.content.split()
        if len(parts) < 2:
            response = "Please provide a system name. Usage: /reinforce <system_name> [view_type]"
            await message.channel.send(response)
            print(f"\nBot response: {response}")
            return

        # Handle help command
        if parts[1].lower() == 'help':
            help_message = create_help_message()
            await message.channel.send(help_message)
            return

        # Handle system names with spaces
        view_type = None  # default view shows menu
        if len(parts) > 2 and parts[-1] in ['json', 'tables', 'hotspots', 'mineable', 'laser', 'core', 'stations']:
            view_type = parts[-1]
            system_name = ' '.join(parts[1:-1])
        else:
            system_name = ' '.join(parts[1:])

        print(f"\nProcessing request for system: {system_name} (view: {view_type})")

        # Fetch and process data
        system_data = fetch_system_data(system_name)
        if not system_data:
            response = f"Error: Could not fetch data for system {system_name}"
            await message.channel.send(response)
            print(f"\nBot response: {response}")
            return

        # Send system header first
        header = create_system_header(system_data)
        header_msg = await message.channel.send(f"```ansi\n{header}\n```")
        print(f"\nBot header sent:\n{header}")

        mining_data = process_mining_data(system_data)

        # If no view type specified, show menu
        if not view_type:
            menu_message = "Show me:"
            view = View()
            for button in create_menu_components():
                # Add system name to custom_id
                button_id = f"{button['custom_id']}:{system_name}"
                view.add_item(Button(
                    label=button['label'],
                    custom_id=button_id,
                    style=button['style']
                ))
            await message.channel.send(menu_message, view=view)
            return

        # Handle specific view types
        if view_type == 'json':
            json_response = json.dumps(mining_data, indent=2)
            await send_chunked_message(message.channel, json_response)
        else:
            if view_type in ['tables', 'hotspots'] and mining_data['hotspots']:
                await send_chunked_message(message.channel, create_hotspots_table(mining_data['hotspots']), chunk_size=10)
            
            if view_type in ['tables', 'mineable'] and mining_data['mineable']:
                await send_chunked_message(message.channel, create_mineable_table(mining_data['mineable']), chunk_size=8)
            
            if view_type in ['tables', 'laser'] and mining_data['laser']:
                await send_chunked_message(message.channel, create_laser_table(mining_data['laser']), chunk_size=6)
            
            if view_type in ['tables', 'core'] and mining_data['core']:
                await send_chunked_message(message.channel, create_core_table(mining_data['core']), chunk_size=6)
            
            if view_type in ['tables', 'stations']:
                await send_chunked_message(message.channel, create_stations_table(system_data), chunk_size=8)

    elif message.content == '99!':
        brooklyn_99_quotes = [
            'I\'m the human form of the 💯 emoji.',
            'Bingpot!',
            (
                'Cool. Cool cool cool cool cool cool cool, '
                'no doubt no doubt no doubt no doubt.'
            ),
        ]
        response = random.choice(brooklyn_99_quotes)
        await message.channel.send(response)
        print(f"\nBot response: {response}")

def keyboard_input():
    """Thread function to detect keyboard input"""
    print("Press 'q' to quit")
    
    # Different keyboard detection methods based on OS
    if platform.system() == 'Windows':
        # Windows implementation using msvcrt
        while not should_exit.is_set():
            if msvcrt.kbhit():
                key = msvcrt.getch().decode().lower()
                if key == 'q':
                    print("\nQuitting via keyboard command...")
                    should_exit.set()
                    asyncio.run_coroutine_threadsafe(clean_shutdown(), client.loop)
                    break
            time.sleep(0.1)  # Small sleep to prevent high CPU usage
    else:
        # Linux/Unix implementation using standard input
        try:
            import sys, tty, termios
            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            try:
                tty.setraw(sys.stdin.fileno())
                while not should_exit.is_set():
                    if sys.stdin.isatty():  # Only try to read if connected to a terminal
                        try:
                            # Check if data is available to read (non-blocking)
                            import select
                            if select.select([sys.stdin], [], [], 0.1)[0]:
                                key = sys.stdin.read(1).lower()
                                if key == 'q':
                                    print("\nQuitting via keyboard command...")
                                    should_exit.set()
                                    asyncio.run_coroutine_threadsafe(clean_shutdown(), client.loop)
                                    break
                        except (OSError, IOError):
                            # Handle case where stdin is not available
                            time.sleep(1)
                    else:
                        # If not connected to a terminal, just sleep
                        time.sleep(1)
            finally:
                # Restore terminal settings
                if sys.stdin.isatty():
                    termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        except (ImportError, AttributeError, termios.error):
            # Fallback for environments where terminal control is not available
            print("Keyboard detection not available. Use Ctrl+C to exit.")
            while not should_exit.is_set():
                time.sleep(1)

# Update interaction handler to parse custom_id
@client.event
async def on_interaction(interaction):
    """Handle button interactions"""
    if not interaction.type == discord.InteractionType.component:
        return

    try:
        # Get the custom_id from the button and parse it
        custom_id = interaction.data.get('custom_id')
        button_type, system_name = custom_id.split(':', 1)
        
        # Acknowledge the interaction immediately
        await interaction.response.defer()
        
        # Fetch fresh system data
        system_data = fetch_system_data(system_name)
        if not system_data:
            await interaction.followup.send("Could not fetch system data. Please try the command again.", ephemeral=True)
            return
        
        # Process the data
        mining_data = process_mining_data(system_data)
        
        # Handle different button clicks
        if button_type == 'view_hotspots' and mining_data['hotspots']:
            await send_chunked_interaction(interaction, create_hotspots_table(mining_data['hotspots']), chunk_size=10)
        elif button_type == 'view_laser' and mining_data['laser']:
            await send_chunked_interaction(interaction, create_laser_table(mining_data['laser']), chunk_size=6)
        elif button_type == 'view_core' and mining_data['core']:
            await send_chunked_interaction(interaction, create_core_table(mining_data['core']), chunk_size=6)
        elif button_type == 'view_mineable' and mining_data['mineable']:
            await send_chunked_interaction(interaction, create_mineable_table(mining_data['mineable']), chunk_size=8)
        elif button_type == 'view_stations':
            await send_chunked_interaction(interaction, create_stations_table(system_data), chunk_size=8)
        else:
            await interaction.followup.send("No data available for this view.", ephemeral=True)
        
    except Exception as e:
        print(f"Error handling button interaction: {e}")
        await interaction.followup.send("An error occurred processing this request.", ephemeral=True)

if __name__ == "__main__":
    # Start keyboard detection thread only if connected to a terminal
    if sys.stdin.isatty():
        keyboard_thread = threading.Thread(target=keyboard_input, daemon=True)
        keyboard_thread.start()
    else:
        print("Not connected to a terminal. Keyboard detection disabled.")
        keyboard_thread = None
    
    try:
        client.run(DISCORD_TOKEN)
    except Exception as e:
        print(f'\nError: {e}')
    finally:
        should_exit.set()
        if not client.is_closed():
            client.close()
        if keyboard_thread and keyboard_thread.is_alive():
            keyboard_thread.join(timeout=1.0)
        sys.exit(0)