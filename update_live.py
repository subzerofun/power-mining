import time
import sys
import zlib
import json
import zmq
import sqlite3
import csv
import os
import argparse
import signal
from datetime import datetime, timezone

# ANSI color codes
YELLOW = '\033[93m'
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
ORANGE = '\033[38;5;208m'  # Add orange color code
RESET = '\033[0m'

# How often (in seconds) to flush changes to DB
DB_UPDATE_INTERVAL = 10

# Debug flag for detailed commodity changes
DEBUG = False

# Path to your existing SQLite database
DB_PATH = "systems.db"

# Path to your commodities.csv cross-reference
# CSV format:
#   id,name
#   advancedcatalysers,Advanced Catalysers
#   ...
COMMODITIES_CSV = os.path.join("data", "commodities_mining.csv")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    print("[STOPPING] EDDN Update Service")
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def load_commodity_map(csv_path):
    """
    Loads a CSV of:
        id,name
        advancedcatalysers,Advanced Catalysers
        ...
    Returns (commodity_map, reverse_map):
      - commodity_map: { eddn_id: local_name }   # e.g. {"alexandrite": "Alexandrite"}
      - reverse_map:   { local_name: eddn_id }   # e.g. {"Alexandrite": "alexandrite"}
    """
    commodity_map = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            eddn_id = row["id"].strip()
            local_name = row["name"].strip()
            # Special case: store as "Void Opal" but handle both forms
            if local_name == "Void Opals":
                local_name = "Void Opal"
            commodity_map[eddn_id] = local_name

    # Build reverse map to handle "deleted" items
    reverse_map = {local_name: eddn_id for eddn_id, local_name in commodity_map.items()}
    
    # Special case: allow both forms to map to the same ID
    if "Void Opal" in reverse_map:
        reverse_map["Void Opals"] = reverse_map["Void Opal"]

    return commodity_map, reverse_map

def main():
    parser = argparse.ArgumentParser(description='EDDN data updater for Power Mining')
    parser.add_argument('--auto', action='store_true', help='Automatically commit changes without asking')
    args = parser.parse_args()

    print(f"[INIT] Starting Live EDDN Update every {DB_UPDATE_INTERVAL} seconds", flush=True)
    commodity_map, reverse_map = load_commodity_map(COMMODITIES_CSV)
    print(f"Loaded {len(commodity_map)} commodities from CSV (mapping EDDN ID -> local name).", flush=True)

    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.connect("tcp://eddn.edcd.io:9500")
    subscriber.setsockopt_string(zmq.SUBSCRIBE, "")  # subscribe to all messages

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    # Buffers:
    #   commodity_buffer[station_name] = {
    #       local_name: (eddn_id, sell_price, demand)
    #   }
    commodity_buffer = {}
    #   power_buffer[system_name] = (controlling_power, power_state)
    power_buffer = {}

    last_update = time.time()
    print(f"Listening to EDDN. Flush changes every {DB_UPDATE_INTERVAL}s. (Press Ctrl+C to stop)", flush=True)
    print("Mode:", "automatic" if args.auto else "manual", flush=True)

    try:
        while True:
            raw_msg = subscriber.recv()
            decompressed = zlib.decompress(raw_msg)
            eddn_data = json.loads(decompressed)

            schema_ref = eddn_data.get("$schemaRef", "").lower()
            message = eddn_data.get("message", {})

            if "commodity" in schema_ref:
                handle_commodity_message(message, commodity_buffer, commodity_map)
            elif "journal" in schema_ref:
                handle_journal_message(message, power_buffer)

            current_time = time.time()
            if current_time - last_update >= DB_UPDATE_INTERVAL:
                print("[DATABASE] Writing to Database starting...", flush=True)
                stations_updated, commodities_updated = flush_commodities_to_db(conn, commodity_buffer, reverse_map, args.auto)
                power_systems_updated = flush_power_to_db(conn, power_buffer, args.auto)
                print(f"[DATABASE] Writing to Database finished. Updated {stations_updated} stations, {commodities_updated} commodities, {power_systems_updated} power systems.", flush=True)
                last_update = current_time

    except KeyboardInterrupt:
        print("[STOPPING] EDDN Update Service", flush=True)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr, flush=True)
    finally:
        # Final flush
        print("[DATABASE] Writing to Database starting...", flush=True)
        stations_updated, commodities_updated = flush_commodities_to_db(conn, commodity_buffer, reverse_map, args.auto)
        power_systems_updated = flush_power_to_db(conn, power_buffer, args.auto)
        print(f"[DATABASE] Writing to Database finished. Updated {stations_updated} stations, {commodities_updated} commodities, {power_systems_updated} power systems.", flush=True)
        conn.close()
        print("[TERMINATED] EDDN Update Service", flush=True)

def handle_commodity_message(msg, commodity_buffer, commodity_map):
    """
    EDDN commodity data structure (v3.0):
    message: {
        "stationName": string,
        "commodities": [
            {
                "name": string,      # This is the EDDN commodity name
                "sellPrice": int,
                "demand": int,
                ...
            }
        ]
    }
    """
    station_name = msg.get("stationName")
    market_id = msg.get("marketId")
    
    # Check if this is a Fleet Carrier (either by economy or station type)
    if msg.get("stationType") == "FleetCarrier" or \
       (msg.get("economies") and msg["economies"][0].get("name") == "Carrier"):
        print(f"{YELLOW}[DEBUG] Skipped Fleet Carrier Data: {station_name}{RESET}", flush=True)
        return
    
    if market_id is None:
        print(f"{YELLOW}[DEBUG] Live update without marketId: {station_name}{RESET}", flush=True)
    
    if not station_name:
        return

    commodities = msg.get("commodities", [])
    station_data = {}

    for c in commodities:
        commodity_name = c.get("name")  # This is the EDDN commodity name
        if not commodity_name:
            continue

        # Only process commodities that are in our mining CSV
        local_name = commodity_map.get(commodity_name.lower())
        if local_name is None:  # Skip if not in our mining commodities list
            continue

        sell_price = c.get("sellPrice", 0)
        demand = c.get("demand", 0)

        # Store (original_name, sell_price, demand, market_id)
        station_data[local_name] = (commodity_name, sell_price, demand, market_id)

    commodity_buffer[station_name] = station_data

def flush_commodities_to_db(conn, commodity_buffer, reverse_map, auto_commit=False):
    """
    Compares old DB data vs new EDDN data for each station.
    Returns (stations_updated, commodities_updated) tuple.
    """
    if not commodity_buffer:
        return 0, 0

    cursor = conn.cursor()
    changes = {}
    total_commodities = 0
    missing_station_ids = 0

    # First check for any NULL station_ids in the database
    cursor.execute("SELECT COUNT(*) FROM station_commodities WHERE station_id IS NULL")
    null_count = cursor.fetchone()[0]
    if null_count > 0:
        print(f"{YELLOW}[DEBUG] Found {null_count} existing entries with NULL station_id{RESET}", flush=True)

    for station_name, new_map in commodity_buffer.items():
        # Get the market_id from the first commodity's data (all entries for a station have the same market_id)
        first_commodity = list(new_map.values())[0] if new_map else None
        market_id = first_commodity[3] if first_commodity and len(first_commodity) > 3 else None
        
        # Step 1: lookup station in 'stations'
        cursor.execute("""
            SELECT system_id64, station_id
              FROM stations
             WHERE station_name = ?
        """, (station_name,))
        row = cursor.fetchone()
        if not row:
            # Skip if we can't find the station - we don't want to store data for unknown stations
            continue
            
        system_id64, st_id = row
        
        # If we have a market_id but no station_id, update the stations table
        if market_id and (st_id is None or st_id != market_id):
            cursor.execute("""
                UPDATE stations 
                SET station_id = ? 
                WHERE station_name = ? AND system_id64 = ?
            """, (market_id, station_name, system_id64))
            conn.commit()
            st_id = market_id
        
        if st_id is None:
            missing_station_ids += 1
            print(f"{YELLOW}[DEBUG] No station_id found for station: {station_name}{RESET}", flush=True)
            continue

        # Step 2: read old data from station_commodities
        cursor.execute("""
            SELECT commodity_name, sell_price, demand
              FROM station_commodities
             WHERE system_id64 = ?
               AND station_name = ?
        """, (system_id64, station_name))
        old_rows = cursor.fetchall()

        # old_map[local_name] = (sell, demand)
        old_map = {r[0]: (r[1], r[2]) for r in old_rows}

        # ---- DEBUG INFO: EDDN vs DB counts ----
        eddn_count = len(new_map)      # how many commodities we got from EDDN
        db_count = len(old_map)        # how many are in DB
        # matched_count: local_name in both
        matched_count = 0  
        # changed_count: matched but price/demand differs
        changed_count = 0

        for local_name, (old_sell, old_demand) in old_map.items():
            if local_name in new_map:
                matched_count += 1
                (_, new_sell, new_demand, _) = new_map[local_name]
                if (new_sell != old_sell) or (new_demand != old_demand):
                    changed_count += 1

        if DEBUG:
            print(f"\n[DEBUG] Station '{station_name}': EDDN commodities={eddn_count}, DB commodities={db_count}")
            print(f"[DEBUG] Matches found in both DB & new EDDN data: {matched_count}, "
                f"of which {changed_count} have different price/demand.")

        # Step 3: detect add/updated/delete as usual
        added_list = []
        updated_list = []
        deleted_list = []

        # (a) Check old DB items => "deleted" or "updated"
        for old_name, (old_sell, old_demand) in old_map.items():
            if old_name not in new_map:
                # Deleted
                old_id = reverse_map.get(old_name, "???")
                deleted_list.append((old_id, old_name, old_sell, old_demand))
            else:
                (new_id, new_sell, new_demand, _) = new_map[old_name]
                if new_sell != old_sell or new_demand != old_demand:
                    updated_list.append((new_id, old_name, old_sell, old_demand, new_sell, new_demand))

        # (b) Check new EDDN items => "added"
        for local_name, (eddn_id, s_price, dem, _) in new_map.items():
            if local_name not in old_map:
                added_list.append((eddn_id, local_name, s_price, dem))

        if added_list or updated_list or deleted_list:
            print(f"\nCommodity changes detected:", flush=True)
            print(f"- {station_name}: {len(updated_list)} updated, {len(added_list)} added, {len(deleted_list)} deleted", flush=True)
            changes[station_name] = {
                "system_id64": system_id64,
                "station_id": st_id,
                "added": added_list,
                "updated": updated_list,
                "deleted": deleted_list,
                "new_data": new_map
            }

    if not changes:
        print("\nNo commodity changes to commit.")
        commodity_buffer.clear()
        return 0, 0

    # Step 4: Show summary
    print("\nCommodity changes detected:")
    for station_name, info in changes.items():
        adds = info["added"]
        upds = info["updated"]
        dels = info["deleted"]

        print(f"  - {station_name}:", end="")
        parts = []
        if adds: parts.append(f"{len(adds)} added")
        if upds: parts.append(f"{len(upds)} updated")
        if dels: parts.append(f"{len(dels)} deleted")
        print(" and ".join(parts) + ".")

        # Show up to 10 added
        if DEBUG:
            for (cid, cname, sp, dm) in adds[:10]:
                print(f"     + Added: [id={cid}, name={cname}], sellPrice={sp}, demand={dm}")
            if len(adds) > 10:
                print("       ... (only first 10 added) ...")

        # Show up to 10 updated
        if DEBUG:
            for (cid, cname, o_s, o_d, n_s, n_d) in upds[:10]:
                print(f"     ~ Updated: [id={cid}, name={cname}], sellPrice {o_s}->{n_s}, demand {o_d}->{n_d}")
            if len(upds) > 10:
                print("       ... (only first 10 updated) ...")

        # Show up to 10 deleted
        if DEBUG:
            for (cid, cname, o_s, o_d) in dels[:10]:
                print(f"     - Deleted: [id={cid}, name={cname}], oldSellPrice={o_s}, oldDemand={o_d}")
            if len(dels) > 10:
                print("       ... (only first 10 deleted) ...")

    # Step 5: commit changes based on mode
    should_commit = auto_commit
    if not auto_commit:
        ans = input("Commit these commodity changes? [Y/N] ").strip().lower()
        should_commit = ans == "y"

    stations_updated = 0
    if should_commit:
        try:
            print("[DATABASE] Writing to Database starting")
            for station_name, info in changes.items():
                sys_id = info["system_id64"]
                st_id = info["station_id"]
                new_map = info["new_data"]

                # Delete old
                cursor.execute("""
                    DELETE FROM station_commodities
                     WHERE system_id64 = ?
                       AND station_name = ?
                """, (sys_id, station_name))

                # Insert new
                ins_sql = """
                    INSERT INTO station_commodities 
                        (system_id64, station_id, station_name, commodity_name, sell_price, demand)
                    VALUES (?, ?, ?, ?, ?, ?)
                """
                for local_name, (cid, sell_p, dmnd, _) in new_map.items():
                    cursor.execute(ins_sql, (sys_id, st_id, station_name, local_name, sell_p, dmnd))
                    total_commodities += 1

                # Update station timestamp
                now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")
                cursor.execute("""
                    UPDATE stations
                       SET update_time = ?
                     WHERE system_id64 = ?
                       AND station_id = ?
                """, (now_str, sys_id, st_id))
                stations_updated += 1

            conn.commit()
            print(f"Updated {stations_updated} stations")
            print(f"Updated {total_commodities} commodities")
            print("[DATABASE] Writing to Database finished")
            if not auto_commit:
                print("Commodity changes committed.")
        except Exception as ex:
            conn.rollback()
            print(f"Error while committing commodity changes: {ex}", file=sys.stderr)
            return 0, 0
    else:
        conn.rollback()
        if not auto_commit:
            print("Commodity changes rolled back.")

    cursor.close()
    commodity_buffer.clear()
    return stations_updated, total_commodities

def handle_journal_message(msg, power_buffer):
    """
    We skip if controlling_power is None. Only do partial power updates otherwise.
    """
    event = msg.get("event", "")
    if event not in ("FSDJump", "Location"):
        return

    system_name = msg.get("StarSystem", "")
    if not system_name:
        return

    powers = msg.get("Powers", [])
    power_state = msg.get("PowerplayState", "")

    controlling_power = None
    if isinstance(powers, list) and len(powers) == 1:
        controlling_power = powers[0]
    elif isinstance(powers, str):
        controlling_power = powers

    if controlling_power is None:
        return

    print(f"[PowerData] system={system_name}, controlling_power={controlling_power}, power_state={power_state}")
    power_buffer[system_name] = (controlling_power, power_state)

def flush_power_to_db(conn, power_buffer, auto_commit=False):
    """
    Updates controlling_power/power_state only if different from what's in DB.
    Returns number of systems updated.
    """
    if not power_buffer:
        return 0

    cursor = conn.cursor()
    changed = []

    for system_name, (new_pwr, new_st) in power_buffer.items():
        cursor.execute("""
            SELECT id64, controlling_power, power_state
              FROM systems
             WHERE name = ?
        """, (system_name,))
        row = cursor.fetchone()
        if row:
            sys_id, old_pwr, old_state = row
            if (old_pwr != new_pwr) or (old_state != new_st):
                cursor.execute("""
                    UPDATE systems
                       SET controlling_power = ?,
                           power_state       = ?
                     WHERE id64 = ?
                """, (new_pwr, new_st, sys_id))
                changed.append((system_name, old_pwr, old_state, new_pwr, new_st))

    systems_updated = 0
    if changed:
        print("\nPower changes detected:")
        print(f"  Updating {len(changed)} system(s).")
        for info in changed[:10]:
            s_name, o_p, o_s, n_p, n_s = info
            print(f"    - {s_name}: from [{o_p}/{o_s}] to [{n_p}/{n_s}]")
        if len(changed) > 10:
            print("      ... (only first 10 shown) ...")

        should_commit = auto_commit
        if not auto_commit:
            ans = input("Commit these power changes? [Y/N] ").strip().lower()
            should_commit = ans == "y"

        if should_commit:
            try:
                print("[DATABASE] Writing to Database starting")
                conn.commit()
                systems_updated = len(changed)
                print(f"Updated {systems_updated} power systems")
                print("[DATABASE] Writing to Database finished")
                if not auto_commit:
                    print("Power changes committed.")
            except Exception as ex:
                conn.rollback()
                print(f"Error while committing power changes: {ex}", file=sys.stderr)
        else:
            conn.rollback()
            if not auto_commit:
                print("Power changes rolled back.")
    else:
        if not auto_commit:
            print("\nNo power changes to commit.")

    cursor.close()
    power_buffer.clear()
    return systems_updated

if __name__ == "__main__":
    main()
