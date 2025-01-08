import ijson
import csv
import sys
from pathlib import Path
import argparse
from tqdm import tqdm

TOTAL_ENTRIES = 346478  # Total number of stations in the input file

def process_json_stream(json_file: str):
    """Stream the JSON file one station at a time to avoid memory issues."""
    with open(json_file, 'rb') as file:
        parser = ijson.items(file, 'item')
        for station in parser:
            yield station

def extract_commodities(json_file: str, output_file: str):
    """Extract unique commodity IDs and names from the stations data."""
    # Dictionary to store unique commodities: id -> name
    commodities = {}
    
    print("Reading stations data...")
    pbar = tqdm(total=TOTAL_ENTRIES, desc="Processing stations", 
                unit="stations", ncols=100)
    
    stations_processed = 0
    stations_with_commodities = 0
    
    for station in process_json_stream(json_file):
        # Process station's commodities if present and not None
        if station.get('commodities'):  # This handles both None and missing key
            stations_with_commodities += 1
            for commodity in station['commodities']:
                commodity_id = commodity.get('id')
                commodity_name = commodity.get('name')
                if commodity_id and commodity_name:
                    commodities[commodity_id] = commodity_name
        
        stations_processed += 1
        pbar.update(1)
        
        # Show intermediate stats every 10000 stations
        if stations_processed % 10000 == 0:
            print(f"\nIntermediate stats: {len(commodities)} unique commodities found")
            print(f"Stations with commodities: {stations_with_commodities}/{stations_processed}")
    
    pbar.close()

    # Write to CSV
    print(f"\nWriting {len(commodities)} commodities to {output_file}")
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name'])  # Header
        for commodity_id, commodity_name in sorted(commodities.items()):
            writer.writerow([commodity_id, commodity_name])

    print(f"Done! Processed {stations_processed} stations ({stations_with_commodities} with commodities)")
    print(f"Found {len(commodities)} unique commodities.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract commodity IDs and names from Elite Dangerous galaxy data')
    parser.add_argument('json_file', help='Path to the input JSON file (galaxy data)')
    parser.add_argument('output_file', help='Path to the output CSV file')
    
    args = parser.parse_args()
    
    if not Path(args.json_file).exists():
        print(f"Input file {args.json_file} does not exist!")
        sys.exit(1)
    
    extract_commodities(args.json_file, args.output_file) 