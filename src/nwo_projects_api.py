import argparse
import os
import requests
import json
from typing import Dict

BASE_URL = 'https://nwopen-api.nwo.nl/NWOpen-API/api/Projects'

def fetch_data(page_nr: int = 1) -> Dict:
    """Fetches JSON data from the NWO Open API for a specific page number."""
    try:
        response = requests.get(BASE_URL, params={'page': page_nr})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise SystemExit(f"Network error occurred: {e}")

def write_json_file(data: Dict, prefix: str, catalog: str, schema: str, page_nr: int) -> None:
    """Writes JSON data to a file in the specified catalog and schema directories."""
    dir_path = f'/Volumes/{catalog}/{schema}/landing/nwo_projects/'
    try:
        os.makedirs(dir_path, exist_ok=True)
        file_path = f'{dir_path}{prefix}_page{page_nr}.json'
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                json.dump(data, f)
            print(f"Data written to {file_path}")
        else:
            print(f"File {file_path} already exists, skipping write.")
    except OSError as e:
        raise SystemExit(f"Error creating directory or writing file: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and store NWO project data.")
    parser.add_argument("--page_nr", required=True, type=str, help="Page number of the data to fetch")
    parser.add_argument("--run_id", required=True, type=str, help="Job run identifier used as prefix for the JSON file")
    parser.add_argument("--catalog", required=True, type=str, help="Catalog name for the directory structure")
    parser.add_argument("--schema", required=True, type=str, help="Schema name for the directory structure")
    args = parser.parse_args()

    # Fetch and write data
    data = fetch_data(int(args.page_nr))
    write_json_file(data, args.run_id, args.catalog, args.schema, int(args.page_nr))