import argparse
from databricks.sdk.runtime import *
from nwo_projects_api import fetch_data
from typing import List

def get_page_numbers() -> List[int]:
    """Retrieves the list of available page numbers from the API."""
    first_page = fetch_data(page_nr=1)
    return list(range(1, first_page['meta']['pages'] + 1))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retrieve and set page numbers for NWO projects.")
    parser.add_argument("--last_n", required=False, type=str, default=None, help="Number of last pages to retrieve")
    last_n = int(parser.parse_args().last_n)

    page_numbers = get_page_numbers()
    if last_n > 0:
        # Ensure last_n is not greater than the available number of pages
        page_numbers = page_numbers[-last_n:] if last_n <= len(page_numbers) else page_numbers
    
    # Set page numbers as task value
    dbutils.jobs.taskValues.set(
        key='page_numbers',
        value=page_numbers
    )