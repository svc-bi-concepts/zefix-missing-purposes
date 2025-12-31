#!/usr/bin/env python3
"""
Scrape Zefix.ch withoutshabpub endpoint for company purposes.
Reads EHRAIDs from CSV files in artefacts folder and outputs flattened CSV.
"""

import csv
import json
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Set, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configuration
ARTEFACTS_DIR = Path(__file__).parent / "artefacts"
OUTPUT_CSV = Path(__file__).parent / "scraped_purposes.csv"
BASE_URL = "https://zefix.ch/ZefixREST/api/v1/firm"  # Base URL for firm endpoint (without www)
# Note: Try withoutshabpub.json endpoint if regular endpoint doesn't have purpose
RATE_LIMIT_DELAY = 0.2  # seconds between requests per worker
MAX_RETRIES = 3
TIMEOUT = 30
MAX_EHRAIDS = None  # No limit - scrape all EHRAIDs
NUM_WORKERS = 20  # Number of parallel workers

# Thread-safe CSV writing
csv_lock = threading.Lock()
fieldnames_lock = threading.Lock()
all_fieldnames = set()
processed_count = 0
processed_lock = threading.Lock()


def setup_session() -> requests.Session:
    """Setup requests session with retry strategy and cookies."""
    session = requests.Session()
    # Disable SSL verification due to sandbox restrictions
    session.verify = False
    # Suppress SSL warnings
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Add headers to mimic browser
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,de;q=0.7',
    })
    
    # Get cookies by visiting the main page first
    try:
        session.get('https://zefix.ch', timeout=10)
    except:
        pass  # Continue even if this fails
    
    return session


def extract_ehrads_from_csv(csv_path: Path) -> Set[str]:
    """Extract unique EHRAIDs from a CSV file."""
    ehrads = set()
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'EHRAID' in row and row['EHRAID']:
                    ehrads.add(row['EHRAID'].strip())
    except Exception as e:
        print(f"Error reading {csv_path}: {e}")
    return ehrads


def get_all_ehrads() -> List[str]:
    """Collect all unique EHRAIDs from all CSV files in artefacts folder."""
    all_ehrads = set()
    csv_files = list(ARTEFACTS_DIR.glob("*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {ARTEFACTS_DIR}")
        return []
    
    print(f"Found {len(csv_files)} CSV file(s)")
    for csv_file in csv_files:
        print(f"  Reading {csv_file.name}...")
        ehrads = extract_ehrads_from_csv(csv_file)
        all_ehrads.update(ehrads)
        print(f"    Found {len(ehrads)} EHRAIDs (total unique: {len(all_ehrads)})")
    
    # Convert to sorted list and limit if specified
    sorted_ehrads = sorted(all_ehrads)
    if MAX_EHRAIDS and len(sorted_ehrads) > MAX_EHRAIDS:
        print(f"  Limiting to first {MAX_EHRAIDS} EHRAIDs")
        sorted_ehrads = sorted_ehrads[:MAX_EHRAIDS]
    
    return sorted_ehrads


def flatten_dict(d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
    """Flatten nested dictionary."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Handle lists - create indexed keys
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(flatten_dict(item, f"{new_key}_{i}", sep=sep).items())
                else:
                    items.append((f"{new_key}_{i}", item))
        else:
            items.append((new_key, v))
    return dict(items)


def get_already_scraped_ehrads() -> Set[str]:
    """Get set of EHRAIDs that have already been scraped (excluding error-only records)."""
    if not OUTPUT_CSV.exists():
        return set()
    
    scraped = set()
    try:
        with open(OUTPUT_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'EHRAID' in row and row['EHRAID']:
                    # Only count as scraped if it doesn't have only an error field
                    # (meaning it was successfully scraped with actual data)
                    has_error_only = 'error' in row and row.get('error') and len([k for k in row.keys() if k not in ['EHRAID', 'error'] and row.get(k)]) == 0
                    if not has_error_only:
                        scraped.add(row['EHRAID'].strip())
    except Exception as e:
        print(f"Warning: Could not read existing CSV: {e}")
    
    return scraped


def get_existing_fieldnames() -> List[str]:
    """Get fieldnames from existing CSV if it exists."""
    if not OUTPUT_CSV.exists():
        return ['EHRAID']
    
    try:
        with open(OUTPUT_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return list(reader.fieldnames or ['EHRAID'])
    except Exception as e:
        print(f"Warning: Could not read existing CSV headers: {e}")
        return ['EHRAID']


def get_field_order() -> List[str]:
    """Define preferred field order for CSV output."""
    # Priority order: EHRAID first, then logical groupings
    priority_fields = [
        'EHRAID',
        'name',
        'purpose',
        'status',
        'ehraid',
        'uid',
        'uidFormatted',
        'chid',
        'chidFormatted',
        'legalFormId',
        'legalSeat',
        'legalSeatId',
        'registerOfficeId',
        'shabDate',
        'deleteDate',
        'cantonalExcerptWeb',
        'translation',
        'rabId',
    ]
    
    # Address fields
    address_fields = [
        'address_organisation',
        'address_careOf',
        'address_street',
        'address_houseNumber',
        'address_addon',
        'address_poBox',
        'address_town',
        'address_swissZipCode',
        'address_country',
    ]
    
    # Other fields (old names, relationships, etc.)
    other_fields = [
        'oldNames',
        'mainOffices',
        'furtherMainOffices',
        'branchOffices',
        'hasTakenOver',
        'wasTakenOverBy',
        'auditFirms',
        'auditFirmFor',
    ]
    
    return priority_fields + address_fields + other_fields


def clean_record_value(value: Any) -> str:
    """Clean record value for CSV output."""
    if value is None:
        return ''
    if isinstance(value, (dict, list)):
        # Convert complex types to JSON string
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def write_record_to_csv(record: Dict[str, Any]):
    """Thread-safe function to write a single record to CSV immediately."""
    global all_fieldnames
    
    with csv_lock:
        # Update fieldnames
        with fieldnames_lock:
            all_fieldnames.update(record.keys())
            
            # Get preferred field order
            preferred_order = get_field_order()
            
            # Build fieldnames: preferred order first, then rest alphabetically
            ordered_fields = []
            remaining_fields = set(all_fieldnames)
            
            # Add preferred fields in order
            for field in preferred_order:
                if field in remaining_fields:
                    ordered_fields.append(field)
                    remaining_fields.remove(field)
            
            # Add any fields that match preferred patterns (e.g., oldNames_0_*)
            pattern_fields = {}
            for field in list(remaining_fields):
                if field.startswith('oldNames_'):
                    pattern_fields[field] = field
                elif field.startswith('address_'):
                    # Already handled in preferred order
                    pass
                else:
                    pattern_fields[field] = field
            
            # Sort pattern fields and add them
            for field in sorted(pattern_fields.keys()):
                if field in remaining_fields:
                    ordered_fields.append(field)
                    remaining_fields.remove(field)
            
            # Add remaining fields alphabetically
            for field in sorted(remaining_fields):
                ordered_fields.append(field)
            
            sorted_fieldnames = ordered_fields
        
        # Clean and prepare record
        complete_record = {}
        for field in sorted_fieldnames:
            value = record.get(field, '')
            complete_record[field] = clean_record_value(value)
        
        # Write to CSV
        file_exists = OUTPUT_CSV.exists()
        mode = 'a' if file_exists else 'w'
        write_header = not file_exists  # Only write header if file doesn't exist
        
        with open(OUTPUT_CSV, mode, encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=sorted_fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerow(complete_record)


def scrape_company_data(session: requests.Session, ehraid: str) -> Dict[str, Any]:
    """Scrape company data from Zefix REST API using withoutShabPub endpoint."""
    # Use withoutShabPub.json endpoint (note capital S in ShabPub)
    url = f"{BASE_URL}/{ehraid}/withoutShabPub.json"
    
    try:
        response = session.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        
        data = response.json()
        
        # Flatten the response
        flattened = flatten_dict(data)
        flattened['EHRAID'] = ehraid
        return flattened
    
    except requests.exceptions.RequestException as e:
        return {'EHRAID': ehraid, 'error': str(e)}
    except json.JSONDecodeError as e:
        return {'EHRAID': ehraid, 'error': f'JSON decode error: {e}'}
    except Exception as e:
        return {'EHRAID': ehraid, 'error': str(e)}


def scrape_and_save(ehraid: str, session: requests.Session, total: int) -> Dict[str, Any]:
    """Scrape a single EHRAID and immediately save to CSV."""
    global processed_count
    
    # Scrape data
    data = scrape_company_data(session, ehraid)
    
    # Write immediately
    write_record_to_csv(data)
    
    # Update counter and print progress
    with processed_lock:
        processed_count += 1
        count = processed_count
    
    # Extract purpose for display
    purpose = data.get('purpose') or data.get('zweck') or data.get('but') or 'N/A'
    purpose_str = purpose[:50] + "..." if len(str(purpose)) > 50 else str(purpose)
    
    print(f"  [{count}/{total}] ✓ EHRAID: {ehraid} - Purpose: {purpose_str}")
    
    # Rate limiting per worker
    time.sleep(RATE_LIMIT_DELAY)
    
    return data


def main():
    """Main scraping function."""
    global processed_count, all_fieldnames
    
    print("=" * 60)
    print("Zefix.ch Purpose Scraper")
    print("=" * 60)
    
    # Get all EHRAIDs
    print("\n1. Collecting EHRAIDs from artefacts folder...")
    all_ehrads = get_all_ehrads()
    
    if not all_ehrads:
        print("No EHRAIDs found. Exiting.")
        return
    
    # Check for already scraped EHRAIDs
    print("\n2. Checking for already scraped EHRAIDs...")
    already_scraped = get_already_scraped_ehrads()
    if already_scraped:
        print(f"  Found {len(already_scraped)} already scraped EHRAIDs")
    
    # Filter out already scraped
    ehrads_to_scrape = [e for e in all_ehrads if e not in already_scraped]
    
    if not ehrads_to_scrape:
        print("  All EHRAIDs have already been scraped!")
        return
    
    print(f"\nTotal EHRAIDs to scrape: {len(ehrads_to_scrape)}")
    print(f"Already scraped: {len(already_scraped)}")
    print(f"Total in dataset: {len(all_ehrads)}")
    
    # Initialize fieldnames from existing CSV
    existing_fieldnames = get_existing_fieldnames()
    all_fieldnames = set(existing_fieldnames)
    
    # Reset processed count
    processed_count = 0
    
    # Scrape data with parallel workers
    print(f"\n3. Scraping data from {BASE_URL}...")
    print(f"   Workers: {NUM_WORKERS}")
    print(f"   Rate limit: {RATE_LIMIT_DELAY}s per worker")
    print(f"   Writing records immediately as they're scraped")
    
    total = len(ehrads_to_scrape)
    
    # Create a session for each worker (sessions are not thread-safe)
    def worker_with_session(ehraid: str) -> Dict[str, Any]:
        session = setup_session()
        return scrape_and_save(ehraid, session, total)
    
    # Use ThreadPoolExecutor for parallel scraping
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Submit all tasks
        future_to_ehraid = {
            executor.submit(worker_with_session, ehraid): ehraid 
            for ehraid in ehrads_to_scrape
        }
        
        # Process completed tasks
        for future in as_completed(future_to_ehraid):
            ehraid = future_to_ehraid[future]
            try:
                future.result()
            except Exception as e:
                print(f"  ✗ Error processing {ehraid}: {e}")
                # Write error record
                error_record = {'EHRAID': ehraid, 'error': f'Exception: {e}'}
                write_record_to_csv(error_record)
    
    elapsed_time = time.time() - start_time
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  EHRAIDs processed in this run: {processed_count}")
    print(f"  Total scraped (including previous runs): {len(already_scraped) + processed_count}")
    print(f"  Time elapsed: {elapsed_time:.1f}s ({elapsed_time/60:.1f} minutes)")
    print(f"  Average time per record: {elapsed_time/processed_count:.2f}s" if processed_count > 0 else "")
    print(f"  Output file: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()

