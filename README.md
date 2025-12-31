# Zefix Purpose Scraper

Scrapes company purposes from Zefix.ch using the `withoutshabpub` endpoint for EHRAIDs found in CSV files in the `artefacts` folder.

## Features

- **Incremental scraping**: Automatically resumes from where it left off
- **Parallel processing**: Uses 20 workers for fast scraping
- **Immediate writing**: Each record is written to CSV as soon as it's scraped
- **Rate limiting**: 0.2 seconds delay per worker to avoid overloading the API
- **Thread-safe**: Safe concurrent access to CSV file
- **Limited scope**: Processes first 1000 EHRAIDs by default

## Setup

1. Activate the virtual environment:
```bash
source .venv/bin/activate
```

2. Dependencies are already installed. If you need to reinstall:
```bash
uv pip install -r requirements.txt
```

## Usage

Run the scraper:
```bash
python scrape_zefix.py
```

The script will:
1. Read all CSV files from the `artefacts` folder
2. Extract unique EHRAIDs (limited to first 1000)
3. Check which EHRAIDs have already been scraped
4. Scrape only the remaining EHRAIDs from the `withoutshabpub` endpoint using 20 parallel workers
5. Write each record immediately to CSV as it's scraped (thread-safe)
6. Flatten the JSON responses
7. Append results to `scraped_purposes.csv`

**Incremental runs**: Simply run the script again - it will automatically skip already scraped EHRAIDs and continue from where it left off.

**Performance**: With 20 workers and 0.2s delay per worker, you can process approximately 100 records per second.

## Configuration

Edit `scrape_zefix.py` to adjust:
- `BASE_URL`: API endpoint URL (default: `https://www.zefix.ch/api/v1/withoutshabpub`)
- `RATE_LIMIT_DELAY`: Delay between requests per worker in seconds (default: 0.2)
- `NUM_WORKERS`: Number of parallel workers (default: 20)
- `MAX_EHRAIDS`: Maximum number of EHRAIDs to process (default: 1000)
- `TIMEOUT`: Request timeout in seconds (default: 30)

## Output

The output CSV (`scraped_purposes.csv`) contains:
- All fields from the API response (flattened)
- EHRAID column for reference
- Error messages if a request fails
- Data is appended incrementally, so you can stop and resume anytime

