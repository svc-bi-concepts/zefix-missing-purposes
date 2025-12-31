#!/usr/bin/env python3
"""
Test web scraping approach for Zefix withoutshabpub endpoint.
"""

import requests
from bs4 import BeautifulSoup
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TEST_EHRAID = "22"

# Try different web URL formats
WEB_URLS = [
    f"https://www.zefix.ch/en/search/entity/id/{TEST_EHRAID}/withoutshabpub",
    f"https://www.zefix.ch/en/search/entity/{TEST_EHRAID}/withoutshabpub",
    f"https://www.zefix.ch/withoutshabpub/{TEST_EHRAID}",
    f"https://www.zefix.ch/api/v1/firm/{TEST_EHRAID}/withoutshabpub",
]

print("=" * 60)
print(f"Testing Zefix web scraping with EHRAID: {TEST_EHRAID}")
print("=" * 60)

for i, url in enumerate(WEB_URLS, 1):
    print(f"\n{i}. Testing: {url}")
    try:
        # Try with headers to mimic a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(url, timeout=10, verify=False, headers=headers)
        print(f"   Status Code: {response.status_code}")
        
        if response.status_code == 200:
            # Check if it's JSON
            try:
                data = response.json()
                print(f"   ✓ Got JSON response!")
                print(f"   Response keys: {list(data.keys())[:10]}...")
                
                # Look for purpose
                purpose_fields = ['purpose', 'zweck', 'but', 'Zweck', 'Purpose']
                for field in purpose_fields:
                    if field in data:
                        print(f"   ✓ Found purpose: {str(data[field])[:100]}...")
                        break
                
                print(f"\n   Sample: {str(data)[:500]}...")
                print(f"\n   ✓ THIS URL WORKS!")
                break
            except:
                # It's HTML, try to parse it
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for purpose in various ways
                purpose_text = None
                
                # Try to find purpose in common locations
                for selector in [
                    '[data-purpose]',
                    '.purpose',
                    '#purpose',
                    '[class*="purpose"]',
                    '[id*="purpose"]',
                ]:
                    elements = soup.select(selector)
                    if elements:
                        purpose_text = elements[0].get_text(strip=True)
                        print(f"   ✓ Found purpose via selector '{selector}': {purpose_text[:100]}...")
                        break
                
                # Try to find text containing "Zweck" or "purpose"
                if not purpose_text:
                    text = soup.get_text()
                    if 'zweck' in text.lower() or 'purpose' in text.lower():
                        print(f"   ✓ Page contains purpose-related text")
                        # Try to extract it
                        for p in soup.find_all(['p', 'div', 'span']):
                            text = p.get_text(strip=True)
                            if 'zweck' in text.lower() or 'purpose' in text.lower():
                                print(f"   Possible purpose: {text[:200]}...")
                                break
                
                print(f"   ✓ Got HTML response (length: {len(response.text)} chars)")
                print(f"   Title: {soup.title.string if soup.title else 'N/A'}")
                
        else:
            print(f"   ✗ Failed with status {response.status_code}")
            if len(response.text) < 500:
                print(f"   Response: {response.text}")
            
    except Exception as e:
        print(f"   ✗ Error: {str(e)[:100]}")

print("\n" + "=" * 60)

