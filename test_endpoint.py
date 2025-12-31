#!/usr/bin/env python3
"""
Test script to verify the Zefix API endpoint with a single EHRAID.
"""

import requests
import json
import urllib3

# Disable SSL warnings for testing
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Test different endpoint formats
TEST_EHRAID = "22"  # From the CSV file

ENDPOINTS = [
    f"https://www.zefix.ch/ZefixREST/api/v1/firm/{TEST_EHRAID}/withoutshabpub",
    f"https://www.zefix.ch/ZefixREST/api/v1/firm/{TEST_EHRAID}",
    f"https://www.zefix.admin.ch/ZefixPublicREST/api/v1/firm/{TEST_EHRAID}/withoutshabpub",
    f"https://www.zefix.admin.ch/ZefixPublicREST/api/v1/firm/{TEST_EHRAID}",
    f"https://www.zefix.ch/api/v1/firm/{TEST_EHRAID}/withoutshabpub",
]

def test_url(url, verify_ssl=True):
    """Test a single URL."""
    try:
        response = requests.get(url, timeout=10, verify=verify_ssl)
        print(f"   Status Code: {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"   ✓ SUCCESS! Got JSON response")
                print(f"   Response keys: {list(data.keys())[:10]}...")  # Show first 10 keys
                
                # Check for purpose field
                purpose_fields = ['purpose', 'zweck', 'but', 'Zweck', 'Purpose']
                found_purpose = False
                for field in purpose_fields:
                    if field in data:
                        purpose = data[field]
                        print(f"   ✓ Found purpose field '{field}': {str(purpose)[:100]}...")
                        found_purpose = True
                        break
                
                if not found_purpose:
                    # Try to find purpose in nested structure
                    def find_purpose(obj, depth=0):
                        if depth > 3:
                            return None
                        if isinstance(obj, dict):
                            for key, value in obj.items():
                                if any(pf in key.lower() for pf in ['purpose', 'zweck', 'but']):
                                    return value
                                result = find_purpose(value, depth+1)
                                if result:
                                    return result
                        elif isinstance(obj, list):
                            for item in obj:
                                result = find_purpose(item, depth+1)
                                if result:
                                    return result
                        return None
                    
                    purpose = find_purpose(data)
                    if purpose:
                        print(f"   ✓ Found purpose in nested structure: {str(purpose)[:100]}...")
                    else:
                        print(f"   ⚠ No purpose field found in response")
                
                # Show sample of data structure
                print(f"\n   Sample data structure:")
                print(json.dumps(data, indent=2, ensure_ascii=False)[:500] + "...")
                
                return True
                
            except json.JSONDecodeError:
                print(f"   ✗ Response is not JSON")
                print(f"   Response text: {response.text[:200]}")
                return False
        else:
            print(f"   ✗ Failed with status {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return False
            
    except requests.exceptions.SSLError as e:
        if verify_ssl:
            print(f"   ✗ SSL Error (with verification): {str(e)[:100]}")
            print(f"   → Retrying without SSL verification...")
            return test_url(url, verify_ssl=False)
        else:
            print(f"   ✗ SSL Error (without verification): {str(e)[:100]}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"   ✗ Request Error: {str(e)[:100]}")
        return False
    except Exception as e:
        print(f"   ✗ Unexpected Error: {str(e)[:100]}")
        return False

print("=" * 60)
print(f"Testing Zefix API endpoint with EHRAID: {TEST_EHRAID}")
print("=" * 60)

success = False
for i, url in enumerate(ENDPOINTS, 1):
    print(f"\n{i}. Testing: {url}")
    if test_url(url):
        print(f"\n   ✓ THIS ENDPOINT WORKS! Use this format.")
        success = True
        break

if not success:
    print("\n   ✗ None of the tested endpoints worked.")
    print("   This might be due to:")
    print("   - Sandbox restrictions blocking SSL connections")
    print("   - Incorrect endpoint format")
    print("   - API requiring authentication")
    print("   - Network/firewall issues")

print("\n" + "=" * 60)
