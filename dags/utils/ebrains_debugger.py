#!/usr/bin/env python3
"""
Fetch datasets from EBRAINS Knowledge Graph.

EBRAINS uses the 'fairgraph' Python library or direct API calls.
This script tries both approaches.

Installation:
    pip install fairgraph

Usage:
    python3 fetch_ebrains.py
"""

import requests
import json
import sys

# EBRAINS Knowledge Graph API endpoint
EBRAINS_API_BASE = "https://core.kg.ebrains.eu/v3-beta"
EBRAINS_SEARCH_URL = f"{EBRAINS_API_BASE}/queries/minds/core/dataset/v1.0.0/search/instances"


def print_section(title):
    """Print a section header."""
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80 + "\n")


def fetch_with_fairgraph():
    """
    Attempt to fetch datasets using the fairgraph library.
    This requires authentication.
    """
    print_section("METHOD 1: Using fairgraph Library (Requires Auth)")
    
    try:
        from fairgraph import KGClient
        from fairgraph.openminds.core import DatasetVersion
        
        print("⚠️  Note: fairgraph requires authentication with EBRAINS account")
        print("This will likely fail without a valid token.\n")
        
        # Try to create client (will fail without token)
        try:
            client = KGClient()
            
            # Query for datasets
            print("Querying for datasets...")
            datasets = DatasetVersion.list(client, size=10)
            
            print(f"\n✅ Found {len(datasets)} datasets:\n")
            
            for i, dataset in enumerate(datasets, 1):
                print(f"{i}. {dataset.name if hasattr(dataset, 'name') else 'Unnamed'}")
                if hasattr(dataset, 'id'):
                    print(f"   ID: {dataset.id}")
                print()
            
            return datasets
            
        except Exception as e:
            print(f"❌ Authentication failed (expected): {e}")
            print("\nTo use fairgraph, you need:")
            print("  1. An EBRAINS account")
            print("  2. Set KG_AUTH_TOKEN environment variable")
            return None
            
    except ImportError:
        print("❌ fairgraph not installed.")
        print("Install with: pip install fairgraph")
        return None


def fetch_with_direct_api():
    """
    Try to fetch datasets using direct API calls (may not require auth for public data).
    """
    print_section("METHOD 2: Direct API Call (Public Search)")
    
    # Try the public search endpoint
    search_url = "https://search.kg.ebrains.eu/api/search"
    
    try:
        print(f"Attempting to query: {search_url}")
        print("Searching for 'dataset' type resources...\n")
        
        # Try a basic search query
        params = {
            'q': '*',
            'category': 'Dataset',
            'size': 10,
            'from': 0
        }
        
        response = requests.get(
            search_url,
            params=params,
            timeout=30
        )
        
        print(f"HTTP Status: {response.status_code}")
        print(f"Content-Type: {response.headers.get('Content-Type', 'Unknown')}")
        print(f"Response Length: {len(response.text)} bytes")
        
        if response.status_code == 200:
            # Check what type of response we got
            print("\n📄 Raw Response (first 500 chars):")
            print(response.text[:500])
            print("...\n")
            
            # Try to parse as JSON
            try:
                data = response.json()
                print("✅ Response is valid JSON")
                print("\n📄 Full API Response:")
                print(json.dumps(data, indent=2))
            except json.JSONDecodeError as e:
                print(f"❌ Response is not JSON: {e}")
                print("\nFull response text:")
                print(response.text)
                return None
            
            # Try to extract datasets
            results = data.get('results', [])
            total = data.get('total', 0)
            
            print(f"\n✅ Found {total} total results, showing {len(results)}:\n")
            
            for i, result in enumerate(results, 1):
                print(f"{i}. {result.get('title', 'Untitled')}")
                print(f"   Type: {result.get('type', 'Unknown')}")
                print(f"   ID: {result.get('id', 'N/A')}")
                if result.get('description'):
                    desc = result['description'][:100]
                    print(f"   Description: {desc}...")
                print()
            
            return results
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            print(response.text[:500])
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return None


def fetch_with_kg_query_api():
    """
    Try using the EBRAINS KG Query API.
    """
    print_section("METHOD 3: KG Query API")
    
    # EBRAINS Query API endpoint
    query_url = "https://kg.ebrains.eu/query/minds/core/dataset/v1.0.0/search/instances"
    
    try:
        print(f"Querying: {query_url}\n")
        
        # Parameters for the query
        params = {
            'databaseScope': 'RELEASED',
            'start': 0,
            'size': 10
        }
        
        response = requests.get(
            query_url,
            params=params,
            timeout=30
        )
        
        print(f"HTTP Status: {response.status_code}")
        print(f"Content-Type: {response.headers.get('Content-Type', 'Unknown')}")
        print(f"Response Length: {len(response.text)} bytes")
        
        if response.status_code == 200:
            # Check what type of response we got
            print("\n📄 Raw Response (first 1000 chars):")
            print(response.text[:1000])
            print("...\n")
            
            # Try to parse as JSON
            try:
                data = response.json()
                print("✅ Response is valid JSON")
                print("\n📄 Full API Response:")
                print(json.dumps(data, indent=2))
            except json.JSONDecodeError as e:
                print(f"❌ Response is not JSON: {e}")
                print("\nThis might be HTML or requires authentication.")
                return None
            
            # Extract results
            results = data.get('results', [])
            total = data.get('total', 0)
            
            print(f"\n✅ Found {total} total datasets, showing {len(results)}:\n")
            
            for i, dataset in enumerate(results, 1):
                print(f"{i}. {dataset.get('title', dataset.get('name', 'Untitled'))}")
                if dataset.get('identifier'):
                    print(f"   ID: {dataset['identifier']}")
                if dataset.get('description'):
                    desc = dataset['description'][:100]
                    print(f"   Description: {desc}...")
                print()
            
            return results
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            print(response.text[:500])
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return None


def try_ebrains_kg_core_api():
    """
    Try the EBRAINS KG Core API v3.
    """
    print_section("METHOD 5: KG Core API v3 (Latest)")
    
    # Try the newer v3 API
    core_url = "https://core.kg.ebrains.eu/v3-beta/queries"
    
    try:
        print(f"Discovering available queries at: {core_url}\n")
        
        response = requests.get(
            core_url,
            timeout=30
        )
        
        print(f"HTTP Status: {response.status_code}")
        print(f"Content-Type: {response.headers.get('Content-Type', 'Unknown')}\n")
        
        if response.status_code == 200:
            try:
                data = response.json()
                print("✅ Response is valid JSON")
                print("\n📄 Available Queries:")
                print(json.dumps(data, indent=2)[:1000] + "...")
                return data
            except json.JSONDecodeError:
                print(f"❌ Response is not JSON")
                print(response.text[:500])
                return None
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            print(response.text[:500])
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def main():
    """Run all methods to try to fetch EBRAINS datasets."""
    print("\n" + "=" * 80)
    print("EBRAINS Dataset Fetcher")
    print("=" * 80)
    print("\nThis script tries multiple methods to fetch datasets from EBRAINS.\n")
    
    results = {}
    
    # Method 1: fairgraph (likely requires auth)
    results['fairgraph'] = fetch_with_fairgraph()
    
    input("\nPress Enter to try Method 2...")
    
    # Method 2: Direct API
    results['direct_api'] = fetch_with_direct_api()
    
    input("\nPress Enter to try Method 3...")
    
    # Method 3: KG Query API
    results['kg_query'] = fetch_with_kg_query_api()
    
    input("\nPress Enter to try Method 4...")
    
    # Method 4: KG Core API v3
    results['kg_core_v3'] = try_ebrains_kg_core_api()
    
    # Summary
    print_section("SUMMARY")
    
    for method, result in results.items():
        status = "✅ SUCCESS" if result else "❌ FAILED"
        print(f"{status} - {method}")
    
    successful = [k for k, v in results.items() if v]
    
    if successful:
        print(f"\n✅ Working methods: {', '.join(successful)}")
        print("\nUse the successful method(s) for your DAG implementation.")
    else:
        print("\n⚠️  All methods failed.")
        print("\nEBRAINS likely requires:")
        print("  1. Authentication token from an EBRAINS account")
        print("  2. Specific API access permissions")
        print("\nVisit: https://ebrains.eu/ to create an account")
    
    print("\n" + "=" * 80 + "\n")
    
    return 0 if successful else 1


if __name__ == "__main__":
    sys.exit(main())