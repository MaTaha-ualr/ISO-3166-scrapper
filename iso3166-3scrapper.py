"""
ISO 3166-3 Web Scraper - Professional Clean Output Only
==========================================
Scrapes ISO 3166-3 data from Wikipedia and produces a single clean JSON file.

Requirements:
- pandas
- requests
- beautifulsoup4

Install: pip install pandas requests beautifulsoup4

Usage:
    python scrape_iso_3166_3.py
    
Output:
    iso_3166_3_cleaned.json - Clean, standardized data
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
from typing import Dict, List, Optional


class ISO3166_3Scraper:
    """Scraper that produces clean, standardized JSON output directly from Wikipedia."""
    
    def __init__(self):
        self.url = "https://en.wikipedia.org/wiki/ISO_3166-3"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
    
    def fetch_table(self) -> pd.DataFrame:
        """Fetch and extract the ISO 3166-3 table from Wikipedia."""
        print("Fetching data from Wikipedia...")
        
        try:
            response = requests.get(self.url, headers=self.headers, timeout=15)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching Wikipedia page: {e}")
            raise
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'class': 'sortable'})
        
        if not table:
            # Fallback: use pandas read_html
            tables = pd.read_html(response.content)
            df = max(tables, key=lambda x: x.shape[0])
        else:
            df = pd.read_html(str(table))[0]
        
        print(f"✓ Fetched {len(df)} records")
        return df
    
    def clean_country_name(self, name: str) -> str:
        """Remove Wikipedia notes and action prefixes from country names."""
        if not name or pd.isna(name):
            return ""
        
        name = str(name)
        
        # Remove Wikipedia note references like [note 1]
        name = re.sub(r'\[note \d+\]', '', name)
        
        # Remove action prefixes
        prefixes = [
            'Merged into ',
            'Name changed to ',
            'Divided into:',
            'Divided into: ',
            'Split into '
        ]
        
        for prefix in prefixes:
            if name.startswith(prefix):
                name = name[len(prefix):]
        
        # Clean whitespace
        return ' '.join(name.split()).strip()
    
    def parse_former_codes(self, codes_str: str) -> Dict[str, Optional[str]]:
        """Parse former codes string into structured format."""
        result = {"alpha2": None, "alpha3": None, "numeric": None}
        
        if pd.isna(codes_str) or not codes_str:
            return result
        
        codes_str = str(codes_str).strip()
        parts = [p.strip() for p in codes_str.split(',')]
        
        for part in parts:
            if re.match(r'^[A-Z]{2}$', part):
                result['alpha2'] = part
            elif re.match(r'^[A-Z]{3}$', part):
                result['alpha3'] = part
            elif re.match(r'^\d{3}$', part):
                result['numeric'] = part
        
        return result
    
    def parse_validity_period(self, period_str: str) -> Dict[str, Optional[int]]:
        """Parse validity period into start and end years as integers."""
        result = {"start": None, "end": None}
        
        if pd.isna(period_str) or not period_str:
            return result
        
        period_str = str(period_str).strip()
        match = re.search(r'(\d{4})[–\-](\d{4})', period_str)
        
        if match:
            result['start'] = int(match.group(1))
            result['end'] = int(match.group(2))
        
        return result
    
    def parse_successors(self, successor_str: str, raw_desc: str) -> List[Dict]:
        """Parse successor countries from the description."""
        successors = []
        
        if pd.isna(successor_str) or not successor_str:
            return successors
        
        successor_str = str(successor_str)
        
        # More robust pattern that handles nested parentheses like "Sint Maarten (Dutch part)"
        # Strategy: find all code patterns (XX, XXX, NNN) and work backwards to find the country name
        
        # First, find all code patterns
        code_pattern = r'\(([A-Z]{2}),\s*([A-Z]{2,3}),\s*(\d{3,4})\)'
        code_matches = list(re.finditer(code_pattern, successor_str))
        
        if not code_matches:
            return successors
        
        for i, code_match in enumerate(code_matches):
            # Get the end position of the previous match (or start of string)
            start_pos = code_matches[i-1].end() if i > 0 else 0
            end_pos = code_match.start()
            
            # Extract the country name between start_pos and the current code
            country_name_raw = successor_str[start_pos:end_pos].strip()
            
            # Remove any leading separators or keywords
            country_name_raw = re.sub(r'^(?:Divided into:|Split into:)\s*', '', country_name_raw, flags=re.IGNORECASE)
            
            # Remove any [note X] references
            country_name_raw = re.sub(r'\[note \d+\]', '', country_name_raw)
            
            # Clean up the country name
            country_name = self.clean_country_name(country_name_raw.strip())
            
            # Skip if the "name" is actually just separator text or too short
            if not country_name or len(country_name) < 3:
                continue
            
            # Fix incomplete country names
            country_name = self.fix_incomplete_name(country_name, raw_desc)
            
            successors.append({
                "name": country_name,
                "alpha2": code_match.group(1),
                "alpha3": code_match.group(2),
                "numeric": code_match.group(3)
            })
        
        return successors
    
    def fix_incomplete_name(self, name: str, raw_description: str) -> str:
        """Fix incomplete country names by checking raw description."""
        if not name or len(name) > 25:
            return name
        
        # Common incomplete patterns
        patterns = {
            "Republic of": r"([\w\s]+,?\s*Republic of)",
            "Democratic Republic of the": r"([\w\s]+,?\s*Democratic Republic of the)",
        }
        
        for incomplete, pattern in patterns.items():
            if name == incomplete or name.endswith(incomplete):
                match = re.search(pattern, raw_description)
                if match:
                    full_name = match.group(1)
                    # Remove any trailing codes in parentheses
                    full_name = re.sub(r'\s*\([A-Z]{2}.*?\)', '', full_name)
                    return full_name.strip()
        
        return name
    
    def determine_transition_type(self, successor_str: str) -> str:
        """Determine the type of transition based on the description."""
        if pd.isna(successor_str) or not successor_str:
            return "other"
        
        desc_lower = str(successor_str).lower()
        
        if "merged into" in desc_lower:
            return "merged"
        elif "name changed" in desc_lower:
            return "name_changed"
        elif "divided into" in desc_lower or "split into" in desc_lower:
            return "divided"
        else:
            return "other"
    
    def process_record(self, row: pd.Series) -> Dict:
        """Process a single record into clean format."""
        # Parse former codes
        former_codes = self.parse_former_codes(row.get('Former codes', ''))
        
        # Parse validity period
        validity = self.parse_validity_period(row.get('Period of validity', ''))
        
        # Get raw description
        raw_desc = str(row.get('New country names and codes', ''))
        
        # Parse successors
        successors = self.parse_successors(raw_desc, raw_desc)
        
        # Determine transition type
        transition_type = self.determine_transition_type(raw_desc)
        
        # Clean former country name
        former_name = self.clean_country_name(row.get('Former country name', ''))
        
        # Get ISO 3166-3 code
        iso_3166_3_code = str(row.get('ISO 3166-3 code', '')).strip()
        if iso_3166_3_code == 'nan':
            iso_3166_3_code = None
        
        # Build the clean record
        record = {
            "former_country": {
                "name": former_name,
                "alpha2": former_codes['alpha2'],
                "alpha3": former_codes['alpha3'],
                "numeric": former_codes['numeric'],
                "iso_3166_3_alpha4": iso_3166_3_code
            },
            "validity_period": validity,
            "transition": {
                "type": transition_type,
                "successors": successors
            }
        }
        
        return record
    
    def scrape_and_clean(self) -> Dict:
        """Main method: scrape Wikipedia and return clean data."""
        # Fetch the table
        df = self.fetch_table()
        
        # Process all records
        print("Processing and cleaning data...")
        clean_records = []
        errors = []
        
        for idx, row in df.iterrows():
            try:
                record = self.process_record(row)
                clean_records.append(record)
            except Exception as e:
                errors.append(f"Row {idx}: {str(e)}")
        
        if errors:
            print(f"⚠ Encountered {len(errors)} errors during processing:")
            for error in errors[:5]:  # Show first 5 errors
                print(f"  - {error}")
        
        # Build the final dataset
        dataset = {
            "metadata": {
                "title": "ISO 3166-3: Formerly Used Country Codes",
                "description": "Codes for country names which have been deleted from ISO 3166-1 since its first publication in 1974",
                "source": self.url,
                "standard": "ISO 3166-3",
                "version": datetime.now().strftime("%Y-%m"),
                "total_records": len(clean_records),
                "last_updated": datetime.now().strftime("%Y-%m-%d")
            },
            "countries": clean_records
        }
        
        print(f"✓ Successfully processed {len(clean_records)} countries")
        return dataset
    
    def save_json(self, output_file: str = 'iso_3166_3_cleaned.json'):
        """Scrape, clean, and save to JSON file."""
        print("="*70)
        print("ISO 3166-3 Web Scraper - Clean Output Generator")
        print("="*70)
        print()
        
        try:
            data = self.scrape_and_clean()
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            print(f"\n✓ Clean data saved to: {output_file}")
            
            # Print summary statistics
            print("\n" + "="*70)
            print("SUMMARY")
            print("="*70)
            print(f"Total countries: {data['metadata']['total_records']}")
            
            # Count transition types
            transition_counts = {}
            for country in data['countries']:
                t_type = country['transition']['type']
                transition_counts[t_type] = transition_counts.get(t_type, 0) + 1
            
            print("\nTransition types:")
            for t_type, count in sorted(transition_counts.items()):
                print(f"  {t_type}: {count}")
            
            # Count by decade
            decade_counts = {}
            for country in data['countries']:
                start = country['validity_period']['start']
                if start:
                    decade = f"{str(start)[:3]}0s"
                    decade_counts[decade] = decade_counts.get(decade, 0) + 1
            
            print("\nBy decade (start year):")
            for decade, count in sorted(decade_counts.items()):
                print(f"  {decade}: {count}")
            
            print("\n" + "="*70)
            print("COMPLETE!")
            print("="*70)
            
            return data
            
        except Exception as e:
            print(f"\n✗ Error: {e}")
            raise


def main():
    """Main execution function."""
    scraper = ISO3166_3Scraper()
    scraper.save_json('iso_3166_3_cleaned.json')


if __name__ == "__main__":
    main()