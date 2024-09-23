import re
import whois
from datetime import datetime, timedelta
import requests
import time
from bs4 import BeautifulSoup
from langdetect import detect, DetectorFactory
import random
import psycopg2
from psycopg2 import sql
import os

# Ensure consistent language detection
DetectorFactory.seed = 0

# Define the path to your zone file
zone_file_path = '/app/tld_files/org.txt'

# Regular expression pattern to parse each line of the zone file
zone_line_pattern = re.compile(
    r'^(?P<name>\S+)\s+'
    r'(?P<ttl>\d+)\s+'
    r'(?P<class>\S+)\s+'
    r'(?P<type>\S+)\s+'
    r'(?P<data>.+)$'
)

# Database connection
def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        database=os.environ.get('DB_NAME', 'your_db_name'),
        user=os.environ.get('DB_USER', 'your_db_user'),
        password=os.environ.get('DB_PASSWORD', 'your_db_password')
    )

# Check if domain exists in database
def domain_in_database(domain):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT expiry_date FROM domains WHERE name = %s", (domain,))
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result

# Update or insert domain info
def update_domain_info(domain, expiry_date):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO domains (name, expiry_date) 
        VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE 
        SET expiry_date = EXCLUDED.expiry_date
    """, (domain, expiry_date))
    conn.commit()
    cur.close()
    conn.close()

def read_zone_file(file_path, debug=False, debug_lines=20):
    unique_domains = set()
    
    with open(file_path, 'r') as file:
        if debug:
            lines = random.sample(file.readlines(), debug_lines)
        else:
            lines = file

        for line in lines:
            line = line.strip()
            if not line or line.startswith(';'):
                continue

            match = zone_line_pattern.match(line)
            if match:
                record = match.groupdict()
                print(f"Adding domain {record['name']} to the set")
                domain_name = record['name'].rstrip('.')
                unique_domains.add(domain_name)
            else:
                print(f"Line skipped (unmatched format): {line}")

    return unique_domains

# Function to perform WHOIS lookup and get expiry date
def get_expiry_date(domain):
    try:
        w = whois.whois(domain)
        expiry_date = w.expiration_date
        if isinstance(expiry_date, list):
            expiry_date = expiry_date[0]
        return expiry_date
    except Exception as e:
        print(f"Error fetching WHOIS for {domain}: {e}")
        return None

# Function to check if the site is in English
def is_english(domain):
    url = f"http://{domain}"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.get_text(strip=True)
            language = detect(text)
            return language == 'en'
        else:
            return False
    except Exception as e:
        print(f"Error fetching content for {domain}: {e}")
        return False

# Function to get snapshots from the Wayback Machine
def get_wayback_snapshots(domain, limit=3):
    url = 'http://web.archive.org/cdx/search/cdx'
    params = {
        'url': domain,
        'output': 'json',
        'fl': 'timestamp,original',
        'collapse': 'digest',
        'limit': limit
    }
    try:
        response = requests.get(url, params=params)
        data = response.json()
        snapshots = []
        for entry in data[1:]:  # Skip header
            timestamp, original_url = entry
            snapshot_url = f"http://web.archive.org/web/{timestamp}/{original_url}"
            snapshots.append(snapshot_url)
        return snapshots
    except Exception as e:
        print(f"Error fetching snapshots for {domain}: {e}")
        return []

# Main execution
if __name__ == "__main__":
    # Full version
    unique_domains = read_zone_file(zone_file_path, debug=True)

    # Lists to store domains
    expired_domains = []
    ten_days_before_expiry_date_domains = []

    # Current date for comparison
    current_date = datetime.now()

    # Iterate over unique domains and check expiry dates
    for domain in unique_domains:
        db_result = domain_in_database(domain)
        if db_result:
            expiry_date = db_result[0]
            if expiry_date > current_date + timedelta(days=30):  # If expiry is more than 30 days away
                continue
        
        print(f"Getting whois data for {domain}")
        time.sleep(2)
        expiry_date = get_expiry_date(domain)
        
        if expiry_date is None:
            print(f"Could not retrieve expiry date for {domain}")
            continue
        
        update_domain_info(domain, expiry_date)
        
        close_expiry_date = expiry_date - timedelta(days=10)
        print(f"Close expiry date: {close_expiry_date} Current date: {current_date} Domain: {domain}")
        if close_expiry_date < current_date:
            print(f"Domain {domain} is close to expiry and will expire on {expiry_date}")
            ten_days_before_expiry_date_domains.append(domain)
            continue
        if expiry_date < current_date:
            print(f"\n{domain} has expired on {expiry_date}")
            expired_domains.append(domain)
        else:
            print(f"{domain} is still active (expires on {expiry_date})")

    # Process expired domains
    for domain in expired_domains:
        print(f"\nProcessing {domain}:")

        # Check if the site is in English
        english = is_english(domain)
        print(f"Is English: {english}")

        # Get snapshots from the Wayback Machine
        snapshots = get_wayback_snapshots(domain)
        print(f"Wayback Machine Snapshots:")
        for snapshot in snapshots:
            print(snapshot)

        # TODO: Implement business category and SEO metrics functionality
        # categories = get_business_category(domain)
        # print(f"Business Categories: {categories}")
        # seo_metrics = get_seo_metrics(domain)
        # print(f"SEO Metrics: {seo_metrics}")
