import re
import psycopg2
import os
from datetime import datetime
import whois
import time
import sys

# Database connection
def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        database=os.environ.get('DB_NAME', 'pbnsupply'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', 'postgres')
    )

# Regular expression pattern to parse each line of the zone file
zone_line_pattern = re.compile(
    r'^(?P<name>\S+)\s+'
    r'(?P<ttl>\d+)\s+'
    r'(?P<class>\S+)\s+'
    r'(?P<type>\S+)\s+'
    r'(?P<data>.+)$'
)

def read_zone_file(file_path):
    unique_domains = set()
    
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith(';'):
                continue

            match = zone_line_pattern.match(line)
            if match:
                record = match.groupdict()
                domain_name = record['name'].rstrip('.')
                print(f"Adding {domain_name} to the set")
                unique_domains.add(domain_name)
            else:
                print(f"Line skipped (unmatched format): {line}")

    return unique_domains

def get_expiry_date(domain):
    try:
        print(f"Fetching WHOIS for {domain}")
        time.sleep(1)
        w = whois.whois(domain)
        expiry_date = w.expiration_date
        if isinstance(expiry_date, list):
            expiry_date = expiry_date[0]
        return expiry_date
    except Exception as e:
        print(f"Error fetching WHOIS for {domain}: {e}")
        return None

def insert_domain(conn, domain, expiry_date):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO domains (name, expiry_date, is_expired) 
        VALUES (%s, %s, %s)
        ON CONFLICT (name) DO UPDATE 
        SET expiry_date = EXCLUDED.expiry_date,
            is_expired = EXCLUDED.is_expired
    """, (domain, expiry_date, expiry_date < datetime.now() if expiry_date else False))
    conn.commit()
    cur.close()

def main(file_path):
    unique_domains = read_zone_file(file_path)
    
    conn = get_db_connection()
    
    for domain in unique_domains:
        print(f"Processing {domain}")
        expiry_date = get_expiry_date(domain)
        insert_domain(conn, domain, expiry_date)
    
    conn.close()
    print("Initial domain loading complete.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python initial_domain_loader.py <file_path>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' does not exist.")
        sys.exit(1)
    
    main(file_path)