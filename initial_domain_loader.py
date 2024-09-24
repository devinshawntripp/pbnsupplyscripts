import re
import psycopg2
import os
from datetime import datetime
import whois
import sys
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import random
import tempfile
import logging
import time

# Configure logging
logging.basicConfig(
    filename='domain_loader.log',
    filemode='a',
    format='%(asctime)s %(levelname)s:%(message)s',
    level=logging.INFO
)

# Database connection
def get_db_connection():
    database_url = os.getenv('DATABASE_URL', '')
    if not database_url:
        logging.error("DATABASE_URL environment variable is not set.")
        sys.exit(1)
    url = urlparse(database_url)
    try:
        conn = psycopg2.connect(
            host=url.hostname,
            database=url.path[1:],
            user=url.username,
            password=url.password,
            port=url.port
        )
        return conn
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        sys.exit(1)

# Regular expression pattern to parse each line of the zone file
zone_line_pattern = re.compile(
    r'^(?P<name>\S+)\s+'
    r'(?P<ttl>\d+)\s+'
    r'(?P<class>\S+)\s+'
    r'(?P<type>\S+)\s+'
    r'(?P<data>.+)$'
)

def read_zone_file(file_path, debug=False):
    logging.info(f"Reading zone file: {file_path}")
    unique_domains = set()
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
        if debug:
            lines = random.sample(lines, min(20, len(lines)))
        for line in lines:
            line = line.strip()
            if not line or line.startswith(';'):
                continue
            match = zone_line_pattern.match(line)
            if match:
                record = match.groupdict()
                domain_name = record['name'].rstrip('.')
                unique_domains.add(domain_name)
        logging.info(f"Total unique domains extracted: {len(unique_domains)}")
    except Exception as e:
        logging.error(f"Error reading zone file: {e}")
        sys.exit(1)
    return unique_domains

# Semaphore for rate limiting
MAX_WORKERS = 2  # Adjust based on acceptable load and WHOIS server policies
semaphore = threading.Semaphore(MAX_WORKERS)

def get_expiry_date(domain):
    with semaphore:
        try:
            logging.debug(f"Fetching WHOIS for {domain}")
            w = whois.whois(domain)
            expiry_date = w.expiration_date
            if isinstance(expiry_date, list):
                expiry_date = expiry_date[0]
            return domain, expiry_date
        except Exception as e:
            logging.warning(f"Error fetching WHOIS for {domain}: {e}")
            return domain, None

def perform_whois_lookups(domains):
    domain_data_list = []
    total_domains = len(domains)
    logging.info(f"Starting WHOIS lookups for {total_domains} domains")
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_domain = {executor.submit(get_expiry_date, domain): domain for domain in domains}
        for i, future in enumerate(as_completed(future_to_domain), 1):
            domain, expiry_date = future.result()
            is_expired = expiry_date < datetime.now() if expiry_date else False
            domain_data_list.append((domain, expiry_date, is_expired))
            if i % 1000 == 0:
                elapsed = time.time() - start_time
                logging.info(f"Processed {i}/{total_domains} domains in {elapsed:.2f} seconds")
    elapsed = time.time() - start_time
    logging.info(f"Completed WHOIS lookups in {elapsed:.2f} seconds")
    return domain_data_list

def insert_domains_batch(conn, domain_data_list):
    cur = conn.cursor()
    total_records = len(domain_data_list)
    logging.info(f"Inserting {total_records} records into the database")
    start_time = time.time()
    query = """
        INSERT INTO domains (name, expiry_date, is_expired) 
        VALUES %s
        ON CONFLICT (name) DO UPDATE 
        SET expiry_date = EXCLUDED.expiry_date,
            is_expired = EXCLUDED.is_expired
    """
    try:
        from psycopg2.extras import execute_values
        execute_values(cur, query, domain_data_list)
        conn.commit()
        elapsed = time.time() - start_time
        logging.info(f"Inserted {total_records} records in {elapsed:.2f} seconds")
    except Exception as e:
        logging.error(f"Error inserting records into the database: {e}")
        conn.rollback()
    finally:
        cur.close()

def main(file_path):
    unique_domains = read_zone_file(file_path, debug=False)
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        logging.info("Creating temporary table")
        cur.execute("""
            CREATE TEMP TABLE temp_domains (
                name VARCHAR(255)
            ) ON COMMIT DROP
        """)

        logging.info("Writing domains to temporary file")
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp_file:
            for domain in unique_domains:
                tmp_file.write(f"{domain}\n")
            tmp_file_name = tmp_file.name

        logging.info("Inserting domains into temporary table using COPY")
        with open(tmp_file_name, 'r') as f:
            cur.copy_from(f, 'temp_domains', columns=('name',))

        # Clean up temporary file
        os.unlink(tmp_file_name)

        logging.info("Finding domains not already in the database")
        cur.execute("""
            SELECT temp_domains.name
            FROM temp_domains
            LEFT JOIN domains ON temp_domains.name = domains.name
            WHERE domains.name IS NULL
        """)
        domains_to_process = [row[0] for row in cur.fetchall()]
        logging.info(f"Total new domains to process: {len(domains_to_process)}")

        if domains_to_process:
            # Perform WHOIS lookups in parallel
            domain_data_list = perform_whois_lookups(domains_to_process)

            # Insert data into the database in batches
            insert_domains_batch(conn, domain_data_list)
        else:
            logging.info("No new domains to process")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")
    logging.info("Initial domain loading complete")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error("Usage: python initial_domain_loader.py <file_path>")
        sys.exit(1)
    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        logging.error(f"Error: File '{file_path}' does not exist.")
        sys.exit(1)
    main(file_path)
