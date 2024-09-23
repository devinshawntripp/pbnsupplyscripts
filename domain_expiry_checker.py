import psycopg2
import os
from datetime import datetime, timedelta
import whois

# Database connection
def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        database=os.environ.get('DB_NAME', 'pbnsupply'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', 'postgres')
    )

def get_domains_to_check(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT name, expiry_date
        FROM domains
        WHERE expiry_date IS NULL OR expiry_date <= %s
    """, (datetime.now() + timedelta(days=30),))
    domains = cur.fetchall()
    cur.close()
    return domains

def update_domain_status(conn, domain, expiry_date):
    cur = conn.cursor()
    cur.execute("""
        UPDATE domains
        SET expiry_date = %s, is_expired = %s
        WHERE name = %s
    """, (expiry_date, expiry_date < datetime.now() if expiry_date else False, domain))
    conn.commit()
    cur.close()

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

if __name__ == "__main__":
    conn = get_db_connection()
    domains_to_check = get_domains_to_check(conn)
    
    for domain, current_expiry in domains_to_check:
        print(f"Checking {domain}")
        new_expiry_date = get_expiry_date(domain)
        if new_expiry_date:
            update_domain_status(conn, domain, new_expiry_date)
            print(f"Updated {domain}: New expiry date: {new_expiry_date}")
        else:
            print(f"Could not update {domain}")
    
    conn.close()
    print("Domain expiry checking complete.")