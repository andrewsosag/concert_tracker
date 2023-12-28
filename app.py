"""import os
import mysql.connector
import json
import requests
import datetime
import boto3
import logging

def lambda_handler(event, context):
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting script...")
    
    # Fetch values from environment variables
    SEATGEEK_CLIENT_ID = os.environ.get('SEATGEEK_CLIENT_ID')
    RDS_HOST = os.environ.get('RDS_HOST')
    RDS_USER = os.environ.get('RDS_USER')
    RDS_PASSWORD = os.environ.get('RDS_PASSWORD')
    RDS_DB_NAME = os.environ.get('RDS_DB_NAME')

    # Fetching and processing events
    logging.info("Fetching events...")
    events = fetch_events(SEATGEEK_CLIENT_ID)

    # Parsing and updating events
    logging.info("Parsing and updating events...")
    parsed_data = parse_event_data(events)

    process_events(parsed_data, RDS_HOST, RDS_USER, RDS_PASSWORD, RDS_DB_NAME)

    # Parsing and updating events
    logging.info("Parsing and updating events...")
    parsed_data = parse_event_data(events)
    for event in parsed_data:
        insert_or_update_event(event, RDS_HOST, RDS_USER, RDS_PASSWORD, RDS_DB_NAME)

    logging.info("Script completed successfully.")
    return {
        'statusCode': 200,
        'body': json.dumps('Data update successful!')
    }


def fetch_events(client_id):
    api_url = 'https://api.seatgeek.com/2/events'
    all_events = []
    page = 1
    while len(all_events) < 30:  # Limiting to 30 events for testing
        params = {
            'client_id': client_id,
            'taxonomies.name': 'concert',
            'per_page': 10,  # Reduced for testing
            'sort': 'score.desc',
            'page': page
        }
        response = requests.get(api_url, params=params)
        if response.status_code != 200:
            print(f"Failed to fetch events: {response.status_code}, Response: {response.text}")
            break
        events = response.json()['events']
        if not events:
            break
        all_events.extend(events)
        page += 1
    return all_events

def parse_event_data(events):
    parsed_data = []
    for event in events:
        event_id = str(event['id'])
        event_name = event['title']
        event_date = event['datetime_local']
        venue_name = event['venue']['name']
        city_name = event['venue']['city']
        lowest_price = event['stats'].get('lowest_price', 'N/A')
        highest_price = event['stats'].get('highest_price', 'N/A')

        parsed_data.append({
            'event_id': event_id,
            'event_name': event_name,
            'event_date': event_date,
            'venue_name': venue_name,
            'city_name': city_name,
            'lowest_price': lowest_price,
            'highest_price': highest_price
        })
    return parsed_data

def create_database(rds_host, rds_user, rds_password, rds_db_name):
    conn = mysql.connector.connect(user=rds_user, password=rds_password, 
                                   host=rds_host, 
                                   database=rds_db_name)
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            event_id VARCHAR(255) UNIQUE NOT NULL,
            event_name VARCHAR(255),
            event_date DATETIME,
            venue_name VARCHAR(255),
            city_name VARCHAR(255)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS event_prices (
            event_id VARCHAR(255) NOT NULL,
            date DATE NOT NULL,
            lowest_price FLOAT,
            highest_price FLOAT,
            PRIMARY KEY (event_id, date),
            FOREIGN KEY (event_id) REFERENCES events(event_id)
        )
    ''')
    conn.commit()
    conn.close()


def insert_event(event, cursor):
    # Insert new event to the events table
    # ...

def update_event_prices(event, cursor):
    # Insert or update the price information in event_prices table
    # ...

def insert_or_update_event(event, rds_host, rds_user, rds_password, rds_db_name):
    conn = mysql.connector.connect(user=rds_user, password=rds_password, 
                                   host=rds_host, 
                                   database=rds_db_name)
    cur = conn.cursor()

    cur.execute("SELECT * FROM events WHERE event_id = %s", (event['event_id'],))
    if not cur.fetchone():
        cur.execute('''
            INSERT INTO events (event_id, event_name, event_date, venue_name, city_name)
            VALUES (%s, %s, %s, %s, %s)
        ''', (event['event_id'], event['event_name'], event['event_date'], event['venue_name'], event['city_name']))

    cur.execute("SELECT * FROM event_prices WHERE event_id = %s AND date = CURRENT_DATE", (event['event_id'],))
    if cur.fetchone():
        cur.execute('''
            UPDATE event_prices
            SET lowest_price = %s, highest_price = %s
            WHERE event_id = %s AND date = CURRENT_DATE
        ''', (event['lowest_price'], event['highest_price'], event['event_id']))
    else:
        cur.execute('''
            INSERT INTO event_prices (event_id, date, lowest_price, highest_price)
            VALUES (%s, CURRENT_DATE, %s, %s)
        ''', (event['event_id'], event['lowest_price'], event['highest_price']))

    conn.commit()
    conn.close()

def process_events(events, rds_host, rds_user, rds_password, rds_db_name):
    conn = mysql.connector.connect(user=rds_user, password=rds_password, host=rds_host, database=rds_db_name)
    cur = conn.cursor()

    for event in events:
        insert_event(event, cur)
        update_event_prices(event, cur)

    conn.commit()
    conn.close()

def main():
    print("Starting script...")
    create_database()
    events = fetch_events()
    parsed_data = parse_event_data(events)
    for event in parsed_data:
        insert_or_update_event(event)

    print("Script completed successfully.")

if __name__ == '__main__':
    lambda_handler(None, None)

import os
import mysql.connector
import json
import logging

def lambda_handler(event, context):
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Lambda test script...")

    # Fetch values from Lambda environment variables
    RDS_HOST = os.environ.get('RDS_HOST')
    RDS_USER = os.environ.get('RDS_USER')
    RDS_PASSWORD = os.environ.get('RDS_PASSWORD')
    RDS_DB_NAME = os.environ.get('RDS_DB_NAME')

    # Test database connection
    try:
        conn = mysql.connector.connect(user=RDS_USER, password=RDS_PASSWORD, host=RDS_HOST, database=RDS_DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        data = cursor.fetchone()
        logging.info("Database version : %s " % data)
        conn.close()
        return {
            'statusCode': 200,
            'body': json.dumps('Lambda: Database connection successful!')
        }
    except Exception as e:
        logging.error("Error while connecting to MySQL: %s", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps('Lambda: Database connection failed!')
        }

# LAMBDA TEST
import os
import mysql.connector
import json
import requests
import logging
import datetime

def lambda_handler(event, context):
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting script...")
    
    SEATGEEK_CLIENT_ID = os.environ.get('SEATGEEK_CLIENT_ID')
    RDS_HOST = os.environ.get('RDS_HOST')
    RDS_USER = os.environ.get('RDS_USER')
    RDS_PASSWORD = os.environ.get('RDS_PASSWORD')
    RDS_DB_NAME = os.environ.get('RDS_DB_NAME')

    events = fetch_events(SEATGEEK_CLIENT_ID)
    parsed_data = parse_event_data(events)

    try:
        process_events(parsed_data, RDS_HOST, RDS_USER, RDS_PASSWORD, RDS_DB_NAME)
        logging.info("Script completed successfully.")
        return {'statusCode': 200, 'body': json.dumps('Data update successful!')}
    except Exception as e:
        logging.error("Error in processing events: %s", str(e))
        return {'statusCode': 500, 'body': json.dumps('Data update failed!')}

# Define fetch_events, parse_event_data as before

def process_events(events, rds_host, rds_user, rds_password, rds_db_name):
    conn = mysql.connector.connect(user=rds_user, password=rds_password, host=rds_host, database=rds_db_name)
    cur = conn.cursor()

    event_insert_queries = []
    event_price_update_queries = []

    for event in events:
        event_insert_queries.append((
            event['event_id'], event['event_name'], event['venue_name'], event['city_name'], 
            event['artist_name'], event['event_date'], event['local_time'], event['url']
        ))
        event_price_update_queries.append((
            event['event_id'], event['event_date'], event['lowest_price'], event['highest_price']
        ))

    batch_insert_events(event_insert_queries, cur)
    batch_update_event_prices(event_price_update_queries, cur)

    conn.commit()
    conn.close()

def batch_insert_events(event_data, cursor):
    insert_query = '''
        INSERT INTO events (event_id, event_name, venue_name, city_name, artist_name, date, local_time, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE event_name = VALUES(event_name), venue_name = VALUES(venue_name), city_name = VALUES(city_name), artist_name = VALUES(artist_name), date = VALUES(date), local_time = VALUES(local_time), url = VALUES(url)
    '''
    cursor.executemany(insert_query, event_data)

def batch_update_event_prices(price_data, cursor):
    update_query = '''
        INSERT INTO event_prices (event_id, date, lowest_price, highest_price)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE lowest_price = VALUES(lowest_price), highest_price = VALUES(highest_price)
    '''
    cursor.executemany(update_query, price_data)

if __name__ == '__main__':
    lambda_handler(None, None)

# Local Test
import os
import mysql.connector
import json
import requests
import logging
import datetime
import time

# Set environment variables for local testing
os.environ['SEATGEEK_CLIENT_ID'] = 'MTc0MjYwMzh8MTcwMDM3MDMwOS45OTA0MTM'
os.environ['RDS_HOST'] = 'mysql-ticket-database.cvdlpool9h4o.us-east-1.rds.amazonaws.com'
os.environ['RDS_USER'] = 'admin'
os.environ['RDS_PASSWORD'] = 'cfT8P??N535k'
os.environ['RDS_DB_NAME'] = 'ticket_data'

def lambda_handler(event, context):
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting script...")

    start_time = time.time()  # Record the start time

    SEATGEEK_CLIENT_ID = os.environ.get('SEATGEEK_CLIENT_ID')
    RDS_HOST = os.environ.get('RDS_HOST')
    RDS_USER = os.environ.get('RDS_USER')
    RDS_PASSWORD = os.environ.get('RDS_PASSWORD')
    RDS_DB_NAME = os.environ.get('RDS_DB_NAME')

    fetch_start = time.time()
    events = fetch_events(SEATGEEK_CLIENT_ID)
    fetch_end = time.time()
    logging.info("Time taken for fetching events: %s seconds", fetch_end - fetch_start)

    parse_start = time.time()
    parsed_data = parse_event_data(events)
    parse_end = time.time()
    logging.info("Time taken for parsing events: %s seconds", parse_end - parse_start)

    process_start = time.time()
    process_events(parsed_data, RDS_HOST, RDS_USER, RDS_PASSWORD, RDS_DB_NAME)
    process_end = time.time()
    logging.info("Time taken for processing events: %s seconds", process_end - process_start)

    total_time = time.time() - start_time
    logging.info("Total script execution time: %s seconds", total_time)

    logging.info("Script completed successfully.")
    return {
        'statusCode': 200,
        'body': json.dumps('Data update successful!')
    }

# Define other functions (fetch_events, parse_event_data, process_events, insert_event, update_event_prices) here...
def fetch_events(client_id):
    api_url = 'https://api.seatgeek.com/2/events'
    params = {
        'client_id': client_id,
        'taxonomies.name': 'concert',
        'per_page': 25,  # Reduced for testing
        'sort': 'score.desc',
    }
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return response.json()['events']
    else:
        logging.error("Failed to fetch events: %s, Response: %s", response.status_code, response.text)
        return []

def parse_event_data(events):
    parsed_data = []
    for event in events:
        parsed_data.append({
            'event_id': event['id'],
            'event_name': event['title'],
            'event_date': event['datetime_local'].split('T')[0],
            'local_time': event['datetime_local'].split('T')[1],
            'venue_name': event['venue']['name'],
            'city_name': event['venue']['city'],
            'artist_name': event['performers'][0]['name'] if event['performers'] else 'N/A',
            'lowest_price': event['stats'].get('lowest_price', 'N/A'),
            'highest_price': event['stats'].get('highest_price', 'N/A'),
            'url': event['url']
        })
    return parsed_data

def process_events(events, rds_host, rds_user, rds_password, rds_db_name):
    conn = mysql.connector.connect(user=rds_user, password=rds_password, host=rds_host, database=rds_db_name)
    cur = conn.cursor()

    for event in events:
        insert_event(event, cur)
        update_event_prices(event, cur)

    conn.commit()
    conn.close()

def insert_event(event, cursor):
    cursor.execute('''
        INSERT INTO events (event_id, event_name, venue_name, city_name, artist_name, date, local_time, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE event_name = VALUES(event_name), venue_name = VALUES(venue_name), city_name = VALUES(city_name), artist_name = VALUES(artist_name), date = VALUES(date), local_time = VALUES(local_time), url = VALUES(url)
    ''', (event['event_id'], event['event_name'], event['venue_name'], event['city_name'], event['artist_name'], event['event_date'], event['local_time'], event['url']))

def update_event_prices(event, cursor):
    cursor.execute('''
        INSERT INTO event_prices (event_id, date, lowest_price, highest_price)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE lowest_price = VALUES(lowest_price), highest_price = VALUES(highest_price)
    ''', (event['event_id'], event['event_date'], event['lowest_price'], event['highest_price']))


if __name__ == '__main__':
    lambda_handler(None, None)


# Lambda Test
import os
import mysql.connector
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import datetime

# Define the fetch_events function
def fetch_events(client_id):
    api_url = 'https://api.seatgeek.com/2/events'
    params = {
        'client_id': client_id,
        'taxonomies.name': 'concert',
        'per_page': 5,  # Reduced for testing
        'sort': 'score.desc',
    }

    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        response = session.get(api_url, params=params, timeout=(10, 30))
        if response.status_code == 200:
            return response.json()['events']
        else:
            logging.error("Failed to fetch events: %s, Response: %s", response.status_code, response.text)
            return []
    except requests.exceptions.ConnectTimeout:
        logging.error("Connection timed out while fetching events.")
        return []

# Define the parse_event_data function
def parse_event_data(events):
    parsed_data = []
    for event in events:
        parsed_data.append({
            'event_id': event['id'],
            'event_name': event['title'],
            'event_date': event['datetime_local'].split('T')[0],
            'local_time': event['datetime_local'].split('T')[1],
            'venue_name': event['venue']['name'],
            'city_name': event['venue']['city'],
            'artist_name': event['performers'][0]['name'] if event['performers'] else 'N/A',
            'lowest_price': event['stats'].get('lowest_price', 'N/A'),
            'highest_price': event['stats'].get('highest_price', 'N/A'),
            'url': event['url']
        })
    return parsed_data

# Define the process_events function
def process_events(events, rds_host, rds_user, rds_password, rds_db_name):
    conn = mysql.connector.connect(user=rds_user, password=rds_password, host=rds_host, database=rds_db_name)
    cur = conn.cursor()

    for event in events:
        insert_event(event, cur)
        update_event_prices(event, cur)

    conn.commit()
    conn.close()

# Define the insert_event function
def insert_event(event, cursor):
    cursor.execute('''
        INSERT INTO events (event_id, event_name, venue_name, city_name, artist_name, date, local_time, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE event_name = VALUES(event_name), venue_name = VALUES(venue_name), city_name = VALUES(city_name), artist_name = VALUES(artist_name), date = VALUES(date), local_time = VALUES(local_time), url = VALUES(url)
    ''', (event['event_id'], event['event_name'], event['venue_name'], event['city_name'], event['artist_name'], event['event_date'], event['local_time'], event['url']))

# Define the update_event_prices function
def update_event_prices(event, cursor):
    cursor.execute('''
        INSERT INTO event_prices (event_id, date, lowest_price, highest_price)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE lowest_price = VALUES(lowest_price), highest_price = VALUES(highest_price)
    ''', (event['event_id'], event['event_date'], event['lowest_price'], event['highest_price']))

# Lambda handler function
def lambda_handler(event, context):
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting script...")
    
    SEATGEEK_CLIENT_ID = os.environ.get('SEATGEEK_CLIENT_ID')
    RDS_HOST = os.environ.get('RDS_HOST')
    RDS_USER = os.environ.get('RDS_USER')
    RDS_PASSWORD = os.environ.get('RDS_PASSWORD')
    RDS_DB_NAME = os.environ.get('RDS_DB_NAME')

    events = fetch_events(SEATGEEK_CLIENT_ID)
    parsed_data = parse_event_data(events)

    try:
        process_events(parsed_data, RDS_HOST, RDS_USER, RDS_PASSWORD, RDS_DB_NAME)
        logging.info("Script completed successfully.")
        return {'statusCode': 200, 'body': json.dumps('Data update successful!')}
    except Exception as e:
        logging.error("Error in processing events: %s", str(e))
        return {'statusCode': 500, 'body': json.dumps('Data update failed!')}

# Main entry point for local testing
if __name__ == '__main__':
    lambda_handler(None, None)

# LOCAL TEST TICKETMASTER
import os
import mysql.connector
import json
import requests
import logging
import datetime
import time

# Set environment variables for local testing
os.environ['TICKETMASTER_API_KEY'] = 'v4kBXKT6lIgCf8t7vGoA5cF7S2Vqq6eZ'
os.environ['RDS_HOST'] = 'mysql-ticket-database.cvdlpool9h4o.us-east-1.rds.amazonaws.com'
os.environ['RDS_USER'] = 'admin'
os.environ['RDS_PASSWORD'] = 'cfT8P??N535k'
os.environ['RDS_DB_NAME'] = 'ticket_data'

# Define RDS variables outside of lambda_handler
RDS_HOST = os.environ.get('RDS_HOST')
RDS_USER = os.environ.get('RDS_USER')
RDS_PASSWORD = os.environ.get('RDS_PASSWORD')
RDS_DB_NAME = os.environ.get('RDS_DB_NAME')

def lambda_handler(event, context):
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting script...")

    start_time = time.time()  # Record the start time

    TICKETMASTER_API_KEY = os.environ.get('TICKETMASTER_API_KEY')

    fetch_start = time.time()
    events = fetch_events(TICKETMASTER_API_KEY)
    fetch_end = time.time()
    logging.info("Time taken for fetching events: %s seconds", fetch_end - fetch_start)

    parse_start = time.time()
    parsed_data = parse_event_data(events)
    parse_end = time.time()
    logging.info("Time taken for parsing events: %s seconds", parse_end - parse_start)

    process_start = time.time()
    process_events(parsed_data, RDS_HOST, RDS_USER, RDS_PASSWORD, RDS_DB_NAME)
    process_end = time.time()
    logging.info("Time taken for processing events: %s seconds", process_end - process_start)

    total_time = time.time() - start_time
    logging.info("Total script execution time: %s seconds", total_time)

    logging.info("Script completed successfully.")
    return {
        'statusCode': 200,
        'body': json.dumps('Data update successful!')
    }

def fetch_events(api_key):
    api_url = 'https://app.ticketmaster.com/discovery/v2/events.json'
    params = {
        'apikey': api_key,
        'countryCode': 'US',  # You can change this as per your requirement
        'size': 25  # Adjust the size as needed
    }
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return response.json().get('_embedded', {}).get('events', [])
    else:
        logging.error("Failed to fetch events: %s, Response: %s", response.status_code, response.text)
        return []

def parse_event_data(events):
    parsed_data = []
    for event in events:
        # Adjusting the parsing according to Ticketmaster's API response format
        parsed_data.append({
            'event_id': event['id'],
            'event_name': event['name'],
            'event_date': event['dates']['start']['localDate'],
            'local_time': event['dates']['start']['localTime'],
            'venue_name': event['_embedded']['venues'][0]['name'] if '_embedded' in event and 'venues' in event['_embedded'] else 'N/A',
            'city_name': event['_embedded']['venues'][0]['city']['name'] if '_embedded' in event and 'venues' in event['_embedded'] else 'N/A',
            'artist_name': event['_embedded']['attractions'][0]['name'] if '_embedded' in event and 'attractions' in event['_embedded'] else 'N/A',
            'lowest_price': 'N/A',  # Ticketmaster API may not provide price in the event list
            'highest_price': 'N/A',  # You might need to make additional API call for price
            'url': event['url']
        })
    return parsed_data

# Define the process_events, insert_event, update_event_prices functions as before
# Define the process_events function
def process_events(events, rds_host, rds_user, rds_password, rds_db_name):
    conn = mysql.connector.connect(user=rds_user, password=rds_password, host=rds_host, database=rds_db_name)
    cur = conn.cursor()

    for event in events:
        insert_event(event, cur)
        update_event_prices(event, cur)

    conn.commit()
    conn.close()

# Define the insert_event function
def insert_event(event, cursor):
    cursor.execute('''
        INSERT INTO events (event_id, event_name, venue_name, city_name, artist_name, date, local_time, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE event_name = VALUES(event_name), venue_name = VALUES(venue_name), city_name = VALUES(city_name), artist_name = VALUES(artist_name), date = VALUES(date), local_time = VALUES(local_time), url = VALUES(url)
    ''', (event['event_id'], event['event_name'], event['venue_name'], event['city_name'], event['artist_name'], event['event_date'], event['local_time'], event['url']))

# Define the update_event_prices function
def update_event_prices(event, cursor):
    cursor.execute('''
        INSERT INTO event_prices (event_id, date, lowest_price, highest_price)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE lowest_price = VALUES(lowest_price), highest_price = VALUES(highest_price)
    ''', (event['event_id'], event['event_date'], event['lowest_price'], event['highest_price']))

if __name__ == '__main__':
    lambda_handler(None, None)


# LAMBDA TEST
import os
import mysql.connector
import json
import requests
import logging
import datetime

def lambda_handler(event, context):
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting script...")

    start_time = datetime.datetime.now()

    TICKETMASTER_API_KEY = os.environ['TICKETMASTER_API_KEY']
    RDS_HOST = os.environ['RDS_HOST']
    RDS_USER = os.environ['RDS_USER']
    RDS_PASSWORD = os.environ['RDS_PASSWORD']
    RDS_DB_NAME = os.environ['RDS_DB_NAME']

    events = fetch_events(TICKETMASTER_API_KEY)
    parsed_data = parse_event_data(events)
    process_events(parsed_data, RDS_HOST, RDS_USER, RDS_PASSWORD, RDS_DB_NAME)

    total_time = datetime.datetime.now() - start_time
    logging.info(f"Total script execution time: {total_time}")

    logging.info("Script completed successfully.")
    return {
        'statusCode': 200,
        'body': json.dumps('Data update successful!')
    }

# Define other functions (fetch_events, parse_event_data, process_events, insert_event, update_event_prices) here...
def fetch_events(api_key):
    api_url = 'https://app.ticketmaster.com/discovery/v2/events.json'
    params = {
        'apikey': api_key,
        'countryCode': 'US',  # You can change this as per your requirement
        'size': 25  # Adjust the size as needed
    }
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return response.json().get('_embedded', {}).get('events', [])
    else:
        logging.error("Failed to fetch events: %s, Response: %s", response.status_code, response.text)
        return []

def parse_event_data(events):
    parsed_data = []
    for event in events:
        # Adjusting the parsing according to Ticketmaster's API response format
        parsed_data.append({
            'event_id': event['id'],
            'event_name': event['name'],
            'event_date': event['dates']['start']['localDate'],
            'local_time': event['dates']['start']['localTime'],
            'venue_name': event['_embedded']['venues'][0]['name'] if '_embedded' in event and 'venues' in event['_embedded'] else 'N/A',
            'city_name': event['_embedded']['venues'][0]['city']['name'] if '_embedded' in event and 'venues' in event['_embedded'] else 'N/A',
            'artist_name': event['_embedded']['attractions'][0]['name'] if '_embedded' in event and 'attractions' in event['_embedded'] else 'N/A',
            'lowest_price': 'N/A',  # Ticketmaster API may not provide price in the event list
            'highest_price': 'N/A',  # You might need to make additional API call for price
            'url': event['url']
        })
    return parsed_data

# Define the process_events, insert_event, update_event_prices functions as before
# Define the process_events function
def process_events(events, rds_host, rds_user, rds_password, rds_db_name):
    conn = mysql.connector.connect(user=rds_user, password=rds_password, host=rds_host, database=rds_db_name)
    cur = conn.cursor()

    for event in events:
        insert_event(event, cur)
        update_event_prices(event, cur)

    conn.commit()
    conn.close()

# Define the insert_event function
def insert_event(event, cursor):
    cursor.execute('''
        INSERT INTO events (event_id, event_name, venue_name, city_name, artist_name, date, local_time, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE event_name = VALUES(event_name), venue_name = VALUES(venue_name), city_name = VALUES(city_name), artist_name = VALUES(artist_name), date = VALUES(date), local_time = VALUES(local_time), url = VALUES(url)
    ''', (event['event_id'], event['event_name'], event['venue_name'], event['city_name'], event['artist_name'], event['event_date'], event['local_time'], event['url']))

# Define the update_event_prices function
def update_event_prices(event, cursor):
    cursor.execute('''
        INSERT INTO event_prices (event_id, date, lowest_price, highest_price)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE lowest_price = VALUES(lowest_price), highest_price = VALUES(highest_price)
    ''', (event['event_id'], event['event_date'], event['lowest_price'], event['highest_price']))

if __name__ == '__main__':
    lambda_handler(None, None)

# Local Test Firestore
import logging
import datetime
import json
import requests  # If you're making HTTP requests
import firebase_admin
from firebase_admin import credentials, firestore

# Firebase Admin SDK initialization
cred = credentials.Certificate('concert-price-tracker-5921b-firebase-adminsdk-ujei5-975fd322a8.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

# Placeholder function to fetch events from an API
def fetch_events(api_key):
    api_url = 'https://app.ticketmaster.com/discovery/v2/events.json'
    params = {
        'apikey': api_key,
        'classificationName': 'music',  # Filter for music events
        'size': 10  # Fetch only 10 events for testing
        # Optionally, add sorting parameters if supported by the API
    }
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return response.json().get('_embedded', {}).get('events', [])
    else:
        logging.error("Failed to fetch events: %s, Response: %s", response.status_code, response.text)
        return []

# Placeholder function to parse fetched event data
def parse_event_data(raw_events):
    parsed_data = []
    for event in raw_events:
        lowest_price = 'N/A'
        highest_price = 'N/A'

        # Extracting price range information if available
        if 'priceRanges' in event:
            prices = event['priceRanges'][0]  # Assuming the first price range contains the required info
            lowest_price = prices.get('min', 'N/A')
            highest_price = prices.get('max', 'N/A')

        # New code to extract state and country
        state = event['_embedded']['venues'][0].get('state', {}).get('name', 'N/A')
        country = event['_embedded']['venues'][0].get('country', {}).get('name', 'N/A')

        parsed_data.append({
            'event_id': event['id'],
            'event_name': event['name'],
            'event_date': event['dates']['start']['localDate'],
            'local_time': event['dates']['start']['localTime'],
            'venue_name': event['_embedded']['venues'][0]['name'] if '_embedded' in event and 'venues' in event['_embedded'] else 'N/A',
            'city_name': event['_embedded']['venues'][0]['city']['name'] if '_embedded' in event and 'venues' in event['_embedded'] else 'N/A',
            'state_name': state,
            'country_name': country,
            'artist_name': event['_embedded']['attractions'][0]['name'] if '_embedded' in event and 'attractions' in event['_embedded'] else 'N/A',
            'lowest_price': lowest_price,
            'highest_price': highest_price,
            'url': event['url']
        })
    return parsed_data

def process_events(events):
    for event in events:
        insert_event(event)
        update_event_prices(event)

def insert_event(event):
    events_ref = db.collection('events')
    event_id = event['event_id']
    events_ref.document(event_id).set({
        'event_name': event['event_name'],
        'venue_name': event['venue_name'],
        'city_name': event['city_name'],
        'state_name': event['state_name'],
        'country_name': event['country_name'],
        'artist_name': event['artist_name'],
        'date': event['event_date'],
        'local_time': event['local_time'],
        'url': event['url']
    })

def update_event_prices(event):
    event_prices_ref = db.collection('event_prices')
    price_id = f"{event['event_id']}-{event['event_date']}"
    event_prices_ref.document(price_id).set({
        'event_id': event['event_id'],
        'date': event['event_date'],
        'lowest_price': event['lowest_price'],
        'highest_price': event['highest_price']
    })

def lambda_handler(event, context):
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting script...")

    start_time = datetime.datetime.now()

    TICKETMASTER_API_KEY = 'v4kBXKT6lIgCf8t7vGoA5cF7S2Vqq6eZ'  # Replace with the correct API key
    raw_events = fetch_events(TICKETMASTER_API_KEY)
    parsed_data = parse_event_data(raw_events)
    process_events(parsed_data)

    total_time = datetime.datetime.now() - start_time
    logging.info(f"Total script execution time: {total_time}")

    logging.info("Script completed successfully.")
    return {
        'statusCode': 200,
        'body': json.dumps('Data update successful!')
    }

if __name__ == '__main__':
    lambda_handler(None, None)
    """

# Cloud Function Firestore
import logging
import datetime
import json
import requests  # If you're making HTTP requests
import firebase_admin
from firebase_admin import credentials, firestore

# Firebase Admin SDK initialization
# cred = credentials.Certificate('concert-price-tracker-5921b-firebase-adminsdk-ujei5-975fd322a8.json')
# firebase_admin.initialize_app(cred)
db = firestore.client()

# Placeholder function to fetch events from an API
def fetch_events(api_key):
    api_url = 'https://app.ticketmaster.com/discovery/v2/events.json'
    params = {
        'apikey': api_key,
        'classificationName': 'music',  # Filter for music events
        'size': 10  # Fetch only 10 events for testing
        # Optionally, add sorting parameters if supported by the API
    }
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return response.json().get('_embedded', {}).get('events', [])
    else:
        logging.error("Failed to fetch events: %s, Response: %s", response.status_code, response.text)
        return []

# Placeholder function to parse fetched event data
def parse_event_data(raw_events):
    parsed_data = []
    for event in raw_events:
        lowest_price = 'N/A'
        highest_price = 'N/A'

        # Extracting price range information if available
        if 'priceRanges' in event:
            prices = event['priceRanges'][0]  # Assuming the first price range contains the required info
            lowest_price = prices.get('min', 'N/A')
            highest_price = prices.get('max', 'N/A')

        # New code to extract state and country
        state = event['_embedded']['venues'][0].get('state', {}).get('name', 'N/A')
        country = event['_embedded']['venues'][0].get('country', {}).get('name', 'N/A')

        parsed_data.append({
            'event_id': event['id'],
            'event_name': event['name'],
            'event_date': event['dates']['start']['localDate'],
            'local_time': event['dates']['start']['localTime'],
            'venue_name': event['_embedded']['venues'][0]['name'] if '_embedded' in event and 'venues' in event['_embedded'] else 'N/A',
            'city_name': event['_embedded']['venues'][0]['city']['name'] if '_embedded' in event and 'venues' in event['_embedded'] else 'N/A',
            'state_name': state,
            'country_name': country,
            'artist_name': event['_embedded']['attractions'][0]['name'] if '_embedded' in event and 'attractions' in event['_embedded'] else 'N/A',
            'lowest_price': lowest_price,
            'highest_price': highest_price,
            'url': event['url']
        })
    return parsed_data

def process_events(events):
    for event in events:
        insert_event(event)
        update_event_prices(event)

def insert_event(event):
    events_ref = db.collection('events')
    event_id = event['event_id']
    events_ref.document(event_id).set({
        'event_name': event['event_name'],
        'venue_name': event['venue_name'],
        'city_name': event['city_name'],
        'state_name': event['state_name'],
        'country_name': event['country_name'],
        'artist_name': event['artist_name'],
        'date': event['event_date'],
        'local_time': event['local_time'],
        'url': event['url']
    })

def update_event_prices(event):
    event_prices_ref = db.collection('event_prices')
    price_id = f"{event['event_id']}-{event['event_date']}"
    event_prices_ref.document(price_id).set({
        'event_id': event['event_id'],
        'date': event['event_date'],
        'lowest_price': event['lowest_price'],
        'highest_price': event['highest_price']
    })

def main_function():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting script...")

    start_time = datetime.datetime.now()

    TICKETMASTER_API_KEY = 'v4kBXKT6lIgCf8t7vGoA5cF7S2Vqq6eZ'  # Replace with the correct API key
    raw_events = fetch_events(TICKETMASTER_API_KEY)
    parsed_data = parse_event_data(raw_events)
    process_events(parsed_data)

    total_time = datetime.datetime.now() - start_time
    logging.info(f"Total script execution time: {total_time}")

    logging.info("Script completed successfully.")
    return {
        'statusCode': 200,
        'body': json.dumps('Data update successful!')
    }