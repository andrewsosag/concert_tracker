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