import requests
import psycopg2
from psycopg2 import extras
import datetime
from config import SEATGEEK_CLIENT_ID, SEATGEEK_CLIENT_SECRET, DATABASE_URL

def fetch_events():
    api_url = 'https://api.seatgeek.com/2/events'
    all_events = []
    page = 1
    while len(all_events) < 5000:
        params = {
            'client_id': SEATGEEK_CLIENT_ID,
            'taxonomies.name': 'concert',
            'per_page': 500,
            'sort': 'score.desc',
            'page': page
        }
        response = requests.get(api_url, params=params)
        if response.status_code != 200:
            print(f"Failed to fetch events: {response.status_code}, Response: {response.text}")
            break
        events = response.json()['events']
        print(f"Fetched page {page} with {len(events)} events.")
        if not events:
            break
        all_events.extend(events)
        page += 1
    return all_events[:5000]  # Limit to first 5000 events

    
def parse_event_data(events):
    parsed_data = []
    for event in events:
        event_id = str(event['id']) # Assuming 'id' is the unique identifier provided by the API
        # event_id = event['id'] 
        event_name = event['title']
        event_date = event['datetime_local']
        venue_name = event['venue']['name']
        city_name = event['venue']['city']
        lowest_price = event['stats'].get('lowest_price', 'N/A')
        highest_price = event['stats'].get('highest_price', 'N/A')

        parsed_data.append({
            'event_id': event_id,  # Include event_id in the dictionary
            'event_name': event_name,
            'event_date': event_date,
            'venue_name': venue_name,
            'city_name': city_name,
            'lowest_price': lowest_price,
            'highest_price': highest_price
        })

    return parsed_data

def create_database():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Drop existing tables if they exist
    cur.execute("DROP TABLE IF EXISTS event_prices CASCADE")
    cur.execute("DROP TABLE IF EXISTS events CASCADE")

    # Create events table
    cur.execute('''
        CREATE TABLE events (
            id SERIAL PRIMARY KEY,
            event_id TEXT UNIQUE NOT NULL,
            event_name TEXT,
            event_date TEXT,
            venue_name TEXT,
            city_name TEXT
        )
    ''')

    # Create event_prices table
    cur.execute('''
        CREATE TABLE event_prices (
            event_id TEXT NOT NULL,
            date DATE NOT NULL,
            lowest_price REAL,
            highest_price REAL,
            PRIMARY KEY (event_id, date),
            FOREIGN KEY (event_id) REFERENCES events(event_id)
        )
    ''')
    conn.commit()
    conn.close()






def insert_or_update_event(event):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Check if the event already exists in 'events' table
    cur.execute("SELECT * FROM events WHERE event_id = %s", (event['event_id'],))
    if not cur.fetchone():
        # Insert a new event
        cur.execute('''
            INSERT INTO events (event_id, event_name, event_date, venue_name, city_name)
            VALUES (%s, %s, %s, %s, %s)
        ''', (event['event_id'], event['event_name'], event['event_date'], event['venue_name'], event['city_name']))

    # Update or insert into 'event_prices' table
    cur.execute("SELECT * FROM event_prices WHERE event_id = %s AND date = CURRENT_DATE", (event['event_id'],))
    if cur.fetchone():
        # Update the existing price data
        cur.execute('''
            UPDATE event_prices 
            SET lowest_price = %s, highest_price = %s 
            WHERE event_id = %s AND date = CURRENT_DATE
        ''', (event['lowest_price'], event['highest_price'], event['event_id']))
    else:
        # Insert new price data
        cur.execute('''
            INSERT INTO event_prices (event_id, date, lowest_price, highest_price)
            VALUES (%s, CURRENT_DATE, %s, %s)
        ''', (event['event_id'], event['lowest_price'], event['highest_price']))

    conn.commit()
    conn.close()


# def update_data(parsed_data):
#    print("Updating events and prices in the database...")
#    for i, event in enumerate(parsed_data, start=1):
#        insert_or_update_event(event)
#        if i % 100 == 0:
#            print(f"Processed {i} of {len(parsed_data)} events.")

def run_query(query):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    conn.close()
    return results

def clear_data():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute('DELETE FROM events')
    conn.commit()
    conn.close()

def delete_past_events():
    today = datetime.datetime.now().strftime("%Y-%m-%dT00:00:00")  # Keep today's concerts
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute('''
        DELETE FROM events 
        WHERE event_date < %s
    ''', (today,))
    conn.commit()
    conn.close()


def clear_old_price_data():
    """
    Clear price data that is older than 90 days.
    """
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    ninety_days_ago = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
    cur.execute("DELETE FROM event_prices WHERE date < %s", (ninety_days_ago,))
    conn.commit()
    conn.close()

def delete_past_events():
    """
    Delete events that have already occurred.
    """
    today = datetime.datetime.now().strftime("%Y-%m-%d")  # Keep today's concerts
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("DELETE FROM events WHERE event_date < %s", (today,))
    conn.commit()
    conn.close()

def batch_update_events(parsed_data):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Prepare batch data for events
    events_batch = [(e['event_id'], e['event_name'], e['event_date'], e['venue_name'], e['city_name']) for e in parsed_data]
    # Prepare batch data for event prices
    prices_batch = [(e['event_id'], datetime.date.today(), e['lowest_price'], e['highest_price']) for e in parsed_data]

    # Batch insert events
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO events (event_id, event_name, event_date, venue_name, city_name) 
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
    """, events_batch)

    # Batch insert or update prices
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO event_prices (event_id, date, lowest_price, highest_price) 
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (event_id, date) 
        DO UPDATE SET lowest_price = EXCLUDED.lowest_price, highest_price = EXCLUDED.highest_price
    """, prices_batch)

    conn.commit()
    cur.close()
    conn.close()
    print(f"Processed batch of {len(parsed_data)} events.")

def main():
    print("Starting script...")
    create_database()
    events = fetch_events()
    if events:
        parsed_data = parse_event_data(events)
        batch_update_events(parsed_data)  # Replace update_data with batch_update_events
        delete_past_events()
        clear_old_price_data()
    print("Script completed successfully.")

if __name__ == '__main__':
    main()