import requests
import sqlite3
import datetime
from config import SEATGEEK_CLIENT_ID, SEATGEEK_CLIENT_SECRET

def fetch_events():
    api_url = 'https://api.seatgeek.com/2/events'
    params = {
        'client_id': SEATGEEK_CLIENT_ID,
        'taxonomies.name': 'concert',  # Filter for music concerts
        'per_page': 500,  # Fetch top 500 events
        'sort': 'score.desc'  # Sort by popularity score, descending
    }

    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return response.json()['events']
    else:
        print(f"Failed to fetch events: {response.status_code}, Response: {response.text}")
        return None
    
def parse_event_data(events):
    parsed_data = []
    for event in events:
        event_id = event['id']  # Assuming 'id' is the unique identifier provided by the API
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
    conn = sqlite3.connect('events.db')
    c = conn.cursor()

    # Drop the existing table if it exists
    c.execute('DROP TABLE IF EXISTS events')

    # Create the table with the new schema
    c.execute('''
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT,
            event_name TEXT,
            event_date TEXT,
            venue_name TEXT,
            city_name TEXT,
            lowest_price REAL,
            highest_price REAL
        )
    ''')
    conn.commit()
    conn.close()

def insert_or_update_event(event):
    conn = sqlite3.connect('events.db')
    c = conn.cursor()

    # Check if the event already exists
    c.execute("SELECT * FROM events WHERE event_id = ?", (event['event_id'],))
    exists = c.fetchone()

    if exists:
        # Update the existing event
        c.execute('''
            UPDATE events 
            SET lowest_price = ?, highest_price = ? 
            WHERE event_id = ?
        ''', (event['lowest_price'], event['highest_price'], event['event_id']))
    else:
        # Insert a new event
        c.execute('''
            INSERT INTO events (event_id, event_name, event_date, venue_name, city_name, lowest_price, highest_price)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (event['event_id'], event['event_name'], event['event_date'], event['venue_name'], event['city_name'], event['lowest_price'], event['highest_price']))
    
    conn.commit()
    conn.close()

def update_data(parsed_data):
    for event in parsed_data:
        insert_or_update_event(event)

def run_query(query):
    conn = sqlite3.connect('events.db')
    c = conn.cursor()
    c.execute(query)
    results = c.fetchall()
    conn.close()
    return results

def clear_data():
    conn = sqlite3.connect('events.db')
    c = conn.cursor()
    c.execute('DELETE FROM events')
    c.execute('UPDATE sqlite_sequence SET seq = 0 WHERE name = "events"')  # Reset auto-increment counter
    conn.commit()
    conn.close()

def delete_past_events():
    today = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    conn = sqlite3.connect('events.db')
    c = conn.cursor()
    c.execute('''
        DELETE FROM events 
        WHERE event_date < ?
    ''', (today,))
    conn.commit()
    conn.close()

def main():
    create_database()
    events = fetch_events()
    if events:
        parsed_data = parse_event_data(events)
        update_data(parsed_data)

        # Optionally, print the top 25 events for verification
        top_25_query = "SELECT * FROM events ORDER BY event_date LIMIT 25;"
        top_25_events = run_query(top_25_query)
        for event in top_25_events:
            print(event)

        # Delete past events
        delete_past_events()

if __name__ == '__main__':
    main()
