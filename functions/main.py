# Welcome to Cloud Functions for Firebase for Python!

# Import necessary libraries
import firebase_admin
from firebase_admin import credentials, firestore
from firebase_functions import pubsub_fn
import datetime
import logging
import requests
import base64
import os

service_account_path = 'concert-price-tracker-5921b-1b44f28c1652.json'

# Initialize Firebase Admin with explicit credentials
if not firebase_admin._apps:
    cred = credentials.Certificate(service_account_path)
    firebase_admin.initialize_app(cred)

db = firestore.client()

def fetch_events(api_key):
    api_url = 'https://app.ticketmaster.com/discovery/v2/events.json'
    params = {'apikey': api_key, 'classificationName': 'music', 'size': 100}
    
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # This will raise an HTTPError if the HTTP request returned an unsuccessful status code
        return response.json().get('_embedded', {}).get('events', [])
    except requests.exceptions.HTTPError as errh:
        logging.error(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        logging.error(f"Error: {err}")
    return []

# Placeholder function to parse fetched event data
def parse_event_data(raw_events):
    parsed_data = []
    for event in raw_events:
        lowest_price = 'N/A'
        highest_price = 'N/A'

        # Extracting price range information if available
        if 'priceRanges' in event:
            prices = event['priceRanges'][0]
            lowest_price = prices.get('min', 'N/A')
            highest_price = prices.get('max', 'N/A')

        # Extracting state and country
        state = event['_embedded']['venues'][0].get('state', {}).get('name', 'N/A')
        country = event['_embedded']['venues'][0].get('country', {}).get('name', 'N/A')

        parsed_data.append({
            'event_id': event['id'],
            'event_name': event['name'],
            'event_date': event['dates']['start']['localDate'],
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

def update_database(parsed_data):
    # Start a batch write operation
    batch = db.batch()

    # Fetch existing events from Firestore
    existing_events = db.collection('events').stream()
    existing_event_ids = set(event.id for event in existing_events)

    new_event_ids = set(event['event_id'] for event in parsed_data)

    # Remove events not in top 100
    events_to_remove = existing_event_ids - new_event_ids
    for event_id in events_to_remove:
        event_ref = db.collection('events').document(event_id)
        price_ref = db.collection('event_prices').document(event_id)
        batch.delete(event_ref)
        batch.delete(price_ref)

    # Commit the batch operation
    batch.commit()

    # Insert new events and update prices
    for event in parsed_data:
        insert_event(event)
        update_event_prices(event)


def clean_up_old_data():
    # Define the time threshold (14 days ago)
    threshold_date = datetime.datetime.now() - datetime.timedelta(days=14)

    # Clean up old prices
    event_prices_ref = db.collection('event_prices')
    old_prices = event_prices_ref.where('date', '<', threshold_date.strftime('%Y-%m-%d')).stream()
    for price in old_prices:
        event_prices_ref.document(price.id).delete()

    # Clean up old events
    events_ref = db.collection('events')
    old_events = events_ref.where('date', '<', threshold_date.strftime('%Y-%m-%d')).stream()
    for event in old_events:
        events_ref.document(event.id).delete()

@pubsub_fn.on_message_published(topic="concert-data-update")
def main_function(event: pubsub_fn.CloudEvent[pubsub_fn.MessagePublishedData]) -> None:
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting script...")

    TICKETMASTER_API_KEY = os.environ.get('TICKETMASTER_API_KEY')
    raw_events = fetch_events(TICKETMASTER_API_KEY)
    parsed_data = parse_event_data(raw_events)
    update_database(parsed_data)
    clean_up_old_data()

    logging.info("Script completed successfully.")