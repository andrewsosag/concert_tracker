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
import traceback
from datetime import datetime, timezone

# Initialize logging
logging.basicConfig(level=logging.INFO)

service_account_path = 'concert-price-tracker-5921b-1b44f28c1652.json'

# Initialize Firebase Admin with explicit credentials
if not firebase_admin._apps:
    cred = credentials.Certificate(service_account_path)
    firebase_admin.initialize_app(cred)

db = firestore.client()

class EventFetchError(Exception):
    pass

def fetch_events(api_key, max_attempts=3):
    logging.info("Starting event fetching")
    attempts = 0
    events = []
    while attempts < max_attempts:
        try:
            page_number = 0
            while len(events) < 500:
                api_url = 'https://app.ticketmaster.com/discovery/v2/events.json'
                params = {
                    'apikey': api_key,
                    'classificationName': 'music',
                    'size': min(200, 500 - len(events)), # Fetch only the required number of events
                    'countryCode': 'US',
                    'page': page_number
                }
                response = requests.get(api_url, params=params)
                response.raise_for_status()
                data = response.json()
                events.extend(data.get('_embedded', {}).get('events', []))
                page_number += 1
                if page_number >= 5:  # Limit to 5 pages to stay within 1000 deep paging limit
                    break
            if len(events) >= 500:
                break
        except requests.exceptions.RequestException as e:
            logging.error(f"Attempt {attempts + 1} failed to fetch events: {e}")
            attempts += 1
            if attempts >= max_attempts:
                raise EventFetchError from e
    logging.info(f"Fetched {len(events)} events")
    return events

# Placeholder function to parse fetched event data
def parse_event_data(raw_events):
    logging.info("Starting event data parsing")
    parsed_data = []
    current_time = datetime.utcnow()

    for event in raw_events:
        # Check if 'sales' data is available and if 'startDateTime' is in the past
        sales_start = event.get('sales', {}).get('public', {}).get('startDateTime')
        if sales_start:
            sales_start_time = datetime.fromisoformat(sales_start.rstrip('Z'))
            if sales_start_time > current_time:
                continue  # Skip the event if sales start date is in the future
        
        lowest_price = 'N/A'
        highest_price = 'N/A'

        # Extracting price range information if available
        if 'priceRanges' in event:
            prices = event['priceRanges'][0]
            lowest_price = prices.get('min', 'N/A')
            highest_price = prices.get('max', 'N/A')

        # Extracting state, country, and genre
        state = event['_embedded']['venues'][0].get('state', {}).get('name', 'N/A')
        country = event['_embedded']['venues'][0].get('country', {}).get('name', 'N/A')
        genres = event.get('classifications', [{}])[0].get('genre', {}).get('name', 'N/A')

        parsed_data.append({
            'event_id': event['id'],
            'event_name': event['name'],
            'event_date': event['dates']['start']['localDate'],
            'venue_name': event['_embedded']['venues'][0]['name'] if '_embedded' in event and 'venues' in event['_embedded'] else 'N/A',
            'city_name': event['_embedded']['venues'][0]['city']['name'] if '_embedded' in event and 'venues' in event['_embedded'] else 'N/A',
            'state_name': state,
            'country_name': country,
            'genre': genres,
            'artist_name': event['_embedded']['attractions'][0]['name'] if '_embedded' in event and 'attractions' in event['_embedded'] else 'N/A',
            'lowest_price': lowest_price,
            'highest_price': highest_price,
            'url': event['url']
        })
    logging.info(f"Parsed {len(parsed_data)} events")
    return parsed_data

def safe_execute(retry_attempts=3):
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < retry_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    logging.error(f"Attempt {attempts} failed in {func.__name__}: {e}")
                    if attempts == retry_attempts:
                        logging.error(f"Final attempt failed in {func.__name__}. Function aborted.")
                        return None
        return wrapper
    return decorator

@safe_execute(retry_attempts=3)
def insert_event(event):
    logging.info(f"Inserting event: {event['event_id']}")
    events_ref = db.collection('events')
    event_id = event['event_id']
    event_doc = events_ref.document(event_id).get()
    if not event_doc.exists or event_doc.to_dict() != event:
        events_ref.document(event_id).set(event)
    logging.info(f"Event inserted: {event['event_id']}")

@safe_execute(retry_attempts=3)
def update_event_prices(event):
    logging.info(f"Updating prices for event: {event['event_id']}")
    event_prices_ref = db.collection('event_prices')
    price_id = f"{event['event_id']}-{datetime.utcnow().strftime('%Y-%m-%d')}"
    price_doc_ref = event_prices_ref.document(price_id)

    # Check if the document for today's date already exists
    if price_doc_ref.get().exists:
        price_doc_ref.update({
            'lowest_price': event['lowest_price'],
            'highest_price': event['highest_price']
        })
    else:
        price_doc_ref.set({
            'event_id': event['event_id'],
            'date': datetime.utcnow().strftime('%Y-%m-%d'),
            'lowest_price': event['lowest_price'],
            'highest_price': event['highest_price']
        })
    logging.info(f"Prices updated for event: {event['event_id']}")

@safe_execute(retry_attempts=3)
def update_database(parsed_data):
    logging.info("Starting database update")
    # Define the maximum number of writes per batch
    MAX_WRITES_PER_BATCH = 500
    total_operations = 0

    # Function to execute batch operations
    def execute_batch(batch, total_ops):
        batch.commit()
        return firestore.client().batch(), 0

    # Start the first batch write operation
    batch = db.batch()
    
    # Fetch existing events from Firestore
    existing_events = db.collection('events').stream()
    existing_event_ids = set(event.id for event in existing_events)
    new_event_ids = set(event['event_id'] for event in parsed_data)

    # Remove events not in the new data set
    events_to_remove = existing_event_ids - new_event_ids
    for event_id in events_to_remove:
        event_ref = db.collection('events').document(event_id)
        price_ref = db.collection('event_prices').document(event_id)
        batch.delete(event_ref)
        batch.delete(price_ref)
        total_operations += 2
        if total_operations >= MAX_WRITES_PER_BATCH:
            batch, total_operations = execute_batch(batch, total_operations)

    # Insert new events and update prices
    for event in parsed_data:
        insert_event(event)
        update_event_prices(event)
        total_operations += 2  # One for the event and one for the price
        if total_operations >= MAX_WRITES_PER_BATCH:
            batch, total_operations = execute_batch(batch, total_operations)

    # Final commit if there are any remaining operations
    if total_operations > 0:
        batch.commit()
    logging.info("Database update completed")


@safe_execute(retry_attempts=3)
def clean_up_old_data():
    logging.info("Starting cleanup of old data")
    # Define the time threshold (14 days ago)
    threshold_date = datetime.utcnow() - datetime.timedelta(days=14)
    event_prices_ref = db.collection('event_prices')
    events_ref = db.collection('events')
    old_prices = db.collection('event_prices').where('date', '<', threshold_date.strftime('%Y-%m-%d')).stream()
    old_events = db.collection('events').where('date', '<', threshold_date.strftime('%Y-%m-%d')).stream()

    # Start batch operations for deleting old prices and events
    batch = db.batch()
    operations_count = 0
    MAX_OPERATIONS_PER_BATCH = 500
    
    # Function to execute and reset the batch
    def commit_batch():
        nonlocal operations_count, batch
        batch.commit()
        batch = db.batch()
        operations_count = 0

    # Clean up old prices
    for price in old_prices:
        batch.delete(event_prices_ref.document(price.id))
        operations_count += 1
        if operations_count >= MAX_OPERATIONS_PER_BATCH:
            commit_batch()

    # Clean up old events
    for event in old_events:
        batch.delete(events_ref.document(event.id))
        operations_count += 1
        if operations_count >= MAX_OPERATIONS_PER_BATCH:
            commit_batch()

    # Commit any remaining operations
    if operations_count > 0:
        commit_batch()
    logging.info("Cleanup completed")


@pubsub_fn.on_message_published(topic="concert-data-update")
def main_function(event: pubsub_fn.CloudEvent[pubsub_fn.MessagePublishedData]) -> None:
    logging.info("Cloud Function execution started")

    TICKETMASTER_API_KEY = os.environ.get('TICKETMASTER_API_KEY')
    try:
        raw_events = fetch_events(TICKETMASTER_API_KEY)
        parsed_data = parse_event_data(raw_events)
        if parsed_data:
            update_database(parsed_data)
            clean_up_old_data()
        else:
            logging.info("No new events to process.")
    except EventFetchError:
        logging.error("Event fetch failed. Exiting function.")
        return

    logging.info("Cloud Function execution completed")