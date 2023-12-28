# Concert Ticket Scraper using GCP

## Project Overview
Ticket Scraper is a Firebase Cloud Function designed to fetch event data from the Ticketmaster API and store it in Firestore. The function is triggered automatically and updates the Firestore database with the latest event information including prices, venues, dates, and artists.

### Key Features
- Fetches event data from Ticketmaster API.
- Parses and processes the data to extract relevant information.
- Updates Firestore database with the latest event details.
- Automated trigger to run the function periodically.

## File Structure
ticket_scraper/
│
├── functions/ # Cloud Function source code
│ ├── pycache/
│ ├── main.py # Main script for the Cloud Function
│ └── requirements.txt # Dependencies for the Cloud Function
│
├── .gitignore # Specifies untracked files to ignore
├── firebase.json # Firebase configuration file
├── firestore.indexes.json # Firestore indexes
├── firestore.rules # Firestore security rules
└── README.md # Project documentation (this file)

## Setup Instructions
To set up the Ticket Scraper project on your local machine:

1. Navigate to the functions directory and create a virtual environment:
```python 
cd functions
python -m venv venv
```

2. Activate the virtual environment:
```python 
On Windows: venv\Scripts\activate
On Unix or MacOS: source venv/bin/activate
```

3. Install the required dependencies:
```python 
pip install -r requirements.txt
```

4. Set up Firebase CLI and login:
```python 
firebase login
```

5. Initialize Firebase in your project directory (follow the CLI prompts):
```python 
firebase init
```

## Deployment
To deploy the Cloud Function to Firebase:

1. Ensure you are in the ticket_scraper directory.

2. Deploy the function using Firebase CLI:
```python 
firebase deploy --only functions
```

3. Verify the deployment in the Firebase Console.

## Monitoring and Logs
After deployment, monitor the function execution and view logs in the Firebase Console under the Functions section.

## Contributions
Feel free to fork this repository and submit pull requests for any improvements or fixes.

