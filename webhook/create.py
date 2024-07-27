import requests
from dotenv import load_dotenv
import os
import json

# Load environment variables
load_dotenv()

# Load API key and webhook URL from environment variables
api_key = os.getenv('MONDAY_API_KEY')  # Ensure this is stored in your .env
webhook_url = os.getenv('NGROK_URL') + '/webhook'  # Make sure this is updated in your .env

# Retrieve board IDs from environment and parse the JSON string into a dictionary
board_ids_json = os.getenv('FAKE_BOARD_IDS')
board_ids = json.loads(board_ids_json)

def create_webhook(board_id, event):
    """Creates a webhook for a specific board and event."""
    url = "https://api.monday.com/v2"
    headers = {"Authorization" : api_key}
    json_payload = {
        'query' : f'''
            mutation {{
                create_webhook (board_id: {board_id}, url: "{webhook_url}", event: {event}) {{
                    id
                }}
            }}
        '''
    }
    response = requests.post(url, json=json_payload, headers=headers)
    print(f"Webhook for {event} on board {board_id}: {response.json()}")

# List of events you want to monitor
events = [
    "create_item", "change_column_value", "create_update",
    "delete_item", "change_name"
]

# Create webhooks for all events on all boards
for board_name, board_id in board_ids.items():
    for event in events:
        create_webhook(board_id, event)
