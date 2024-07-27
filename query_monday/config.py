import os
from dotenv import load_dotenv

load_dotenv()

URL = "https://api.monday.com/v2"

HEADERS = {
  'Authorization': os.getenv('MONDAY_API_KEY'),
  'Content-Type': 'application/json'
}