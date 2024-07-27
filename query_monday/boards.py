# query_monday/boards.py
import requests
from query_monday.config import URL, HEADERS

# make function to get all board's id and name
def get_all_boards(folder_id=870675):
    query = f'''
query {{
    boards(limit: 1000 workspace_ids:[{folder_id}]) {{
      id
      name
      type
    }}
}}
'''
    body = {'query': query}
    response = requests.post(URL, json=body, headers=HEADERS)
    return response.json()


