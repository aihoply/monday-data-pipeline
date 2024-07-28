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

# Function to get the name of a board using its board_id
def get_name(board_id):
    query = f'''
query {{
    boards(ids: {board_id}) {{
        name
    }}
}}
'''
    body = {'query': query}
    response = requests.post(URL, json=body, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        if 'data' in data and 'boards' in data['data'] and len(data['data']['boards']) > 0:
            return data['data']['boards'][0]['name']
    return None