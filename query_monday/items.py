# query_monday/items.py
import aiohttp
import asyncio
from query_monday.config import URL, HEADERS


async def get_updates_of_items(item_ids):
    # Format the list of item IDs into a GraphQL array string
    ids_formatted = ', '.join(map(str, item_ids))
    query = f'''
    query {{
        items(ids: [{ids_formatted}]) {{
            id
            name
            updates {{
                id
                assets {{
                    id
                    name
                    created_at
                    file_size
                    file_extension
                    url
                }}
                creator {{
                    account {{
                        name
                    }}
                }}
                created_at
                text_body
            }}
        }}
    }}
    '''
    data = {'query': query}
    async with aiohttp.ClientSession() as session:
        async with session.post(URL, json=data, headers=HEADERS) as response:
            response_data = await response.json()
            if 'errors' in response_data or 'data' not in response_data:
                print('Error:', response_data)
                return None
            return response_data['data']['items']

        



# make function to get cursor
async def getCursor(board_id, cursor_data='cursor: null'):
    query = '''
    query {
        boards(ids: ''' + str(board_id) + ''') {
            id
            name
            items_count
            items_page(limit: 500 ''' + cursor_data + ''') {
                cursor
            }
        }
    }
            '''
    body = {'query': query}
    async with aiohttp.ClientSession() as session:
        async with session.post(URL, json=body, headers=HEADERS) as response:
            response_data = await response.json()
            if 'errors' in response_data:
                print('Error:', response_data)
                return None
            cursor = str(response_data['data']['boards'][0]['items_page']['cursor'])
            return cursor


# get all items from a board id, including detailed item properties and pagination with cursors
async def get_all_items(board_id, cursor_data='cursor: null'):
    print('Getting items from board id:', board_id)
    print('Discovering every cursor to get all items')
    cursor_list = ['null']

    async with aiohttp.ClientSession() as session:
        cursor = await getCursor(board_id, cursor_data)
        while cursor != 'None':
            cursor_list.append(cursor)
            cursor = await getCursor(board_id, cursor_data=f'cursor: "{cursor}"')

        print('All cursors:', cursor_list)
        print('Total cursors:', len(cursor_list))

        response = []
        for cursor in cursor_list:
            print('Request items from cursor:', cursor)
            cursor_data = 'cursor: null' if cursor == 'null' else f'cursor: "{cursor}"'
            query = '''
            query {
            boards(ids: ''' + str(board_id) + ''') {
                id
                name
                items_count
                items_page(limit: 500 ''' + cursor_data + ''') {
                cursor
                items {
                    id
                    name
                    group {
                    id title
                    }
                    board {
                    id name
                    }
                    column_values {
                        id
                        value
                        text
                        type
                        column {
                            title
                        }
                        ... on MirrorValue {
                            display_value
                        }
                        ... on TextValue {
                            text
                        }
                        ... on LongTextValue {
                            text
                        }
                        ... on DateValue {
                            date
                        }
                        ... on PeopleValue {
                            persons_and_teams {
                            id
                            kind
                            }
                        }
                        ... on DropdownValue {
                            values {
                            label
                            }
                        }
                        ... on CheckboxValue {
                            checked
                        }
                        ... on EmailValue {
                            email
                        }
                        ... on LinkValue {
                            url
                        }
                        ... on ButtonValue {
                            text
                        }
                        ... on PhoneValue {
                            phone country_short_name
                        }
                        ... on StatusValue {
                            label
                        }
                        ... on CountryValue {
                            country {
                            name
                            }
                        }
                        ... on RatingValue {
                            rating
                        }
                        ... on VoteValue {
                            voter_ids
                        }
                        ... on FileValue {
                            text value
                        }
                        ... on LocationValue {
                            address city street
                        }
                        ... on TimelineValue {
                            from
                            to
                        }
                        ... on TagsValue {
                            tag_ids
                        }
                        ... on WorldClockValue {
                            timezone
                        }
                    }
                }
                }
            }
            }
            '''
            body = {'query': query}
            async with session.post(URL, json=body, headers=HEADERS) as r:
                result_data = await r.json()
                if 'errors' in result_data:
                    print('Error:', result_data)
                    return
                items = result_data['data']['boards'][0]['items_page']['items']
                response.extend(items)
                print('Appended items to response list')
                print('Total items:', len(items))
                print('Total response:', len(response))
        return response


# get all items from a board id using pagination with cursors
async def get_all_items_simple(board_id, cursor_data='cursor: null'):
    print('Getting items from board id:', board_id)
    print('Discovering every cursor to get all items')
    cursor_list = ['null']

    async with aiohttp.ClientSession() as session:
        cursor = await getCursor(board_id, cursor_data)
        while cursor != 'None':
            cursor_list.append(cursor)
            cursor = await getCursor(board_id, cursor_data=f'cursor: "{cursor}"')

        print('All cursors:', cursor_list)
        print('Total cursors:', len(cursor_list))

        response = []
        for cursor in cursor_list:
            print('Request items from cursor:', cursor)
            cursor_data = 'cursor: null' if cursor == 'null' else f'cursor: "{cursor}"'
            query = '''
            query {
            boards(ids: ''' + str(board_id) + ''') {
                id
                name
                items_count
                items_page(limit: 500 ''' + cursor_data + ''') {
                cursor
                items {
                    id
                    name
                    group {
                        id title
                    }
                }
                }
            }
            }
            '''
            body = {'query': query}
            async with session.post(URL, json=body, headers=HEADERS) as r:
                if 'errors' in (await r.json()):
                    print('Error:', await r.json())
                    return
                items = (await r.json())['data']['boards'][0]['items_page']['items']
                response.extend(items)
                print('Appended items to response list')
                print('Total items:', len(items))
                print('Total response:', len(response))
        return response

# get a single item
async def get_item(item_id):
    query = f"""
    query {{
      items(ids:[{item_id}]) {{
        id
        name
        board {{name}}
        group {{ id title }}
        column_values {{
          id
          value
          text
          type
          column {{
            id
            title
            type
          }}
          ... on MirrorValue {{
            display_value
          }}
          ... on TextValue {{
            text
          }}
          ... on LongTextValue {{
            text
          }}
          ... on DateValue {{
            date
          }}
          ... on PeopleValue {{
            persons_and_teams {{
              id
              kind
            }}
          }}
          ... on DropdownValue {{
            values {{
              label
            }}
          }}
          ... on CheckboxValue {{
            checked
          }}
          ... on EmailValue {{
            email
          }}
          ... on LinkValue {{
            url
          }}
          ... on ButtonValue {{
            text
          }}
          ... on PhoneValue {{
            phone
            country_short_name
          }}
          ... on StatusValue {{
            label
          }}
          ... on CountryValue {{
            country {{
              name
            }}
          }}
          ... on RatingValue {{
            rating
          }}
          ... on VoteValue {{
            voter_ids
          }}
          ... on FileValue {{
            text
            value
          }}
          ... on LocationValue {{
            address
            city
            street
          }}
          ... on TimelineValue {{
            from
            to
          }}
          ... on TagsValue {{
            tag_ids
          }}
          ... on WorldClockValue {{
            timezone
          }}
        }}
        updates {{
            id
            assets {{
                id
                name
                created_at
                file_size
                file_extension
                url
            }}
            creator {{
                account {{
                    name
                }}
            }}
            created_at
            text_body
        }}
      }}
    }}"""
    body = {'query': query}
    async with aiohttp.ClientSession() as session:
        async with session.post(URL, json=body, headers=HEADERS) as r:
            item_data = await r.json()
            return item_data['data']['items'][0]



# get name and group of an item
from db.conect_mongo import monday_db
async def get_name_n_group(item_id):
    query = '''
    query {
        items (ids: ['''+ str(item_id) +''']) {
            name
            group {
                title
            }
        }
    }
    '''
    data = {"query": query}
    async with aiohttp.ClientSession() as session:
        async with session.post(URL, headers=HEADERS, json=data) as response:
            response_data = await response.json()
            item = response_data['data']['items'][0]
            return f"{item['name']} - {item['group']['title']}"

    # for collection_name in monday_db.list_collection_names():
    #     print('Scanning in ', collection_name)
    #     collection = monday_db[collection_name]
    #     item = collection.find_one({'id': f"{item_id}"})
    #     if item:
    #         return f"{item['name'] - item['group']}"


