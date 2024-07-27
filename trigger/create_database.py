# trigger/create_base.py
from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import asyncio
from concurrent.futures import ThreadPoolExecutor
from db.conect_mongo import monday_db  # Assuming monday_db is correctly initialized as a MongoDB database
from query_monday.items import get_all_items
from tools.string import remove_leading_number
from proxy.raw_data import cleaning
from tools.json import print_json
from query_monday.boards import get_all_boards
from trigger import create_employee_list, create_group_list, fetch_updates, processing_each_board

def clean_item(item):
    return cleaning(item)

async def clean_and_store_items(board, executor):
    collection_board_name = remove_leading_number(board['name'])
    raw_base_all_items = await get_all_items(board_id=board['id'])

    # Create a list of tasks for cleaning each item using ThreadPoolExecutor
    loop = asyncio.get_running_loop()
    clean_tasks = [loop.run_in_executor(executor, clean_item, item) for item in raw_base_all_items]
    cleaned_items_list = await asyncio.gather(*clean_tasks)

    print('Done cleaned Item, Example:')
    if cleaned_items_list:
        print_json(cleaned_items_list[0])

    # Save cleaned data to MongoDB in the collection named after the board
    collection = monday_db[collection_board_name]
    if cleaned_items_list:
        try:
            collection.insert_many(cleaned_items_list)
            print(f"Successfully inserted {len(cleaned_items_list)} items into MongoDB.")
        except Exception as e:
            print(f"Failed to insert items into MongoDB: {e}")
    else:
        print(f"No items to insert for board {board['name']}.")

async def rereate_monday_database():

    # Fetch all boards from Monday.com API
    all_boards = get_all_boards()
    print("Get all boards:")

    for board in all_boards:
        board = remove_leading_number(board)
        monday_db.drop_collection(board)

    # Create a ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        # Create tasks for each board to process in parallel
        board_tasks = [clean_and_store_items(board, executor) for board in all_boards['data']['boards'] if board['type'] == 'board']
        # Execute all board tasks concurrently
        await asyncio.gather(*board_tasks)

    processing_each_board.process_all_boards()

    fetch_updates.fetch_all_updates()

    create_employee_list.recreate_employee_list()

    create_group_list.recreate_group_list()

    print("All data has been cleaned and stored in MongoDB.")


