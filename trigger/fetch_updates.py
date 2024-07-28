# trigger/fetch_updates.py
from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from pymongo import UpdateOne
import asyncio
from concurrent.futures import ThreadPoolExecutor
from db.conect_mongo import monday_db  # Assuming monday_db is correctly initialized as a MongoDB database
from query_monday.items import get_updates_of_items, get_all_items
from tools.string import remove_leading_number
from query_monday.boards import get_all_boards

def format_update(update):
    """Format a single update into a specified string format."""
    person = update['creator']['account']['name']
    time = update['created_at']
    message = update['text_body'].replace('\n', ' ').strip()  # Clean up newlines and strip extra spaces
    return f"{person} - {time} - {message}"

async def clean_and_store_updates(board, executor):
    """Process updates for a single board and perform bulk update in MongoDB."""
    collection_name = remove_leading_number(board['name'])
    all_items = await get_all_items(board_id=board['id'])

    item_ids = [item['id'] for item in all_items]
    batch_size = 100
    bulk_operations = []

    for i in range(0, len(item_ids), batch_size):
        batch_ids = item_ids[i:i + batch_size]
        updates = await get_updates_of_items(batch_ids)  # Fetch updates for a batch of IDs

        for item in updates:
            if item['updates']:
                # Clean and format updates using ThreadPoolExecutor
                loop = asyncio.get_running_loop()
                clean_tasks = [loop.run_in_executor(executor, format_update, update) for update in item['updates']]
                cleaned_updates_list = await asyncio.gather(*clean_tasks)

                if cleaned_updates_list:
                    # Prepare bulk update operation
                    bulk_operations.append(
                        UpdateOne(
                            {'id': item['id']},
                            {'$set': {'update comments': cleaned_updates_list}}
                        )
                    )

    if bulk_operations:
        # Execute all bulk operations at once
        collection = monday_db[collection_name]
        try:
            result = collection.bulk_write(bulk_operations)
            print(f"Bulk update completed: {result.bulk_api_result}")
        except Exception as e:
            print(f"Failed to perform bulk update in MongoDB: {e}")

    print(f"Finished processing updates for board {board['name']}.")

async def fetch_all_updates():
    """Main function to process updates for all boards."""
    all_boards = get_all_boards()
    print("Fetching all boards...")

    with ThreadPoolExecutor() as executor:
        board_tasks = [clean_and_store_updates(board, executor) for board in all_boards['data']['boards'] if board['type'] == 'board']
        await asyncio.gather(*board_tasks)

    print("All updates have been processed and stored.")


# asyncio.run(fetch_all_updates())