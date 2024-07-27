from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import json
import asyncio
from query_monday.items import get_all_items, get_updates_of_items
from db.conect_mongo import monday_db  # Correcting typo in the import statement
from proxy.raw_data import cleaning
from trigger.fetch_updates import format_update
import os
from dotenv import load_dotenv
from proxy.board_relation_handler import convert_connected_board_values

load_dotenv()

BOARD_IDS = json.loads(os.getenv("REAL_BOARD_IDS"))

async def recreate_board(board_id):
    board_name = next((name for name, id in BOARD_IDS.items() if id == board_id), None)
    if board_name:
        collection = monday_db[board_name]
        collection.drop()  # Drop the existing collection

        raw_board_items = await get_all_items(board_id)
        item_ids = [item['id'] for item in raw_board_items]

        # Handling large numbers of item IDs by splitting them into batches
        batch_size = 100
        all_item_updates = {}
        for i in range(0, len(item_ids), batch_size):
            batch_item_ids = item_ids[i:i + batch_size]
            updates = await get_updates_of_items(item_ids=batch_item_ids)
            for update in updates:
                all_item_updates[update["id"]] = update["updates"]  # Assuming update structure contains 'updates' list

        board_items = []
        for item in raw_board_items:
            cleaned_item = cleaning(item)
            item_updates = [format_update(update) for update in all_item_updates.get(item['id'], [])]
            cleaned_item["update comments"] = item_updates
            board_items.append(cleaned_item)

        # Insert the new items into the newly created collection
        if board_items:
            collection.insert_many(board_items)

        # Convert connected board values after insertion
        convert_connected_board_values(board_name)
        print(f"Recreated and populated collection for board '{board_name}' with updated items.")


