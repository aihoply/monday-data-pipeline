# trigger/items.py
from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from query_monday.items import get_item
from proxy.raw_data import cleaning
from tools.json import print_json
from tools.string import remove_leading_number
from trigger.fetch_updates import format_update
from proxy.board_relation_handler import convert_connected_board_values_for_item
from db.conect_mongo import monday_db  # Correcting the typo in the import statement

async def get_item_and_yeet_to_mongo(item_id):
    # Get item metadata and update comments
    raw_item = await get_item(item_id)

    # Clean and process the item
    item = cleaning(raw_item)

    # Format update comments
    update_list = []
    for update in raw_item.get("updates", []):  # Safeguard with get in case 'updates' key is missing
        update_comments = format_update(update)
        update_list.append(update_comments)
    item["update comments"] = update_list

    # Convert connected board values
    item = convert_connected_board_values_for_item(item)

    # MongoDB operations
    board_name = remove_leading_number(raw_item["board"]["name"])
    collection = monday_db[board_name]

    # Find and update or insert the item
    update_result = collection.update_one(
        {"id": item["id"]},  # Assuming the '_id' is consistent and used for item identification
        {"$set": item},
        upsert=True  # Creates a new document if no document matches the filter
    )
    if update_result.matched_count:
        print(f"Updated existing item with id: {item['id']}")
    elif update_result.upserted_id:
        print(f"Inserted new item with id: {update_result.upserted_id}")
    else:
        print("No changes made to the database.")


async def delete_item_in_mongo(item_id):
    # Retrieve the item metadata to determine the collection it belongs to
    raw_item = await get_item(item_id)
    if not raw_item:
        print("Item not found or unable to retrieve item details.")
        return

    board_name = remove_leading_number(raw_item["board"]["name"])
    collection = monday_db[board_name]

    # Delete the item from MongoDB
    delete_result = collection.delete_one({"id": item_id})
    if delete_result.deleted_count:
        print(f"Successfully deleted item with id: {item_id}")
    else:
        print("No item found with the specified ID, nothing was deleted.")