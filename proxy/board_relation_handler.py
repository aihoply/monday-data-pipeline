from db.conect_mongo import monday_db
import pymongo
import json

def convert_connected_board_values(board_name: str):
    collection = monday_db[board_name]
    items = collection.find({})
    collection_names = monday_db.list_collection_names()  # Cached collection names

    all_linked_ids = set()
    item_updates = []

    for item in items:
        for field_name, field_value in item.items():
            if isinstance(field_value, dict)  and 'linkedPulseIds' in field_value:
                try:
                    linked_ids_info = field_value
                    if 'linkedPulseIds' in linked_ids_info:
                        linked_ids = [str(link['linkedPulseId']) for link in linked_ids_info['linkedPulseIds']]
                        all_linked_ids.update(linked_ids)
                        item_updates.append((item['_id'], field_name, linked_ids))  # Save item ID, field, and linked IDs
                except json.JSONDecodeError:
                    continue

    # Fetch all linked documents in a batch
    linked_items = {}
    for col_name in collection_names:
        linked_collection = monday_db[col_name]
        for doc in linked_collection.find({"id": {"$in": list(all_linked_ids)}}):
            linked_items[doc['id']] = doc

    # Prepare bulk update operations
    bulk_updates = []
    for item_id, field_name, linked_ids in item_updates:
        result_list = [f"{linked_items[id].get('name', 'No Name')} - {linked_items[id].get('group', 'No Group')}" 
                       for id in linked_ids if id in linked_items]
        if result_list:
            update_data = {'$set': {field_name: result_list}}
            bulk_updates.append(pymongo.UpdateOne({'_id': item_id}, update_data))

    # Execute bulk update
    if bulk_updates:
        result = collection.bulk_write(bulk_updates)
        print(f"Bulk update completed with {result.modified_count} documents updated.")



def convert_connected_board_values_for_item(item: dict):
    collection_names = monday_db.list_collection_names()  # Cached collection names

    linked_ids = set()
    linked_data_updates = {}

    # Process the single item to find all linkedPulseIds
    for field_name, field_value in item.items():
        if isinstance(field_value, dict) and 'linkedPulseIds' in field_value:
            linked_ids_info = field_value
            try:
                linked_ids.update(str(link['linkedPulseId']) for link in linked_ids_info['linkedPulseIds'])
                linked_data_updates[field_name] = [str(link['linkedPulseId']) for link in linked_ids_info['linkedPulseIds']]
            except KeyError:
                continue

    # Fetch all linked documents
    linked_items = {}
    for col_name in collection_names:
        linked_collection = monday_db[col_name]
        if linked_ids:
            for doc in linked_collection.find({"id": {"$in": list(linked_ids)}}):
                linked_items[doc['id']] = doc

    # Format the linked data in the item
    for field_name, ids in linked_data_updates.items():
        result_list = [f"{linked_items[id].get('name', 'No Name')} - {linked_items[id].get('group', 'No Group')}"
                       for id in ids if id in linked_items]
        if result_list:
            item[field_name] = result_list

    return item
