from quart import Quart, request, jsonify
from trigger.boards import recreate_board
from trigger.items import get_item_and_yeet_to_mongo, delete_item_in_mongo

app = Quart(__name__)

@app.route('/webhook', methods=['POST'])
async def webhook():
    data = await request.get_json()  # Change to async call
    
    # Handle the 'challenge' verification
    challenge = data.get('challenge')
    if challenge:
        return jsonify({'challenge': challenge}), 200

    event = data.get('event', {})
    event_type = event.get('type', 'Unknown')
    print(f"Webhook received for {event_type}: {data}")

    # Handle pulse/item-related events
    if event_type in ['create_pulse', 'update_name', 'update_column_value', 'create_update']:
        item_id = event.get('pulseId')
        if item_id:
            await get_item_and_yeet_to_mongo(item_id)
            print(f"Processed item {event_type} for item ID: {item_id}")

    elif event_type == 'move_pulse_into_group':
        item_id = event.get('pulseId')
        new_group_id = event.get('destGroupId')
        if item_id and new_group_id:
            print(f"Item ID {item_id} moved to Group ID {new_group_id}")
            await get_item_and_yeet_to_mongo(item_id)

    elif event_type == 'delete_pulse':
        item_id = event.get('itemId')
        if item_id:
            await delete_item_in_mongo(item_id)
            print(f"Deleted item with ID: {item_id}")

    # Handle column-related events
    elif event_type == 'create_column':
        board_id = event.get('boardId')
        if board_id:
            await recreate_board(board_id)
            print(f"Processed creation of a new column for board ID: {board_id}")

    return jsonify({'status': 'success'}), 200

if __name__ == '__main__':
    app.run(port=5000, debug=True)
