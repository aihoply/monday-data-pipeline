from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from trigger.boards import recreate_board
from trigger.items import get_item_and_yeet_to_mongo, delete_item_in_mongo

# Create the FastAPI app
app = FastAPI()

# Define a Pydantic model for the data structure you expect (if applicable)
class Event(BaseModel):
    type: str
    pulseId: int = None
    destGroupId: int = None
    boardId: int = None

class WebhookData(BaseModel):
    event: Event
    challenge: str = None

# Define the route for handling webhooks
@app.post('/webhook')
async def webhook(data: WebhookData):
    # Handle the 'challenge' verification
    if data.challenge:
        return JSONResponse({'challenge': data.challenge}, status_code=status.HTTP_200_OK)

    event_type = data.event.type
    print(f"Webhook received for {event_type}: {data}")

    # Handle pulse/item-related events
    if event_type in ['create_pulse', 'update_name', 'update_column_value', 'create_update']:
        item_id = data.event.pulseId
        if item_id:
            await get_item_and_yeet_to_mongo(item_id)
            print(f"Processed item {event_type} for item ID: {item_id}")

    elif event_type == 'move_pulse_into_group':
        item_id = data.event.pulseId
        new_group_id = data.event.destGroupId
        if item_id and new_group_id:
            print(f"Item ID {item_id} moved to Group ID {new_group_id}")
            await get_item_and_yeet_to_mongo(item_id)

    elif event_type == 'delete_pulse':
        item_id = data.event.pulseId
        if item_id:
            await delete_item_in_mongo(item_id)
            print(f"Deleted item with ID: {item_id}")

    # Handle column-related events
    elif event_type == 'create_column':
        board_id = data.event.boardId
        if board_id:
            await recreate_board(board_id)
            print(f"Processed creation of a new column for board ID: {board_id}")

    return JSONResponse({'status': 'success'}, status_code=status.HTTP_200_OK)

# Start the FastAPI app
if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
