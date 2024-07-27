# trigger/processing_each_board.py
from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from proxy.board_relation_handler import convert_connected_board_values
from db.conect_mongo import monday_db
from concurrent.futures import ThreadPoolExecutor

def process_all_boards():
    # Retrieve all board names from the database
    board_names = monday_db.list_collection_names()

    # Create a ThreadPoolExecutor to manage concurrency
    with ThreadPoolExecutor(max_workers=len(board_names)) as executor:
        # Map the convert_connected_board_values function to all board names
        futures = {executor.submit(convert_connected_board_values, board_name): board_name for board_name in board_names}
        
        # Process the results as they complete
        for future in futures:
            board_name = futures[future]
            try:
                # Ensure the future completes without errors
                future.result()
                print(f"Successfully processed board: {board_name}")
            except Exception as exc:
                print(f"Error processing board {board_name}: {exc}")


