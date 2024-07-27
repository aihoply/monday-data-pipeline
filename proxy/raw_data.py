import re
import json
import sys

def clean_field_name(field_name):
    # Remove "link to", numbers, and dots
    clean_name = field_name.replace("link to", "")  # Remove "link to"
    clean_name = re.sub(r'\d+', '', clean_name)  # Remove all numbers
    clean_name = clean_name.replace(".", "")  # Remove dots
    return clean_name.strip()  # Remove any leading/trailing whitespace


def cleaning(item_object):
    # Basic item information
    cleaned_item = {
        'id': item_object['id'],
        'name': item_object['name'],
        'group': item_object['group']['title']
    }
    
    # Process column values
    if 'column_values' in item_object:
        for column in item_object['column_values']:
            # Sanitize the column title to use as the key
            key = clean_field_name(column['column']['title'])
            
            # Determine the value to use based on the type of the column
            if column['type'] in ['text', 'long_text', 'email', 'phone', 'link']:
                value = column['text'] if column['text'] not in [None, 'null', ''] else ""

            elif column['type'] in ['date', 'timeline']:
                value = column.get('date', '')

            elif column['type'] == 'people':
                # Assuming the people data needs to be simplified to names or IDs
                people = json.loads(column['value'])['personsAndTeams'] if column['value'] else []
                value = ', '.join([str(person['id']) for person in people])

            elif column['type'] == 'dropdown':
                value = column['text'] if column['text'] not in [None, 'null', ''] else ""

            elif column['type'] == 'mirror':
                value = column.get('display_value', '')

            elif column['type'] == 'status':
                value = column.get('label', '')

            elif column['type'] == 'checkbox':
                value = 'Checked' if column['checked'] == 'true' else 'Unchecked'

            elif column['type'] == 'file':
                # Extracting details from JSON formatted string
                try:
                    files_info = json.loads(column['value'])['files'] if column['value'] else []
                    value = [{ 'name': file['name'], 'link': file['linkToFile'] } for file in files_info]
                except json.JSONDecodeError:
                    value = column['value']

            elif column['type'] == 'country':
                # Parse JSON to extract country name
                try:
                    value = json.loads(column['value'])['country']['name'] if column['value'] else ""
                except json.JSONDecodeError:
                    value = column['value']

            elif column['type'] == 'rating':
                value = column.get('rating', '')

            elif column['type'] == 'board_relation':
                try:
                    # Parse JSON value if needed
                    value = json.loads(column['value']) if column['value'] else {}
                except json.JSONDecodeError:
                    value = column['value']

            elif column['value'] is not None and column['value'] not in ['null', '']:
                value = column['value']

            else:
                value = ''  # Default to empty string if no valid data
            
            # Assign the value to the sanitized key in the cleaned item
            cleaned_item[key] = value
        
    return cleaned_item
