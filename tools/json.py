# tools/json.py
import json
from pygments import highlight, lexers, formatters

def print_json(data):
    # Convert the Python dictionary to a JSON string
    formatted_json = json.dumps(data, indent=4)
    
    # Colorize the JSON string
    colorful_json = highlight(formatted_json, lexers.JsonLexer(), formatters.TerminalFormatter())
    
    # Print the colorized JSON
    print(colorful_json)