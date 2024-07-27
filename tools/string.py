# tools/string.py
import re

def remove_leading_number(text):
    # This regex matches any leading digits followed by a dot and a space
    return re.sub(r'^\d+\.\s+', '', text)