# trigger/create_group_list.py
from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from db.conect_mongo import monday_db

task_collections = [
    'FBT Employee Contribution', 'Quarterly BAS', 'SMSF Tax and FR', 'Company Tax and FR',
    'Individual Tax Return', 'General Admin Task', 'Monthly IAS', 'TPAR Lodgement',
    'Annual BAS', 'Payroll', 'Partnership Tax', 'Trust Tax and FR'
]

def recreate_group_list():
    contact_collection = monday_db['Contact list']
    business_collection = monday_db['Business list']
    group_list_collection = monday_db['Group list']

    # Clear existing group list
    group_list_collection.delete_many({})

    # Fetch contacts and businesses
    contacts = list(contact_collection.find({}))
    businesses = list(business_collection.find({}))

    group_dict = {}

    # Process contacts with simplified format
    for contact in contacts:
        group_id = contact.get('Group ID')
        contact_summary = f"{contact.get('name', 'Unknown Contact')} - {contact.get('group', '')}"
        if group_id:
            group_dict.setdefault(group_id, {'contacts': [], 'businesses': [], 'tasks': {}})['contacts'].append(contact_summary)

    # Process businesses with simplified format
    for business in businesses:
        group_id = business.get('Group ID')
        business_summary = f"{business.get('name', 'Unknown Business')} - {business.get('group', '')} - {business.get('Industry', 'Unkown Industry')}"
        if group_id:
            group_dict.setdefault(group_id, {'contacts': [], 'businesses': [], 'tasks': {}})['businesses'].append(business_summary)

    # Process tasks with simplified format
    for collection_name in task_collections:
        task_collection = monday_db[collection_name]
        tasks = list(task_collection.find({}))

        for task in tasks:
            group_id = task.get('Group ID')
            if group_id:
                task_summary =  f"{task.get('name', 'Unknown Task')} - {task.get('group', '')}"
                
                # Ensure the dictionary is properly initialized
                if group_id not in group_dict:
                    group_dict[group_id] = {'contacts': [], 'businesses': [], 'tasks': {}}
                
                # Initialize the list for tasks under the collection if not already done
                group_dict[group_id]['tasks'].setdefault(collection_name, []).append(task_summary)

    # Insert into group list collection
    for group_id, group_data in group_dict.items():
        group_list_collection.insert_one({
            'group_id': group_id,
            'contacts': group_data['contacts'],
            'businesses': group_data['businesses'],
            'tasks': group_data['tasks']
        })
