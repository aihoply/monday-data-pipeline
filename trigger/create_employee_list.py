# trigger/create_employee_list.py
from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from db.conect_mongo import monday_db
import logging

task_collections = [
    'FBT Employee Contribution', 'Quarterly BAS', 'SMSF Tax and FR', 'Company Tax and FR',
    'Individual Tax Return', 'General Admin Task', 'Monthly IAS', 'TPAR Lodgement',
    'Annual BAS', 'Payroll', 'Partnership Tax', 'Trust Tax and FR'
]

def recreate_employee_list():
    employee_tasks_collection = monday_db['Employee list']

    # Clear existing employee tasks
    employee_tasks_collection.delete_many({})

    employee_dict = {}

    for collection_name in task_collections:
        task_collection = monday_db[collection_name]
        tasks = list(task_collection.find({}))

        for task in tasks:
            # Handling both 'Person' and 'People' fields
            employee_names = task.get('Person') or task.get('People')
            if employee_names:
                # Split the names by comma and strip spaces
                for employee_name in [name.strip() for name in employee_names.split(',')]:
                    if employee_name:
                        if employee_name not in employee_dict:
                            employee_dict[employee_name] = {'tasks': {}}
                        
                        # Simplified task description format: "name - group_title"
                        task_description = f"{task.get('name', 'Unknown Task')} - {task.get('group', 'No Date')}"
                        employee_dict[employee_name].setdefault('tasks', {}).setdefault(collection_name, []).append(task_description)
            else:
                logging.warning(f"Task {task.get('id', 'Unknown')} in collection {collection_name} has no assigned person.")

    # Insert into employee collection
    for employee_name, employee_data in employee_dict.items():
        employee_tasks_collection.insert_one({
            'employee': employee_name,
            'tasks_assigned': employee_data['tasks']
        })


