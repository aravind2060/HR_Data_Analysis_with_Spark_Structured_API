import csv
import random

def generate_departments_data(filename, seed=42):
    random.seed(seed)  # Set the random seed for reproducibility
    
    fieldnames = ['department_id', 'department_name', 'manager_id']
    
    department_names = ['Marketing', 'Sales', 'IT', 'HR']

    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for i, department in enumerate(department_names, start=101):
            writer.writerow({
                'department_id': i,
                'department_name': department,
                'manager_id': 1000 + i  # Assign manager IDs starting from 1001
            })

if __name__ == "__main__":
    generate_departments_data('././input-data/departments.csv')
