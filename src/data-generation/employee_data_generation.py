import csv
import random

def generate_employees_data(filename, num_records=100, seed=42):
    random.seed(seed)  # Set the random seed for reproducibility
    
    fieldnames = ['employee_id', 'name', 'department_id', 'age', 'gender', 'tenure', 
                  'satisfaction_rating', 'performance_score', 'salary', 'left_company', 'last_performance_review_date']

    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(1, num_records + 1):
            writer.writerow({
                'employee_id': i,
                'name': f'a{i}',  # Sequential names: a1, a2, a3, etc.
                'department_id': random.randint(101, 104),  # Random department_id between 101 and 104
                'age': random.randint(25, 60),  # Random age between 25 and 60
                'gender': random.choice(['M', 'F']),  # Random gender
                'tenure': round(random.uniform(1.0, 10.0), 1),  # Random tenure between 1 and 10 years
                'satisfaction_rating': round(random.uniform(1.0, 5.0), 1),  # Satisfaction rating between 1 and 5
                'performance_score': round(random.uniform(1.0, 5.0), 1),  # Performance score between 1 and 5
                'salary': random.randint(50000, 100000),  # Random salary between 50k and 100k
                'left_company': random.randint(0, 1),  # Random 0 or 1 for left_company
                'last_performance_review_date': f'2023-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}'  # Random review date in 2023
            })

if __name__ == "__main__":
    generate_employees_data('././input-data/employees.csv', num_records=1000)
