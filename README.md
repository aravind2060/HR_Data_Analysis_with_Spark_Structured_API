# Employee Engagement Analysis with Spark

## 1. Introduction

This project aims to analyze employee data using Apache Spark's Structured APIs. You will perform various operations such as filtering, grouping, and aggregation to uncover insights that can help improve employee retention, satisfaction, and compensation at XYZ Corporation.

## 2. Prerequisites

You can run this project either locally using PySpark or via Docker. Below are the installation and execution steps for both options:

### Option 1: Local Setup with PySpark
1. Install PySpark using pip:
   ```bash
   pip install pyspark
   ```

2. To run the analysis:
   ```bash
   python src/task.py
   ```

### Option 2: Docker Setup
1. Docker compose up:
   ```bash
   docker compose up -d
   ```
2. Copy your source code into containers 

3. Execute the script:
   ```bash
   spark-submit task1_identify_departments_high_satisfaction.py
   ```

## 3. Overview & Objectives

### Overview

The goal of this project is to use Spark Structured APIs to analyze HR-related datasets and provide actionable insights. You will manipulate and analyze employee and department data, focusing on key metrics such as satisfaction, tenure, and performance.

### Objectives

- Utilize Spark Structured APIs for data manipulation and analysis.
- Perform advanced filtering, grouping, and aggregation operations.
- Derive insights on employee retention, satisfaction, and compensation.

### Dataset Introduction

You are provided with two CSV files:

1. **employees.csv**: Contains details about employees, including their department, age, tenure, satisfaction, salary, and whether they have left the company.
2. **departments.csv**: Contains information about the departments and their respective managers.

## 4. Assignment Tasks & Expected Output

### Task 1: Identify Top 2 Highest Paid Employees in Each Department

- Join the `employees` and `departments` datasets on `department_id`.
- Group by `department_name`, then order employees by `salary` within each group.
- Select the top 2 highest-paid employees in each department.

**Expected Output**:
```
department_name  | employee_id | name          | salary
---------------------------------------------------------
Marketing        | 3           | Carol Williams| 72000
Marketing        | 7           | Grace Wilson  | 70000
Sales            | 8           | Henry Moore   | 95000
Sales            | 5           | Emma Davis    | 90000
...
```

### Task 2: Calculate Average Tenure of Employees Who Left vs. Those Who Stayed

- Group by `department_name` and `left_company` status.
- Calculate the average tenure for both groups.

**Expected Output**:
```
department_name  | left_company | average_tenure
------------------------------------------------
Marketing        | 0            | 4.1
Marketing        | 1            | N/A
Sales            | 0            | 8.5
...
```

### Task 3: Determine Departments with Significant Gender-Based Salary Disparities

- Group by `department_name` and `gender`.
- Calculate the average salary by gender and identify departments where the salary difference exceeds 10%.

**Expected Output**:
```
department_name | gender | average_salary | salary_diff_percentage
-------------------------------------------------------------------
Marketing       | F      | 70000          | -
Marketing       | M      | N/A            | -
Sales           | F      | 90000          | 0%
...
```

### Task 4: Identify Employees Eligible for Promotion Based on Tenure and Performance Score

- Filter employees with tenure ≥ 3 years and performance score ≥ 4.5.
- List the eligible employees along with their details.

**Expected Output**:
```
employee_id | name          | department_name | tenure | performance_score
---------------------------------------------------------------------------
3           | Carol Williams| Marketing       | 5.1    | 4.8
5           | Emma Davis    | Sales           | 7.0    | 4.9
...
```

### Task 5: Identify Departments with the Highest Employee Turnover Rate and Average Salaries

- Group by `department_name` to calculate the turnover rate and average salary.
- Identify the departments with the highest turnover rates.

**Expected Output**:
```
department_name | turnover_rate (%) | average_salary
----------------------------------------------------
Sales           | 15.0              | 90000
IT              | 10.0              | 64500
...
```


---
