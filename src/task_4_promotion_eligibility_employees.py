from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def initialize_spark(app_name="Task4_Promotion_Eligibility"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def load_data(spark, emp_file_path, dept_file_path):
    employees_df = spark.read.csv(emp_file_path, header=True, inferSchema=True)
    departments_df = spark.read.csv(dept_file_path, header=True, inferSchema=True)
    return employees_df, departments_df

def find_promotion_eligible(employees_df, departments_df):
    # Filter employees based on tenure and performance score criteria
    eligible_employees_df = employees_df.filter(
        (col("tenure") >= 3) & (col("performance_score") >= 4.5)
    )
    
    # Join with departments dataset to get department_name
    result_df = eligible_employees_df.join(
        departments_df, on="department_id", how="inner"
    ).select(
        "employee_id", "name","department_name" ,"tenure", "performance_score", "salary"
    )
    
    return result_df


def write_output(result_df, output_path):
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    spark = initialize_spark()
    emp_file = "/workspaces/HR_Data_Analysis_with_Spark_Structured_API/input-data/employees.csv"
    dept_file = "/workspaces/HR_Data_Analysis_with_Spark_Structured_API/input-data/departments.csv"
    output_file = "/workspaces/HR_Data_Analysis_with_Spark_Structured_API/output/task4"
    
    employees_df, departments_df = load_data(spark, emp_file, dept_file)
    result_df = find_promotion_eligible(employees_df,departments_df)
    
    write_output(result_df, output_file)
    spark.stop()

if __name__ == "__main__":
    main()
