from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

def initialize_spark(app_name="Task2_Avg_Tenure_Left_vs_Stayed"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def load_data(spark, emp_file_path, dept_file_path):
    employees_df = spark.read.csv(emp_file_path, header=True, inferSchema=True)
    departments_df = spark.read.csv(dept_file_path, header=True, inferSchema=True)
    return employees_df, departments_df

def calculate_avg_tenure(employees_df, departments_df):
    # Join the datasets on department_id
    joined_df = employees_df.join(departments_df, on="department_id", how="inner")
    
    # Group by department_name and left_company, then calculate average tenure
    avg_tenure_df = joined_df.groupBy("department_name", "left_company").agg(
        avg("tenure").alias("average_tenure")
    )
    
    # Fill missing values with "N/A" for cases where no employees left or stayed
    result_df = avg_tenure_df.fillna({"average_tenure": "N/A"})
    
    return result_df

def write_output(result_df, output_path):
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    spark = initialize_spark()
    emp_file = "/workspaces/HR_Data_Analysis_with_Spark_Structured_API/input-data/employees.csv"
    dept_file = "/workspaces/HR_Data_Analysis_with_Spark_Structured_API/input-data/departments.csv"
    output_file = "/workspaces/HR_Data_Analysis_with_Spark_Structured_API/output/task2"
    
    employees_df, departments_df = load_data(spark, emp_file, dept_file)
    result_df = calculate_avg_tenure(employees_df,departments_df)
    
    write_output(result_df, output_file)
    spark.stop()

if __name__ == "__main__":
    main()
