from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank


def initialize_spark(app_name="Task1_Top_2_Highest_Paid_Employees"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def load_data(spark, emp_file_path, dept_file_path):
    employees_df = spark.read.csv(emp_file_path, header=True, inferSchema=True)
    departments_df = spark.read.csv(dept_file_path, header=True, inferSchema=True)
    return employees_df, departments_df

def find_top_2_highest_paid(employees_df, departments_df):
    # Join the datasets on department_id
    joined_df = employees_df.join(departments_df, on="department_id", how="inner")
    
    # Define a window partitioned by department_name and ordered by salary descending
    window_spec = Window.partitionBy("department_name").orderBy(col("salary").desc())
    
    # Add a rank column
    ranked_df = joined_df.withColumn("rank", rank().over(window_spec))
    
    # Filter for top 2 employees in each department
    top_2_df = ranked_df.filter(col("rank") <= 2).select(
        "department_name", "employee_id", "name", "salary", "rank"
    )
    
    return top_2_df

def write_output(result_df, output_path):
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    spark = initialize_spark()
    emp_file = "/workspaces/HR_Data_Analysis_with_Spark_Structured_API/input-data/employees.csv"
    dept_file = "/workspaces/HR_Data_Analysis_with_Spark_Structured_API/input-data/departments.csv"
    output_file = "/workspaces/HR_Data_Analysis_with_Spark_Structured_API/output/task1"
    
    employees_df, departments_df = load_data(spark, emp_file, dept_file)
    result_df = find_top_2_highest_paid(employees_df,departments_df)
    
    write_output(result_df, output_file)
    spark.stop()

if __name__ == "__main__":
    main()
