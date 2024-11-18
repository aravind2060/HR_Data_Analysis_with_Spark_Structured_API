from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when

def initialize_spark(app_name="Task5_Turnover_Rate_Analysis"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def load_data(spark, emp_file_path, dept_file_path):
    employees_df = spark.read.csv(emp_file_path, header=True, inferSchema=True)
    departments_df = spark.read.csv(dept_file_path, header=True, inferSchema=True)
    return employees_df, departments_df

def analyze_turnover_rate(employees_df, departments_df):
    # Join the datasets on department_id
    joined_df = employees_df.join(departments_df, on="department_id", how="inner")
    
    # Calculate total employees and employees who left for each department
    turnover_df = joined_df.groupBy("department_name").agg(
        count("*").alias("total_employees"),
        count(when(col("left_company") == 1, 1)).alias("employees_left"),
        avg("salary").alias("average_salary")
    )
    
    # Calculate turnover rate
    result_df = turnover_df.withColumn(
        "turnover_rate",
        (col("employees_left") / col("total_employees")) * 100
    ).orderBy(col("turnover_rate").desc())
    
    return result_df

def write_output(result_df, output_path):
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    spark = initialize_spark()
    emp_file = "/workspaces/HR_Data_Analysis_with_Spark_Structured_API/input-data/employees.csv"
    dept_file = "/workspaces/HR_Data_Analysis_with_Spark_Structured_API/input-data/departments.csv"
    output_file = "/workspaces/HR_Data_Analysis_with_Spark_Structured_API/output/task5"
    
    employees_df, departments_df = load_data(spark, emp_file, dept_file)
    result_df = analyze_turnover_rate(employees_df,departments_df)
    
    write_output(result_df, output_file)
    spark.stop()

if __name__ == "__main__":
    main()
