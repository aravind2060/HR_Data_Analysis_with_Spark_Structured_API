from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, abs as spark_abs, greatest
from pyspark.sql import functions as F

def initialize_spark(app_name="Task3_Gender_Salary_Disparity"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def load_data(spark, emp_file_path, dept_file_path):
    employees_df = spark.read.csv(emp_file_path, header=True, inferSchema=True)
    departments_df = spark.read.csv(dept_file_path, header=True, inferSchema=True)
    return employees_df, departments_df

def find_salary_disparity(employees_df, departments_df):
    # Join the datasets on department_id
    joined_df = employees_df.join(departments_df, on="department_id", how="inner")
    
    # Calculate average salary grouped by department_name and gender
    gender_salary_df = joined_df.groupBy("department_name", "gender").agg(
        avg("salary").alias("average_salary")
    )
    
    # Pivot the data to have separate columns for male and female average salaries
    pivoted_df = gender_salary_df.groupBy("department_name").pivot("gender").agg(
        avg("average_salary")
    ).withColumnRenamed("F", "female_avg_salary").withColumnRenamed("M", "male_avg_salary")
    
    # Compute percentage difference
    disparity_df = pivoted_df.withColumn(
        "percent_difference",
        (spark_abs(col("male_avg_salary") - col("female_avg_salary")) / 
         greatest(col("male_avg_salary"), col("female_avg_salary"))) * 100
    )
    
    # Filter departments with significant disparity (>10%)
    significant_disparity_df = disparity_df.filter(col("percent_difference") > 0.2)
    
    return significant_disparity_df

def write_output(result_df, output_path):
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    spark = initialize_spark()
    emp_file = "/workspaces/HR_Data_Analysis_with_Spark_Structured_API/input-data/employees.csv"
    dept_file = "/workspaces/HR_Data_Analysis_with_Spark_Structured_API/input-data/departments.csv"
    output_file = "/workspaces/HR_Data_Analysis_with_Spark_Structured_API/output/task3"
    
    employees_df, departments_df = load_data(spark, emp_file, dept_file)
    result_df = find_salary_disparity(employees_df,departments_df)
    
    write_output(result_df, output_file)
    spark.stop()

if __name__ == "__main__":
    main()
