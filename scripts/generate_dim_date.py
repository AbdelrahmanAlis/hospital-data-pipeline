from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import subprocess

def check_hdfs_path_exists(path):
    """
    Helper to check if the HDFS directory already exists.
    This prevents re-running logic if the dimension is already seeded.
    """
    try:
        # Using subprocess to check HDFS status
        result = subprocess.run(['hdfs', 'dfs', '-test', '-d', path])
        return result.returncode == 0
    except:
        return False

def generate_dim_date(spark):
    output_path = "hdfs://namenode:9000/healthcare_silver_data/dim_date"
    
    # Check if we already have the date dimension
    if check_hdfs_path_exists(output_path):
        print(f"Dimension table already exists at {output_path}. Skipping generation.")
        return

    print("Generating Date Dimension...")
    
    # Generate the range of dates
    df_date = spark.sql("SELECT sequence(to_date('2020-01-01'), to_date('2026-12-31'), interval 1 day) as date_range") \
                   .withColumn("date", f.explode(f.col("date_range")))
    
    # Transform into calendar attributes
    dim_date = df_date.select(
        f.col("date").alias("date_key"),
        f.year(f.col("date")).alias("year"),
        f.month(f.col("date")).alias("month"),
        f.dayofmonth(f.col("date")).alias("day"),
        f.date_format(f.col("date"), "EEEE").alias("day_name"),
        # In Spark, 1=Sunday, 7=Saturday. Adjusting for standard weekend logic.
        f.when(f.dayofweek(f.col("date")).isin(1, 7), True).otherwise(False).alias("is_weekend")
    )
    
    # Use 'ignore' mode: If the path exists, it will do nothing.
    # This is the safest way to ensure "run once" behavior.
    dim_date.write.parquet(output_path, mode="ignore")
    print("Date Dimension generation complete.")

if __name__ == "__main__":
    # Initialize Spark Session
    # Note: Using .master("local[*]") is often more stable when running via Airflow 
    # but I kept your Spark Master config as you are using the Spark Cluster containers.
    spark = SparkSession.builder \
        .appName("Generate_Dim_Date") \
        .master("spark://spark:7077") \
        .config("spark.jars", "/opt/airflow/drivers/postgresql-42.7.10.jar") \
        .config("spark.driver.extraClassPath", "/opt/airflow/drivers/postgresql-42.7.10.jar") \
        .config("spark.executor.extraClassPath", "/opt/airflow/drivers/postgresql-42.7.10.jar") \
        .getOrCreate()

    try:
        generate_dim_date(spark)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        spark.stop()