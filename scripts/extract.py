import sys
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_all():
    spark = SparkSession.builder \
    .appName("Healthcare_Extraction_layer") \
    .master("spark://spark:7077") \
    .config("spark.jars", "/opt/airflow/drivers/postgresql-42.7.10.jar") \
    .config("spark.driver.extraClassPath", "/opt/airflow/drivers/postgresql-42.7.10.jar") \
    .config("spark.executor.extraClassPath", "/opt/airflow/drivers/postgresql-42.7.10.jar") \
    .getOrCreate()
    
    base_input_path = "/opt/airflow/data/"
    base_output_path = "hdfs://namenode:9000/healthcare_raw_data/"
    
    tables = {
        "appointments": "appointments_parquet",
        "billing": "billing_parquet",
        "doctors": "doctors_parquet",
        "patients": "patients_parquet",
        "treatments": "treatments_parquet"
    }

    for file_name, hdfs_name in tables.items():
        try:
            input_path = f"{base_input_path}{file_name}.csv"
            output_path = f"{base_output_path}{hdfs_name}"
            
            logger.info(f"Processing {file_name}...")

            df = spark.read.csv(input_path, header=True, inferSchema=True)
            
            df.write.parquet(output_path, mode="overwrite")
            
            logger.info(f"Successfully extracted {file_name}")
            
        except Exception:
            logger.error(f"Error processing {file_name}")
            continue

    spark.stop()

if __name__ == "__main__":
    extract_all()