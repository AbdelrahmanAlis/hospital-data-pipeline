import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_to_postgres(df, table_name):
    jdbc_url = "jdbc:postgresql://postgres_general:5432/hospital_db"
    db_properties = {
        "user": "admin",         
        "password": "admin",     
        "driver": "org.postgresql.Driver"
    }

    logger.info(f"Uploading {table_name} to PostgreSQL...")
    df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=db_properties)

def transform_gold():
    spark = SparkSession.builder \
    .appName("Healthcare_Gold_Layer") \
    .master("spark://spark:7077") \
    .config("spark.jars", "/opt/airflow/drivers/postgresql-42.7.10.jar") \
    .config("spark.driver.extraClassPath", "/opt/airflow/drivers/postgresql-42.7.10.jar") \
    .config("spark.executor.extraClassPath", "/opt/airflow/drivers/postgresql-42.7.10.jar") \
    .getOrCreate()
    #spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    
    try:
        logger.info("Reading from Silver Layer...")
        df_appointments = spark.read.parquet("hdfs://namenode:9000/healthcare_silver_data/appointments_cleaned")
        df_treatments = spark.read.parquet("hdfs://namenode:9000/healthcare_silver_data/treatments_cleaned")
        df_billing = spark.read.parquet("hdfs://namenode:9000/healthcare_silver_data/billing_cleaned")
        df_doctors = spark.read.parquet("hdfs://namenode:9000/healthcare_silver_data/doctors_cleaned")
        df_patients = spark.read.parquet("hdfs://namenode:9000/healthcare_silver_data/patients_cleaned")

        logger.info("Creating Fact Appointments...")
        fact_appointments = df_appointments \
            .join(df_treatments, "appointment_id", "left") \
            .join(df_billing, ["patient_id", "treatment_id"], "left") \
            .select(
                'appointment_id', 'patient_id', 'doctor_id', 'treatment_id',
                'date_id', 'status', 'amount', 'reason_for_visit'
            ).orderBy(f.col('amount').asc())

        dim_doctors = df_doctors.select(
                        'doctor_id',
                        'full_name',
                        'specialization',
                        'years_experience',
                        'hospital_branch'
                        ).orderBy(f.col('years_experience').asc())
        dim_patients = df_patients.select(
                    'patient_id',
                    'full_name',
                    'gender',
                    'age',
                    'address_region',
                    'insurance_provider'
                    ).orderBy(f.col('age').asc())

        dim_treatments = df_treatments.select(
                'treatment_id',
                'treatment_type',
                'description',
                'cost'
                 ).orderBy(f.col('cost').asc())   


        load_to_postgres(dim_doctors, "dim_doctors")
        load_to_postgres(dim_patients, "dim_patients")
        load_to_postgres(dim_treatments, "dim_treatments")
        # load_to_postgres(dim_date, "dim_date")
        load_to_postgres(fact_appointments, "fact_appointments")

        logger.info("Gold Layer Completed! Everything is in PostgreSQL.")

    except Exception as e:
        logger.error(f"Gold Layer Failed: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    transform_gold()