import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_most_common(df, column_name):
    result = df.filter(f.col(column_name).isNotNull()) \
               .groupBy(column_name).count() \
               .orderBy(f.desc("count")).first()
    return result[0] if result else "UNKNOWN"

def process_doctors(spark):

    df = spark.read.parquet("hdfs://namenode:9000/healthcare_raw_data/doctors_parquet")
    df = df.drop_duplicates().dropna(how='all', subset=['doctor_id', 'first_name', 'last_name'])
    for col_name in ['first_name', 'last_name', 'specialization', 'hospital_branch']:
        df = df.withColumn(col_name, f.trim(f.initcap(f.col(col_name))))
    
    mean_exp = int(df.select(f.mean('years_experience')).collect()[0][0] or 0)
    most_common_spec = get_most_common(df, "specialization")
    
    df = df.fillna({
        'first_name': 'UNKNOWN',
        'last_name': 'UNKNOWN',
        'specialization': most_common_spec,
        'phone_number': 'UNKNOWN',
        'years_experience': mean_exp,
        'hospital_branch': 'Central Hospital'
    })
    
    df = df.withColumn(
        'full_name',
         f.concat_ws(' ', f.col('first_name'), f.col('last_name'))) \
         .withColumn('doctor_id', f.regexp_replace(f.col('doctor_id'), 'D', '').cast('int'))
    
    df.write.parquet("hdfs://namenode:9000/healthcare_silver_data/doctors_cleaned", mode="overwrite")

def process_patients(spark):
    df = spark.read.parquet("hdfs://namenode:9000/healthcare_raw_data/patients_parquet")
    
    df = df.withColumn('birth_date_year', f.year('date_of_birth')) \
           .withColumn('address_part', f.split(f.col('address'), ' ').getItem(1)) 
    
    df = df.withColumn('date_of_birth', f.to_date('date_of_birth')) \
           .withColumn('registration_date', f.to_date('registration_date'))
    
    common_gender = get_most_common(df, 'gender')
    common_year = get_most_common(df, 'birth_date_year')
    common_insurance = get_most_common(df, 'insurance_provider')
    
    df = df.drop_duplicates().dropna(how='all', subset=['patient_id', 'first_name', 'last_name'])
    
    df = df.fillna({
        'first_name': 'UNKNOWN', 'last_name': 'UNKNOWN',
        'gender': common_gender, 'date_of_birth': f"{common_year}-01-01",
        'insurance_provider': common_insurance
    })
    
    df = df.withColumn('registration_date', f.coalesce(f.col('registration_date'), f.current_date())) \
           .withColumn('age', f.year(f.current_date()) - f.col('birth_date_year')) \
           .withColumn('full_name', f.concat_ws(' ', f.initcap(f.col('first_name')), f.initcap(f.col('last_name'))))\
           .withColumn('patient_id', f.regexp_replace(f.col('patient_id'), 'P', '').cast('int'))\
           .withColumnRenamed('address_part','address_region')
    
    df.write.parquet("hdfs://namenode:9000/healthcare_silver_data/patients_cleaned", mode="overwrite")

def process_appointments(spark):
    df = spark.read.parquet("hdfs://namenode:9000/healthcare_raw_data/appointments_parquet")
    
    df = df.drop_duplicates().dropna(how='all', subset=['appointment_id','patient_id','doctor_id'])
    
    df = df.withColumn("appointment_timestamp", 
                       f.to_timestamp(f.concat_ws(" ", f.col("appointment_date"), f.col("appointment_time")), "yyyy-MM-dd HH:mm:ss")) \
           .withColumn('date_id', f.regexp_replace(f.col('appointment_date'), '-', '').cast('int'))\
           .withColumn('appointment_id', f.regexp_replace(f.col('appointment_id'), 'A', '').cast('int'))\
           .withColumn('patient_id', f.regexp_replace(f.col('patient_id'), 'P', '').cast('int'))\
           .withColumn('doctor_id', f.regexp_replace(f.col('doctor_id'), 'D', '').cast('int'))

    
    common_reason = get_most_common(df, 'reason_for_visit')
    common_status = get_most_common(df, 'status')
    
    df = df.withColumn('appointment_date', f.coalesce(f.col('appointment_date'), f.current_date())) \
           .fillna({'reason_for_visit': common_reason, 'status': common_status})\
           
    
    df.write.parquet("hdfs://namenode:9000/healthcare_silver_data/appointments_cleaned", mode="overwrite")

def process_billing(spark):
    df = spark.read.parquet("hdfs://namenode:9000/healthcare_raw_data/billing_parquet")
    
    df = df.drop_duplicates().dropna(how='all', subset=['bill_id', 'patient_id', 'amount'])
    
    df = df.withColumn('bill_date', f.to_date('bill_date'))\
           .withColumn('amount', f.col('amount').cast('float'))
    
    common_method = get_most_common(df, 'payment_method')
    common_status = get_most_common(df, 'payment_status')
    
    df = df.withColumn('bill_date', f.coalesce(f.col('bill_date'), f.current_date())) \
           .fillna({'payment_method': common_method, 'payment_status': common_status})\
           .withColumn('bill_id', f.regexp_replace(f.col('bill_id'), 'B', '').cast('int'))\
           .withColumn('patient_id', f.regexp_replace(f.col('patient_id'), 'P', '').cast('int'))\
           .withColumn('treatment_id', f.regexp_replace(f.col('treatment_id'), 'T', '').cast('int'))
    
    df.write.parquet("hdfs://namenode:9000/healthcare_silver_data/billing_cleaned", mode="overwrite")

def process_treatments(spark):
    df = spark.read.parquet("hdfs://namenode:9000/healthcare_raw_data/treatments_parquet")
    
    df = df.drop_duplicates().dropna(how='all', subset=['treatment_id','treatment_type'])
    
    df = df.withColumn('treatment_date', f.to_date('treatment_date'))
    df = df.withColumn('cost', f.col('cost').cast('float'))
    
    common_type = get_most_common(df, 'treatment_type')
    common_cost = get_most_common(df, 'cost')
    
    df = df.withColumn('treatment_date', f.coalesce(f.col('treatment_date'), f.current_date())) \
           .fillna({'treatment_type': common_type, 'cost': common_cost})\
           .withColumn('treatment_id', f.regexp_replace(f.col('treatment_id'), 'T', '').cast('int'))\
           .withColumn('appointment_id', f.regexp_replace(f.col('appointment_id'), 'A', '').cast('int'))
    
    df.write.parquet("hdfs://namenode:9000/healthcare_silver_data/treatments_cleaned", mode="overwrite")


def process_silver_layer():
    spark = SparkSession.builder \
    .appName("Healthcare_Silver_Layer") \
    .master("spark://spark:7077") \
    .config("spark.jars", "/opt/airflow/drivers/postgresql-42.7.10.jar") \
    .config("spark.driver.extraClassPath", "/opt/airflow/drivers/postgresql-42.7.10.jar") \
    .config("spark.executor.extraClassPath", "/opt/airflow/drivers/postgresql-42.7.10.jar") \
    .getOrCreate()
    try:
        process_doctors(spark)
        process_patients(spark)
        process_appointments(spark)
        process_billing(spark)
        process_treatments(spark)
        logger.info("Silver Layer Transformation Completed Successfully!")
    finally:
        spark.stop()

if __name__ == "__main__":
    process_silver_layer()