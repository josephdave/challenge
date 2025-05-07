from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.types import TimestampType
import json

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType
)

def migrate_csv_to_sql(csv_path: str, table_name: str, schema=None):

    jdbc_url = os.getenv("DATABASE_URL")
    props = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    print(csv_path)
    
    spark = SparkSession.builder \
        .appName("CSVToSQL") \
        .config("spark.local.dir", os.getenv("SPARK_TEMP_DIR", "./temp")) \
        .config("spark.jars", os.getenv("SPARK_MYSQL_JAR", "C:\spark-3.5.5-bin-hadoop3\jars\mysql-connector-j-8.0.33.jar")) \
        .getOrCreate()
    
    # Load data from CSV
    reader = spark.read.option("header", False)
    if schema is not None:
        reader = reader.schema(schema)
    df = reader.csv(csv_path)

    df_clean = df.dropna()

    # Insert into the target table
    df_clean.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode="append",
        properties=props
    )

def load_schema_from_json(path: str) -> StructType:
    with open(path, "r", encoding="utf-8") as f:
        schema_dict = json.load(f)
    return StructType.fromJson(schema_dict)
    
if __name__ == "__main__":
    # Parameters taken from environment variables (MySQL)
    root_dir = Path(__file__).resolve().parent.parent
    dotenv_path = root_dir / ".env"
    load_dotenv(dotenv_path=dotenv_path)

    # MIGRATION OF EMPLOYEES
    schema_hired = load_schema_from_json(os.path.join(os.getenv("SCHEMA_PATH", "data/schema"), "schema_hired_employees.json"),)
    # schema_hired = StructType([
    #     StructField("id", IntegerType(), False),
    #     StructField("name", StringType(), True),
    #     StructField("datetime", TimestampType(), True),
    #     StructField("department_id", IntegerType(), True),
    #     StructField("job_id", IntegerType(), True),
    # ])

    migrate_csv_to_sql(
        csv_path=os.path.join(os.getenv("CSV_PATH", "data/csv"), "hired_employees.csv"),
        table_name="hired_employees",
        schema=schema_hired,
    )

    # MIGRATION OF DEPARTMENTS
    schema_depts = load_schema_from_json(os.path.join(os.getenv("SCHEMA_PATH", "data/schema"), "schema_depts.json"),)
    # schema_depts = StructType([
    #     StructField("id", IntegerType(), False),
    #     StructField("department", StringType(), True),
    # ])
    
    migrate_csv_to_sql(
        csv_path=os.path.join(os.getenv("CSV_PATH", "data/csv"), "departments.csv"),
        table_name="departments",
        schema=schema_depts
    )

    # MIGRATION OF JOBS
    schema_jobs = load_schema_from_json(os.path.join(os.getenv("SCHEMA_PATH", "data/schema"), "schema_jobs.json"),)
    # schema_jobs = StructType([
    #     StructField("id", IntegerType(), False),
    #     StructField("job", StringType(), True),
    # ])

    migrate_csv_to_sql(
        csv_path=os.path.join(os.getenv("CSV_PATH", "data/csv"), "jobs.csv"),
        table_name="jobs",
        schema=schema_jobs
    )
