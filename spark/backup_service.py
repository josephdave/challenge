import os
from datetime import datetime
from pathlib import Path
import sys
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from dotenv import load_dotenv


root_dir = Path(__file__).resolve().parent.parent
dotenv_path = root_dir / ".env"
load_dotenv(dotenv_path=dotenv_path)

BACKUP_BASE_DIR = Path(os.getenv("BACKUP_BASE_DIR", "backups"))


spark = (
    SparkSession.builder
    .appName("BackupService")
    .config("spark.driver.memory", "2g")
    .config("spark.jars.packages", 
            os.getenv("SPARK_AVRO_PACKAGE", "org.apache.spark:spark-avro_2.12:3.5.5"))
    .config("spark.jars", os.getenv("SPARK_MYSQL_JAR"))
    .config("spark.python.executable", sys.executable)
    .getOrCreate()
)

# ----------------------------------------------------------
# Backup table to AVRO files
# ----------------------------------------------------------
def backup_table(table_name: str) -> Path:

    jdbc_url = os.getenv("DATABASE_URL")
    props = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS", ""),
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # read the table
    df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", props["user"])
        .option("password", props["password"])
        .option("driver", props["driver"])
        .load()
    )

    # build output path with an ISO timestamp
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_dir = BACKUP_BASE_DIR / table_name / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    # write as Avro
    (
        df.write
          .format("avro")
          .mode("overwrite")
          .save(str(out_dir))
    )

    return out_dir
