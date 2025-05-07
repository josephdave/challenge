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

# ----------------------------------------------------------
# list backup timestamps for a table
# ----------------------------------------------------------
def list_backup_timestamps(table_name: str) -> list[str]:
    tbl_dir = BACKUP_BASE_DIR / table_name
    if not tbl_dir.is_dir():
        raise FileNotFoundError(f"No backups found for table '{table_name}'")
    return sorted(
        p.name
        for p in tbl_dir.iterdir()
        if p.is_dir()
    )


# ----------------------------------------------------------
# Restore table from a backup timestamp
# ----------------------------------------------------------
def restore_table(table_name: str, timestamp: str) -> None:
    backup_path = BACKUP_BASE_DIR / table_name / timestamp
    if not backup_path.is_dir():
        raise FileNotFoundError(
            f"Backup '{timestamp}' for table '{table_name}' not found"
        )

    df = spark.read.format("avro").load(str(backup_path))

    jdbc_url = os.getenv("DATABASE_URL")
    props = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS", ""),
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    (
        df.write
          .format("jdbc")
          .option("url", jdbc_url)
          .option("dbtable", table_name)
          .option("user", props["user"])
          .option("password", props["password"])
          .option("driver", props["driver"])
          .mode("overwrite")
          .save()
    )