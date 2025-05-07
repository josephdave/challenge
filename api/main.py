import os
import sys
import json
from pathlib import Path
from typing import List
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, model_validator
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import to_timestamp, col
from spark.backup_service import backup_table  

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

app = FastAPI(title="API")


spark = (
    SparkSession.builder
    .appName("IngestService")
    .config("spark.driver.memory", "2g")
    .config("spark.local.dir", os.getenv("SPARK_TEMP_DIR", "./temp")) \
    .config("spark.jars", os.getenv("SPARK_MYSQL_JAR", "C:\spark-3.5.5-bin-hadoop3\jars\mysql-connector-j-8.0.33.jar")) \
    .config("spark.python.worker.reuse", "true")
    .config("spark.python.executable", sys.executable)
    .getOrCreate()
)

basedir = Path(__file__).resolve().parent
env_path = basedir / '.env'
load_dotenv(dotenv_path=env_path)


SCHEMA_DIR = os.getenv("SCHEMA_PATH", "data/schema")
table_schemas = {}
for fn in os.listdir(SCHEMA_DIR):
    if fn.startswith("schema_") and fn.endswith(".json"):
        table = fn[len("schema_"):-len(".json")]
        with open(os.path.join(SCHEMA_DIR, fn), "r") as f:
            schema_json = json.load(f)
            table_schemas[table] = StructType.fromJson(schema_json)


# -----------------------------------------------------------------------------
# Shema loader
# -----------------------------------------------------------------------------

class IngestRequest(BaseModel):
    table: str = Field(..., description="Target table name")
    rows: List[dict] = Field(
        ..., 
        min_items=1, 
        max_items=1000, 
        description="List of records to insert (1–1000)"
    )

    @model_validator(mode="before")
    def check_table_exists(cls, values):
        table = values["table"]
        if table not in table_schemas:
            raise ValueError(f"Unknown table '{table}'. Available: {list(table_schemas)}")
        return values


# -----------------------------------------------------------------------------
# /ingest
# -----------------------------------------------------------------------------

@app.post("/ingest")
async def ingest_data(req: IngestRequest):
    schema = table_schemas[req.table]

    # Fiel required check
    for row in req.rows:
        for field in schema.fields:
            # Si el campo no está en los datos y no es nullable
            if field.name not in row and not field.nullable:
                raise HTTPException(
                    status_code=422, 
                    detail=f"Field '{field.name}' is required but missing in input data"
                )
    
    
    raw_df = spark.createDataFrame(req.rows)
    
    # Timestamp conversion
    for field in schema.fields:
        if field.name in raw_df.columns and isinstance(field.dataType, TimestampType):
            if not field.nullable:
                null_count = raw_df.filter(col(field.name).isNull()).count()
                if null_count > 0:
                    raise HTTPException(
                        status_code=422, 
                        detail=f"Field '{field.name}' contains null values but is defined as non-nullable"
                    )
            
            raw_df = raw_df.withColumn(
                field.name,
                to_timestamp(col(field.name), "yyyy-MM-dd HH:mm:ss")
            )

    try:
        raw_df.createOrReplaceTempView("temp_data")
        
        # SQL Data conversion
        select_parts = []
        for field in schema.fields:
            if field.name in raw_df.columns:
                if isinstance(field.dataType, TimestampType):
                    select_parts.append(f"to_timestamp(`{field.name}`, 'yyyy-MM-dd HH:mm:ss') as `{field.name}`")
                else:
                    select_parts.append(f"CAST(`{field.name}` AS {field.dataType.simpleString()}) as `{field.name}`")
            elif field.nullable:
                select_parts.append(f"NULL as `{field.name}`")
        
        select_expr = ", ".join(select_parts)
        df = spark.sql(f"SELECT {select_expr} FROM temp_data")
        
        print("Schema after conversion:")
        df.printSchema()
        
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Schema validation error: {str(e)}")

    # Database writing
    jdbc_url = os.getenv("DATABASE_URL")
    props = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS", ""),
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    try:
        df.write.jdbc(
            url=jdbc_url,
            table=req.table,
            mode="append",
            properties=props
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error writing to database: {str(e)}")

    return {"status": "success", "ingested": len(req.rows)}

# -----------------------------------------------------------------------------
# /backup
# -----------------------------------------------------------------------------
@app.get("/backup")
async def backup_all():
    results = {}
    for tbl in table_schemas:
        try:
            results[tbl] = str(backup_table(tbl))
        except Exception as e:
            results[tbl] = f"error: {e}"
    return {"status": "success", "backups": results}