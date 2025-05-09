import os
import sys
import json
from pathlib import Path
from typing import List
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field, model_validator
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import to_timestamp, col
from .auth import get_current_user
from spark.backup_service import backup_table, list_backup_timestamps, restore_table 

router = APIRouter(prefix="/backup", tags=["backup"], dependencies=[Depends(get_current_user)])

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
# /backup
# -----------------------------------------------------------------------------
@router.get("")
async def backup_all():
    results = {}
    for tbl in table_schemas:
        try:
            results[tbl] = str(backup_table(tbl))
        except Exception as e:
            results[tbl] = f"error: {e}"
    return {"status": "success", "backups": results}


# -----------------------------------------------------------------------------
# /backup list
# -----------------------------------------------------------------------------
@router.get("/{table_name}/timestamps")
async def get_timestamps(table_name: str):
    if table_name not in table_schemas:
        raise HTTPException(404, f"Unknown table '{table_name}'")
    try:
        ts = list_backup_timestamps(table_name)
    except FileNotFoundError as e:
        raise HTTPException(404, detail=str(e))
    return {"table": table_name, "timestamps": ts}

# -----------------------------------------------------------------------------
# /restore to timestamp
# -----------------------------------------------------------------------------
@router.post("/{table_name}/{timestamp}")
async def restore_from_backup(table_name: str, timestamp: str):
    if table_name not in table_schemas:
        raise HTTPException(404, f"Unknown table '{table_name}'")
    try:
        restore_table(table_name, timestamp)
    except FileNotFoundError as e:
        raise HTTPException(404, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Restore failed: {e}")
    return {"status": "success", "table": table_name, "restored_from": timestamp}