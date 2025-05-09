
from datetime import datetime, timedelta
import os
import sys
import json
import logging
from pathlib import Path
from typing import List
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, Query, status
from fastapi.responses import HTMLResponse
from pyspark.sql.functions import year, quarter, col
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from pydantic import BaseModel, Field, model_validator
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import to_timestamp, col
from spark.backup_service import backup_table, list_backup_timestamps, restore_table 
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
import numpy as np 

app = FastAPI(title="API")

basedir = Path(__file__).resolve().parent
env_path = basedir / '.env'
load_dotenv(dotenv_path=env_path)

print("sys.executable ->", sys.executable)

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# --------------------------------------------------
# set up a module-level logger
logger = logging.getLogger("ingest")
if not logger.handlers:
    fh = logging.FileHandler("ingest_errors.log")
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.setLevel(logging.INFO)
# --------------------------------------------------

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

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

if not (SECRET_KEY and USER and PASSWORD):
    raise RuntimeError("SECRET_KEY, USER, and PASSWORD must be set in .env")


# -----------------------------------------------------------------------------
# JWT & OAuth2 setup
# -----------------------------------------------------------------------------
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def create_access_token(sub: str, expires_delta: timedelta):
    to_encode = {"sub": sub, "exp": datetime.utcnow() + expires_delta}
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user = payload.get("sub")
        if user != USER:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return user

# -----------------------------------------------------------------------------
# token endpoint
# -----------------------------------------------------------------------------
app = FastAPI(title="Simplified Secure API")

@app.post("/token")
async def login(form: OAuth2PasswordRequestForm = Depends()):
    if form.username != USER or form.password != PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token(
        sub=USER,
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    return {"access_token": token, "token_type": "bearer"}


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

@app.post("/ingest", dependencies=[Depends(get_current_user)])
async def ingest_data(req: IngestRequest):
    schema = table_schemas[req.table]

    for row in req.rows:
        for field in schema.fields:
            if field.name not in row and not field.nullable:
                msg = f"Missing required field '{field.name}' in row: {row}"
                logger.error(msg)
                raise HTTPException(status_code=422, detail=msg)

    raw_df = SparkSession.builder.getOrCreate().createDataFrame(req.rows)

    for field in schema.fields:
        if field.name in raw_df.columns and isinstance(field.dataType, TimestampType):
            if not field.nullable:
                null_count = raw_df.filter(col(field.name).isNull()).count()
                if null_count > 0:
                    msg = f"Field '{field.name}' contains nulls but is non-nullable—rows: {null_count}"
                    logger.error(msg)
                    raise HTTPException(status_code=422, detail=msg)

            raw_df = raw_df.withColumn(
                field.name,
                to_timestamp(col(field.name), "yyyy-MM-dd HH:mm:ss")
            )

    try:
        raw_df.createOrReplaceTempView("temp_data")
        select_parts = []
        for field in schema.fields:
            if field.name in raw_df.columns:
                if isinstance(field.dataType, TimestampType):
                    select_parts.append(
                        f"to_timestamp(`{field.name}`, 'yyyy-MM-dd HH:mm:ss') as `{field.name}`"
                    )
                else:
                    select_parts.append(
                        f"CAST(`{field.name}` AS {field.dataType.simpleString()}) as `{field.name}`"
                    )
            elif field.nullable:
                select_parts.append(f"NULL as `{field.name}`")

        df = SparkSession.builder.getOrCreate().sql(
            f"SELECT {', '.join(select_parts)} FROM temp_data"
        )
    except Exception as e:
        msg = f"Schema validation error: {e}"
        logger.error(msg, exc_info=True)
        raise HTTPException(status_code=422, detail=msg)

    # 5) Write to DB
    try:
        jdbc_url = os.getenv("DATABASE_URL")
        props = {
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASS", ""),
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        df.write.jdbc(
            url=jdbc_url,
            table=req.table,
            mode="append",
            properties=props
        )
    except Exception as e:
        msg = f"Error writing to database: {e}"
        logger.error(msg, exc_info=True)
        raise HTTPException(status_code=500, detail=msg)

    return {"status": "success", "ingested": len(req.rows)}

# -----------------------------------------------------------------------------
# /backup
# -----------------------------------------------------------------------------
@app.get("/backup", dependencies=[Depends(get_current_user)])
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
@app.get("/backup/{table_name}/timestamps", dependencies=[Depends(get_current_user)])
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
@app.post("/restore/{table_name}/{timestamp}", dependencies=[Depends(get_current_user)])
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

# -----------------------------------------------------------------------------
# /report hiring_quarterly
# -----------------------------------------------------------------------------
def get_hiring_quarterly_data(year_int: int) -> List[dict]:
    """
    Synchronous helper that returns exactly:
      [ {"department":…, "job":…, "quarter":…, "count":…}, … ]
    """
    jdbc_url = os.getenv("DATABASE_URL")
    props = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS", ""),
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    subq = """
      (SELECT he.datetime, d.department, j.job
       FROM hired_employees he
       JOIN departments d ON he.department_id = d.id
       JOIN jobs j        ON he.job_id        = j.id
      ) AS subq
    """

    df = (
        spark.read
             .jdbc(url=jdbc_url, table=subq, properties=props)
             .filter(year(col("datetime")) == year_int)
             .withColumn("quarter", quarter(col("datetime")))
             .groupBy("department", "job", "quarter")
             .count()
             .orderBy("department", "job", "quarter")
    )

    # This **must** run and return a list (even if empty)
    return [row.asDict() for row in df.collect()]

@app.get("/metrics/hiring_quarterly/{target_year}", dependencies=[Depends(get_current_user)])
async def hiring_quarterly(target_year: int):
    data = get_hiring_quarterly_data(target_year)
    return {"year": target_year, "data": data}

@app.get(
    "/reports/hiring.html",
    response_class=HTMLResponse
)
async def hiring_quarterly_report_html(
    year: int = Query(2021, description="Year to report on")
):
    # 1) Reuse your existing helper
    data = get_hiring_quarterly_data(year)

    # 2) Build DataFrame
    df = pd.DataFrame(data)
    if df.empty:
        return HTMLResponse(f"""
        <html><body>
          <h1>No hiring data found for {year}</h1>
        </body></html>
        """, status_code=200)

    # 3) Pivot: index=quarter, columns=department, values=sum(count)
    pivot = df.pivot_table(
        index="quarter",
        columns="department",
        values="count",
        aggfunc="sum",
        fill_value=0
    ).sort_index()

    # 4) Plot stacked bar chart
    quarters = pivot.index.tolist()
    departments = pivot.columns.tolist()
    bottoms = np.zeros(len(quarters))

    fig, ax = plt.subplots(figsize=(8, 5))
    for dept in departments:
        ax.bar(
            quarters,
            pivot[dept],
            bottom=bottoms,
            label=dept
        )
        bottoms += pivot[dept].values

    ax.set_xticks(quarters)
    ax.set_xlabel("Quarter")
    ax.set_ylabel("Number of Hires")
    ax.set_title(f"Number of Hires per Quarter by Department ({year})")
    ax.legend(title="Department", bbox_to_anchor=(1, 0.5))
    plt.tight_layout()

    # 5) Render to PNG in memory and base64-encode
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=100)
    plt.close(fig)
    buf.seek(0)
    img_b64 = base64.b64encode(buf.read()).decode("ascii")

    # 6) Return HTML with embedded image
    html = f"""
    <html>
      <head>
        <meta charset="utf-8"/>
        <title>Hiring Quarterly Report</title>
        <style>
          body {{ font-family: sans-serif; margin: 2rem; }}
          img {{ max-width: 100%; height: auto; }}
        </style>
      </head>
      <body>
        <h1>Hiring Quarterly Report ({year})</h1>
        <img src="data:image/png;base64,{img_b64}" alt="Hiring Quarterly Report">
      </body>
    </html>
    """
    return HTMLResponse(content=html)

# -----------------------------------------------------------------------------
# /report departments
# -----------------------------------------------------------------------------
def get_departments_above_average(year_int: int) -> List[dict]:
    """
    Returns:
      [
        {"id": 1, "department": "Maintenance", "count": 42},
        ...
      ]
    for departments whose 2021 hires > average hires across all departments.
    """
    jdbc_url = os.getenv("DATABASE_URL")
    props = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS", ""),
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    subq = """
      (SELECT he.datetime,
              d.id AS dept_id,
              d.department
       FROM hired_employees he
       JOIN departments d
         ON he.department_id = d.id
      ) AS subq
    """

    df = (
        spark.read
             .jdbc(url=jdbc_url, table=subq, properties=props)
             # filter by year
             .filter(year(col("datetime")) == year_int)
    )

    # aggregate per department
    agg = df.groupBy("dept_id", "department") \
            .count() \
            .withColumnRenamed("count", "hires")

    # compute mean
    mean_val = agg.agg({"hires": "avg"}).collect()[0][0]

    # filter above average, order desc
    above = (
        agg.filter(col("hires") > mean_val)
           .orderBy(col("hires").desc())
    )

    return [
        {"id": row["dept_id"], "department": row["department"], "count": row["hires"]}
        for row in above.collect()
    ]

@app.get(
    "/metrics/departments_above_average/{year_int}",
    dependencies=[Depends(get_current_user)]
)
async def metrics_departments_above_average(year_int: int):
    data = get_departments_above_average(year_int)
    return {"year": year_int, "data": data}

@app.get(
    "/reports/departments_above_average.html",
    response_class=HTMLResponse
)
async def departments_above_average_report_html(
    year: int = Query(2021, description="Year to report on")
):
    data = get_departments_above_average(year)
    if not data:
        return HTMLResponse(f"""
        <html><body>
          <h1>No data found for {year}</h1>
        </body></html>
        """, status_code=200)

    # build DataFrame
    df = pd.DataFrame(data)

    # pie chart
    fig, ax = plt.subplots(figsize=(6,6))
    ax.pie(
        df["count"],
        labels=df["department"],
        autopct="%1.1f%%",
        startangle=90
    )
    ax.axis("equal")
    ax.set_title(f"Departments Above Average Hires in {year}")

    # render to base64
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=100)
    plt.close(fig)
    buf.seek(0)
    img_b64 = base64.b64encode(buf.read()).decode("ascii")

    html = f"""
    <html>
      <head>
        <meta charset="utf-8"/>
        <title>Departments Above Average Hires</title>
        <style>
          body {{ font-family: sans-serif; margin:2rem; }}
          img {{ max-width: 100%; height: auto; }}
        </style>
      </head>
      <body>
        <h1>Departments Above Average Hires ({year})</h1>
        <img src="data:image/png;base64,{img_b64}" alt="Pie chart of departments">
      </body>
    </html>
    """
    return HTMLResponse(content=html)