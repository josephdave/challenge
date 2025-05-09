import os
import sys
import json
from pathlib import Path
from typing import List
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, Query
import numpy as np
from pyspark.sql import SparkSession
from fastapi.responses import HTMLResponse
import pandas as pd, matplotlib.pyplot as plt, io, base64
from pyspark.sql.functions import year, quarter, col

from .auth import get_current_user

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

router = APIRouter(tags=["reports"], dependencies=[Depends(get_current_user)])


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

@router.get("/metrics/hiring_quarterly/{target_year}", dependencies=[Depends(get_current_user)])
async def hiring_quarterly(target_year: int):
    data = get_hiring_quarterly_data(target_year)
    return {"year": target_year, "data": data}

@router.get(
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

@router.get(
    "/metrics/departments_above_average/{year_int}",
    dependencies=[Depends(get_current_user)]
)
async def metrics_departments_above_average(year_int: int):
    data = get_departments_above_average(year_int)
    return {"year": year_int, "data": data}

@router.get(
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