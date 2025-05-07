# Data Migration and Backup API

This project provides a FastAPI-based service for data ingestion, backup, and restoration using PySpark and MySQL. It includes functionality to migrate CSV data to a MySQL database, perform backups, and restore data from backups.

## Features

- **Data Ingestion**: Insert data into MySQL tables with schema validation.
- **Backup**: Create backups of MySQL tables.
- **Restore**: Restore tables from specific backup timestamps.
- **Schema Management**: Load table schemas dynamically from JSON files.

## Requirements

- Python 3.8+
- MySQL database
- PySpark
- FastAPI
- MySQL Connector JAR file

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up the `.env` file in the root directory with the following variables:
   ```env
   DATABASE_URL=jdbc:mysql://<host>:<port>/<database>
   DB_USER=<your-database-username>
   DB_PASS=<your-database-password>
   SPARK_TEMP_DIR=./temp
   SPARK_MYSQL_JAR=C:\spark-3.5.5-bin-hadoop3\jars\mysql-connector-j-8.0.33.jar
   SCHEMA_PATH=data/schema
   CSV_PATH=data/csv

4. Place your schema JSON files in the `data/schema` directory and your CSV files in the `data/csv` directory.

## Usage

### Running the API

Start the FastAPI server:
```bash
uvicorn api.main:app --reload
```

The API will be available at `http://127.0.0.1:8000`.

### Endpoints

#### **Ingest Data**
- **POST** `/ingest`
- **Description**: Insert data into a MySQL table.
- **Request Body**:
  ```json
  {
    "table": "table_name",
    "rows": [
      { "field1": "value1", "field2": "value2" }
    ]
  }
  ```

#### **Backup All Tables**
- **GET** `/backup`
- **Description**: Create backups for all tables.

#### **List Backup Timestamps**
- **GET** `/backup/{table_name}/timestamps`
- **Description**: List available backup timestamps for a table.

#### **Restore Table**
- **POST** `/restore/{table_name}/{timestamp}`
- **Description**: Restore a table to a specific backup timestamp.

### Data Migration Script

The `spark/migration.py` script migrates CSV data to MySQL tables. Run it as follows:
```bash
python migration.py
```

## Project Structure

```
.
├── api/
│   ├── main.py          # FastAPI application
├── spark/
│   ├── migration.py     # Data migration script
│   ├── backup_service.py # Backup and restore utilities
├── data/
│   ├── schema/          # JSON schema files
│   ├── csv/             # CSV data files
├── .env                 # Environment variables
├── requirements.txt     # Python dependencies
└── README.md            # Project documentation
```

## License

This project is licensed under the MIT License.
```
