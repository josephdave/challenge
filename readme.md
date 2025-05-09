# Data Migration and Backup API

This project provides a FastAPI-based service for data ingestion, backup, and restoration using PySpark and MySQL. It includes functionality to migrate CSV data to a MySQL database, perform backups, and restore data from backups. The API also includes secure authentication using JWT tokens.

## Features

- **Data Ingestion**: Insert data into MySQL tables with schema validation.
- **Backup**: Create backups of MySQL tables.
- **Restore**: Restore tables from specific backup timestamps.
- **Schema Management**: Load table schemas dynamically from JSON files.
- **Secure Authentication**: OAuth2 with JWT for secure access to endpoints.
- **Reports and Metrics**: Generate visual and tabular reports for hiring data and department performance.

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
   SECRET_KEY=<your-secret-key>
   ALGORITHM=HS256
   ACCESS_TOKEN_EXPIRE_MINUTES=30
   USER=<your-username>
   PASSWORD=<your-password>
   ```

4. Place your schema JSON files in the `data/schema` directory and your CSV files in the `data/csv` directory.

## Usage

### Running the API

Start the FastAPI server:
```bash
uvicorn api.main:app --reload
```

The API will be available at `http://127.0.0.1:8000`.

### Endpoints

#### **Token Endpoint**
- **POST** `/token`
- **Description**: Obtain a JWT token for authentication.
- **Request Body** (form-data example):
  ```bash
  curl -X POST http://l27.0.0.1:8000/token \
    -F "username=<username>" \
    -F "password=<password>>"
  ```
  ```
- **Response**:
  ```json
  {
    "access_token": "your-jwt-token",
    "token_type": "bearer"
  }
  ```

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
- **Authentication**: Requires a valid JWT token in the `Authorization` header.

#### **Backup All Tables**
- **GET** `/backup`
- **Description**: Create backups for all tables.
- **Authentication**: Requires a valid JWT token in the `Authorization` header.

#### **List Backup Timestamps**
- **GET** `/backup/{table_name}/timestamps`
- **Description**: List available backup timestamps for a table.
- **Authentication**: Requires a valid JWT token in the `Authorization` header.

#### **Restore Table**
- **POST** `/restore/{table_name}/{timestamp}`
- **Description**: Restore a table to a specific backup timestamp.
- **Authentication**: Requires a valid JWT token in the `Authorization` header.

#### **Metrics: Hiring Quarterly**
- **GET** `/metrics/hiring_quarterly/{target_year}`
- **Description**: Retrieve the number of hires per quarter by department and job for a specific year.
- **Response**:
  ```json
  {
    "year": 2021,
    "data": [
      { "department": "Engineering", "job": "Developer", "quarter": 1, "count": 10 },
      { "department": "HR", "job": "Recruiter", "quarter": 2, "count": 5 }
    ]
  }
  ```

#### **Report: Hiring Quarterly (HTML)**
![Image](https://github.com/user-attachments/assets/520d633a-8441-45b8-a46c-fecb931d55bb)
- **GET** `/reports/hiring.html`
- **Description**: Generate a visual report (stacked bar chart) of hires per quarter by department for a specific year.
- **Query Parameters**:
  - `year`: The year to report on (default: 2021).
- **Response**: HTML page with an embedded chart.

#### **Metrics: Departments Above Average**
- **GET** `/metrics/departments_above_average/{year_int}`
- **Description**: Retrieve departments with above-average hires for a specific year.
- **Response**:
  ```json
  {
    "year": 2021,
    "data": [
      { "id": 1, "department": "Engineering", "count": 50 },
      { "id": 2, "department": "HR", "count": 30 }
    ]
  }
  ```

#### **Report: Departments Above Average (HTML)**
![Image](https://github.com/user-attachments/assets/2c0e04f1-b116-46d7-9fe6-440c91076120)
- **GET** `/reports/departments_above_average.html`
- **Description**: Generate a visual report (pie chart) of departments with above-average hires for a specific year.
- **Query Parameters**:
  - `year`: The year to report on (default: 2021).
- **Response**: HTML page with an embedded chart.

### Data Migration Script

The `spark/migration.py` script migrates CSV data to MySQL tables. Run it as follows:
```bash
python spark/migration.py
```

## Security

- **Authentication**: All endpoints (except `/token`) require a valid JWT token for access.
- **Environment Variables**: Sensitive information such as database credentials and secret keys are stored in the `.env` file and loaded securely.

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
