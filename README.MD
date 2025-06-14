# FPSO Failure Sensors Data Pipeline

## Project Overview

This project implements a robust data pipeline for processing equipment failure sensor data collected from FPSO (Floating Production Storage and Offloading) vessels. Using a Medallion Architecture, the pipeline ingests raw log files, refines and enriches the data, and stores results in a PostgreSQL database. An interactive Streamlit dashboard provides key insights and visualizations.

## Features

- **Medallion Architecture** (Bronze, Silver, Gold) for progressive data refinement
- **Apache Spark (PySpark)** for scalable data processing
- **PostgreSQL** for reliable storage of structured data
- **Streamlit Dashboard** for real-time analytics and visualizations
- **Docker & Docker Compose** for containerized deployment
- **Environment-configurable** via `.env` file

## Prerequisites

- **Docker** and **Docker Compose** installed on your machine
- `.env` file with database and pgAdmin credentials
- Input data files:
  - `equipment_failure_sensors.txt`
  - `equipment_sensors.csv`
  - `equipment.json`

## Setup & Usage

Follow these steps to get the pipeline up and running:

1. **Create Environment Variables**\
   Create in the project root the file `.env`\
   It should contain:

   ```ini
   POSTGRES_USER=shape
   POSTGRES_PASSWORD=shape
   POSTGRES_DB=fpso
   PGADMIN_DEFAULT_EMAIL=shape@shape.com
   PGADMIN_DEFAULT_PASSWORD=shape
   ```
   Note: For add database in pgadmin, use hostname: host.docker.internal

2. **Prepare Data Directory**\
   Create a `data` folder in the project root and add the following files:

   ```text
   data/
   ├── equipment_failure_sensors.txt
   ├── equipment_sensors.csv
   └── equipment.json
   ```

3. **Build Docker Images**\
   From the project root, run:

   ```bash
   docker-compose build
   ```

4. **Start Core Services**\
   Launch PostgreSQL, pgAdmin, and other background services in detached mode:

   ```bash
   docker-compose up -d
   ```

5. **Run the Spark Pipeline**\
   Execute the data processing pipeline using the Spark container:

   ```bash
   docker-compose up spark
   ```

6. **Launch the Dashboard (Optional)**\
   Start the Streamlit dashboard to explore results:

   ```bash
   docker-compose up dashboard
   ```

   Access it at [http://localhost:8501](http://localhost:8501).

## Project Structure

```
├── docker-compose.yml       # Docker Compose configuration
├── Dockerfile               # Spark Docker image build instructions
├── spark-defaults.conf      # Spark configuration (JDBC, timezone)
├── requirements.txt         # Python package dependencies
├── .env                     # Environment variables (not committed)
├── data/                    # Raw input data files
├── scripts/                 # PySpark pipeline scripts
└── dash.py                  # Streamlit dashboard application
```

## Technologies & Tools

- **Apache Spark (PySpark)**
- **PostgreSQL & pgAdmin**
- **Docker & Docker Compose**
- **Streamlit**, **Pandas**, **Altair**, **SQLAlchemy**

## License

This project is released under the MIT License.

