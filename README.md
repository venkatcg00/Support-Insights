# Support-Insights (Work In Progress)
A Synthetic call center data generator & ETL pipeline for insights into customer support performance.

## Features
- **Realistic call center data generation** using Python.
- **Web UI for monitoring & logs** using HTTPio library.
- **Dockerized Setup** for easy deployment
- **Upcoming:** ETL pipeline (Ariflow + Pyspark) & Power BI dashboard

## Tech Stack
- **Python, HTTPio** (for UI & data generator)
- **MySQL, MongoDB, MinIO (S3 compatible), Kafka Streams** (for data storage)
- **Docker & Docker Compose** (for containerization)
- **Airflow, Pyspark** (ETL - WIP)
- **Power BI** (for KPIs & analytics - WIP)

## Getting Started
1. Clone the repo:
   ```bash
   git clone https://github/com/venkatcg00/Support-Insights.git
   cd Support-Insights
   ```
2. Setup the Docker Containers:
   ```bash
   docker-compose up --build
   ```
3. Run the Python Generators (Wait for the docker to start all the containers):
   ```bash
   docker exec -it python-runner /bin/bash
   python /app/scripts/Data_Generator_Orchestrator.py
   ```
4. Access the Web UI at `http:localhost:1212`

## Roadmap
**Completed:**
- Docker Setup
- Data Generator
- Web UI to monitor the data generators

**In Progress**
- ETL pipeline with Airflow & PySpark
- Power BI dashboard
- Pytests
- CI/CD Integration using github actions
