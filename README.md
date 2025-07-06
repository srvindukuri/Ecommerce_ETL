# E-commerce ETL Project

This project showcases a complete ETL (Extract, Transform, Load) pipeline built using **Apache Airflow**, **PySpark**, **PostgreSQL**, and **Docker**. The pipeline simulates an E-commerce business scenario, handling data related to customers, orders, and products.

## 📁 Project Structure

Ecommerce\_ETL\_Project/
├── dags/                      # Airflow DAGs for scheduling and orchestration
├── data/                      # Input CSV files (orders, customers, products)
├── tmp/                       # Temporary staging directory
├── docker-compose.yaml        # Docker config for services
├── dockerfile                 # Dockerfile to build PySpark environment
├── requirements.txt           # Python package dependencies
└── README.md                  # Project documentation

## ⚙️ Technologies Used

- **Apache Airflow** – DAG scheduling and orchestration
- **Apache Spark (PySpark)** – Data transformation
- **PostgreSQL** – Target database for loading final data
- **Docker + Docker Compose** – Environment management
- **pgAdmin** – Database GUI for PostgreSQL

## 🔄 ETL Workflow Overview

1. **Extract**:
   - Reads raw CSV files from `/data` folder

2. **Transform**:
   - Cleans null/malformed data
   - Applies business logic and joins
   - Generates dimension and fact tables

3. **Load**:
   - Inserts cleaned/transformed data into PostgreSQL tables:
     - `dim_customers`
     - `dim_products`
     - `fact_orders`
     - `agg_order_metrics`

## 🚀 How to Run the Project

1. **Clone the Repository**

```bash
git clone https://github.com/srvindukuri/Ecommerce_ETL.git
cd Ecommerce_ETL

2. **Start Docker Containers**

```bash
docker-compose up --build

3. **Open Services**

* Airflow UI → [http://localhost:8080](http://localhost:8080)
  (Login: `admin` / `admin`)
* pgAdmin UI → [http://localhost:5050](http://localhost:5050)
  (Login: `admin@admin.com` / `admin`)

4. **Trigger DAG**

* In Airflow UI, enable and trigger `ecommerce_etl_dag`
* Monitor Spark job logs and task status

## 📊 Outputs

* **dim\_customers**: Clean customer profiles
* **dim\_products**: Standardized product details
* **fact\_orders**: All transactional sales data
* **agg\_order\_metrics**: Monthly revenue and order counts

## ✅ Features Covered

* PySpark partitioning & caching for performance
* Broadcast joins for dimension data
* Airflow DAGs with retry logic and task separation
* Dockerized environment for reproducibility
* PostgreSQL integration with pgAdmin UI

## 📌 Next Steps (Optional Improvements)

* Integrate Great Expectations for data quality checks
* Add unit tests for PySpark transformations
* Load to AWS S3 / GCS for cloud ETL practice
* Schedule daily automated DAG runs

## 👨‍💻 Author

**Ravi Varma Indukuri**
Aspiring Data Engineer | PySpark | Airflow | Docker | PostgreSQL

