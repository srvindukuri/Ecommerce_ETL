from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import col, when, count, isnan
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import year, month

#Step:1 - Initialize Spark Session
def get_spark_session():
    return SparkSession.builder.appName("Ecommerce_ETL").config("spark.jars.packages", "org.postgresql:postgresql:42.3.1") \
        .getOrCreate()

# step:2 - Extract CSV's

def extract_csv():
    spark = get_spark_session()
    base_path = "D:/AirFlow_Projects/ETL_Projects/Ecomerce_etl_project/data"

    orders = spark.read.csv("/opt/airflow/data/orders.csv", header=True, inferSchema=True)
    customers = spark.read.csv("/opt/airflow/data/customers.csv", header=True, inferSchema=True)
    products = spark.read.csv("/opt/airflow/data/products.csv", header=True, inferSchema=True)

    orders.write.mode("overwrite").parquet("/opt/airflow/tmp/orders")
    customers.write.mode("overwrite").parquet("/opt/airflow/tmp/customers")
    products.write.mode("overwrite").parquet("/opt/airflow/tmp/products")

    spark.stop()


#step:3 - Transform with Pyspark

def transform_with_pyspark():
    spark = get_spark_session()

    # Step 1: Read parquet files
    orders = spark.read.parquet("/opt/airflow/tmp/orders")
    customers = spark.read.parquet("/opt/airflow/tmp/customers")
    products = spark.read.parquet("/opt/airflow/tmp/products")

    #Step 2: Cache frequently reused datasets
    customers.cache()
    products.cache()

    #Step 3: Broadcast dimension tables (assuming they’re small)
    enriched = orders.join(broadcast(customers), "customer_id", "left").join(broadcast(products), "product_id", "left")

    # #Join Transformations
    # enriched = orders.join(customers, "customer_id","left").join(products, "product_id","left")

    # step 4 :Transformation - Calucating total order value
    enriched = enriched.withColumn("total_Price", enriched.quantity * enriched.price)

    # Step 5: Extract year/month for partitioning (based on order_date)    
    enriched = enriched.withColumn("year", year("order_date")).withColumn("month", month("order_date"))

    # Step 6: Write output partitioned by year/month
    enriched.write.mode("overwrite").partitionBy("year", "month").parquet("/opt/airflow/tmp/final_data")

    # enriched.write.mode("overwrite").parquet("/opt/airflow/tmp/final_data")
    spark.stop()

def quality_checks():
    spark = get_spark_session()
    orders = spark.read.parquet("/opt/airflow/tmp/orders")
    customers = spark.read.parquet("/opt/airflow/tmp/customers")
    products = spark.read.parquet("/opt/airflow/tmp/products")

    # Null Checks
    print("Nulls in orders:")
    orders.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in orders.columns]).show()

    print("Nulls in customers:")
    customers.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in customers.columns]).show()

    print("Nulls in products:")
    products.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in products.columns]).show()

    # Duplicate Checks
    print(f"Duplicate orders: {orders.count() - orders.dropDuplicates().count()}")
    print(f"Duplicate customers: {customers.count() - customers.dropDuplicates().count()}")
    print(f"Duplicate products: {products.count() - products.dropDuplicates().count()}")

    spark.stop()


def load_to_postgresSQL():
    spark = get_spark_session()

    final_df = spark.read.parquet("/opt/airflow/tmp/final_data")

    final_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/ecommerce") \
        .option("dbtable", "enriched_orders_V2") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    spark.stop()

with DAG(
    dag_id="import_books_to_postgres",
    start_date=datetime(2025, 6, 10),
    schedule=None,
    catchup=False
) as dag:
    extract_task = PythonOperator(
        task_id="extract_csv",
        python_callable=extract_csv
        )
    transform_task = PythonOperator(
        task_id="transform_with_pyspark",
        python_callable=transform_with_pyspark
    )
    load_task = PythonOperator(
        task_id="load_to_postgresSQL",
        python_callable=load_to_postgresSQL
    )
    quality_task = PythonOperator(
        task_id="quality_checks",
        python_callable=quality_checks
    )

extract_task >> quality_task >> transform_task >> load_task


