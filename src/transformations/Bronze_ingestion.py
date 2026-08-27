from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

catalog = spark.conf.get("catalog_name")
schema = spark.conf.get("schema_name")
schema_bronze = f"bronze_{schema}"


@dp.materialized_view(
    name=f"{catalog}.{schema}.{schema_bronze}_customer",
    comment="customer raw data ingestion"
)
@dp.expect_or_drop(
    "valid_customer_id",
    "customer_id IS NOT NULL"
)
def customer_bronze_snapshot_bundle():
    return(
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", "false")
        .load("abfss://retail@amitdatabricksdemosa.dfs.core.windows.net/input/Retail_data_source/Customer.csv")
        .withColumn("load_timestamp", current_timestamp(
    )))

@dp.materialized_view(
    name=f"{catalog}.{schema}.{schema_bronze}_product",
    comment="product raw data ingestion"
)
@dp.expect_or_drop(
    "valid_product_id",
    "product_id IS NOT NULL"
)

def product_bronze_snapshot_bundle():
    return(
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", "false")
        .load("abfss://retail@amitdatabricksdemosa.dfs.core.windows.net/input/Retail_data_source/Product.csv")
        .withColumn("load_timestamp", current_timestamp(
    )))
@dp.materialized_view(
    name=f"{catalog}.{schema}.{schema_bronze}_Sales",
    comment="Sales raw data ingestion"
)
@dp.expect_or_drop(
    "Valid sales id",
    "sales_id IS NOT NULL"
)
def Sales_bronze_snapshot_bundle():
    return(
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", "false")
        .load("abfss://retail@amitdatabricksdemosa.dfs.core.windows.net/input/Retail_data_source/Retail_Sales.csv")
        .withColumn("load_timestamp", current_timestamp(
    )))

