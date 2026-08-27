from pyspark import pipelines as dp

catalog = spark.conf.get("catalog_name")
schema = spark.conf.get("schema_name")
schema_silver = f"silver_{schema}"
schema_bronze = f"bronze_{schema}"


dp.create_streaming_table(name=f"{catalog}.{schema}.{schema_silver}_customer")
dp.create_auto_cdc_from_snapshot_flow(
    target=f"{catalog}.{schema}.{schema_silver}_customer",
    source=f"{catalog}.{schema}.{schema_bronze}_customer",
    keys=["customer_id"],
    stored_as_scd_type=2
)
dp.create_streaming_table(name=f"{catalog}.{schema}.{schema_silver}_product")
dp.create_auto_cdc_from_snapshot_flow(
    target=f"{catalog}.{schema}.{schema_silver}_product",
    source=f"{catalog}.{schema}.{schema_bronze}_product",
    keys=["product_id"],
    stored_as_scd_type=1
)
dp.create_streaming_table(name=f"{catalog}.{schema}.{schema_silver}_sales")
dp.create_auto_cdc_from_snapshot_flow(
    target=f"{catalog}.{schema}.{schema_silver}_sales",
    source=f"{catalog}.{schema}.{schema_bronze}_Sales",
    keys=["sales_id"],
    stored_as_scd_type=1
)
