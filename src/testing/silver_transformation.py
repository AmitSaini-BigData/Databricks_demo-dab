from pyspark import pipelines as dp

dp.create_streaming_table(name="silver_customer_sdp")
dp.create_auto_cdc_from_snapshot_flow(
    target="silver_customer_sdp",
    source="customer_bronze_snapshot",
    keys=["customer_id"],
    stored_as_scd_type=2
)

dp.create_streaming_table(name="silver_product_sdp")
dp.create_auto_cdc_from_snapshot_flow(
    target="silver_product_sdp",
    source="product_bronze_snapshot",
    keys=["product_id"],
    stored_as_scd_type=1
)

dp.create_streaming_table(name="silver_sales_sdp")
dp.create_auto_cdc_from_snapshot_flow(
    target="silver_sales_sdp",
    source="Sales_bronze_snapshot",
    keys=["sales_id"],
    stored_as_scd_type=1
)
