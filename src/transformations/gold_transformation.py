from pyspark import pipelines as dp
from pyspark.sql.functions import concat_ws, sum, countDistinct

catalog = spark.conf.get("catalog_name")
schema = spark.conf.get("schema_name")
schema_gold = f"gold_{schema}"
schema_silver = f"silver_{schema}"


@dp.materialized_view(
    name=f"{catalog}.{schema}.{schema_gold}_dim_customer",
)
def gold_dim_customer_bundle():
    return (spark.read.table(f"{catalog}.{schema}.{schema_silver}_customer")
    .filter("__END_AT IS NULL")
    )

@dp.materialized_view(
    name=f"{catalog}.{schema}.{schema_gold}_fact_sales",
)
def gold_fact_sales():
    sales=spark.read.table(f"{catalog}.{schema}.{schema_silver}_sales")
    customer=spark.read.table(f"{catalog}.{schema}.{schema_gold}_dim_customer")
    product= spark.read.table(f"{catalog}.{schema}.{schema_silver}_product")
    return sales.join(customer, sales.customer_id == customer.customer_id, "left").join(product, sales.product_id == product.product_id, "left").select(
        sales.sales_id,
        sales.order_date,
        customer.customer_id,
        concat_ws(" ", customer.first_name, customer.last_name).alias("customer_name"),
        product.product_id,
        product.product_name,
        sales.quantity,
        sales.unit_price,
        (sales.quantity * sales.unit_price).alias("Total_Sales")
    )
@dp.materialized_view(
    name=f"{catalog}.{schema}.{schema_gold}_sales_by_product",
)
def gold_sales_by_product_bundle():
    return (
        spark.read.table(f"{catalog}.{schema}.{schema_gold}_fact_sales")
        .groupBy("product_id", "product_name")
        .agg(
            sum("Total_Sales").alias("Total_Sales"),
            countDistinct("sales_id").alias("Total_Sales_Count"),
        )
    )
@dp.materialized_view(
    name=f"{catalog}.{schema}.{schema_gold}_sales_by_customer",
)
def gold_sales_by_customer_bundle():
    return (
        spark.read.table(f"{catalog}.{schema}.{schema_gold}_fact_sales")
        .groupBy("customer_id", "customer_name")
        .agg(
            sum("Total_Sales").alias("Total_Sales"),
            countDistinct("sales_id").alias("Total_Sales_Count"),
        )
    )