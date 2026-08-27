CREATE OR REFRESH MATERIALIZED VIEW gold_dim_customer
AS
SELECT * FROM
  silver_customer_sdp
  WHERE __END_AT is null;

CREATE OR REFRESH MATERIALIZED VIEW gold_fact_sales
AS
SELECT 
s.sales_id,
P.product_name,
concat(c.first_name ,c.last_name) as Customer_Name,
s.order_date,
s.quantity,
s.unit_price,
(s.quantity*s.unit_price) as Total_Sales


FROM gold_dim_customer c join silver_sales_sdp s on c.customer_id = s.customer_id join silver_product_sdp p on p.product_id = s.product_id;