\pset pager off

SELECT * FROM staging.stg_sales;
SELECT * FROM dw.dim_store;

SELECT 
    f.sales_key, 
    f.order_id, 
    d.full_date AS order_date, 
    cs.store_id AS store_id,
    cs.store_name AS store_name,
    cs.barangay AS barangay,
    cs.city AS city,
    p.product_name,
    f.quantity,
    f.unit_price,
    f.line_total
FROM dw.fact_sales f
JOIN dw.dim_date d ON f.date_key = d.date_key
JOIN dw.dim_store hs ON f.store_key = hs.store_key
JOIN dw.dim_Store cs ON hs.store_id = cs.store_id AND cs.is_current = TRUE
JOIN dw.dim_product p ON f.product_key = p.product_key;