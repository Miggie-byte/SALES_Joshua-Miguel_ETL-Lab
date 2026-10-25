\pset pager off

SELECT * FROM staging.stg_sales;
SELECT * FROM dw.dim_store;

SELECT DISTINCT
    s.store_id,
    s.store_name,
    s.barangay,
    s.city
FROM staging.stg_sales s WHERE NOT EXISTS (
    SELECT 1 FROM dw.dim_store d
    WHERE s.store_id = d.store_id
    AND d.is_current = TRUE
);