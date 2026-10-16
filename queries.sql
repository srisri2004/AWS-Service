-- Run in the <project>-wg workgroup, database <project>_db. Table `sales` is created by the Glue crawler.

-- Daily revenue (latest version of each order, cancelled excluded)
WITH latest AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn FROM sales)
SELECT order_date, COUNT(*) AS orders, SUM(quantity) AS units, ROUND(SUM(revenue), 2) AS revenue
FROM latest WHERE rn = 1 AND status <> 'CANCELLED'
GROUP BY order_date ORDER BY order_date;

-- Top products
WITH latest AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn FROM sales)
SELECT product, SUM(quantity) AS units, ROUND(SUM(revenue), 2) AS revenue
FROM latest WHERE rn = 1 AND status <> 'CANCELLED'
GROUP BY product ORDER BY revenue DESC;

-- Audit: rows loaded per source file (partition pruning on ingest_date keeps scans small)
SELECT ingest_date, batch_id, COUNT(*) AS rows_loaded
FROM sales WHERE ingest_date >= date_add('day', -7, current_date)
GROUP BY ingest_date, batch_id ORDER BY ingest_date DESC;
