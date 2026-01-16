-- Day 8: Unity Catalog Governance
-- Topics: Catalog, Schema, Tables, Permissions, Views

-- -----------------------------------
-- 1. Create Catalog & Schemas
-- -----------------------------------
CREATE CATALOG IF NOT EXISTS ecommerce;
USE CATALOG ecommerce;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- -----------------------------------
-- 2. Register Delta Tables
-- -----------------------------------
CREATE TABLE IF NOT EXISTS bronze.events
USING DELTA
LOCATION '/delta/bronze/events';

CREATE TABLE IF NOT EXISTS silver.events
USING DELTA
LOCATION '/delta/silver/events';

CREATE TABLE IF NOT EXISTS gold.products
USING DELTA
LOCATION '/delta/gold/products';

-- -----------------------------------
-- 3. Access Control (GRANT / REVOKE)
-- -----------------------------------
GRANT SELECT ON TABLE gold.products TO `analysts@company.com`;
GRANT ALL PRIVILEGES ON SCHEMA silver TO `engineers@company.com`;

-- -----------------------------------
-- 4. Controlled Access View
-- -----------------------------------
CREATE OR REPLACE VIEW gold.top_products AS
SELECT
    product_name,
    revenue,
    conversion_rate
FROM gold.products
WHERE purchases > 10
ORDER BY revenue DESC
LIMIT 100;
