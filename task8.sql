TRUNCATE superstore;
COPY superstore
FROM 'C:/temp/global.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ','
);
SELECT COUNT(*) FROM superstore;

SELECT
    customer_name,
    SUM(sales) AS total_sales
FROM superstore
GROUP BY customer_name
ORDER BY total_sales DESC;

SELECT
    region,
    customer_name,
    SUM(sales) AS total_sales,
    ROW_NUMBER() OVER(
        PARTITION BY region
        ORDER BY SUM(sales) DESC
    ) AS row_num
FROM superstore
GROUP BY region, customer_name;

SELECT
    region,
    customer_name,
    SUM(sales) AS total_sales,
    RANK() OVER(
        PARTITION BY region
        ORDER BY SUM(sales) DESC
    ) AS rank_val
FROM superstore
GROUP BY region, customer_name;

SELECT
    region,
    customer_name,
    SUM(sales) AS total_sales,
    DENSE_RANK() OVER(
        PARTITION BY region
        ORDER BY SUM(sales) DESC
    ) AS dense_rank_val
FROM superstore
GROUP BY region, customer_name;

SELECT
    order_date,
    sales,
    SUM(sales) OVER(
        ORDER BY order_date
    ) AS running_sales
FROM superstore
ORDER BY order_date;

WITH monthly AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        SUM(sales) AS monthly_sales
    FROM superstore
    GROUP BY 1
)
SELECT
    month,
    monthly_sales,
    monthly_sales -
    LAG(monthly_sales) OVER (ORDER BY month) AS mom_growth
FROM monthly
ORDER BY month;

WITH ranked_products AS (
    SELECT
        category,
        product_name,
        SUM(sales) AS total_sales,
        DENSE_RANK() OVER(
            PARTITION BY category
            ORDER BY SUM(sales) DESC
        ) AS rnk
    FROM superstore
    GROUP BY category, product_name
)
SELECT *
FROM ranked_products
WHERE rnk <= 3;

WITH monthly AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        SUM(sales) AS monthly_sales
    FROM superstore
    GROUP BY 1
)
SELECT
    month,
    monthly_sales,
    monthly_sales -
    LAG(monthly_sales) OVER (ORDER BY month) AS mom_growth
FROM monthly
ORDER BY month;
