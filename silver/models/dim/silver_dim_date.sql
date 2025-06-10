{{ config(materialized='table', alias='DimDate', schema='dimensions') }}

WITH DateGenerator AS (
    -- Assuming a CTE or table that generates a series of dates
    SELECT
        -- Replace this with your actual date generation logic
        CAST('2020-01-01' AS DATE) AS DATE
        -- Add more dates as needed
)

SELECT
    CAST(strftime('%Y%m%d', DATE) AS INT) AS DATE_KEY,
    DATE AS FULL_DATE,
    strftime('%w', DATE) AS DAY_OF_WEEK,
    CAST(strftime('%d', DATE) AS INT) AS DAY_OF_MONTH,
    CAST(strftime('%m', DATE) AS INT) AS MONTH,
    CAST(strftime('%W', DATE) AS INT) / 13 + 1 AS QUARTER,
    CAST(strftime('%Y', DATE) AS INT) AS YEAR
FROM
    DateGenerator
WHERE
    DATE_KEY IS NOT NULL
    AND FULL_DATE IS NOT NULL
    AND strftime('%w', DATE) IN ('0', '1', '2', '3', '4', '5', '6')
    AND CAST(strftime('%d', DATE) AS INT) BETWEEN 1 AND 31
    AND CAST(strftime('%m', DATE) AS INT) BETWEEN 1 AND 12
    AND CAST(strftime('%W', DATE) AS INT) / 13 + 1 BETWEEN 1 AND 4
    AND CAST(strftime('%Y', DATE) AS INT) > 0
