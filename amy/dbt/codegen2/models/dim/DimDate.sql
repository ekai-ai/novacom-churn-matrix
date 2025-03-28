{{ config(materialized='table') }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="dateadd(year, 10, current_date)"
       )
    }}
)

SELECT
    CAST(TO_VARCHAR(date_day, 'YYYYMMDD') AS INTEGER) AS DateSK,
    date_day AS Date,
    DAYNAME(date_day) AS DayOfWeek,
    DAYOFMONTH(date_day) AS DayOfMonth,
    DAYOFYEAR(date_day) AS DayOfYear,
    WEEKOFYEAR(date_day) AS WeekOfYear,
    MONTH(date_day) AS Month,
    MONTHNAME(date_day) AS MonthName,
    QUARTER(date_day) AS Quarter,
    YEAR(date_day) AS Year,
    CASE 
        WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE -- Assuming Sunday=0, Saturday=6
        ELSE FALSE 
    END AS IsWeekend,
    (date_day = LAST_DAY(date_day, 'MONTH')) AS IsMonthEnd,
    (date_day = LAST_DAY(date_day, 'QUARTER')) AS IsQuarterEnd,
    (date_day = LAST_DAY(date_day, 'YEAR')) AS IsYearEnd
FROM 
    date_spine