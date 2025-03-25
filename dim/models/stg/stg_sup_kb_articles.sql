{{ config(materialized='view', alias='stg_sup_kb_articles', schema='staging') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('raw', 'SUP_KB_ARTICLES') }}
),

renamed AS (
    SELECT
        KB_ARTICLE_ID,
        TITLE,
        CONTENT,
        CATEGORY,
        ARTICLE_AUTHOR,
        VERSION_NUMBER,
        EXTERNAL_LINK,
        LAST_UPDATED,
        CREATED_AT,
        UPDATED_AT
    FROM
        source
)

SELECT
    *
FROM
    renamed