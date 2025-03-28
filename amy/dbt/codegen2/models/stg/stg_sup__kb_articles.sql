{{ config(materialized='view') }}

WITH source AS (
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
    FROM {{ source('raw', 'SUP_KB_ARTICLES') }}
)

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
FROM source