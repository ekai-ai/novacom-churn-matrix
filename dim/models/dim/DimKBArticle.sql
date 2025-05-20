{{ config(materialized='table') }}

with source as (
    select
        abs(hash(KB_ARTICLE_ID)) as KBArticleSK,
        KB_ARTICLE_ID,
        trim(TITLE) as TITLE,
        CONTENT,
        CATEGORY,
        ARTICLE_AUTHOR,
        VERSION_NUMBER,
        EXTERNAL_LINK,
        LAST_UPDATED
    from {{ ref('stg_sup_kb_articles') }}
    where KB_ARTICLE_ID IS NOT NULL
      and CATEGORY IN ('Technical', 'Billing', 'Account', 'General')
      and LAST_UPDATED IS NOT NULL
)

select *
from source
;