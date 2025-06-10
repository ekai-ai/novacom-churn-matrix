{{ config(materialized='table', alias='DimOpportunity', schema='dimensions', docs={'node_color': 'blue'}) }}

WITH
  stg_opportunities AS (
    SELECT
      *
    FROM
      {{ ref('stg_crm_opportunities') }}
  )
SELECT
  {{ dbt_utils.generate_surrogate_key(['OPPORTUNITY_ID']) }} AS OpportunitySK,
  OPPORTUNITY_ID,
  ACCOUNT_ID,
  STAGE,
  PIPELINE_STAGE,
  AMOUNT,
  PROBABILITY_OF_CLOSE,
  CLOSE_DATE,
  ASSIGNED_SALES_REP,
  NEXT_STEP
FROM
  stg_opportunities
WHERE
  OPPORTUNITY_ID IS NOT NULL
  AND STAGE IN (
    'Prospecting',
    'Qualification',
    'Needs Analysis',
    'Value Proposition',
    'Negotiation',
    'Closed Won',
    'Closed Lost'
  )
  AND AMOUNT >= 0
  AND PROBABILITY_OF_CLOSE BETWEEN 0 AND 1
  AND CLOSE_DATE IS NOT NULL