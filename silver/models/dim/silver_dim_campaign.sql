{{ config(materialized='table', schema='dimensions', alias='DimCampaign', unique_key='CampaignSK', description='Marketing campaign dimension.') }}

WITH
  SourceTable AS (
    SELECT
      *
    FROM
      {{ ref('stg_mkt_campaigns') }}
  ),
  RenamedColumns AS (
    SELECT
      CAMPAIGN_ID,
      TRIM(INITCAP(NAME)) AS NAME,
      CAMPAIGN_TYPE,
      OBJECTIVE,
      TARGET_AUDIENCE,
      CONVERSION_GOAL,
      START_DATE,
      END_DATE,
      BUDGET
    FROM
      SourceTable
    WHERE
      CAMPAIGN_ID IS NOT NULL
      AND CAMPAIGN_TYPE IN ('Email', 'SMS', 'Social Media', 'Direct Mail')
      AND START_DATE <= END_DATE
      AND BUDGET >= 0
  )
SELECT
  ROW_NUMBER() OVER (ORDER BY CAMPAIGN_ID) AS CampaignSK,
  CAMPAIGN_ID,
  NAME,
  CAMPAIGN_TYPE,
  OBJECTIVE,
  TARGET_AUDIENCE,
  CONVERSION_GOAL,
  START_DATE,
  END_DATE,
  BUDGET
FROM
  RenamedColumns