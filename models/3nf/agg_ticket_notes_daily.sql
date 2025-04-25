{% docs agg_ticket_notes_daily_description %}
Daily pre-aggregate for ticket notes trends. Summarizes counts by INTERNAL_FLAG and averages response/cadence metrics from fact_ticket_notes. Speeds up time-series and KPI queries as per performance requirements.
{% enddocs %}

{{ config(
    materialized='incremental',
    unique_key='note_date'
) }}

with note_data as (
    select
      note_date::date as note_date,
      internal_flag,
      first_response_hours,
      inter_note_interval_hours
    from {{ ref('fact_ticket_notes') }}
    {% if is_incremental() %}
      where note_date > (select max(note_date) from {{ this }})
    {% endif %}
)

select
  note_date,
  count(*)                                  as total_notes,
  sum(case when internal_flag then 1 else 0 end)  as internal_notes,
  sum(case when internal_flag then 0 else 1 end)  as external_notes,
  avg(first_response_hours)                 as avg_first_response_hours,
  avg(inter_note_interval_hours)            as avg_inter_note_interval_hours
from note_data
group by note_date
order by note_date;