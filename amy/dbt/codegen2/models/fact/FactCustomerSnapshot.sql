{{ config(materialized='table') }}
with customer_activity as (
    select
        dc.CustomerSK,
        date_trunc('month', inv.INVOICE_DATE) as SnapshotDate,
        cust.CHURNLABEL as ChurnLabel,
        sum(inv.TOTAL_AMOUNT) as TotalBilledAmountPeriod,
        sum(pay.AMOUNT) as TotalPaidAmountPeriod,
        (sum(inv.TOTAL_AMOUNT) - sum(pay.AMOUNT)) as OutstandingBalanceEndPeriod,
        datediff('day', max(pay.PAYMENT_DATE), date_trunc('month', inv.INVOICE_DATE)) as DaysSinceLastPayment,
        count(distinct psa.SERVICE_ID) as ActiveServicesCount,
        sum(nw.DATA_CONSUMED) / 1024 / 1024 / 1024 as TotalDataConsumedGBPeriod,
        count(distinct tic.TICKET_ID) as SupportTicketsOpenedPeriod,
        sum(case when tic.STATUS = 'resolved' then 1 else 0 end) as SupportTicketsResolvedPeriod,
        avg(case when tic.STATUS = 'resolved' then datediff('hour', tic.CREATED_DATE, tic.RESOLUTION_DATE) end) as AvgTicketResolutionTimePeriod,
        count(distinct ct.CAMPAIGN_ID) as MarketingCampaignsTargetedPeriod,
        sum(case when ct.RESPONSE_FLAG then 1 else 0 end) as MarketingResponsesPeriod,
        count(distinct nw_out.OUTAGE_ID) as NetworkOutagesCountPeriod,
        acc.ACCOUNT_BALANCE as CurrentAccountBalance
    from {{ ref('stg_crm__customers') }} cust
    join {{ ref('dim_customer') }} dc on cust.CUSTOMER_ID = dc.CUSTOMER_ID
    left join {{ ref('stg_bil__invoices') }} inv on cust.CUSTOMER_ID = inv.CUSTOMER_ID
    left join {{ ref('stg_bil__payments') }} pay on inv.INVOICE_ID = pay.INVOICE_ID
    left join {{ ref('stg_prv__service_assignments') }} psa on cust.CUSTOMER_ID = psa.CUSTOMER_ID
    left join {{ ref('stg_nwk__usage') }} nw on nw.ASSIGNMENT_ID = psa.ASSIGNMENT_ID
    left join {{ ref('stg_sup__tickets') }} tic on cust.CUSTOMER_ID = tic.CUSTOMER_ID
    left join {{ ref('stg_mkt__campaign_targets') }} ct on cust.CUSTOMER_ID = ct.CUSTOMER_ID
    left join {{ ref('stg_nwk__outages') }} nw_out on nw_out.ASSIGNMENT_ID = nw.ASSIGNMENT_ID
    left join {{ ref('stg_crm__accounts') }} acc on cust.CUSTOMER_ID = acc.CUSTOMER_ID
    group by
        dc.CustomerSK,
        date_trunc('month', inv.INVOICE_DATE),
        cust.CHURNLABEL,
        acc.ACCOUNT_BALANCE
)

select
    ca.CustomerSK,
    dd.DateSK as SnapshotDateSK,
    ca.ChurnLabel,
    ca.TotalBilledAmountPeriod,
    ca.TotalPaidAmountPeriod,
    ca.OutstandingBalanceEndPeriod,
    ca.DaysSinceLastPayment,
    ca.ActiveServicesCount,
    ca.TotalDataConsumedGBPeriod,
    ca.SupportTicketsOpenedPeriod,
    ca.SupportTicketsResolvedPeriod,
    ca.AvgTicketResolutionTimePeriod,
    ca.MarketingCampaignsTargetedPeriod,
    ca.MarketingResponsesPeriod,
    ca.NetworkOutagesCountPeriod,
    ca.CurrentAccountBalance
from customer_activity ca
join {{ ref('dim_date') }} dd on ca.SnapshotDate = dd.Date
;