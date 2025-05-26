# Marketing Campaigns - Dimensional Warehouse

## Business Purpose
Tracks marketing campaign effectiveness, customer targeting, and response rates to optimize marketing spend and improve customer acquisition and retention.

## Data Models

### DimCampaign
**Business Description:** Marketing campaign master information and objectives
**Business Use Cases:** Campaign performance analysis, budget tracking, ROI measurement
**Data Lineage:** Raw marketing data → Staging (stg_mkt_campaigns) → DimCampaign
**Update Frequency:** Weekly
**Key Business Fields:**
- Campaign Type: Category (Acquisition, Retention, Upsell, Cross-sell)
- Objective: Business goal (Customer Acquisition, Revenue Growth, etc.)
- Target Audience: Customer segment focus
- Budget: Allocated marketing spend
- Start/End Dates: Campaign timeline

### Fact_Campaign_Target
**Business Description:** Customer targeting and response tracking for marketing campaigns
**Business Use Cases:** Response rate analysis, customer engagement measurement, targeting effectiveness
**Data Lineage:** Raw marketing data → Staging (stg_mkt_campaign_targets) → Fact_Campaign_Target
**Update Frequency:** Daily
**Key Business Fields:**
- Customer targeting assignments
- Response flags and dates
- Channel used for outreach
- Campaign assignment dates
- Contact history tracking

### DimOpportunity
**Business Description:** Sales opportunities and pipeline management
**Business Use Cases:** Sales forecasting, conversion analysis, pipeline reporting
**Data Lineage:** Raw CRM data → Staging (stg_crm_opportunities) → DimOpportunity
**Update Frequency:** Daily
**Key Business Fields:**
- Pipeline Stage: Sales process position
- Opportunity Amount: Potential revenue value
- Probability: Likelihood of closing
- Assigned Sales Rep: Responsible salesperson
- Close Date: Expected completion