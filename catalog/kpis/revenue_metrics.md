# Revenue Metrics - KPI Layer

## Business Purpose
Provides key financial performance indicators to track revenue growth, customer profitability, and business health across all customer segments and service lines.

## KPI Models

### ARPU (Average Revenue Per User)
**Business Description:** Measures average monthly revenue generated per customer
**Business Use Cases:** Revenue benchmarking, pricing strategy, customer value assessment
**Data Lineage:** DimCustomer + Fact_Invoice → Revenue calculations → ARPU
**Calculation Method:** Total revenue divided by active customers for time period
**Update Frequency:** Monthly
**Business Metrics:**
- Monthly ARPU by customer segment
- ARPU trends over time
- ARPU comparison across service types
- Regional ARPU variations

### Customer Lifetime Value (CLV)
**Business Description:** Predicts total revenue a customer will generate over their relationship
**Business Use Cases:** Customer acquisition investment, retention prioritization, profitability analysis
**Data Lineage:** DimCustomer + Fact_Invoice + FactServiceAssignment → CLV calculation
**Calculation Method:** Average monthly revenue × customer tenure × retention probability
**Update Frequency:** Monthly
**Business Metrics:**
- CLV by customer segment and tier
- CLV vs Customer Acquisition Cost ratio
- CLV trends and projections
- High-value customer identification

### Customer Acquisition Cost (CAC)
**Business Description:** Measures cost to acquire a new customer through marketing and sales
**Business Use Cases:** Marketing ROI analysis, budget allocation, acquisition efficiency
**Data Lineage:** DimCampaign + Fact_Campaign_Target + DimCustomer → CAC calculation
**Calculation Method:** Total acquisition spend divided by new customers acquired
**Update Frequency:** Monthly
**Business Metrics:**
- CAC by acquisition channel
- CAC vs CLV ratios
- CAC trends over time
- Campaign-specific acquisition costs