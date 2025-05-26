# Financial Analysis Data Models

## fact_invoice
**Business Purpose**: Revenue tracking and billing analysis for financial reporting

**Data Lineage**: 
- **Source**: Billing Invoices table → Staging (stg_bil_invoices) → fact_invoice
- **Update Frequency**: Daily
- **Relationships**: Links to DimCustomer and dim_date
- **Business Rules**: Aggregates invoice items for total amounts

**Business Use Cases**:
- Monthly/quarterly revenue reporting
- Customer billing analysis
- Payment trend analysis
- Revenue forecasting
- Accounts receivable management

**Key Business Metrics**:
- `total_amount`: Invoice total value
- `tax_amount`: Tax component
- `discount_amount`: Applied discounts
- `due_date`: Payment deadline
- `status`: Payment status (Paid, Pending, Overdue)

**Sample Business Questions**:
- What is our monthly recurring revenue?
- Which customers have overdue payments?
- What's the average invoice amount by customer segment?

---

## FactServiceAssignment
**Business Purpose**: Service delivery tracking and customer service portfolio management

**Data Lineage**: 
- **Source**: Provisioning Service Assignments → Staging (stg_prv_service_assignments) → FactServiceAssignment
- **Update Frequency**: Real-time (as services are activated/deactivated)
- **Relationships**: Links to DimCustomer, DimService, and dim_date

**Business Use Cases**:
- Service adoption rate analysis
- Customer service mix optimization
- Service lifecycle management
- Revenue per service tracking
- Customer upselling opportunities

**Key Business Metrics**:
- `status`: Service state (Active, Terminated, Suspended)
- `start_date`: Service activation date
- `end_date`: Service termination date
- Service duration calculations

**Sample Business Questions**:
- How many services does each customer have?
- What's the average service tenure?
- Which services are most frequently cancelled?

---

## fact_campaign_target
**Business Purpose**: Marketing campaign effectiveness and customer response tracking

**Data Lineage**: 
- **Source**: Marketing Campaign Targets → Staging (stg_mkt_campaign_targets) → fact_campaign_target
- **Update Frequency**: Daily
- **Relationships**: Links to DimCustomer, DimCampaign, and dim_date

**Business Use Cases**:
- Campaign ROI calculation
- Customer response rate analysis
- Marketing channel effectiveness
- Target audience optimization
- Customer acquisition cost tracking

**Key Business Metrics**:
- `response_flag`: Customer responded (Yes/No)
- `assigned_date`: When customer was targeted
- `last_contact_date`: Most recent interaction
- `channel`: Communication method used

**Sample Business Questions**:
- What's the response rate by campaign type?
- Which customer segments respond best to campaigns?
- What's our cost per acquisition by channel?

---

## dim_date
**Business Purpose**: Time dimension for all date-based analysis and reporting

**Data Lineage**: 
- **Source**: Generated time dimension → dim_date
- **Update Frequency**: Annual (pre-populated for future dates)
- **Coverage**: Supports historical and future date analysis

**Business Use Cases**:
- Time-series analysis
- Seasonal trend identification
- Fiscal year reporting
- Quarter-over-quarter comparisons
- Day-of-week pattern analysis

**Key Business Attributes**:
- `full_date`: Complete date value
- `year`, `quarter`, `month`: Hierarchical groupings
- `day_of_week`: Weekday analysis
- `is_weekend`: Weekend identification
- `fiscal_quarter`: Business fiscal periods

**Sample Business Questions**:
- How do sales vary by season?
- What are our weekend vs weekday patterns?
- How does this quarter compare to last quarter?