# Business Key Performance Indicators (KPIs)

## ARPU (Average Revenue Per User)
**Business Purpose**: Measure the average revenue generated per customer to track business performance and pricing effectiveness

**Data Lineage**: 
- **Source**: fact_invoice → Customer aggregation → Monthly ARPU calculation
- **Calculation**: Total Revenue ÷ Number of Active Customers
- **Update Frequency**: Daily
- **Time Periods**: Monthly, Quarterly, Annual views

**Business Use Cases**:
- Executive dashboard reporting
- Pricing strategy optimization
- Customer segment comparison
- Revenue trend analysis
- Investor and stakeholder reporting

**Key Insights Provided**:
- Revenue performance by customer segment
- ARPU trends over time
- Impact of pricing changes
- Customer tier value comparison

**Business Questions Answered**:
- What's our average revenue per customer this month?
- How does ARPU vary by customer segment?
- Is our ARPU increasing or decreasing over time?
- Which customer tier generates the highest ARPU?

---

## Customer Retention Rate
**Business Purpose**: Track customer loyalty and business sustainability by measuring how well we retain customers over time

**Data Lineage**: 
- **Source**: DimCustomer status changes → Cohort analysis → Retention rate calculation
- **Calculation**: (Customers at End - New Customers) ÷ Customers at Start × 100
- **Update Frequency**: Daily
- **Methodology**: Cohort-based analysis with monthly retention tracking

**Business Use Cases**:
- Customer success program evaluation
- Churn reduction strategy effectiveness
- Customer lifetime value forecasting
- Service quality impact assessment
- Competitive positioning analysis

**Key Insights Provided**:
- Monthly retention rates by customer segment
- Retention trends over time
- Impact of service changes on retention
- Early warning indicators for retention issues

**Business Questions Answered**:
- What percentage of customers do we retain each month?
- Which customer segments have the highest retention?
- How has our retention rate changed over the last year?
- What's the impact of support issues on retention?

---

## Customer Lifetime Value (CLV)
**Business Purpose**: Estimate the total revenue a customer will generate to guide acquisition and retention investment decisions

**Data Lineage**: 
- **Source**: ARPU + Retention Rate → Predictive CLV modeling
- **Calculation**: (Average Monthly Revenue × Gross Margin) ÷ Monthly Churn Rate
- **Update Frequency**: Monthly
- **Methodology**: Historical revenue analysis with retention projections

**Business Use Cases**:
- Customer acquisition cost justification
- Marketing budget allocation
- Customer segment prioritization
- Service investment decisions
- Long-term revenue forecasting

**Key Insights Provided**:
- CLV by customer segment and tier
- CLV trends and projections
- ROI of customer retention programs
- High-value customer identification

**Business Questions Answered**:
- What's the lifetime value of our customers?
- Which customer segments are most valuable?
- How much should we spend to acquire new customers?
- What's the ROI of improving retention by 5%?

---

## Customer Acquisition Cost (CAC)
**Business Purpose**: Track the cost efficiency of acquiring new customers to optimize marketing and sales investments

**Data Lineage**: 
- **Source**: Marketing campaign costs + Sales expenses → New customer attribution → CAC calculation
- **Calculation**: Total Acquisition Costs ÷ Number of New Customers
- **Update Frequency**: Monthly
- **Attribution**: Campaign and channel-specific cost tracking

**Business Use Cases**:
- Marketing ROI optimization
- Sales efficiency measurement
- Channel performance comparison
- Budget allocation decisions
- Customer acquisition strategy refinement

**Key Insights Provided**:
- CAC by marketing channel
- CAC trends over time
- CAC vs CLV ratio analysis
- Channel efficiency comparison

**Business Questions Answered**:
- How much does it cost to acquire a new customer?
- Which marketing channels are most cost-effective?
- Is our CAC improving or getting worse?
- What's our CAC payback period?

---

## Service Adoption Rate
**Business Purpose**: Measure how quickly and effectively customers adopt new services to guide product development and marketing

**Data Lineage**: 
- **Source**: FactServiceAssignment → Service activation tracking → Adoption rate calculation
- **Calculation**: Customers with Service ÷ Total Eligible Customers × 100
- **Update Frequency**: Daily
- **Segmentation**: By customer segment, service type, and time period

**Business Use Cases**:
- Product launch success measurement
- Cross-selling opportunity identification
- Service portfolio optimization
- Customer education program effectiveness
- Revenue growth planning

**Key Insights Provided**:
- Adoption rates by service and customer segment
- Time-to-adoption analysis
- Service bundle penetration
- Adoption trend forecasting

**Business Questions Answered**:
- How quickly are customers adopting our new services?
- Which customer segments adopt services fastest?
- What's the penetration rate of our premium services?
- How can we improve service adoption rates?