# Customer Analytics Data Mart

## Business Requirement
Business teams need a unified view of customer behavior, revenue patterns, service usage, and churn risk indicators to optimize customer retention strategies, personalize marketing campaigns, and improve customer lifetime value across all customer segments.

## Data Models

### Customer 360 View
**Business Description:** Complete customer profile with all touchpoints and behaviors
**Business Use Cases:** Customer relationship management, personalized marketing, account management
**Data Lineage:** DimCustomer + Fact_Invoice + FactServiceAssignment + FactSupportTicket + Fact_Campaign_Target + FactNetworkUsage → Customer 360 View
**Update Frequency:** Daily
**Key Business Insights:**
- Complete customer revenue history and trends
- Service portfolio and utilization patterns
- Support interaction history and satisfaction
- Marketing campaign response behavior
- Churn risk indicators and early warning signals
- Customer tenure and lifecycle stage

### Customer Segmentation Analysis
**Business Description:** Customer groups based on value, behavior, and characteristics
**Business Use Cases:** Targeted marketing, pricing strategy, resource allocation, retention programs
**Data Lineage:** Customer 360 View → Segmentation algorithms → Customer segments with performance metrics
**Update Frequency:** Weekly
**Key Business Insights:**
- Traditional segments (Residential, SMB, Enterprise, Government) performance
- Value-based segments (High/Medium/Low value customers)
- Behavioral segments (Loyal, At-risk, New, Churned)
- Lifecycle segments (Onboarding, Growing, Mature, Veteran)
- Segment prioritization scores for business focus

### Churn Prediction Features
**Business Description:** Machine learning features for predicting customer churn probability
**Business Use Cases:** Proactive retention, risk management, customer success programs
**Data Lineage:** All dimensional data → Feature engineering → ML-ready dataset with 80+ features
**Update Frequency:** Daily
**Key Business Applications:**
- Early churn warning system
- Retention campaign targeting
- Customer health scoring
- Risk-based customer prioritization
- Predictive analytics for customer success teams

## Business Value
- **Customer Retention:** Identify at-risk customers before they churn
- **Revenue Optimization:** Focus on high-value customer segments
- **Personalization:** Tailor services and offers to customer behavior
- **Operational Efficiency:** Prioritize resources on most valuable customers