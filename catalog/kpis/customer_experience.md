# Customer Experience - KPI Layer

## Business Purpose
Tracks customer satisfaction, retention, and engagement metrics to improve customer experience and reduce churn across all touchpoints.

## KPI Models

### Customer Retention Rate
**Business Description:** Measures percentage of customers who remain active over time periods
**Business Use Cases:** Churn prevention, customer success measurement, retention strategy effectiveness
**Data Lineage:** DimCustomer + FactServiceAssignment → Retention calculation
**Calculation Method:** (Customers at end - new customers) / customers at start × 100
**Update Frequency:** Monthly
**Business Metrics:**
- Monthly and annual retention rates
- Retention by customer segment and tier
- Retention trends and seasonal patterns
- Service-specific retention rates

### Service Adoption Rate
**Business Description:** Tracks how quickly customers adopt new services and multi-service bundles
**Business Use Cases:** Product portfolio optimization, cross-sell effectiveness, customer journey analysis
**Data Lineage:** DimService + FactServiceAssignment + DimCustomer → Adoption calculation
**Calculation Method:** Customers with service / total eligible customers × 100
**Update Frequency:** Monthly
**Business Metrics:**
- Adoption rates by service type
- Multi-service bundle penetration
- Adoption velocity (time to adoption)
- Customer segment adoption patterns

### Support Quality Metrics
**Business Description:** Measures customer support effectiveness and satisfaction
**Business Use Cases:** Service quality improvement, agent performance, customer satisfaction
**Data Lineage:** DimSupportTicket + FactSupportTicket + DimCustomer → Support metrics
**Calculation Method:** Various support KPIs including resolution time, first-call resolution
**Update Frequency:** Daily
**Business Metrics:**
- Average resolution time by priority
- First-call resolution rate
- Customer satisfaction scores
- Support volume trends by category