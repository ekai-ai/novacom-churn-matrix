# Network Management Data Models

## FactNetworkUsage
**Business Purpose**: Network utilization tracking and capacity planning for infrastructure management

**Data Lineage**: 
- **Source**: Network Usage logs → Staging (stg_nwk_usage) → FactNetworkUsage
- **Update Frequency**: Daily (aggregated from hourly logs)
- **Relationships**: Links to service assignments and dim_date
- **Data Volume**: High-volume fact table with usage patterns

**Business Use Cases**:
- Network capacity planning
- Customer usage pattern analysis
- Service performance monitoring
- Data consumption trend analysis
- Peak usage identification

**Key Business Metrics**:
- `data_consumed`: Amount of data used
- `usage_peak`: Peak usage during the period
- `usage_cost`: Cost associated with usage
- `usage_type`: Type of network usage
- `usage_unit`: Measurement unit (GB, TB)

**Sample Business Questions**:
- What's the average data consumption per customer?
- When do we experience peak network usage?
- Which services consume the most bandwidth?
- How is data usage trending over time?

---

## FactNetworkOutage
**Business Purpose**: Network reliability tracking and service level agreement monitoring

**Data Lineage**: 
- **Source**: Network Outage incidents → Staging (stg_nwk_outages) → FactNetworkOutage
- **Update Frequency**: Real-time (as outages are reported and resolved)
- **Relationships**: Links to service assignments, dim_date for start/end times
- **Critical Data**: Service availability and customer impact tracking

**Business Use Cases**:
- Service Level Agreement (SLA) monitoring
- Network reliability analysis
- Customer impact assessment
- Root cause analysis
- Infrastructure improvement planning

**Key Business Metrics**:
- `outage_type`: Planned vs Unplanned outages
- `region`: Geographic impact area
- `impacted_customers_count`: Number of affected customers
- `cause`: Root cause classification
- `resolution`: How the issue was resolved
- Duration calculations (start_time to end_time)

**Sample Business Questions**:
- What's our network uptime percentage?
- How many customers are affected by outages monthly?
- What are the most common causes of outages?
- How quickly do we resolve network issues?
- Which regions experience the most outages?

---

## FactSupportTicket
**Business Purpose**: Customer service performance tracking and support quality management

**Data Lineage**: 
- **Source**: Support Tickets → Staging (stg_sup_tickets) → FactSupportTicket
- **Update Frequency**: Real-time (as tickets are created and updated)
- **Relationships**: Links to DimCustomer, DimSupportTicket, dim_date
- **Integration**: Connects with network issues for correlation analysis

**Business Use Cases**:
- Customer service KPI tracking
- Support team workload management
- Service quality analysis
- Customer satisfaction monitoring
- Issue resolution time tracking

**Key Business Metrics**:
- `created_date`: When issue was reported
- `resolution_date`: When issue was resolved
- Resolution time calculations
- Ticket volume by period
- Customer satisfaction scores

**Sample Business Questions**:
- What's our average ticket resolution time?
- How many support tickets are created daily?
- Which issues take longest to resolve?
- How does support volume correlate with outages?
- What's our first-call resolution rate?