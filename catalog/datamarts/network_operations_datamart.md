# Network Operations Data Mart

## Business Requirement
Operations teams require real-time visibility into network performance, service reliability metrics, outage patterns, and root cause analysis to maintain SLA compliance, minimize service disruptions, and proactively address infrastructure issues before they impact customers.

## Data Models

### Network Performance Dashboard
**Business Description:** Comprehensive network health and performance monitoring
**Business Use Cases:** SLA monitoring, capacity planning, regional performance analysis
**Data Lineage:** FactNetworkOutage + FactNetworkUsage + FactServiceAssignment + DimCustomer + DimService → Performance aggregations
**Update Frequency:** Real-time (hourly updates)
**Key Business Insights:**
- Regional network performance and availability
- Service type reliability metrics
- Monthly performance trends and patterns
- Customer impact analysis by outage
- Peak usage and capacity utilization

### Service Reliability Metrics
**Business Description:** Individual service assignment reliability and customer impact
**Business Use Cases:** SLA compliance tracking, customer compensation, service quality improvement
**Data Lineage:** FactServiceAssignment + FactNetworkOutage + FactNetworkUsage + FactSupportTicket → Service-level reliability scores
**Update Frequency:** Daily
**Key Business Insights:**
- Service availability percentages by customer
- Mean Time Between Failures (MTBF) analysis
- Mean Time To Repair (MTTR) tracking
- Customer-specific reliability scores
- SLA compliance status and violations

### Outage Root Cause Analysis
**Business Description:** Systematic analysis of network outages and their underlying causes
**Business Use Cases:** Infrastructure investment planning, preventive maintenance, risk mitigation
**Data Lineage:** FactNetworkOutage + FactServiceAssignment + DimCustomer → Root cause patterns and recommendations
**Update Frequency:** After each outage (real-time analysis)
**Key Business Insights:**
- Most frequent outage causes and their trends
- Seasonal and temporal outage patterns
- Customer impact assessment by cause
- Priority scoring for infrastructure improvements
- Recommended actions for prevention

## Business Value
- **Service Excellence:** Maintain industry-leading uptime and reliability
- **Cost Optimization:** Prevent outages through predictive maintenance
- **Customer Satisfaction:** Minimize service disruptions and their impact
- **Operational Efficiency:** Focus resources on highest-impact improvements