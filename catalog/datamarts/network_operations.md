# Network Operations Data Mart

## Business Requirement
**Optimize network performance, ensure service reliability, and minimize customer impact through proactive network monitoring and data-driven operational excellence.**

---

## network_performance_dashboard
**Business Purpose**: Provide comprehensive network health monitoring and performance metrics for operational decision-making

**Data Lineage**: 
- **Sources**: FactNetworkOutage + FactNetworkUsage + FactServiceAssignment + DimCustomer + DimService
- **Processing**: Multi-dimensional aggregation by region, service type, and time periods
- **Update Frequency**: Daily with real-time outage updates
- **Scope**: Regional, service-level, and temporal performance analysis

**Business Use Cases**:
- Network operations center (NOC) dashboards
- Executive operational reporting
- Regional performance management
- Service level agreement monitoring
- Infrastructure investment planning

**Key Business Insights**:
- Network availability and uptime metrics
- Regional performance comparison
- Service-specific reliability patterns
- Customer impact assessment
- Peak usage and capacity trends

**Business Questions Answered**:
- What's our overall network availability this month?
- Which regions are experiencing the most outages?
- How does network performance vary by service type?
- Are we meeting our SLA commitments?
- Where should we invest in infrastructure improvements?

---

## service_reliability_metrics
**Business Purpose**: Track service-level reliability and customer experience to ensure service quality standards and SLA compliance

**Data Lineage**: 
- **Sources**: FactServiceAssignment + FactNetworkOutage + FactNetworkUsage + FactSupportTicket
- **Processing**: Service assignment-level reliability scoring with customer impact analysis
- **Update Frequency**: Daily
- **Methodology**: MTBF, MTTR, and availability calculations with reliability scoring

**Business Use Cases**:
- Service Level Agreement monitoring
- Customer satisfaction improvement
- Service quality benchmarking
- Reliability-based customer segmentation
- Proactive service maintenance planning

**Key Business Insights**:
- Service reliability scores and trends
- Mean time between failures (MTBF)
- Mean time to repair (MTTR)
- Customer-specific service performance
- SLA compliance tracking

**Business Questions Answered**:
- Which services are most reliable?
- Are we meeting our customer SLA commitments?
- Which customers experience the most service issues?
- How quickly do we resolve service problems?
- What's our service availability percentage?

---

## outage_root_cause_analysis
**Business Purpose**: Identify patterns and root causes of network outages to enable proactive prevention and continuous improvement

**Data Lineage**: 
- **Sources**: FactNetworkOutage + FactServiceAssignment + DimCustomer + temporal analysis
- **Processing**: Root cause categorization, pattern analysis, and predictive trend identification
- **Update Frequency**: Daily with comprehensive monthly analysis
- **Analytics**: Statistical pattern recognition and trend analysis

**Business Use Cases**:
- Infrastructure improvement planning
- Preventive maintenance scheduling
- Root cause elimination programs
- Equipment replacement prioritization
- Operational process improvement

**Key Business Insights**:
- Most common outage causes and their impact
- Temporal patterns in outage occurrence
- Regional and equipment-specific reliability issues
- Outage resolution effectiveness
- Predictive maintenance opportunities

**Business Questions Answered**:
- What are the leading causes of network outages?
- Are outages increasing or decreasing over time?
- Which equipment or regions need attention?
- How effective are our resolution processes?
- What patterns can help us prevent future outages?

**Operational Applications**:
- Predictive maintenance scheduling
- Equipment replacement planning
- Process improvement initiatives
- Staff training prioritization
- Vendor performance evaluation