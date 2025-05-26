# Novacom Data Catalog - Business User Guide

## Overview
This catalog provides business users with comprehensive documentation of all data assets in the Novacom analytics platform, including data sources, business definitions, and use cases.

## Data Architecture Layers

### 1. Dimensional Warehouse (Silver Layer)
**Purpose:** Clean, structured data organized for analysis and reporting
**Location:** `/dimensional_warehouse/`
**Models:**
- [Customer Data](dimensional_warehouse/customer_data.md) - Customer master data, services, and billing
- [Support Operations](dimensional_warehouse/support_operations.md) - Tickets, knowledge base, and resolutions
- [Network Infrastructure](dimensional_warehouse/network_infrastructure.md) - Network performance and outages
- [Marketing Campaigns](dimensional_warehouse/marketing_campaigns.md) - Campaign tracking and sales pipeline

### 2. KPI Layer (Metrics Layer)
**Purpose:** Pre-calculated business metrics and key performance indicators
**Location:** `/kpis/`
**Models:**
- [Revenue Metrics](kpis/revenue_metrics.md) - ARPU, CLV, Customer Acquisition Cost
- [Customer Experience](kpis/customer_experience.md) - Retention, satisfaction, adoption rates

### 3. Business Data Marts (Gold Layer)
**Purpose:** Business-specific aggregated data for specialized analysis
**Location:** `/datamarts/`
**Models:**
- [Customer Analytics Data Mart](datamarts/customer_analytics_datamart.md) - 360° customer view, segmentation, churn prediction
- [Network Operations Data Mart](datamarts/network_operations_datamart.md) - Performance monitoring, reliability, root cause analysis

## Data Lineage Overview

```
Raw Data Sources (Bronze)
    ↓
Staging Models (Data Validation & Cleansing)
    ↓
Dimensional Warehouse (Silver - Kimball Star Schema)
    ↓
KPI Models (Business Metrics)
    ↓
Business Data Marts (Gold - Purpose-Built Analytics)
```

## Business Use Cases by Department

### Marketing Team
- **Customer Segmentation:** Identify target audiences for campaigns
- **Campaign Performance:** Track ROI and response rates
- **Churn Prevention:** Identify at-risk customers for retention campaigns
- **CLV Analysis:** Focus acquisition spend on high-value prospects

### Customer Success Team
- **Customer 360 View:** Complete customer relationship visibility
- **Health Scoring:** Monitor customer satisfaction and engagement
- **Support Analytics:** Track resolution times and satisfaction
- **Retention Programs:** Proactive customer success management

### Operations Team
- **Network Performance:** Monitor SLA compliance and reliability
- **Outage Management:** Root cause analysis and prevention
- **Capacity Planning:** Usage trends and infrastructure needs
- **Service Quality:** Customer impact assessment

### Finance Team
- **Revenue Analytics:** ARPU, CLV, and profitability analysis
- **Customer Acquisition:** CAC and ROI measurement
- **Financial Reporting:** Customer lifetime value and churn impact
- **Budget Planning:** Data-driven investment decisions

### Executive Leadership
- **Business Performance:** Key metrics dashboard and trends
- **Strategic Planning:** Customer insights for business strategy
- **Risk Management:** Churn risk and customer health monitoring
- **Competitive Analysis:** Market position and customer value

## Data Refresh Schedule
- **Real-time:** Network outages, support tickets
- **Hourly:** Network performance metrics
- **Daily:** Customer data, service assignments, usage
- **Weekly:** Service catalog, knowledge base
- **Monthly:** KPI calculations, segmentation analysis

## Data Quality Standards
- **Completeness:** All critical business fields populated
- **Accuracy:** Data validation rules applied at all layers
- **Consistency:** Standardized definitions across all models
- **Timeliness:** SLA-based refresh schedules maintained
- **Lineage:** Full traceability from source to consumption

## Getting Started
1. **Identify Your Use Case:** Review the business use cases by department
2. **Find Relevant Data:** Use the model documentation to locate needed data
3. **Understand the Lineage:** Review how data flows from source to final model
4. **Access Requirements:** Contact the data team for access permissions
5. **Report Issues:** Use the data team's support channel for questions or issues

## Support
For questions about data definitions, access, or reporting issues, contact the Data Analytics team.