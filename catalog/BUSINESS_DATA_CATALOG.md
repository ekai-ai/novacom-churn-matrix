# Novacom Business Data Catalog

## Overview
This catalog provides business users with comprehensive documentation of all data assets available in the Novacom analytics platform. Each data model includes business purpose, data sources, and how to use the information for decision-making.

## Data Architecture Layers

### 1. Dimensional Warehouse (SILVER Schema)
**Purpose**: Clean, structured data organized for business analysis
**Location**: `dim/` folder models
**Business Value**: Provides reliable, consistent data for all reporting needs

### 2. Key Performance Indicators (KPIs Schema) 
**Purpose**: Pre-calculated business metrics and performance indicators
**Location**: `kpis2/` folder models
**Business Value**: Ready-to-use metrics for executive dashboards and operational monitoring

### 3. Business Data Marts (DATAMARTS Schema)
**Purpose**: Specialized analytical views for specific business functions
**Location**: `datamarts/` folder models
**Business Value**: Department-specific insights and advanced analytics

## Quick Navigation

| Business Area | Models | Primary Use Case |
|---------------|--------|------------------|
| [Customer Management](./dimensional_warehouse/customer_models.md) | Customer, Account, Contact dimensions | Customer relationship management |
| [Service Operations](./dimensional_warehouse/service_models.md) | Service, Assignment fact tables | Service delivery tracking |
| [Financial Analysis](./dimensional_warehouse/financial_models.md) | Invoice, Payment fact tables | Revenue and billing analysis |
| [Support Operations](./dimensional_warehouse/support_models.md) | Support Ticket dimensions and facts | Customer service performance |
| [Network Management](./dimensional_warehouse/network_models.md) | Network Usage, Outage fact tables | Infrastructure monitoring |
| [Marketing Analytics](./dimensional_warehouse/marketing_models.md) | Campaign dimensions and targeting facts | Campaign effectiveness |
| [Business KPIs](./kpis/business_kpis.md) | ARPU, Retention, CLV, CAC metrics | Executive reporting |
| [Customer Analytics](./datamarts/customer_analytics.md) | 360-view, Segmentation, Churn prediction | Customer insights |
| [Network Operations](./datamarts/network_operations.md) | Performance, Reliability, Root cause analysis | Operational excellence |

## Data Freshness & Updates
- **Dimensional Warehouse**: Updated daily at 2 AM EST
- **KPIs**: Refreshed daily at 4 AM EST (after dimensional warehouse)
- **Data Marts**: Refreshed daily at 6 AM EST (after KPIs)

## Support & Questions
For questions about data definitions or access, contact:
- **Business Analysts**: analytics-team@novacom.com
- **Data Engineering**: data-engineering@novacom.com