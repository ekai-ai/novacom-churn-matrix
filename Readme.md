# Novacom Telecom Data Analytics Repository

This repository contains a complete data analytics solution for Novacom, a fictional telecom company, following medallion architecture principles. It provides a silver layer dimensional data model and KPI framework for analyzing customer data, service usage, billing, and churn metrics.

## Repository Structure

The repository is organized into two main DBT (Data Build Tool) projects:

### 1. Silver Layer - Dimensional Model (`/silver` directory)
This directory contains the silver layer of the medallion architecture with a Kimball-style dimensional data model:
- **Staging models** (`/silver/models/stg/`) - Bronze to silver transformation, data cleaning and validation
- **Silver dimension tables** (`/silver/models/dim/`) - Cleaned, conformed business entities with `silver_dim_*` naming
- **Silver fact tables** (`/silver/models/fact/`) - Validated business processes with `silver_fact_*` naming
- **Sources configuration** (`/silver/models/sources.yml`) - Definitions of bronze layer source data tables

### 2. KPIs and Metrics (`/kpis2` directory)
This directory contains a metrics framework with calculations for key business indicators:
- **KPI models** (`/kpis2/models/kpi/`) - SQL models that calculate metrics like:
  - Average Revenue Per User (ARPU)
  - Customer Retention Rate
  - Customer Lifetime Value (CLV)
  - Customer Acquisition Cost (CAC)
  - Service Adoption Rate

## Semantic Layer

The semantic layer provides a business-friendly interface to the dimensional model and KPIs:

- **`novacom_semantic_layer.yml`** - A comprehensive semantic model that defines:
  - Entities (customers, invoices, services, etc.)
  - Relationships between entities
  - Metrics and calculations
  - Time dimensions for analysis
  - Business-specific perspectives

- **`novacom_data_lineage.yml`** - Documents the flow of data:
  - Source datasets from operational systems
  - Transformation processes
  - Final semantic entities
  - Metric dependencies and formulas

## Getting Started

1. Set up DBT with appropriate database connections as defined in the project files
2. Run the silver layer build:
   ```
   cd silver
   dbt run
   ```
3. Run the KPIs and metrics calculations:
   ```
   cd kpis2
   dbt run
   ```
4. Use the semantic layer schema (`novacom_semantic_layer.yml`) for connecting BI tools like Tableau, Power BI, or Snowflake Cortex Analyst

## Key Use Cases

This data model is designed to support these primary analytics use cases:
- Customer churn prediction and analysis
- Revenue and profitability tracking by customer segment
- Service adoption and usage patterns
- Customer lifetime value optimization
- Marketing campaign effectiveness
- Network performance and outage impact analysis

## Recommended Tools

This repository works best with:
- DBT for transformation
- Snowflake as a data warehouse
- Tableau, Power BI, or Looker for visualization
- Snowflake Cortex Analyst for semantic modeling
