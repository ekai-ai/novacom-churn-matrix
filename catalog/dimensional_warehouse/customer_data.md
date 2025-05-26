# Customer Data - Dimensional Warehouse

## Business Purpose
Provides a complete view of customer information, service relationships, and transaction history for customer management, billing, and analytics.

## Data Models

### DimCustomer
**Business Description:** Master customer information including demographics, segments, and status
**Business Use Cases:** Customer lookup, segmentation analysis, demographics reporting
**Data Lineage:** Raw CRM data → Staging (stg_crm_customers) → DimCustomer
**Update Frequency:** Daily
**Key Business Fields:**
- Customer ID: Unique customer identifier
- Customer Segment: Business classification (Residential, SMB, Enterprise, Government)
- Customer Tier: Value classification (Bronze, Silver, Gold, Platinum)
- Status: Current relationship status (Active, Inactive, Churned)
- Demographics: Name, address, contact information

### DimService
**Business Description:** Catalog of all telecommunications services offered
**Business Use Cases:** Service portfolio management, pricing analysis, service adoption tracking
**Data Lineage:** Raw provisioning data → Staging (stg_prv_services) → DimService
**Update Frequency:** Weekly
**Key Business Fields:**
- Service Type: Category (Internet, TV, Mobile, Voice)
- Monthly Cost: Standard pricing for service
- Service Name: Marketing name for service
- Active Flag: Whether service is currently offered

### FactServiceAssignment
**Business Description:** Tracks which customers have which services over time
**Business Use Cases:** Service adoption analysis, customer portfolio tracking, churn analysis
**Data Lineage:** Raw provisioning data → Staging (stg_prv_service_assignments) → FactServiceAssignment
**Update Frequency:** Daily
**Key Business Fields:**
- Customer-Service relationship with start/end dates
- Assignment status (Active, Terminated, Suspended)
- Service provisioning status

### Fact_Invoice
**Business Description:** Customer billing and payment transactions
**Business Use Cases:** Revenue reporting, customer payment analysis, financial reconciliation
**Data Lineage:** Raw billing data → Staging (stg_bil_invoices, stg_bil_invoice_items) → Fact_Invoice
**Update Frequency:** Daily
**Key Business Fields:**
- Invoice amounts (total, tax, discount)
- Payment status and dates
- Customer billing relationship