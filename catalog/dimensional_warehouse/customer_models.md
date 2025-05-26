# Customer Management Data Models

## DimCustomer
**Business Purpose**: Master customer information for all customer-related analysis and segmentation

**Data Lineage**: 
- **Source**: CRM Customer table → Staging (stg_crm_customers) → DimCustomer
- **Update Frequency**: Daily
- **Data Quality**: Validates customer segments, tiers, and contact information

**Business Use Cases**:
- Customer segmentation analysis (Residential, SMB, Enterprise, Government)
- Customer tier management (Bronze, Silver, Gold, Platinum)
- Geographic analysis and regional performance
- Customer lifecycle tracking

**Key Business Fields**:
- `customer_segment`: Business type classification
- `customer_tier`: Service level classification
- `status`: Active/Inactive/Churned status
- Geographic fields: city, state, country

---

## DimOpportunity
**Business Purpose**: Sales pipeline and opportunity tracking for revenue forecasting

**Data Lineage**: 
- **Source**: CRM Opportunities table → Staging (stg_crm_opportunities) → DimOpportunity
- **Update Frequency**: Daily
- **Relationships**: Links to DimCustomer via customer relationship

**Business Use Cases**:
- Sales pipeline analysis and forecasting
- Win/loss rate tracking by stage
- Sales representative performance
- Revenue opportunity sizing

**Key Business Fields**:
- `stage`: Current sales stage
- `probability_of_close`: Likelihood of closing
- `amount`: Potential revenue value
- `close_date`: Expected closure date

---

## DimCampaign
**Business Purpose**: Marketing campaign management and performance tracking

**Data Lineage**: 
- **Source**: Marketing Campaigns table → Staging (stg_mkt_campaigns) → DimCampaign
- **Update Frequency**: Daily
- **Integration**: Used with campaign targeting facts

**Business Use Cases**:
- Campaign ROI analysis
- Marketing channel effectiveness
- Target audience analysis
- Budget allocation optimization

**Key Business Fields**:
- `campaign_type`: Type of marketing campaign
- `objective`: Campaign goal
- `target_audience`: Intended audience
- `budget`: Campaign investment

---

## DimSupportTicket
**Business Purpose**: Customer support case management and service quality tracking

**Data Lineage**: 
- **Source**: Support Tickets table → Staging (stg_sup_tickets) → DimSupportTicket
- **Update Frequency**: Real-time (multiple times daily)
- **Integration**: Links to customer and resolution metrics

**Business Use Cases**:
- Customer service performance monitoring
- Support workload management
- Service quality analysis
- Issue escalation tracking

**Key Business Fields**:
- `priority`: Urgency level (High, Medium, Low)
- `status`: Current state (Open, In Progress, Resolved)
- `channel_of_contact`: How customer contacted support
- `category`: Type of issue

---

## DimService
**Business Purpose**: Service catalog and offering management

**Data Lineage**: 
- **Source**: Provisioning Services table → Staging (stg_prv_services) → DimService
- **Update Frequency**: Weekly (as services change)
- **Usage**: Referenced by service assignment facts

**Business Use Cases**:
- Service portfolio analysis
- Pricing optimization
- Service adoption tracking
- Revenue per service analysis

**Key Business Fields**:
- `service_type`: Category (Internet, TV, Mobile, Voice)
- `service_name`: Specific service offering
- `monthly_cost`: Standard pricing
- `active_flag`: Currently offered status

---

## DimKBArticle
**Business Purpose**: Knowledge base content management for self-service support

**Data Lineage**: 
- **Source**: Support KB Articles table → Staging (stg_sup_kb_articles) → DimKBArticle
- **Update Frequency**: As articles are updated
- **Purpose**: Support content effectiveness analysis

**Business Use Cases**:
- Self-service adoption tracking
- Content gap analysis
- Support deflection measurement
- Knowledge base optimization

**Key Business Fields**:
- `category`: Topic classification
- `title`: Article subject
- `version_number`: Content version tracking
- `article_author`: Content creator