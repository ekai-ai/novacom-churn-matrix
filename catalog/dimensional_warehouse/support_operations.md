# Support Operations - Dimensional Warehouse

## Business Purpose
Tracks customer support interactions, ticket resolution, and knowledge base usage for service quality improvement and customer satisfaction analysis.

## Data Models

### DimSupportTicket
**Business Description:** Master information about customer support requests and issues
**Business Use Cases:** Support performance analysis, customer satisfaction tracking, workload management
**Data Lineage:** Raw support data → Staging (stg_sup_tickets) → DimSupportTicket
**Update Frequency:** Real-time (hourly updates)
**Key Business Fields:**
- Ticket Priority: Urgency level (Low, Medium, High, Critical)
- Channel: How customer contacted us (Phone, Email, Chat, Self-Service)
- Assigned Agent: Support representative handling the case
- Status: Current ticket state (Open, In Progress, Resolved, Closed)

### DimKBArticle
**Business Description:** Knowledge base articles and self-service content
**Business Use Cases:** Content effectiveness analysis, self-service optimization, agent training
**Data Lineage:** Raw support data → Staging (stg_sup_kb_articles) → DimKBArticle
**Update Frequency:** Weekly
**Key Business Fields:**
- Article Category: Topic classification
- Article Author: Content creator
- Version Number: Content revision tracking
- Last Updated: Content freshness indicator

### FactSupportTicket
**Business Description:** Support ticket lifecycle and resolution metrics
**Business Use Cases:** SLA monitoring, agent performance, customer satisfaction analysis
**Data Lineage:** Raw support data → Staging (stg_sup_tickets, stg_sup_ticket_notes) → FactSupportTicket
**Update Frequency:** Real-time (hourly updates)
**Key Business Fields:**
- Creation and resolution dates
- Time to resolution metrics
- Customer and ticket relationship
- Resolution outcome tracking