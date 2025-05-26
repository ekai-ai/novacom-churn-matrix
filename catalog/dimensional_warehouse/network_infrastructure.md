# Network Infrastructure - Dimensional Warehouse

## Business Purpose
Monitors network performance, service reliability, and infrastructure health to ensure optimal customer experience and operational efficiency.

## Data Models

### FactNetworkUsage
**Business Description:** Customer data consumption and network utilization patterns
**Business Use Cases:** Capacity planning, usage-based billing, customer behavior analysis
**Data Lineage:** Raw network data → Staging (stg_nwk_usage) → FactNetworkUsage
**Update Frequency:** Daily
**Key Business Fields:**
- Data Consumed: Amount of data used by customer
- Usage Type: Service category (Internet, Video, Voice)
- Usage Cost: Associated charges for consumption
- Peak Usage: Maximum usage during time period

### FactNetworkOutage
**Business Description:** Network service interruptions and their impact on customers
**Business Use Cases:** SLA monitoring, reliability reporting, root cause analysis
**Data Lineage:** Raw network data → Staging (stg_nwk_outages) → FactNetworkOutage
**Update Frequency:** Real-time (as outages occur)
**Key Business Fields:**
- Outage Type: Planned vs Unplanned interruptions
- Duration: Length of service interruption
- Impacted Customers: Number of affected subscribers
- Cause: Root cause of outage (Equipment, Weather, etc.)
- Region: Geographic area affected

### Equipment Tracking
**Business Description:** Network equipment inventory and maintenance status
**Business Use Cases:** Asset management, preventive maintenance, capacity planning
**Data Lineage:** Raw network data → Staging (stg_nwk_equipment) → Equipment dimensions
**Update Frequency:** Daily
**Key Business Fields:**
- Equipment Type: Device category (Router, Switch, etc.)
- Location: Physical deployment site
- Maintenance Schedule: Service and upgrade tracking
- Status: Operational condition