# Customer Analytics Data Mart

## Business Requirement
**Enable comprehensive customer insights and predictive analytics to reduce churn, optimize customer value, and improve customer experience through data-driven decision making.**

---

## customer_360_view
**Business Purpose**: Provide a complete view of each customer's relationship with Novacom for personalized customer management

**Data Lineage**: 
- **Sources**: DimCustomer + fact_invoice + FactServiceAssignment + FactSupportTicket + fact_campaign_target + FactNetworkUsage
- **Processing**: Customer-level aggregation with behavioral indicators and churn risk scoring
- **Update Frequency**: Daily
- **Data Scope**: All active and historical customers with complete interaction history

**Business Use Cases**:
- Customer service representative dashboard
- Account manager customer review meetings
- Personalized marketing campaign targeting
- Proactive customer retention outreach
- Customer health score monitoring

**Key Business Insights**:
- Complete customer revenue and service history
- Support interaction patterns and satisfaction
- Usage behavior and engagement levels
- Churn risk indicators and warning signs
- Customer lifetime value estimation

**Business Questions Answered**:
- What's this customer's complete relationship history with us?
- Which customers are at highest risk of churning?
- What's the total value of each customer relationship?
- How engaged is this customer with our services?
- Which customers should we prioritize for retention efforts?

---

## customer_segmentation_analysis
**Business Purpose**: Identify distinct customer groups with similar characteristics and behaviors to enable targeted business strategies

**Data Lineage**: 
- **Source**: customer_360_view aggregated by customer segment and tier
- **Processing**: Statistical analysis with segment performance metrics and behavioral patterns
- **Update Frequency**: Daily
- **Methodology**: Traditional demographic + behavioral + value-based segmentation

**Business Use Cases**:
- Marketing strategy development
- Product development prioritization
- Pricing strategy optimization
- Resource allocation decisions
- Customer experience customization

**Key Business Insights**:
- Segment profitability and growth potential
- Churn rates and retention challenges by segment
- Service adoption patterns by customer type
- Revenue concentration and diversification
- Segment-specific behavior patterns

**Business Questions Answered**:
- Which customer segments are most profitable?
- How do different segments respond to our services?
- Where should we focus our retention efforts?
- Which segments have the highest growth potential?
- How should we customize our approach by segment?

---

## churn_prediction_features
**Business Purpose**: Provide machine learning-ready data to predict which customers are likely to churn and enable proactive retention

**Data Lineage**: 
- **Source**: customer_360_view with extensive feature engineering
- **Processing**: Behavioral feature extraction, risk indicator calculation, and ML model preparation
- **Update Frequency**: Daily
- **Target Variable**: Historical churn outcomes for model training

**Business Use Cases**:
- Proactive customer retention campaigns
- Customer success team prioritization
- Early warning system for account managers
- Retention program effectiveness measurement
- Predictive analytics model development

**Key Business Insights**:
- Early warning indicators of customer churn
- Behavioral patterns that predict churn risk
- Effectiveness of retention interventions
- Customer health scoring and monitoring
- Risk-based customer prioritization

**Business Questions Answered**:
- Which customers are most likely to churn in the next 90 days?
- What behaviors indicate a customer is at risk?
- How effective are our retention efforts?
- Which factors most strongly predict churn?
- How can we prioritize our limited retention resources?

**Machine Learning Applications**:
- Churn prediction models (Random Forest, XGBoost)
- Customer health scoring algorithms
- Retention campaign targeting models
- Customer lifetime value prediction
- Next-best-action recommendation engines