# SaaS Data Models

This directory contains dbt models for analyzing SaaS customer health and risk metrics.

## Data Model Overview

The data model has the following tables:
- `customers`, `subscriptions`, `login_events`, and `support_tickets` as the base / raw tables
- `customer_daily_metrics` as an aggregated, daily metrics table for each customer
- `customers_enriched` with the most recent metrics and attributes for each customer

## Customer Daily Metrics Grain

The `customer_daily_metrics` model is built at a customer-date grain for several reasons:

   - Enables tracking of metrics over time at the smallest, useful grain
   - Allows for trend analysis and rollups of larger time periods using simple aggregations
   - Supports both point-in-time and period-over-period comparisons
   - Can serve as the foundation for any new lookback or windowed metrics

## Risk Score Calculation

The raw customer risk score is calculated using a combination of metrics that indicate potential customer health issues, all with an absolute value between 0 and 1. Percent change values are multiplied by -1 to give higher scores for declining metrics.

1. Subscription Health
   - pct_change_products_90d * -1
   - pct_change_arr_90d * -1
   - pct_change_mrr_90d * -1

2. Usage Health
   - pct_change_logins_3m * -1
   - pct_change_unique_users_3m * -1

3. Support Health
   - (open_bug_tickets + open_incident_tickets + tickets_open_14plus_days) / open_tickets
   - Essentially, the percentage of open tickets that are considered of higher concern

Each of these components is then added together to give the raw score. The final score is then normalized across the customer base for each day, scaled between 0 and 100, with 100 being the highest risk customer for that day.

normalized_score = 100 * (raw_score - min_score) / (max_score - min_score)

### Risk Score Assumptions

1. With the raw score being additive, there is the assumption that each component has equal weight
2. Positive values are included in percentage change metrics, so there is the assumption that positive changes in these values lead to less risk
3. That there is no correlation / multicollinearity between each component

## Potential Improvements

For future risk score iterations, actual customer churn dates would be helpful. From there, a model could be trained to predict the probability of churn for future months based on that data. Any additional data that could help give useful signals to the model, like feature usage data or support ticket topics, would improve the usefulness and accuracy of the churn probability.

## Related Documentation

- [Entity Relationship Diagram](erd.md)
- [Dependency Graph](dag.md)
- Individual model documentation in respective .yml files 