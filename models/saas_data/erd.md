```mermaid
erDiagram
    customers ||--o{ subscriptions : "has"
    customers ||--o{ login_events : "has"
    customers ||--o{ support_tickets : "has"
    customers ||--o{ customer_daily_metrics : "has"
    customer_daily_metrics ||--o{ customers_enriched : "latest"

    customers {
        string customer_id PK
        date acquisition_date
        string region
        string industry
    }

    subscriptions {
        string customer_id FK
        string product_id
        int quantity
        date start_date
        date end_date
        int arr
    }

    login_events {
        string user_id
        string customer_id FK
        string user_type
        timestamp login_timestamp
    }

    support_tickets {
        string ticket_id PK
        string customer_id FK
        string status
        timestamp created_date
        string ticket_category
        timestamp closed_date
    }

    customer_daily_metrics {
        string record_key PK
        string customer_id FK
        date date
        int num_products
        numeric total_arr
        numeric total_mrr
        numeric pct_change_products_90d
        numeric pct_change_arr_90d
        numeric pct_change_mrr_90d
        int num_logins
        int num_unique_users
        int admin_logins
        int admin_unique_users
        int advanced_logins
        int advanced_unique_users
        int standard_logins
        int standard_unique_users
        int num_logins_last_3m
        int num_unique_users_last_3m
        int num_logins_prev_3m
        int num_unique_users_prev_3m
        numeric pct_change_logins_3m
        numeric pct_change_unique_users_3m
        int total_tickets_to_date
        int open_tickets
        int open_bug_tickets
        int open_incident_tickets
        int open_question_tickets
        int open_feature_request_tickets
        int tickets_open_14plus_days
        numeric avg_days_to_resolution
        boolean is_logins_declining
        boolean is_unique_users_declining
        boolean has_high_aging_tickets
        numeric customer_risk_score_raw
        numeric customer_risk_score
    }

    customers_enriched {
        string customer_id PK,FK
        date acquisition_date
        string region
        string industry
        date last_metrics_date
        int num_products
        numeric total_arr
        numeric total_mrr
        numeric pct_change_products_90d
        numeric pct_change_arr_90d
        numeric pct_change_mrr_90d
        int num_logins
        int num_unique_users
        int admin_logins
        int admin_unique_users
        int advanced_logins
        int advanced_unique_users
        int standard_logins
        int standard_unique_users
        int num_logins_last_3m
        int num_unique_users_last_3m
        int num_logins_prev_3m
        int num_unique_users_prev_3m
        numeric pct_change_logins_3m
        numeric pct_change_unique_users_3m
        int total_tickets_to_date
        int open_tickets
        int open_bug_tickets
        int open_incident_tickets
        int open_question_tickets
        int open_feature_request_tickets
        int tickets_open_14plus_days
        numeric avg_days_to_resolution
        boolean is_logins_declining
        boolean is_unique_users_declining
        boolean has_high_aging_tickets
        numeric customer_risk_score_raw
        numeric customer_risk_score
        int days_since_acquisition
        int days_since_last_activity
    }
```

# Entity Relationship Diagram for SaaS Data Models

This diagram shows the relationships between all models in the saas_data directory:

1. **customers**: Base table containing customer information
   - Primary key: `customer_id`
   - Contains basic customer attributes (acquisition date, region, industry)

2. **subscriptions**: Customer subscription data
   - Foreign key: `customer_id` references customers
   - Contains product assignments, quantities, and revenue information

3. **login_events**: User login activity
   - Foreign key: `customer_id` references customers
   - Contains user type and login timestamp information

4. **support_tickets**: Customer support tickets
   - Primary key: `ticket_id`
   - Foreign key: `customer_id` references customers
   - Contains ticket status, category, and timing information

5. **customer_daily_metrics**: Daily metrics for each customer
   - Primary key: `record_key` (surrogate key)
   - Foreign key: `customer_id` references customers
   - Contains comprehensive daily metrics including:
     - Subscription metrics (products, ARR, MRR)
     - Login metrics (by user type)
     - Support ticket metrics
     - Risk scores

6. **customers_enriched**: Latest metrics for each customer
   - Primary key: `customer_id`
   - Foreign key: `customer_id` references customers
   - Contains the most recent metrics from customer_daily_metrics plus:
     - Days since acquisition
     - Days since last activity

## Key Relationships:
- One customer can have many subscriptions
- One customer can have many login events
- One customer can have many support tickets
- One customer can have many daily metrics
- One customer has one enriched record (latest metrics) 