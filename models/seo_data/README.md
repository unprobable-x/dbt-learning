# Google Search Console Metrics Models

## Branded vs Unbranded Classification

Search terms are classified as branded or unbranded based on their similarity to restaurant names. The classification is performed in `int_gsc_search_terms.sql` using Snowflake's `jarowinkler_similarity` function to compare each search term against the restaurant's name. This allows us to:

- Identify branded searches that directly reference the restaurant
- Separate unbranded searches that are more likely to be discovery-based
- Calculate performance metrics for each category independently

One enhcancemnt that could be made with larger restaurant dimension is to check the similarity between the restaurant name and __any other__ restaurant name to see where branded searches for other restaurants are turning up results for that restaruant.

## Restaurant-Level Metrics (`gsc_restaurant_metrics.sql`)

This model aggregates GSC data at the restaurant level, providing:

- Overall metrics (all search terms)
- Branded metrics (restaurant name matches)
- Unbranded metrics (non-branded searches)

Key features:
- Calculates both average and median metrics for CTR and position
- Tracks performance in top 3, 10, and 20 positions
- Includes percentage breakdowns of branded vs unbranded metrics
- Joins with cuisine data for restaurant categorization

## Cuisine-Level Metrics (`gsc_cuisine_metrics.sql`)

This model aggregates the restaurant-level metrics by cuisine type, enabling:

- Analysis of search performance across cuisine categories
- Comparison of branded vs unbranded metrics by cuisine
- Identification of high-performing cuisines
- Restaurant count per cuisine for context

The model:
- Unnests the cuisine array to handle restaurants with multiple cuisines
- Aggregates metrics across all restaurants in each cuisine
- Maintains the same metric structure as the restaurant model
- Provides cuisine-specific performance insights 