/*
Provided sufficient volume, alert on:
  - pct_success lower than expected
  - pct_low_value greater than expected
  - transactions_per_amount greater than expected
  - transactions_per_card greater than expected
  - transactions_per_ip_address greater than expected

Calculate the MAD (Median Absolute Deviation) for each of the above metrics.
Add one to serverity score for each metric 3SD (standard deviations) from the median.
Alert should flag if any metric is 3SD from the median.
Prefer median and MAD over mean as it will resist beign inflated from fraud spikes.

Baselines (median/MAD) are computed per merchant, since normal volume and mix
vary by merchant. 1.4826 scales MAD to be consistent with a normal
distribution's standard deviation, so the "3SD" thresholds above apply.
*/

CREATE OR REPLACE VIEW fraud_analytics.vw_card_testing_monitor AS (

with transactions as (
  SELECT
    *
  FROM
    `pub-sub-fraud-detection.fraud_staging.transactions`)

, datetime_enriched as (
  SELECT
    *,
    DATE(timestamp) as date,
    EXTRACT(HOUR from timestamp) as hour,
    EXTRACT(MINUTE from timestamp) as minute
  FROM
    transactions
)

, retried_cards as (
  SELECT
    merchant_id,
    date,
    hour,
    minute,
    card_id,
    COUNT(*)
  FROM
    datetime_enriched
  GROUP BY
    merchant_id,
    date,
    hour,
    minute,
    card_id
  HAVING
    COUNT(*) > 1
)

, minute_metrics as (
  SELECT
    de.merchant_id,
    de.date,
    de.hour,
    de.minute,
    COUNT(*) as total_transactions,
    COUNTIF(status = 'approved') as total_approved_transactions,
    COUNTIF(status = 'declined') as total_declined_transactions,
    ROUND(SAFE_DIVIDE(COUNTIF(status = 'approved'), COUNT(*)) * 100, 2) as pct_success,
    AVG(amount) as avg_amount_usd,
    COUNTIF(amount < 10) total_low_value_transactions,
    ROUND(SAFE_DIVIDE(COUNTIF(amount < 10), COUNT(*)) * 100, 2) as pct_low_value,
    COUNT(DISTINCT amount) as unique_amounts,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT amount)) transactions_per_amount,
    COUNT(DISTINCT de.card_id) as unique_cards,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT de.card_id)) transactions_per_card,
    COUNT(DISTINCT source_ip) as unique_ip_addresses,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT source_ip)) transactions_per_ip_address,
    COUNT(rc.card_id) as retried_cards
  FROM
    datetime_enriched as de
    LEFT JOIN retried_cards as rc
    ON de.merchant_id = rc.merchant_id
    AND de.date = rc.date
    AND de.hour = rc.hour
    AND de.minute = rc.minute
    AND de.card_id = rc.card_id
  GROUP BY
    merchant_id,
    date,
    hour,
    minute
)

, with_medians as (
  SELECT
    *,
    PERCENTILE_CONT(pct_success, 0.5) OVER (PARTITION BY merchant_id) as median_pct_success,
    PERCENTILE_CONT(pct_low_value, 0.5) OVER (PARTITION BY merchant_id) as median_pct_low_value,
    PERCENTILE_CONT(transactions_per_amount, 0.5) OVER (PARTITION BY merchant_id) as median_transactions_per_amount,
    PERCENTILE_CONT(transactions_per_card, 0.5) OVER (PARTITION BY merchant_id) as median_transactions_per_card,
    PERCENTILE_CONT(transactions_per_ip_address, 0.5) OVER (PARTITION BY merchant_id) as median_transactions_per_ip_address
  FROM
    minute_metrics
)

, with_deviations as (
  SELECT
    *,
    ABS(pct_success - median_pct_success) as dev_pct_success,
    ABS(pct_low_value - median_pct_low_value) as dev_pct_low_value,
    ABS(transactions_per_amount - median_transactions_per_amount) as dev_transactions_per_amount,
    ABS(transactions_per_card - median_transactions_per_card) as dev_transactions_per_card,
    ABS(transactions_per_ip_address - median_transactions_per_ip_address) as dev_transactions_per_ip_address
  FROM
    with_medians
)

, with_mad as (
  SELECT
    *,
    PERCENTILE_CONT(dev_pct_success, 0.5) OVER (PARTITION BY merchant_id) * 1.4826 as sd_pct_success,
    PERCENTILE_CONT(dev_pct_low_value, 0.5) OVER (PARTITION BY merchant_id) * 1.4826 as sd_pct_low_value,
    PERCENTILE_CONT(dev_transactions_per_amount, 0.5) OVER (PARTITION BY merchant_id) * 1.4826 as sd_transactions_per_amount,
    PERCENTILE_CONT(dev_transactions_per_card, 0.5) OVER (PARTITION BY merchant_id) * 1.4826 as sd_transactions_per_card,
    PERCENTILE_CONT(dev_transactions_per_ip_address, 0.5) OVER (PARTITION BY merchant_id) * 1.4826 as sd_transactions_per_ip_address
  FROM
    with_deviations
)

, with_flags as (
  SELECT
    *,
    IFNULL(SAFE_DIVIDE(median_pct_success - pct_success, sd_pct_success) > 3, FALSE) as flag_pct_success,
    IFNULL(SAFE_DIVIDE(pct_low_value - median_pct_low_value, sd_pct_low_value) > 3, FALSE) as flag_pct_low_value,
    IFNULL(SAFE_DIVIDE(transactions_per_amount - median_transactions_per_amount, sd_transactions_per_amount) > 3, FALSE) as flag_transactions_per_amount,
    IFNULL(SAFE_DIVIDE(transactions_per_card - median_transactions_per_card, sd_transactions_per_card) > 3, FALSE) as flag_transactions_per_card,
    IFNULL(SAFE_DIVIDE(transactions_per_ip_address - median_transactions_per_ip_address, sd_transactions_per_ip_address) > 3, FALSE) as flag_transactions_per_ip_address
  FROM
    with_mad
)

SELECT
  merchant_id,
  date,
  hour,
  minute,
  total_transactions,
  total_approved_transactions,
  total_declined_transactions,
  pct_success,
  avg_amount_usd,
  total_low_value_transactions,
  pct_low_value,
  unique_amounts,
  transactions_per_amount,
  unique_cards,
  transactions_per_card,
  unique_ip_addresses,
  transactions_per_ip_address,
  retried_cards,
  (CAST(flag_pct_success AS INT64)
    + CAST(flag_pct_low_value AS INT64)
    + CAST(flag_transactions_per_amount AS INT64)
    + CAST(flag_transactions_per_card AS INT64)
    + CAST(flag_transactions_per_ip_address AS INT64)
  ) as severity_score,
  total_transactions >= 10
    AND (flag_pct_success
      OR flag_pct_low_value
      OR flag_transactions_per_amount
      OR flag_transactions_per_card
      OR flag_transactions_per_ip_address
    ) as is_alert,
  -- names of the metrics that tripped the 3SD threshold, empty if none
  ARRAY(
    SELECT metric FROM UNNEST([
      IF(flag_pct_success, 'pct_success', NULL),
      IF(flag_pct_low_value, 'pct_low_value', NULL),
      IF(flag_transactions_per_amount, 'transactions_per_amount', NULL),
      IF(flag_transactions_per_card, 'transactions_per_card', NULL),
      IF(flag_transactions_per_ip_address, 'transactions_per_ip_address', NULL)
    ]) as metric
    WHERE metric IS NOT NULL
  ) as alerted_on
FROM
  with_flags
)
