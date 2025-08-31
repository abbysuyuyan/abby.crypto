WITH base AS (
  SELECT date_trunc('week', block_time) AS week_start, tx_from AS wallet
  FROM dex.trades
  WHERE project = '{{project}}'
    AND blockchain = '{{chain}}'
    AND block_time >= CAST('{{start_date}}' AS TIMESTAMP)
    AND block_time <  CAST('{{end_date}}'   AS TIMESTAMP)
  GROUP BY 1,2
),
first_seen AS (
  SELECT wallet, MIN(week_start) AS cohort_week_start
  FROM base
  GROUP BY 1
),
activity AS (
  SELECT b.wallet, b.week_start, f.cohort_week_start
  FROM base b
  JOIN first_seen f ON b.wallet = f.wallet
),
matrix AS (
  SELECT
    cohort_week_start,
    CAST(date_diff('week', cohort_week_start, week_start) AS integer) AS week_offset,
    COUNT(DISTINCT wallet) AS active_wallets
  FROM activity
  GROUP BY 1,2
),
sizes AS (
  SELECT cohort_week_start, SUM(active_wallets) AS cohort_size
  FROM matrix
  WHERE week_offset = 0
  GROUP BY 1
),
latest_cohorts AS (
  SELECT DISTINCT cohort_week_start
  FROM matrix
  ORDER BY cohort_week_start DESC
  LIMIT {{cohort_limit}}
),
m2 AS (
  SELECT m.* FROM matrix m
  JOIN latest_cohorts lc ON m.cohort_week_start = lc.cohort_week_start
),
s2 AS (
  SELECT s.* FROM sizes s
  JOIN latest_cohorts lc ON s.cohort_week_start = lc.cohort_week_start
),
base_query AS (
  SELECT
    m2.cohort_week_start,
    m2.week_offset,
    s2.cohort_size
  FROM m2
  LEFT JOIN s2 ON m2.cohort_week_start = s2.cohort_week_start
)
SELECT ROUND(AVG(cohort_size), 0) AS avg_cohort_size
FROM base_query
WHERE week_offset = 0;
