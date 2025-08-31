WITH base AS (
  SELECT date_trunc('week', block_time) AS week_start,
         tx_from AS wallet
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
  SELECT b.wallet,
         b.week_start,
         f.cohort_week_start
  FROM base b
  JOIN first_seen f ON b.wallet = f.wallet
),
matrix AS (
  SELECT cohort_week_start,
         CAST(date_diff('week', cohort_week_start, week_start) AS integer) AS week_offset,
         COUNT(DISTINCT wallet) AS active_wallets
  FROM activity
  GROUP BY 1,2
),
sizes AS (
  SELECT cohort_week_start,
         SUM(active_wallets) AS cohort_size
  FROM matrix
  WHERE week_offset = 0
  GROUP BY 1
),
latest_cohorts AS (
  SELECT DISTINCT cohort_week_start
  FROM matrix
  ORDER BY cohort_week_start DESC
  LIMIT 10
),
joined AS (
  SELECT m.cohort_week_start,
         m.week_offset,
         m.active_wallets,
         s.cohort_size,
         ROUND(100.0 * m.active_wallets / NULLIF(s.cohort_size,0), 2) AS retention_pct
  FROM matrix m
  JOIN latest_cohorts lc ON m.cohort_week_start = lc.cohort_week_start
  LEFT JOIN sizes s ON m.cohort_week_start = s.cohort_week_start
)
SELECT ROUND(AVG(retention_pct),1) AS avg_w4_retention
FROM joined
WHERE week_offset = 4;
