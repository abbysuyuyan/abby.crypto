WITH base AS (
  SELECT date_trunc('week', block_time) AS week_start,
         tx_from AS wallet
  FROM dex.trades
  WHERE lower(project)=lower('{{project}}')
    AND lower(blockchain)=lower('{{chain}}')
    AND block_time >= CAST('{{start_date}}' AS TIMESTAMP)
    AND block_time <  CAST('{{end_date}}'   AS TIMESTAMP)
  GROUP BY 1,2
),
labeled AS (
  SELECT
    week_start,
    wallet,
    MIN(week_start) OVER (PARTITION BY wallet) AS cohort_week_start,
    CAST(date_diff('week',
                   MIN(week_start) OVER (PARTITION BY wallet),
                   week_start) AS integer) AS week_offset
  FROM base
),
latest AS (
  SELECT cohort_week_start
  FROM (SELECT DISTINCT cohort_week_start
        FROM labeled
        ORDER BY cohort_week_start DESC
        LIMIT {{cohort_limit}})
),
agg AS (
  SELECT l.cohort_week_start,
         l.week_offset,
         COUNT(DISTINCT l.wallet) AS active_wallets
  FROM labeled l
  JOIN latest t ON l.cohort_week_start = t.cohort_week_start
  GROUP BY 1,2
),
sizes AS (
  SELECT cohort_week_start, active_wallets AS cohort_size
  FROM agg
  WHERE week_offset = 0
)
SELECT
  a.cohort_week_start,
  a.week_offset,
  a.active_wallets,
  s.cohort_size,
  ROUND(100.0 * a.active_wallets / NULLIF(s.cohort_size,0), 2) AS retention_pct
FROM agg a
LEFT JOIN sizes s ON a.cohort_week_start = s.cohort_week_start
ORDER BY a.cohort_week_start, a.week_offset;
