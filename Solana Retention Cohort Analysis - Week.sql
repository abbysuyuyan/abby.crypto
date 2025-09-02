WITH params AS (
  SELECT
    date_trunc('week', now())                       AS this_week,
    date_trunc('week', date_add('year', -1, now())) AS start_week
),

first_seen AS (
  SELECT
    t.trader_id                           AS address,
    MIN(date_trunc('week', t.block_time)) AS cohort_week
  FROM dex_solana.trades t
  CROSS JOIN params p
  WHERE t.block_time < date_add('week', 1, p.this_week)   
  GROUP BY 1
),

cohort_filtered AS (
  SELECT address, cohort_week
  FROM first_seen fs
  CROSS JOIN params p
  WHERE fs.cohort_week >= p.start_week
),

cohort_size AS (
  SELECT cohort_week, COUNT(*) AS first_users
  FROM cohort_filtered
  GROUP BY 1
),

weekly_active AS (
  SELECT
    date_trunc('week', t.block_time) AS week,
    t.trader_id                      AS address
  FROM dex_solana.trades t
  CROSS JOIN params p
  WHERE t.block_time >= p.start_week
    AND t.block_time <  date_add('week', 1, p.this_week)
  GROUP BY 1, 2
),

retained AS (
  SELECT
    cf.cohort_week,
    date_diff('week', cf.cohort_week, wa.week) AS week_n,
    wa.address
  FROM weekly_active wa
  JOIN cohort_filtered cf
    ON wa.address = cf.address
  WHERE wa.week >= cf.cohort_week
    AND date_diff('week', cf.cohort_week, wa.week) BETWEEN 0 AND 9
),

retention AS (
  SELECT
    cohort_week,
    week_n,
    COUNT(DISTINCT address) AS retained
  FROM retained
  GROUP BY 1,2
)

SELECT
  r.cohort_week,
  cs.first_users AS cohort_size,

  format('%.1f%%', 100.0 * MAX(CASE WHEN week_n = 0 THEN CAST(retained AS DOUBLE) / cs.first_users END)) AS week_0,
  format('%.1f%%', 100.0 * MAX(CASE WHEN week_n = 1 THEN CAST(retained AS DOUBLE) / cs.first_users END)) AS week_1,
  format('%.1f%%', 100.0 * MAX(CASE WHEN week_n = 2 THEN CAST(retained AS DOUBLE) / cs.first_users END)) AS week_2,
  format('%.1f%%', 100.0 * MAX(CASE WHEN week_n = 3 THEN CAST(retained AS DOUBLE) / cs.first_users END)) AS week_3,
  format('%.1f%%', 100.0 * MAX(CASE WHEN week_n = 4 THEN CAST(retained AS DOUBLE) / cs.first_users END)) AS week_4,
  format('%.1f%%', 100.0 * MAX(CASE WHEN week_n = 5 THEN CAST(retained AS DOUBLE) / cs.first_users END)) AS week_5,
  format('%.1f%%', 100.0 * MAX(CASE WHEN week_n = 6 THEN CAST(retained AS DOUBLE) / cs.first_users END)) AS week_6,
  format('%.1f%%', 100.0 * MAX(CASE WHEN week_n = 7 THEN CAST(retained AS DOUBLE) / cs.first_users END)) AS week_7,
  format('%.1f%%', 100.0 * MAX(CASE WHEN week_n = 8 THEN CAST(retained AS DOUBLE) / cs.first_users END)) AS week_8,
  format('%.1f%%', 100.0 * MAX(CASE WHEN week_n = 9 THEN CAST(retained AS DOUBLE) / cs.first_users END)) AS week_9

FROM retention r
JOIN cohort_size cs
  ON r.cohort_week = cs.cohort_week
GROUP BY r.cohort_week, cs.first_users
ORDER BY r.cohort_week;
