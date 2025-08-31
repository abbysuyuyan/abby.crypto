WITH params AS (
  SELECT
    from_iso8601_timestamp('2025-01-01T00:00:00Z') AS event_ts,
    CAST(date_trunc('day', from_iso8601_timestamp('2025-01-01T00:00:00Z')) AS date) AS event_day
),
daily AS (
  SELECT
    CAST(date_trunc('day', block_time) AS date) AS day,
    COUNT(DISTINCT tx_from) AS dau,
    COUNT(*)                AS trades,
    SUM(amount_usd)         AS volume_usd
  FROM dex.trades
  WHERE project='{{project}}'
    AND blockchain='{{chain}}'
    AND block_time >= (SELECT event_ts FROM params) - INTERVAL '30' DAY
    AND block_time <  (SELECT event_ts FROM params) + INTERVAL '30' DAY
  GROUP BY 1
),
phase_avg AS (
  SELECT
    SUM(CASE WHEN day <  (SELECT event_day FROM params) THEN 1 ELSE 0 END) AS pre_days,
    SUM(CASE WHEN day >= (SELECT event_day FROM params) THEN 1 ELSE 0 END) AS post_days,
    AVG(CASE WHEN day <  (SELECT event_day FROM params) THEN dau        END) AS pre_avg_dau,
    AVG(CASE WHEN day >= (SELECT event_day FROM params) THEN dau        END) AS post_avg_dau,
    AVG(CASE WHEN day <  (SELECT event_day FROM params) THEN trades     END) AS pre_avg_trades,
    AVG(CASE WHEN day >= (SELECT event_day FROM params) THEN trades     END) AS post_avg_trades,
    AVG(CASE WHEN day <  (SELECT event_day FROM params) THEN volume_usd END) AS pre_avg_volume_usd,
    AVG(CASE WHEN day >= (SELECT event_day FROM params) THEN volume_usd END) AS post_avg_volume_usd
  FROM daily
)
SELECT
  'pre'  AS phase,
  pre_days  AS days,
  pre_avg_dau        AS avg_dau,
  pre_avg_trades     AS avg_trades,
  pre_avg_volume_usd AS avg_volume_usd,
  CAST(NULL AS DOUBLE) AS dau_uplift_vs_pre,
  CAST(NULL AS DOUBLE) AS trades_uplift_vs_pre,
  CAST(NULL AS DOUBLE) AS volume_uplift_vs_pre
FROM phase_avg
UNION ALL
SELECT
  'post',
  post_days,
  post_avg_dau,
  post_avg_trades,
  post_avg_volume_usd,
  post_avg_dau        / NULLIF(pre_avg_dau,        0) - 1,
  post_avg_trades     / NULLIF(pre_avg_trades,     0) - 1,
  post_avg_volume_usd / NULLIF(pre_avg_volume_usd, 0) - 1
FROM phase_avg
ORDER BY phase;
