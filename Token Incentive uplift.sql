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
    SUM(amount_usd)         AS amount_usd
  FROM dex.trades
  WHERE project='uniswap'
    AND blockchain='ethereum'
    AND block_time >= (SELECT event_ts FROM params) - INTERVAL '30' DAY
    AND block_time <  (SELECT event_ts FROM params) + INTERVAL '30' DAY
  GROUP BY 1
),
phase_avg AS (
  SELECT
    AVG(CASE WHEN day <  (SELECT event_day FROM params) THEN dau        END) AS pre_avg_dau,
    AVG(CASE WHEN day <  (SELECT event_day FROM params) THEN trades     END) AS pre_avg_trades,
    AVG(CASE WHEN day <  (SELECT event_day FROM params) THEN amount_usd END) AS pre_avg_volume_usd,
    AVG(CASE WHEN day >= (SELECT event_day FROM params) THEN dau        END) AS post_avg_dau,
    AVG(CASE WHEN day >= (SELECT event_day FROM params) THEN trades     END) AS post_avg_trades,
    AVG(CASE WHEN day >= (SELECT event_day FROM params) THEN amount_usd END) AS post_avg_volume_usd
  FROM daily
)
SELECT 'dau' AS metric,
       0     AS pre,
       ROUND( (post_avg_dau        / NULLIF(pre_avg_dau,        0) - 1) * 100, 1 ) / 100 AS post
FROM phase_avg
UNION ALL
SELECT 'trades',
       0,
       ROUND( (post_avg_trades     / NULLIF(pre_avg_trades,     0) - 1) * 100, 1 ) / 100
FROM phase_avg
UNION ALL
SELECT 'volume_usd',
       0,
       ROUND( (post_avg_volume_usd / NULLIF(pre_avg_volume_usd, 0) - 1) * 100, 1 ) / 100
FROM phase_avg;
