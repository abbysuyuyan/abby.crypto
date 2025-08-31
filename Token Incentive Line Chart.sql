WITH params AS (
  SELECT
    from_iso8601_timestamp('2025-08-01T00:00:00Z') AS event_ts,
    CAST(date_trunc('day', from_iso8601_timestamp('2025-08-01T00:00:00Z')) AS date) AS event_day
),
daily AS (
  SELECT
    CAST(date_trunc('day', block_time) AS date) AS day,
    COUNT(DISTINCT tx_from) AS dau,
    COUNT(*)                AS trades,
    SUM(amount_usd)         AS volume_usd
  FROM dex.trades
  WHERE project='uniswap'
    AND blockchain='ethereum'
    AND block_time >= (SELECT event_ts FROM params) - INTERVAL '30' DAY
    AND block_time <  (SELECT event_ts FROM params) + INTERVAL '30' DAY
  GROUP BY 1
)
SELECT
  day,
  CASE WHEN day < (SELECT event_day FROM params) THEN 'pre' ELSE 'post' END AS phase,
  dau, trades, volume_usd
FROM daily
ORDER BY day;
