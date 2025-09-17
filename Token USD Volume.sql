WITH base AS (
  SELECT
    DATE_TRUNC('day', block_time) AS day,
    amount_usd, 
    COALESCE(NULLIF(token_sold_symbol, ''), 'Unknown') AS sold_sym,
    COALESCE(NULLIF(token_bought_symbol, ''), 'Unknown') AS bought_sym
  FROM dex_solana.trades
  WHERE block_time >= NOW() - INTERVAL '365' DAY
    AND amount_usd > 0
),
legs AS (
  SELECT day, sold_sym AS token, amount_usd AS usd FROM base
  UNION ALL
  SELECT day, bought_sym AS token, amount_usd AS usd FROM base
),
top AS (
  SELECT token
  FROM legs
  GROUP BY token
  ORDER BY SUM(usd) DESC
  LIMIT 10
),
daily_volumes AS (
  SELECT
    day,
    CASE WHEN token IN (SELECT token FROM top) THEN token ELSE 'Others' END AS token_bucket,
    SUM(usd) AS usd
  FROM legs
  GROUP BY 1, 2
)
SELECT 
  *,
  ROUND(100.0 * usd / SUM(usd) OVER (PARTITION BY day), 2) AS usd_share_pct
FROM daily_volumes
ORDER BY day, token_bucket;
