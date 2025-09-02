WITH base AS (
  SELECT
    CAST(date_trunc('day', block_time) AS date) AS day,
    amount_usd,
    COALESCE(NULLIF(token_sold_symbol, ''), 'Unknown')  AS sold_sym,
    COALESCE(NULLIF(token_bought_symbol, ''), 'Unknown') AS bought_sym
  FROM dex_solana.trades
  WHERE block_time >= now() - INTERVAL '365' DAY
),
legs AS (
  SELECT day, sold_sym  AS token, amount_usd / 2.0 AS usd FROM base
  UNION ALL
  SELECT day, bought_sym AS token, amount_usd / 2.0 AS usd FROM base
),
sum_by_token AS (
  SELECT token, SUM(CAST(usd AS DOUBLE)) AS usd30
  FROM legs
  GROUP BY 1
),
top AS (
  SELECT token
  FROM (
    SELECT token, usd30, ROW_NUMBER() OVER (ORDER BY usd30 DESC) AS rn
    FROM sum_by_token
  )
  WHERE rn <= 10
),
labeled AS (
  SELECT
    day,
    CASE WHEN token IN (SELECT token FROM top) THEN token ELSE 'Others' END AS token_bucket,
    SUM(CAST(usd AS DOUBLE)) AS usd
  FROM legs
  GROUP BY 1,2
),
with_share AS (
  SELECT
    day,
    token_bucket,
    usd,
    ROUND(100.0 * usd / NULLIF(SUM(usd) OVER (PARTITION BY day), 0), 2) AS usd_share_pct
  FROM labeled
)
SELECT *
FROM with_share
ORDER BY day, token_bucket;
