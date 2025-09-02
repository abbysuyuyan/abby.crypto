WITH src AS (  
  SELECT block_time, token_sold_symbol, token_bought_symbol
  FROM dex_solana.trades
  WHERE block_time >= date_add('day', -364, date_trunc('week', now()))
    AND (
      COALESCE(token_sold_symbol,   '') <> '' OR
      COALESCE(token_bought_symbol, '') <> ''
    )
),
legs AS (  
  SELECT
    CAST(date_trunc('week', block_time) AS date) AS week,
    tok AS token
  FROM src
  CROSS JOIN UNNEST(ARRAY[
    COALESCE(NULLIF(token_sold_symbol,   ''), 'Unknown'),
    COALESCE(NULLIF(token_bought_symbol, ''), 'Unknown')
  ]) AS u(tok)
),
counts AS (
  SELECT week, token, COUNT(*) AS tx_count
  FROM legs
  GROUP BY 1,2
),
ranked AS (
  SELECT
    week,
    token,
    tx_count,
    ROW_NUMBER() OVER (PARTITION BY week ORDER BY tx_count DESC) AS rn
  FROM counts
),
bucketed AS (
  SELECT
    week,
    CASE WHEN rn <= 15 THEN token ELSE 'Others' END AS token_bucket,
    SUM(tx_count) AS tx_count
  FROM ranked
  GROUP BY 1,2
)
SELECT
  week,
  token_bucket,
  tx_count,
  ROUND(100.0 * tx_count / NULLIF(SUM(tx_count) OVER (PARTITION BY week), 0), 2) AS share_pct
FROM bucketed
ORDER BY week, token_bucket;
