WITH daily_volumes AS (
  SELECT
    date_trunc('day', block_time) AS day,
    'solana' AS chain,
    SUM(amount_usd) AS volume
  FROM dex_solana.trades
  WHERE block_time >= now() - INTERVAL '365' DAY
  GROUP BY 1
  
  UNION ALL
  
  SELECT
    date_trunc('day', block_time) AS day,
    lower(blockchain) AS chain,
    SUM(amount_usd) AS volume
  FROM dex.trades
  WHERE block_time >= now() - INTERVAL '365' DAY
  GROUP BY 1, 2
)
SELECT * FROM daily_volumes
ORDER BY day, chain;
