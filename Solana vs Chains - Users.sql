SELECT
  CAST(date_trunc('day', block_time) AS date) AS day,
  'solana' AS chain,
  COUNT(DISTINCT trader_id) AS users
FROM dex_solana.trades
WHERE block_time >= now() - INTERVAL '365' DAY
GROUP BY 1

UNION ALL

SELECT
  CAST(date_trunc('day', block_time) AS date) AS day,
  lower(blockchain) AS chain,
  COUNT(DISTINCT tx_from) AS users
FROM dex.trades
WHERE block_time >= now() - INTERVAL '365' DAY
GROUP BY 1,2
ORDER BY day, chain;
