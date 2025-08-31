SELECT
  CAST(date_trunc('day', block_time) AS date) AS day,
  'solana' AS chain,
  COUNT(*) AS txs
FROM dex_solana.trades
WHERE block_time >= now() - INTERVAL '90' DAY
GROUP BY 1

UNION ALL

SELECT
  CAST(date_trunc('day', block_time) AS date) AS day,
  lower(blockchain) AS chain,
  COUNT(*) AS txs
FROM dex.trades
WHERE block_time >= now() - INTERVAL '90' DAY
GROUP BY 1,2
ORDER BY day, chain;
