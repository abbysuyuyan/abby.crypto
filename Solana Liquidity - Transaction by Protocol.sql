SELECT
  DATE(block_time) AS day,
  CASE
    WHEN project IN ('raydium', 'raydium_launchlab') THEN 'raydium'
    WHEN project = 'whirlpool' THEN 'orca'
    ELSE project
  END AS project,
  COUNT(*) AS trades
FROM dex_solana.trades
WHERE block_time >= CURRENT_DATE - INTERVAL '365' DAY
  AND block_time < CURRENT_DATE
  AND project IN ('raydium', 'raydium_launchlab', 'whirlpool', 'meteora', 'pumpswap', 'pumpdotfun', 'solfi')
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
