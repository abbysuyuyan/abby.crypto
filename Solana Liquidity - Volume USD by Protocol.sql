SELECT
  DATE(block_time) AS day,
  CASE
    WHEN project IN ('raydium', 'raydium_launchlab') THEN 'raydium'
    WHEN project = 'whirlpool' THEN 'orca'
    ELSE project
  END AS project,
  SUM(amount_usd) AS volume_usd
FROM dex_solana.trades
WHERE block_time >= CURRENT_DATE - INTERVAL '365' DAY
  AND block_time < CURRENT_DATE
  AND amount_usd > 0
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
