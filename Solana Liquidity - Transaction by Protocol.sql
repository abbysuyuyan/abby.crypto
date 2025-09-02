WITH src AS (
  SELECT
    date_trunc('week', block_time) AS wk,
    CASE
      WHEN project IN ('raydium','raydium_launchlab') THEN 'raydium'
      WHEN project = 'whirlpool'                      THEN 'orca'
      ELSE project
    END AS project
  FROM dex_solana.trades
  WHERE block_time >= date_add('week', -26, date_trunc('week', current_timestamp))
    AND project IN ('raydium','raydium_launchlab','whirlpool','meteora','pumpswap','pumpdotfun','solfi')
)
SELECT
  wk,
  project,
  COUNT(*) AS trades
FROM src
GROUP BY 1,2
ORDER BY wk, project;
