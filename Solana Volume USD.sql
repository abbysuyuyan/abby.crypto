WITH filtered AS (
  SELECT
    CAST(date_trunc('day', block_time) AS date) AS day,
    CASE
      WHEN lower(project) IN ('raydium','raydium_launchlab') THEN 'raydium'
      WHEN lower(project) = 'whirlpool'                      THEN 'orca'
      WHEN lower(project) = 'meteora'                        THEN 'meteora'
      WHEN lower(project) = 'pumpswap'                       THEN 'pumpswap'
      WHEN lower(project) = 'pumpdotfun'                     THEN 'pumpdotfun'
      WHEN lower(project) = 'solfi'                          THEN 'solfi'
      ELSE lower(project)
    END AS project,
    CAST(amount_usd AS DOUBLE) AS trade_usd
  FROM dex_solana.trades
  WHERE block_time >= now() - INTERVAL '365' DAY
    AND (
      (lower(coalesce(token_bought_mint_address,'')) = 'so11111111111111111111111111111111111111112'
       AND lower(coalesce(token_sold_mint_address ,'')) IN (
         'epjfwdd5aufqssqem2qn1xzybapc8g4weggkzwytdt1v',  -- USDC
         'es9vmfrzacermjfrf4h2fyd4kconky11mcce8benwnyb'   -- USDT
       ))
      OR
      (lower(coalesce(token_sold_mint_address ,'')) = 'so11111111111111111111111111111111111111112'
       AND lower(coalesce(token_bought_mint_address,'')) IN (
         'epjfwdd5aufqssqem2qn1xzybapc8g4weggkzwytdt1v',  -- USDC
         'es9vmfrzacermjfrf4h2fyd4kconky11mcce8benwnyb'   -- USDT
       ))
    )
),
daily_pct AS (
  SELECT
    day,
    project,
    approx_percentile(trade_usd, 0.50) AS p50,
    approx_percentile(trade_usd, 0.90) AS p90,
    approx_percentile(trade_usd, 0.95) AS p95,
    approx_percentile(trade_usd, 0.99) AS p99
  FROM filtered
  GROUP BY 1,2
)
SELECT day, project, 'p50' AS metric, p50 AS usd FROM daily_pct
UNION ALL
SELECT day, project, 'p90', p90 FROM daily_pct
UNION ALL
SELECT day, project, 'p95', p95 FROM daily_pct
UNION ALL
SELECT day, project, 'p99', p99 FROM daily_pct
ORDER BY day, project,
  CASE metric WHEN 'p50' THEN 1 WHEN 'p90' THEN 2 WHEN 'p95' THEN 3 ELSE 4 END;
