WITH base AS (
  SELECT
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
)
SELECT
  project,
  approx_percentile(trade_usd, 0.50) AS p50_trade,
  approx_percentile(trade_usd, 0.90) AS p90_trade,
  approx_percentile(trade_usd, 0.95) AS p95_trade,
  approx_percentile(trade_usd, 0.99) AS p99_trade,
  SUM(trade_usd) AS volume_usd,
  SUM(CASE WHEN trade_usd >= 100000 THEN trade_usd ELSE 0 END) / NULLIF(SUM(trade_usd),0) AS whale100k_share
FROM base
GROUP BY 1
ORDER BY whale100k_share DESC;
