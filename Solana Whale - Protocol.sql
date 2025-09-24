WITH src AS (
  SELECT
    date_trunc('week', block_time) AS wk,
    CASE
      WHEN project IN ('raydium','raydium_launchlab') THEN 'raydium'
      WHEN project = 'whirlpool'                      THEN 'orca'
      ELSE project
    END AS project,
    amount_usd,
    lower(coalesce(token_bought_mint_address,'')) AS buy_mint,
    lower(coalesce(token_sold_mint_address ,''))  AS sell_mint
  FROM dex_solana.trades
  WHERE block_time >= date_add('week', -52, date_trunc('week', current_timestamp))
    AND project IN ('raydium','raydium_launchlab','whirlpool','meteora','pumpswap','pumpdotfun','solfi')
),
sol_usdx AS (
  SELECT wk, project, amount_usd
  FROM src
  WHERE
    (buy_mint = 'so11111111111111111111111111111111111111112' AND sell_mint IN (
      'epjfwdd5aufqssqem2qn1xzybapc8g4weggkzwytdt1v',  -- USDC
      'es9vmfrzacermjfrf4h2fyd4kconky11mcce8benwnyb'   -- USDT
    ))
    OR
    (sell_mint = 'so11111111111111111111111111111111111111112' AND buy_mint IN (
      'epjfwdd5aufqssqem2qn1xzybapc8g4weggkzwytdt1v',
      'es9vmfrzacermjfrf4h2fyd4kconky11mcce8benwnyb'
    ))
)
SELECT
  wk,
  project,
  SUM(CASE WHEN amount_usd >= 100000 THEN 1 ELSE 0 END) AS whale100k_trades
FROM sol_usdx
GROUP BY 1,2
ORDER BY wk, project;
