WITH sol_tx AS (
  SELECT
    CAST(date_trunc('day', block_time) AS date) AS day,
    COUNT(*) AS sol_tx_count
  FROM solana.transactions
  WHERE block_time >= current_date - INTERVAL '90' DAY
    AND block_time <  current_date
  GROUP BY 1
),
wsol_tx AS (
  SELECT
    CAST(date_trunc('day', block_time) AS date) AS day,
    COUNT(*) AS wsol_tx_count
  FROM dex_solana.trades
  WHERE block_time >= current_date - INTERVAL '90' DAY
    AND block_time <  current_date
    AND (
      lower(coalesce(token_bought_mint_address,'')) = 'so11111111111111111111111111111111111111112' OR
      lower(coalesce(token_sold_mint_address,  '')) = 'so11111111111111111111111111111111111111112'
    )
  GROUP BY 1
)
SELECT
  COALESCE(s.day, w.day) AS day,
  COALESCE(s.sol_tx_count, 0)  AS sol_tx_count,
  COALESCE(w.wsol_tx_count, 0) AS wsol_tx_count
FROM sol_tx s
FULL OUTER JOIN wsol_tx w
  ON s.day = w.day
ORDER BY day;
