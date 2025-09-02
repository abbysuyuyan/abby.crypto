WITH sol_usd AS (
  SELECT f.day, f.sol_fee_amount * p.sol_avg_price AS sol_fee_usd
  FROM (
    SELECT
      CAST(date_trunc('day', block_time) AS date) AS day,
      SUM(fee) / 1e9 AS sol_fee_amount        
    FROM solana.transactions
    WHERE block_time >= current_date - INTERVAL '90' DAY
      AND block_time <  current_date
    GROUP BY 1
  ) f
  JOIN (
    SELECT
      CAST(date_trunc('day', minute) AS date) AS day,
      AVG(price) AS sol_avg_price
    FROM prices.usd
    WHERE symbol = 'SOL'
      AND minute >= current_date - INTERVAL '90' DAY
      AND minute <  current_date
    GROUP BY 1
  ) p ON f.day = p.day
),
wsol_usd AS (
  SELECT
    CAST(date_trunc('day', block_time) AS date) AS day,
    SUM(amount_usd) AS wsol_tx_usd
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
  s.sol_fee_usd,
  w.wsol_tx_usd
FROM sol_usd s
FULL OUTER JOIN wsol_usd w ON s.day = w.day
ORDER BY day;
