WITH daily_trades AS (
  SELECT 
    DATE_TRUNC('day', block_time) AS day,
    token_sold_symbol,
    token_bought_symbol
  FROM dex_solana.trades
  WHERE block_time >= CURRENT_DATE - INTERVAL '365' DAY
    AND block_time < CURRENT_DATE
    AND (token_sold_symbol != '' OR token_bought_symbol != '')
),
token_legs AS (
  -- 賣出的 token
  SELECT 
    day,
    COALESCE(NULLIF(token_sold_symbol, ''), 'Unknown') AS token
  FROM daily_trades
  WHERE token_sold_symbol IS NOT NULL
  
  UNION ALL
  
  -- 買入的 token
  SELECT 
    day,
    COALESCE(NULLIF(token_bought_symbol, ''), 'Unknown') AS token
  FROM daily_trades
  WHERE token_bought_symbol IS NOT NULL
),
-- 聚合
daily_counts AS (
  SELECT 
    day,
    token,
    COUNT(*) AS tx_count
  FROM token_legs
  GROUP BY 1, 2
),
ranked AS (
  SELECT
    day,
    token,
    tx_count,
    ROW_NUMBER() OVER (PARTITION BY day ORDER BY tx_count DESC) AS rn
  FROM daily_counts
  WHERE tx_count > 0  
),
-- 最終分組和計算
final_result AS (
  SELECT
    day,
    CASE 
      WHEN rn <= 15 THEN token 
      ELSE 'Others' 
    END AS token_bucket,
    SUM(tx_count) AS tx_count
  FROM ranked
  GROUP BY 1, 2
)
SELECT
  day,
  token_bucket,
  tx_count,
  ROUND(
    100.0 * tx_count / SUM(tx_count) OVER (PARTITION BY day),
    2
  ) AS share_pct
FROM final_result
ORDER BY day DESC, tx_count DESC;
