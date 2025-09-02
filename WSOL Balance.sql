SELECT
  CAST(date_trunc('day', block_time) AS date)                      AS day,
  SUM(CASE WHEN lower(token_bought_mint_address) = 'so11111111111111111111111111111111111111112'
           THEN amount_usd ELSE 0 END)                            AS buy_usd,
  SUM(CASE WHEN lower(token_sold_mint_address)  = 'so11111111111111111111111111111111111111112'
           THEN amount_usd ELSE 0 END)                            AS sell_usd,
  SUM(CASE WHEN lower(token_bought_mint_address) = 'so11111111111111111111111111111111111111112'
           THEN amount_usd ELSE 0 END)
  - SUM(CASE WHEN lower(token_sold_mint_address)  = 'so11111111111111111111111111111111111111112'
           THEN amount_usd ELSE 0 END)                            AS net_flow_usd
FROM dex_solana.trades
WHERE block_time >= current_date - INTERVAL '365' DAY
  AND block_time <  current_date
  AND (
    lower(token_bought_mint_address) = 'so11111111111111111111111111111111111111112'
    OR lower(token_sold_mint_address)  = 'so11111111111111111111111111111111111111112'
  )
GROUP BY 1
ORDER BY 1;
