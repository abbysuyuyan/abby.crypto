WITH p AS (
  SELECT FROM_HEX(REPLACE('{{wallet}}','0x','')) AS addr
)
SELECT
  ROUND(
    COALESCE(SUM(CASE WHEN t.seller = p.addr THEN t.amount_usd END), 0)
  , 2) AS "Total Sell (USD)"
FROM nft.trades t
JOIN p ON 1=1
WHERE t.blockchain = '{{chain}}'
  AND t.block_time >= CAST('{{start_date}}' AS TIMESTAMP)
  AND t.block_time <  CAST('{{end_date}}'   AS TIMESTAMP)
  AND (t.buyer = p.addr OR t.seller = p.addr);
