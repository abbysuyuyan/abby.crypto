WITH p AS (
  SELECT FROM_HEX(REPLACE('{{wallet}}','0x','')) AS addr
),
tx AS (
  SELECT
    t.nft_contract_address,
    t.amount_usd,
    (t.buyer  = p.addr) AS is_buy,
    (t.seller = p.addr) AS is_sell
  FROM nft.trades t
  JOIN p ON 1=1
  WHERE (t.buyer = p.addr OR t.seller = p.addr)
    AND t.blockchain = '{{chain}}'
    AND t.block_time >= CAST('{{start_date}}' AS TIMESTAMP)
    AND t.block_time <  CAST('{{end_date}}'   AS TIMESTAMP)
)
SELECT
  CONCAT('0x', TO_HEX(nft_contract_address)) AS nft_contract,
  SUM(CASE WHEN is_buy  THEN amount_usd ELSE 0 END) AS total_buy_usd,
  SUM(CASE WHEN is_sell THEN amount_usd ELSE 0 END) AS total_sell_usd
FROM tx
GROUP BY 1
ORDER BY GREATEST(total_buy_usd, total_sell_usd) DESC;
