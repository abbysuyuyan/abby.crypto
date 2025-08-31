WITH p AS (
  SELECT FROM_HEX(REPLACE('{{wallet}}','0x','')) AS addr
),
tx AS (
  SELECT
    date_trunc('day', t.block_time)                         AS day,
    CONCAT('0x', TO_HEX(t.nft_contract_address))            AS nft_contract,
    t.amount_usd,
    (t.buyer  = p.addr)                                     AS is_buy,
    (t.seller = p.addr)                                     AS is_sell
  FROM nft.trades t
  JOIN p ON 1=1
  WHERE (t.buyer = p.addr OR t.seller = p.addr)
    AND t.blockchain = '{{chain}}'
    AND t.block_time >= CAST('{{start_date}}' AS TIMESTAMP)
    AND t.block_time <  CAST('{{end_date}}'   AS TIMESTAMP)
)
SELECT
  day,
  nft_contract,
  SUM(CASE WHEN is_buy  THEN amount_usd ELSE 0 END) AS buy_usd,
  SUM(CASE WHEN is_sell THEN amount_usd ELSE 0 END) AS sell_usd,
  SUM(CASE WHEN is_buy  THEN amount_usd ELSE 0 END)
- SUM(CASE WHEN is_sell THEN amount_usd ELSE 0 END) AS net_flow_usd
FROM tx
GROUP BY 1, 2
ORDER BY 1, 2;
