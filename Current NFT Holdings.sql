WITH p AS (
  SELECT FROM_HEX(REPLACE('{{wallet}}','0x','')) AS addr
),
tx AS (
  SELECT
    t.nft_contract_address,
    t.token_id,
    (t.buyer  = p.addr) AS is_buy,
    (t.seller = p.addr) AS is_sell,
    t.amount_usd
  FROM nft.trades t
  JOIN p ON 1=1
  WHERE (t.buyer = p.addr OR t.seller = p.addr)
    AND t.blockchain = '{{chain}}'
    AND t.block_time >= CAST('{{start_date}}' AS TIMESTAMP)
    AND t.block_time <  CAST('{{end_date}}'   AS TIMESTAMP)
),
per_token AS (
  SELECT
    nft_contract_address,
    token_id,
    SUM(CASE WHEN is_buy  THEN 1 WHEN is_sell THEN -1 ELSE 0 END) AS qty,
    SUM(CASE WHEN is_buy  THEN amount_usd ELSE 0 END)            AS buy_usd,
    SUM(CASE WHEN is_sell THEN amount_usd ELSE 0 END)            AS sell_usd
  FROM tx
  GROUP BY 1,2
)
SELECT
  CONCAT('0x', TO_HEX(nft_contract_address)) AS nft_contract,
  COUNT(*)                                    AS tokens_held,
  SUM(buy_usd)                                AS total_buy_usd,
  SUM(sell_usd)                               AS total_sell_usd
FROM per_token
WHERE qty > 0
GROUP BY 1
ORDER BY tokens_held DESC;
