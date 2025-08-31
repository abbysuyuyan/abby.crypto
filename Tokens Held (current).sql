WITH p AS (
  SELECT FROM_HEX(REPLACE('{{wallet}}','0x','')) AS addr
),
tx AS (
  SELECT t.buyer, t.seller, t.nft_contract_address, t.token_id
  FROM nft.trades t
  JOIN p ON 1=1
  WHERE t.blockchain = '{{chain}}'
    AND t.block_time >= CAST('{{start_date}}' AS TIMESTAMP)
    AND t.block_time <  CAST('{{end_date}}'   AS TIMESTAMP)
    AND (t.buyer = p.addr OR t.seller = p.addr)
),
holdings AS (
  SELECT
    nft_contract_address,
    token_id,
    SUM(CASE WHEN buyer  = (SELECT addr FROM p) THEN 1 ELSE 0 END)
  - SUM(CASE WHEN seller = (SELECT addr FROM p) THEN 1 ELSE 0 END) AS qty
  FROM tx
  GROUP BY 1,2
)
SELECT
  COALESCE(SUM(CASE WHEN qty > 0 THEN 1 ELSE 0 END), 0) AS "Tokens Held"
FROM holdings;
