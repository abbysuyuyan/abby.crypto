SELECT
  CAST(day AS date) AS day,
  category,
  SUM(tvl_usd)      AS tvl_usd
FROM dune.abbysuyuyan.dataset_solana_defillama_protocol_tvl
WHERE protocol = 'ALL_PROTOCOLS'
  AND CAST(day AS date) >= current_date - INTERVAL '365' DAY
  AND CAST(day AS date) <  current_date
GROUP BY 1,2
ORDER BY 1,2;
