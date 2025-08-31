SELECT
  blockchain,
  DATE_TRUNC('week', block_time),
  SUM(CAST(amount_usd AS DOUBLE)) AS usd_volume
FROM
  dex."trades" AS t 
WHERE
 block_time > NOW() - INTERVAL '365' day
GROUP BY
  1,  2
