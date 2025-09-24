WITH
src AS (
  SELECT 
    CAST(day AS DATE) AS day,
    lower(chain) AS chain,
    CAST(tvl_usd AS DOUBLE) AS tvl_usd
  FROM dune.abbysuyuyan.dataset_defillama_chain_tvl_top30
),
bounds AS (
  SELECT MAX(day) AS last_day FROM src
),
latest_per_chain AS (
  SELECT
    chain, day, tvl_usd,
    ROW_NUMBER() OVER (PARTITION BY chain ORDER BY day DESC) AS rn
  FROM src
  WHERE day >= (SELECT last_day - INTERVAL '14' DAY FROM bounds)
),
top30 AS (
  SELECT chain
  FROM (
    SELECT chain, tvl_usd
    FROM latest_per_chain
    WHERE rn = 1
    ORDER BY tvl_usd DESC
    LIMIT 30
  )
),
rng AS (
  SELECT
    (SELECT last_day - INTERVAL '365' DAY FROM bounds) AS start_day,
    (SELECT last_day FROM bounds) AS end_day
)
SELECT
  s.day,
  s.chain,
  s.tvl_usd,
  AVG(s.tvl_usd) OVER (
    PARTITION BY s.chain
    ORDER BY s.day
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS tvl_ma7,
  ROUND(
    100.0 * s.tvl_usd / NULLIF(SUM(s.tvl_usd) OVER (PARTITION BY s.day), 0),
    4
  ) AS share_pct_in_top30
FROM src s
JOIN top30 t ON s.chain = t.chain
WHERE s.day BETWEEN (SELECT start_day FROM rng) AND (SELECT end_day FROM rng)
ORDER BY s.day, s.chain;
