WITH t AS (
  SELECT
    amount_usd,
    lower(coalesce(nullif(token_sold_symbol,''),'?'))  AS sold,
    lower(coalesce(nullif(token_bought_symbol,''),'?')) AS bought
  FROM dex_solana.trades
  WHERE block_time >= now() - INTERVAL '7' DAY
),
legs AS (
  SELECT 'out' AS dir, upper(bought) AS token, CAST(amount_usd AS DOUBLE) AS usd
  FROM t WHERE regexp_like(sold, '^(sol|wsol|wrapped sol)$')
  UNION ALL
  SELECT 'in'  AS dir, upper(sold)  AS token, CAST(amount_usd AS DOUBLE) AS usd
  FROM t WHERE regexp_like(bought, '^(sol|wsol|wrapped sol)$')
),
sum_token_dir AS (
  SELECT token, dir, SUM(usd) AS usd
  FROM legs
  GROUP BY 1,2
),
tot AS (
  SELECT token, SUM(usd) AS total_usd
  FROM sum_token_dir
  GROUP BY 1
),
top AS (
  SELECT token
  FROM (
    SELECT token, total_usd, ROW_NUMBER() OVER (ORDER BY total_usd DESC) AS rn
    FROM tot
  )
  WHERE rn <= 20
),
wide AS (
  SELECT
    CASE WHEN s.token IN (SELECT token FROM top) THEN s.token ELSE 'Others' END AS token_bucket,
    SUM(CASE WHEN dir='out' THEN usd ELSE 0 END) AS out_usd,
    SUM(CASE WHEN dir='in'  THEN usd ELSE 0 END) AS in_usd
  FROM sum_token_dir s
  GROUP BY 1
)
SELECT
  token_bucket,
  out_usd,
  in_usd,
  (out_usd - in_usd) AS net_usd,
  ROUND(100.0 * out_usd / NULLIF(out_usd + in_usd, 0), 2) AS out_share_pct
FROM wide
ORDER BY net_usd DESC;
