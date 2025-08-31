WITH rng AS (
  SELECT
    now() - INTERVAL '24' hour AS start_ts,
    now()                      AS end_ts
),
hourly AS (
  SELECT
    date_trunc('hour', minute) AS ts,
    upper(symbol)              AS symbol,
    AVG(price)                 AS price_usd
  FROM prices.usd
  WHERE minute >= (SELECT start_ts FROM rng)
    AND minute <  (SELECT end_ts   FROM rng)
    AND upper(symbol) IN ('BTC','ETH','SOL')
  GROUP BY 1,2
),
ret AS (
  SELECT
    ts,
    symbol,
    100.0 * (price_usd / NULLIF(LAG(price_usd) OVER (
                  PARTITION BY symbol ORDER BY ts
              ), 0) - 1.0) AS ret_pct
  FROM hourly
)
SELECT ts, symbol, ret_pct
FROM ret
WHERE ret_pct IS NOT NULL
ORDER BY ts, symbol;
