WITH rng AS (
  SELECT
    now() - INTERVAL '168' hour AS start_ts,
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
with_base AS (
  SELECT
    ts,
    symbol,
    price_usd,
    FIRST_VALUE(price_usd) OVER (
      PARTITION BY symbol
      ORDER BY ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS base_price
  FROM hourly
)
SELECT
  ts,
  symbol,
  100.0 * (price_usd / base_price - 1.0) AS cum_return_pct 
FROM with_base
WHERE base_price IS NOT NULL
ORDER BY ts, symbol;
