WITH rng AS (
  SELECT
    date_trunc('day', now() - INTERVAL '180' day) AS start_ts,
    date_trunc('day', now())                      AS end_ts
),
daily AS (
  SELECT
    date_trunc('day', minute) AS ts,
    upper(symbol)             AS symbol,
    AVG(price)                AS price_usd
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
  FROM daily
)
SELECT
  ts,
  symbol,
  100.0 * price_usd / base_price AS index_100
FROM with_base
ORDER BY ts, symbol;
