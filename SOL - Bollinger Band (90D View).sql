WITH base AS (
  SELECT
    date_trunc('day', minute) AS day,
    price,
    minute
  FROM prices.usd
  WHERE symbol = 'SOL'
    AND minute >= now() - interval '90' day
),
daily AS (
  SELECT
    day,
    (ARRAY_AGG(price ORDER BY minute ASC ))[1] AS open,
    (ARRAY_AGG(price ORDER BY minute DESC))[1] AS close
  FROM base
  GROUP BY 1
),
ma_bb AS (
  SELECT
    day,
    open,
    close,
    AVG(close) OVER (ORDER BY day ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
    AVG(close) OVER (ORDER BY day ROWS BETWEEN 39 PRECEDING AND CURRENT ROW) AS ma40,
    AVG(close)   OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS sma30,
    STDDEV(close)OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS std30
  FROM daily
)
SELECT
  day,
  open,
  close,
  ma20,
  ma40,
  sma30 + 2*std30 AS upper_band,
  sma30 - 2*std30 AS lower_band
FROM ma_bb
ORDER BY day;
