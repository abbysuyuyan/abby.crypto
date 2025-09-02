WITH minute_price AS (
  SELECT date_trunc('day', minute) AS day, price, minute
  FROM prices.usd
  WHERE symbol = 'SOL' AND minute >= now() - interval '90' day
),
daily AS (
  SELECT day, (ARRAY_AGG(price ORDER BY minute DESC))[1] AS close
  FROM minute_price GROUP BY 1
),
chg AS (
  SELECT
    day,
    close,
    GREATEST(close - LAG(close) OVER (ORDER BY day), 0) AS gain,
    GREATEST(LAG(close) OVER (ORDER BY day) - close, 0) AS loss
  FROM daily
),
rsi AS (
  SELECT
    day,
    close,
    AVG(gain) OVER (ORDER BY day ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain14,
    AVG(loss) OVER (ORDER BY day ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss14
  FROM chg
)
SELECT
  day,
  close,
  CASE
    WHEN avg_loss14 IS NULL OR avg_loss14 = 0 THEN 100.0
    ELSE 100.0 - 100.0 / (1.0 + (avg_gain14 / avg_loss14))
  END AS rsi14
FROM rsi
ORDER BY day;
